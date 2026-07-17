from flask import Flask, request, jsonify, send_from_directory
from flask_sock import Sock
import requests
import time
import threading
import json
import re
import math
import uuid
import os
import traceback
from datetime import datetime, timezone, timedelta
import websocket  # librería websocket-client

app = Flask(__name__)
sock = Sock(app)

# ----------------------------------
# CACHE TTL GENERALIZADO (todos los endpoints REST a Binance)
# ----------------------------------
# ANTES: solo /depth y /futures/depth tenían cache. klines (pedido 4
# VECES por refresh desde main.py: 5m, 15m, 1h + timeframe operativo)
# y ticker24hr/premiumIndex/openInterest pegaban directo a Binance en
# cada request, sin absorber ráfagas de sesiones simultáneas -- esa
# era la causa real del -1003 (weight ban), no un pedido puntual mal
# hecho. Ahora TODOS los endpoints pasan por el mismo cache genérico.
#
# TTL por tipo de dato (no todos necesitan el mismo):
#   - klines/ticker/depth: cambian rápido, TTL corto (4s) para no
#     sentirse "viejo" en un refresh de 15s.
#   - premiumIndex (funding) / openInterest: cambian mucho más lento
#     en la realidad (funding se recalcula cada 5min-8h según symbol),
#     TTL más largo (8s) no le resta información útil y absorbe más
#     ráfagas.
_CACHE = {}
_CACHE_LOCK = threading.Lock()

TTL_RAPIDO = 4
TTL_LENTO = 8


def _get_con_cache(cache_key, fetch_fn, ttl_segundos=TTL_RAPIDO):
    """
    Cachea (body, status) por cache_key durante ttl_segundos. Si dos
    sesiones piden lo mismo dentro de la ventana, la segunda reusa la
    respuesta sin generar un pedido nuevo a Binance -- esto es lo que
    hace que N sesiones abiertas a la vez consuman el peso de Binance
    UNA sola vez por ventana, no N veces.
    """
    ahora = time.time()
    with _CACHE_LOCK:
        entrada = _CACHE.get(cache_key)
        if entrada and (ahora - entrada[0]) < ttl_segundos:
            return entrada[1], entrada[2]

    body, status = fetch_fn()

    with _CACHE_LOCK:
        _CACHE[cache_key] = (ahora, body, status)

    return body, status


# ----------------------------------
# CIRCUIT BREAKER — deja de pegarle a Binance mientras dure un ban -1003
# ----------------------------------
# Binance devuelve el -1003 como {"code": -1003, "msg": "...IP banned
# until <timestamp_ms>..."}. Sin esto, el proxy seguía intentando el
# request en CADA refresh de CADA sesión mientras el ban seguía
# vigente -- cada intento paga el timeout completo (hasta 8s) y,
# según cómo Binance cuente peso de pedidos rechazados, puede estar
# alimentando el propio ban en vez de dejarlo enfriar.
#
# _BAN_HASTA se trackea por GRUPO de rate-limit, no por endpoint --
# spot (ticker/klines/depth, comparten el mismo bucket de peso de la
# API spot) y futures (premiumIndex/openInterest/futures-depth, bucket
# separado) tienen límites independientes en Binance.
_BAN_HASTA = {"spot": 0, "futures": 0}

_PATRON_BAN_TS = re.compile(r"banned until (\d+)")


def _registrar_si_es_ban(grupo, body):
    """
    Si body es un error -1003 de Binance, extrae el timestamp de
    "banned until X" y lo guarda en _BAN_HASTA[grupo]. Si no matchea
    el patrón esperado (formato de mensaje cambia), usa un fallback de
    60s desde ahora -- mejor un enfriamiento conservador que seguir
    pegándole a ciegas.
    """
    if not isinstance(body, dict) or body.get("code") != -1003:
        return

    msg = body.get("msg", "")
    match = _PATRON_BAN_TS.search(msg)

    if match:
        _BAN_HASTA[grupo] = int(match.group(1)) / 1000.0  # ms -> s
    else:
        _BAN_HASTA[grupo] = time.time() + 60


def _grupo_baneado(grupo):
    return time.time() < _BAN_HASTA[grupo]


def _respuesta_ban_activo(grupo):
    restante = max(0, int(_BAN_HASTA[grupo] - time.time()))
    return {
        "error": (
            f"IP baneada temporalmente por Binance (peso excedido, grupo '{grupo}'). "
            f"Se recupera sola en ~{restante}s -- el proxy no está reintentando "
            f"mientras tanto para no extender el ban."
        ),
        "code": -1003,
        "ban_restante_segundos": restante,
    }, 429


# Dominios de Binance a probar en orden. Si Render bloquea uno
# (poco probable, pero por si acaso) probamos el siguiente.
DOMINIOS_SPOT = [
    "https://api.binance.com",
    "https://data-api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
]
DOMINIO_FUTURES = "https://fapi.binance.com"
DOMINIO_BYBIT = "https://api.bybit.com"


def _proxy_get(dominios, path, params, grupo="spot"):
    """
    Prueba cada dominio hasta que uno responda OK. Devuelve
    (json_body, status_code). Ahora también registra el ban si
    Binance devuelve -1003, para que el circuit breaker lo detecte en
    el próximo request de este mismo grupo.
    """
    ultimo_error = "sin detalle"
    for dominio in dominios:
        url = f"{dominio}{path}"
        try:
            r = requests.get(url, params=params, timeout=8)
            body = r.json()
            _registrar_si_es_ban(grupo, body)
            return body, r.status_code
        except Exception as e:
            ultimo_error = str(e)
            continue
    return {"error": f"Todos los dominios fallaron: {ultimo_error}"}, 502


def _proxy_get_simple(url, params, grupo="futures"):
    """Mismo patrón que _proxy_get pero para un único dominio fijo
    (futures/Bybit), sin lista de fallback."""
    try:
        r = requests.get(url, params=params, timeout=8)
        body = r.json()
        _registrar_si_es_ban(grupo, body)
        return body, r.status_code
    except Exception as e:
        return {"error": str(e)}, 502


# ============================================================
# MOTOR DE PREDICCIONES — genera tesis de mercado automáticas
# ============================================================
#
# Objetivo: correr SOLO (hilo en background, mismo patrón que el
# WebSocket relay de depth más abajo) y dejar una "tesis" nueva cada
# INTERVALO_PREDICCION_HORAS, sin depender de que alguien tenga el
# dashboard de Streamlit abierto -- por eso vive acá, en el proxy que
# corre 24/7 como servicio, y no en main.py (que solo ejecuta cuando
# hay una sesión de navegador activa).
#
# ALCANCE HONESTO: esto es una versión LIVIANA del cálculo completo
# que main.py hace cada 15s (Flip Local/Global separados, Walls con
# vencimientos filtrados a semanales reales, Imán Dorado de 3
# fuentes, etc.). Acá se usa un solo Flip (instrumentos con
# vencimiento <= 21 días) y un swing/tendencia simple sobre velas
# 15m -- suficiente para una tesis de "hacia dónde probablemente vaya
# el precio y con qué nivel de confianza", no para operar scalp. No
# se usa NumPy (para no forzar una dependencia nueva en este repo);
# con instrumentos de Deribit corriendo cada 5hs (no cada 15s) los
# loops en Python puro son perfectamente aceptables en performance.

INTERVALO_PREDICCION_HORAS = 5  # ~4-5 tesis por día
INTERVALO_REINTENTO_MINUTOS = 10  # si un ciclo falla (ban activo, timeout en frío al
                                    # despertar del sleep de Render, etc.), reintentar
                                    # pronto -- NO esperar las 5hs completas del
                                    # intervalo normal, o un fallo nocturno te deja sin
                                    # tesis toda la mañana siguiente.
MAX_PREDICCIONES_GUARDADAS = 14  # ~2.9 días de historial (deja margen sobre las 7 que se muestran)
COOLDOWN_ON_DEMAND_SEGUNDOS = 600  # 10 min -- antes eran 120s (2 min), subido tras el episodio de
                                     # bans en cadena: con la tesis "vencida" indefinidamente (ciclos
                                     # fallando) y varias sesiones reconectando cada 15s, 2 min de
                                     # cooldown seguía dejando hasta 30 intentos/hora justo en el peor
                                     # momento. 10 min baja eso a 6/hora como máximo.
RUTA_PREDICCIONES = "predicciones.json"

_PREDICCIONES = []
_PREDICCIONES_LOCK = threading.Lock()
_GENERACION_LOCK = threading.Lock()  # exclusión mutua entre el hilo de fondo y el disparo on-demand
_ULTIMO_INTENTO_GENERACION = 0  # timestamp (epoch) del último intento -- cooldown del disparo on-demand
_ULTIMO_ERROR_GENERACION = None  # última razón de fallo (string), expuesta en /predicciones para diagnóstico rápido sin depender de leer logs de Render


def _norm_pdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)


def _gamma_bs(spot, strike, vol_anual, dias, tasa=0.0):
    """Gamma de Black-Scholes, sin dependencias externas (ver nota de alcance arriba)."""
    if dias <= 0 or vol_anual <= 0 or spot <= 0 or strike <= 0:
        return 0.0
    t = dias / 365.0
    try:
        d1 = (math.log(spot / strike) + (tasa + 0.5 * vol_anual ** 2) * t) / (vol_anual * math.sqrt(t))
    except (ValueError, ZeroDivisionError):
        return 0.0
    return _norm_pdf(d1) / (spot * vol_anual * math.sqrt(t))


def _obtener_instrumentos_deribit_interno():
    """
    Mismo criterio que obtener_instrumentos_deribit en main.py, pero
    autocontenido acá -- Deribit no bloquea la IP de Render (a
    diferencia de Binance/Bybit), así que no hace falta pasar por
    ningún proxy adicional, se pide directo.
    """
    try:
        r = requests.get(
            "https://www.deribit.com/api/v2/public/get_book_summary_by_currency",
            params={"currency": "BTC", "kind": "option"},
            timeout=10,
        )
        resumen = r.json()["result"]
        instrumentos = []
        for item in resumen:
            partes = item.get("instrument_name", "").split("-")
            if len(partes) != 4:
                continue
            _, venc_str, strike_str, tipo_letra = partes
            try:
                strike = float(strike_str)
            except ValueError:
                continue
            oi = item.get("open_interest", 0) or 0
            iv = item.get("mark_iv", None)
            if iv is None or oi <= 0:
                continue
            try:
                fecha = datetime.strptime(venc_str, "%d%b%y").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            instrumentos.append({
                "strike": strike,
                "tipo": "call" if tipo_letra == "C" else "put",
                "oi": float(oi),
                "iv": float(iv) / 100.0,
                "vencimiento": fecha,
            })
        return instrumentos
    except Exception:
        return None


def _gex_en_precio(instrumentos, spot_hipotetico, ahora):
    total = 0.0
    for inst in instrumentos:
        dias = (inst["vencimiento"] - ahora).total_seconds() / 86400.0
        if dias <= 0:
            continue
        g = _gamma_bs(spot_hipotetico, inst["strike"], inst["iv"], dias)
        gex = g * inst["oi"] * (spot_hipotetico ** 2) * 0.01
        total += gex if inst["tipo"] == "call" else -gex
    return total


def _calcular_flip_simple(instrumentos, spot, ahora, rango_pct=0.08, pasos=41):
    """Cruce de signo del GEX más cercano al spot, y el GEX al spot actual."""
    if not instrumentos:
        return None, None

    precio_min = spot * (1 - rango_pct)
    precio_max = spot * (1 + rango_pct)
    paso = (precio_max - precio_min) / (pasos - 1)

    curva = [(precio_min + i * paso, None) for i in range(pasos)]
    curva = [(p, _gex_en_precio(instrumentos, p, ahora)) for p, _ in curva]
    gex_spot = _gex_en_precio(instrumentos, spot, ahora)

    flip = None
    mejor_dist = None
    for i in range(len(curva) - 1):
        pa, ga = curva[i]
        pb, gb = curva[i + 1]
        if ga == 0:
            cand = pa
        elif ga * gb < 0:
            proporcion = abs(ga) / (abs(ga) + abs(gb))
            cand = pa + proporcion * (pb - pa)
        else:
            continue
        d = abs(cand - spot)
        if mejor_dist is None or d < mejor_dist:
            mejor_dist = d
            flip = cand

    return flip, gex_spot


def _encontrar_wall_simple(instrumentos, tipo, spot, ahora):
    """Mismo score que main.py (OI x gamma x peso_tiempo x peso_distancia), sin NumPy."""
    por_strike = {}
    for inst in instrumentos:
        if inst["tipo"] != tipo:
            continue
        por_strike.setdefault(inst["strike"], []).append(inst)

    mejor = None
    mejor_score = -1.0

    for strike, candidatos in por_strike.items():
        oi_total = sum(c["oi"] for c in candidatos)
        candidatos.sort(key=lambda c: c["vencimiento"])
        ref = candidatos[0]
        dias = max((ref["vencimiento"] - ahora).total_seconds() / 86400.0, 0.01)
        gamma = _gamma_bs(spot, strike, ref["iv"], dias)
        peso_tiempo = 1.0 / math.sqrt(max(dias, 0.5))
        dist_pct = abs((strike - spot) / spot) * 100
        peso_distancia = math.exp(-dist_pct / 2.5)
        score = oi_total * gamma * peso_tiempo * peso_distancia

        if score > mejor_score:
            mejor_score = score
            mejor = {"strike": strike, "oi": oi_total, "distancia_pct": (strike - spot) / spot * 100}

    return mejor


def _swing_niveles(velas, ventana=5, max_niveles=3):
    """velas: formato crudo de Binance klines (lista de listas)."""
    highs = [float(v[2]) for v in velas]
    lows = [float(v[3]) for v in velas]
    n = len(velas)

    swing_highs, swing_lows = [], []
    for i in range(ventana, n - ventana):
        vh = highs[i - ventana:i + ventana + 1]
        vl = lows[i - ventana:i + ventana + 1]
        if highs[i] == max(vh):
            swing_highs.append(highs[i])
        if lows[i] == min(vl):
            swing_lows.append(lows[i])

    precio_actual = float(velas[-1][4])
    resistencias = sorted([h for h in swing_highs if h > precio_actual])[:max_niveles]
    soportes = sorted([l for l in swing_lows if l < precio_actual], reverse=True)[:max_niveles]
    return soportes, resistencias


def _tendencia_sma(velas, periodo=20):
    cierres = [float(v[4]) for v in velas]
    if len(cierres) < periodo:
        return "neutral"
    sma = sum(cierres[-periodo:]) / periodo
    ultimo = cierres[-1]
    if ultimo > sma * 1.0005:
        return "alcista"
    if ultimo < sma * 0.9995:
        return "bajista"
    return "neutral"


def _agregar_etapas(candidatos):
    """
    Convierte una lista de (nivel, fuente) en hasta 3 etapas ordenadas,
    evitando 2 niveles casi pegados (ej. wall y swing a $30 de
    distancia cuentan como "la misma etapa"). Elevada a nivel de módulo
    (antes vivía adentro de _generar_prediccion) porque ahora la usan
    TANTO el escenario A como el escenario B contingente -- ver
    _construir_escenario_b.
    """
    vistos = set()
    resultado = []
    for nivel, fuente in sorted(candidatos, key=lambda c: c[0]):
        clave = round(nivel / 50)
        if clave in vistos:
            continue
        vistos.add(clave)
        resultado.append({"nivel": round(nivel, 1), "fuente": fuente})
        if len(resultado) >= 3:
            break
    return resultado


def _construir_escenario_b(sesgo_a, invalidacion_a, invalidacion_a_fuente, precio_emision,
                            soportes, resistencias, call_wall, put_wall, flip):
    """
    ESCENARIO B (contingente) -- pedido explícito del usuario: "donde
    se invalida A, ahí arranca B". No es una tesis independiente desde
    cero, es la CONTINUACIÓN lógica: si el nivel que sostenía la tesis
    A se rompe, ese mismo quiebre habilita la tesis opuesta, con sus
    propias etapas (mismos candidatos que ya se calcularon para A --
    swings, walls, flip -- pero del lado opuesto y más allá del punto
    de partida) y su propia invalidación.

    Invalidación de B: el precio de emisión ORIGINAL de la tesis A. Si
    el precio recupera ese nivel, ya volvió al terreno donde A todavía
    era válida -- ninguno de los dos escenarios tiene sentido seguir
    evaluándolo más allá de ese punto. Simétrico, sin inventar un
    criterio nuevo.

    Devuelve None si sesgo_a es neutral, si no hay invalidación de A
    definida, o si no se encontró ningún nivel claro más allá del
    punto de partida (mejor no armar un B vacío a forzar uno con
    candidatos inventados).
    """
    if sesgo_a not in ("alcista", "bajista") or invalidacion_a is None:
        return None

    sesgo_b = "bajista" if sesgo_a == "alcista" else "alcista"

    if sesgo_b == "bajista":
        candidatos = [(s, "Imán soporte (liquidez)") for s in soportes if s < invalidacion_a]
        if put_wall and put_wall["strike"] < invalidacion_a:
            candidatos.append((put_wall["strike"], "Put Wall (OI opciones)"))
        if flip is not None and flip < invalidacion_a:
            candidatos.append((flip, "Flip Gamma (régimen)"))
        candidatos.sort(key=lambda c: -c[0])
        etapas_b = _agregar_etapas([(-n, f) for n, f in candidatos])
        etapas_b = [{"nivel": -e["nivel"], "fuente": e["fuente"]} for e in etapas_b]
    else:
        candidatos = [(r, "Imán resistencia (liquidez)") for r in resistencias if r > invalidacion_a]
        if call_wall and call_wall["strike"] > invalidacion_a:
            candidatos.append((call_wall["strike"], "Call Wall (OI opciones)"))
        if flip is not None and flip > invalidacion_a:
            candidatos.append((flip, "Flip Gamma (régimen)"))
        etapas_b = _agregar_etapas(candidatos)

    if not etapas_b:
        return None

    return {
        "sesgo": sesgo_b,
        "punto_partida": invalidacion_a,
        "punto_partida_fuente": invalidacion_a_fuente,
        "etapas": etapas_b,
        "invalidacion": precio_emision,
        "invalidacion_fuente": "Recuperación del precio de emisión original (tesis A)",
    }


def _armar_resultado_escenario(etapas_alcanzadas, etapas_total, invalidada, sesgo, precio_partida,
                                niveles_ordenados, velas_ventana):
    """
    Puntaje de UN escenario (A o B) ya recorrido cronológicamente --
    factorizado acá porque la misma lógica de scoring aplica a ambos,
    ver _evaluar_resultado_prediccion para el recorrido cronológico en
    sí (que sí difiere entre A y B).
    """
    if invalidada:
        acierto_pct = round((etapas_alcanzadas / etapas_total) * 100 * 0.3) if etapas_total else 0
        estado = "invalidada"
    elif etapas_total > 0 and etapas_alcanzadas == etapas_total:
        acierto_pct = 100
        estado = "cumplida"
    elif etapas_alcanzadas > 0:
        acierto_pct = round((etapas_alcanzadas / etapas_total) * 100)
        estado = "parcial"
    elif etapas_total > 0 and velas_ventana:
        primera_etapa = niveles_ordenados[0]
        distancia_total = abs(primera_etapa - precio_partida)
        precio_final_ventana = float(velas_ventana[-1][4])
        avance = (
            (precio_final_ventana - precio_partida) if sesgo == "alcista"
            else (precio_partida - precio_final_ventana)
        )
        progreso = max(0.0, min(avance / distancia_total, 1.0)) if distancia_total > 0 else 0.0
        acierto_pct = round(progreso * 100)
        estado = "en_curso"
    else:
        acierto_pct = 0
        estado = "sin_datos"

    return {
        "acierto_pct": acierto_pct,
        "estado": estado,
        "invalidada": invalidada,
        "etapas_alcanzadas": etapas_alcanzadas,
        "etapas_total": etapas_total,
    }


PESO_ESCENARIO_A = 0.65  # la tesis principal siempre pesa más...
PESO_ESCENARIO_B = 0.35  # ...pero B compensa para que "A invalidada" no quede en 0 sin matices


def _evaluar_resultado_prediccion(pred, velas_ventana):
    """
    Recorre CRONOLÓGICAMENTE las velas transcurridas desde la emisión de
    una tesis hasta ahora. Ahora en DOS fases:

    FASE A: evalúa la tesis principal -- ¿tocó las etapas proyectadas
    en orden, o tocó la invalidación de A antes?

    FASE B (solo si A se invalidó Y la tesis tiene escenario_b): a
    partir de la vela donde A se invalidó, sigue recorriendo pero
    ahora evaluando el escenario contingente -- ¿tocó las etapas de B,
    o volvió a superar la invalidación de B (que es el precio de
    emisión original) antes?

    El % final es un promedio ponderado (A pesa más, ver
    PESO_ESCENARIO_A/B) -- si A nunca se invalidó, B ni se evalúa y el
    % es 100% el de A.

    Devuelve dict: {acierto_pct, estado, etapas_alcanzadas, etapas_total,
    invalidada, escenario_b_resultado, peso_a, peso_b}
    """
    sesgo = pred.get("sesgo")
    etapas = pred.get("etapas", [])
    invalidacion = pred.get("invalidacion")
    precio_emision = pred.get("precio_emision")
    escenario_b = pred.get("escenario_b")

    if sesgo == "neutral" or not etapas or precio_emision is None or not velas_ventana:
        return {
            "acierto_pct": None, "estado": "sin_direccion", "invalidada": False,
            "etapas_alcanzadas": 0, "etapas_total": len(etapas),
            "escenario_b_resultado": None, "peso_a": 1.0, "peso_b": 0.0,
        }

    # --- FASE A ---
    niveles_a = sorted([e["nivel"] for e in etapas], reverse=(sesgo == "bajista"))

    invalidada_a = False
    idx_a = 0
    idx_vela_quiebre = None

    for i, vela in enumerate(velas_ventana):
        high, low = float(vela[2]), float(vela[3])

        if invalidacion is not None:
            if sesgo == "alcista" and low <= invalidacion:
                invalidada_a = True
                idx_vela_quiebre = i
                break
            if sesgo == "bajista" and high >= invalidacion:
                invalidada_a = True
                idx_vela_quiebre = i
                break

        while idx_a < len(niveles_a):
            nivel = niveles_a[idx_a]
            tocado = (sesgo == "alcista" and high >= nivel) or (sesgo == "bajista" and low <= nivel)
            if tocado:
                idx_a += 1
            else:
                break

    resultado_a = _armar_resultado_escenario(
        idx_a, len(niveles_a), invalidada_a, sesgo, precio_emision, niveles_a, velas_ventana,
    )

    # --- FASE B (solo si A se invalidó y hay escenario contingente) ---
    resultado_b = None

    if invalidada_a and escenario_b and idx_vela_quiebre is not None:
        velas_fase_b = velas_ventana[idx_vela_quiebre:]
        sesgo_b = escenario_b.get("sesgo")
        invalidacion_b = escenario_b.get("invalidacion")
        precio_partida_b = escenario_b.get("punto_partida", precio_emision)
        niveles_b = sorted(
            [e["nivel"] for e in escenario_b.get("etapas", [])], reverse=(sesgo_b == "bajista")
        )

        invalidada_b = False
        idx_b = 0

        for vela in velas_fase_b:
            high, low = float(vela[2]), float(vela[3])

            if invalidacion_b is not None:
                if sesgo_b == "alcista" and low <= invalidacion_b:
                    invalidada_b = True
                    break
                if sesgo_b == "bajista" and high >= invalidacion_b:
                    invalidada_b = True
                    break

            while idx_b < len(niveles_b):
                nivel = niveles_b[idx_b]
                tocado = (sesgo_b == "alcista" and high >= nivel) or (sesgo_b == "bajista" and low <= nivel)
                if tocado:
                    idx_b += 1
                else:
                    break

        resultado_b = _armar_resultado_escenario(
            idx_b, len(niveles_b), invalidada_b, sesgo_b, precio_partida_b, niveles_b, velas_fase_b,
        )

    if resultado_b is not None:
        acierto_pct = round(
            resultado_a["acierto_pct"] * PESO_ESCENARIO_A + resultado_b["acierto_pct"] * PESO_ESCENARIO_B
        )
        peso_b_usado = PESO_ESCENARIO_B
    else:
        acierto_pct = resultado_a["acierto_pct"]
        peso_b_usado = 0.0

    return {
        "acierto_pct": acierto_pct,
        "estado": resultado_a["estado"],
        "invalidada": invalidada_a,
        "etapas_alcanzadas": resultado_a["etapas_alcanzadas"],
        "etapas_total": resultado_a["etapas_total"],
        "escenario_b_resultado": resultado_b,
        "peso_a": 1.0 - peso_b_usado,
        "peso_b": peso_b_usado,
    }


def _sellar_predicciones_pendientes_sin_lock(velas_completas, ahora):
    """
    ASUME que _PREDICCIONES_LOCK ya está tomado por quien llama (ver uso
    en _hilo_generador_predicciones) -- threading.Lock no es reentrante,
    así que esta función NO toma el lock por su cuenta.

    Recorre las predicciones guardadas que todavía no tienen "resultado"
    y, para cada una, arma la ventana de velas desde su ts_emision hasta
    ahora y la sella con _evaluar_resultado_prediccion. Una vez sellada
    (resultado != None) queda FIJA para siempre -- no se vuelve a
    recalcular en ciclos futuros, es una foto de lo que pasó durante esa
    tesis, no una métrica que se mueve con el tiempo.
    """
    if not velas_completas:
        return

    for p in _PREDICCIONES:
        if p.get("resultado") is not None:
            continue
        try:
            ts_emision = datetime.fromisoformat(p["ts_emision"])
        except Exception:
            continue

        velas_ventana = [
            v for v in velas_completas
            if datetime.fromtimestamp(v[0] / 1000, tz=timezone.utc) >= ts_emision
        ]
        if len(velas_ventana) < 2:
            continue  # todavía no pasó tiempo suficiente como para evaluar nada

        p["resultado"] = _evaluar_resultado_prediccion(p, velas_ventana)
        p["resultado"]["sellado_ts"] = ahora.isoformat()


def _generar_prediccion():
    """
    Arma UNA tesis de mercado completa. Devuelve (None, None) si falta
    algún dato crítico (velas o ban activo) -- mejor no emitir nada a
    emitir una tesis fabricada con datos incompletos.

    Devuelve (pred_dict, velas_completas): velas_completas es la lista
    cruda de klines (limit=100, ~25hs) que además de usarse acá para
    tendencia/swing, se reutiliza afuera (en el hilo) para SELLAR con
    % de acierto las tesis anteriores -- así no hace falta un segundo
    pedido a Binance solo para eso.

    PESO CONTRA BINANCE (ajustado): antes pedía limit=400, que cae en
    el bucket de peso 2 de Binance (101-500 velas). Bajado a limit=100
    -- mismo bucket de peso 1 que usa el resto de la app (klines,
    ticker, etc.) -- porque el disparo on-demand (ver
    _generar_si_corresponde) hace que este pedido ya NO dependa solo
    del reloj del hilo de fondo cada 5hs, sino que también puede
    dispararse desde cualquier sesión que entra al dashboard con la
    tesis vencida. Eso suma peso real que antes no existía, así que
    hay que mantenerlo lo más liviano posible.

    LÍMITE ACEPTADO por esta reducción: 25hs de historial alcanza de
    sobra para sellar la tesis pendiente típica (~5-6hs de brecha entre
    una tesis y la siguiente), pero si el servicio estuvo caído varios
    ciclos seguidos (>25hs sin generar nada), esas tesis muy viejas se
    quedan sin sellar un poco más -- no rompen nada, solo esperan al
    próximo ciclo con margen suficiente.
    """
    global _ULTIMO_ERROR_GENERACION

    if _grupo_baneado("spot") or _grupo_baneado("futures"):
        _ULTIMO_ERROR_GENERACION = "Ban activo (circuit breaker spot o futures) al momento del intento"
        return None, None

    ahora = datetime.now(timezone.utc)

    velas_completas, status = _proxy_get(
        DOMINIOS_SPOT, "/api/v3/klines",
        {"symbol": "BTCUSDT", "interval": "15m", "limit": 100}, grupo="spot",
    )
    if not isinstance(velas_completas, list) or len(velas_completas) < 30:
        _ULTIMO_ERROR_GENERACION = (
            f"Klines insuficientes o inválidas: status_http={status}, "
            f"tipo_respuesta={type(velas_completas).__name__}, "
            f"contenido={str(velas_completas)[:200]}"
        )
        return None, None

    velas = velas_completas  # ahora la ventana corta y la completa son la misma (100 velas, ~25hs)

    # Ventana de detección de swings: 12hs (48 velas de 15m) -- coherente
    # con que la tesis solo vale ~4-5hs (estructura de días atrás no es
    # relevante para el próximo movimiento de corto plazo). "Para saber
    # qué pasa en las próximas 4hs, mirar las últimas 12hs" (pedido
    # explícito del usuario).
    velas_para_swings = velas_completas[-48:] if len(velas_completas) >= 48 else velas_completas
    soportes, resistencias = _swing_niveles(velas_para_swings, ventana=5, max_niveles=4)
    tendencia = _tendencia_sma(velas)

    precio_actual = float(velas[-1][4])

    funding_valor = None
    fbody, _ = _proxy_get_simple(f"{DOMINIO_FUTURES}/fapi/v1/premiumIndex", {"symbol": "BTCUSDT"}, grupo="futures")
    if isinstance(fbody, dict) and "lastFundingRate" in fbody:
        funding_valor = float(fbody["lastFundingRate"]) * 100

    instrumentos = _obtener_instrumentos_deribit_interno()
    flip, gex_spot, call_wall, put_wall, regimen = None, None, None, None, None

    if instrumentos:
        limite_venc = ahora + timedelta(days=21)
        filtrados = [i for i in instrumentos if i["vencimiento"] <= limite_venc]
        if filtrados:
            flip, gex_spot = _calcular_flip_simple(filtrados, precio_actual, ahora)
            call_wall = _encontrar_wall_simple(filtrados, "call", precio_actual, ahora)
            put_wall = _encontrar_wall_simple(filtrados, "put", precio_actual, ahora)
            if gex_spot is not None:
                regimen = "Long Gamma (contención)" if gex_spot > 0 else "Short Gamma (momentum)"

    # --- Puntaje de dirección: tendencia como driver principal, régimen
    # gamma como amplificador si Short Gamma refuerza esa misma
    # tendencia, y el flip como confirmación adicional si está del lado
    # esperado. Deliberadamente simple -- ver docstring del módulo. ---
    dir_pts = 0
    if tendencia == "alcista":
        dir_pts += 30
    elif tendencia == "bajista":
        dir_pts -= 30

    if gex_spot is not None and gex_spot < 0 and tendencia != "neutral":
        dir_pts += 20 if tendencia == "alcista" else -20

    if flip is not None:
        if flip > precio_actual and tendencia == "alcista":
            dir_pts += 10
        elif flip < precio_actual and tendencia == "bajista":
            dir_pts -= 10

    if dir_pts >= 15:
        sesgo = "alcista"
    elif dir_pts <= -15:
        sesgo = "bajista"
    else:
        sesgo = "neutral"

    confianza = min(round(abs(dir_pts) / 60 * 100), 95)
    if sesgo == "neutral":
        confianza = min(confianza, 40)

    etapas = []
    invalidacion = None
    invalidacion_fuente = None

    # Distancia mínima entre el precio actual y cualquier nivel candidato
    # a etapa -- pedido explícito del usuario: las proyecciones quedaban
    # demasiado cortas (100-150 USD), sin margen real para una tesis de
    # varias horas. 350 USD queda a mitad del rango pedido (300-500).
    # Distancia mínima entre el precio actual y cualquier nivel candidato
    # a etapa (profit) -- pedido explícito del usuario: 300 USD como piso,
    # apuntando a objetivos de 300-600 USD. El stop (invalidación) NO
    # lleva este piso -- con la ventana de 12hs ya cae naturalmente en
    # el rango de 200-300 que el usuario espera para el stop, sin
    # necesidad de un filtro adicional ahí.
    DISTANCIA_MINIMA_ETAPA_USD = 300

    if sesgo == "alcista":
        candidatos = [
            (r, "Imán resistencia (liquidez)") for r in resistencias
            if r > precio_actual + DISTANCIA_MINIMA_ETAPA_USD
        ]
        if call_wall and call_wall["strike"] > precio_actual + DISTANCIA_MINIMA_ETAPA_USD:
            candidatos.append((call_wall["strike"], "Call Wall (OI opciones)"))
        if flip is not None and flip > precio_actual + DISTANCIA_MINIMA_ETAPA_USD:
            candidatos.append((flip, "Flip Gamma (régimen)"))
        etapas = _agregar_etapas(candidatos)
        if soportes:
            invalidacion = round(soportes[0], 1)
            invalidacion_fuente = "Imán soporte reciente"

    elif sesgo == "bajista":
        candidatos = [
            (r, "Imán soporte (liquidez)") for r in soportes
            if r < precio_actual - DISTANCIA_MINIMA_ETAPA_USD
        ]
        if put_wall and put_wall["strike"] < precio_actual - DISTANCIA_MINIMA_ETAPA_USD:
            candidatos.append((put_wall["strike"], "Put Wall (OI opciones)"))
        if flip is not None and flip < precio_actual - DISTANCIA_MINIMA_ETAPA_USD:
            candidatos.append((flip, "Flip Gamma (régimen)"))
        # ordenamos de más cercano a más lejano igual (candidatos ya vienen < precio_actual)
        candidatos.sort(key=lambda c: -c[0])
        etapas = _agregar_etapas([(-n, f) for n, f in candidatos])
        etapas = [{"nivel": -e["nivel"], "fuente": e["fuente"]} for e in etapas]
        if resistencias:
            invalidacion = round(resistencias[0], 1)
            invalidacion_fuente = "Imán resistencia reciente"

    else:
        resistencias_lejos = [r for r in resistencias if r > precio_actual + DISTANCIA_MINIMA_ETAPA_USD]
        soportes_lejos = [s for s in soportes if s < precio_actual - DISTANCIA_MINIMA_ETAPA_USD]
        if resistencias_lejos:
            etapas.append({"nivel": round(resistencias_lejos[0], 1), "fuente": "Imán resistencia (liquidez)"})
        if soportes_lejos:
            etapas.append({"nivel": round(soportes_lejos[0], 1), "fuente": "Imán soporte (liquidez)"})

    escenario_b = _construir_escenario_b(
        sesgo, invalidacion, invalidacion_fuente, round(precio_actual, 1),
        soportes, resistencias, call_wall, put_wall, flip,
    )

    if etapas:
        resumen = (
            f"Sesgo {sesgo} ({confianza}% confianza) — tendencia 15M {tendencia}"
            f"{', régimen ' + regimen if regimen else ''}. "
            f"Próxima etapa: ${etapas[0]['nivel']:,.0f} ({etapas[0]['fuente']})."
        )
    else:
        resumen = (
            f"Sesgo {sesgo} ({confianza}% confianza) — tendencia 15M {tendencia}"
            f"{', régimen ' + regimen if regimen else ''}. Sin niveles claros de continuación este ciclo."
        )

    return {
        "id": str(uuid.uuid4()),
        "ts_emision": ahora.isoformat(),
        "valido_hasta": (ahora + timedelta(hours=INTERVALO_PREDICCION_HORAS + 1)).isoformat(),
        "precio_emision": round(precio_actual, 1),
        "sesgo": sesgo,
        "confianza": confianza,
        "tendencia": tendencia,
        "regimen": regimen,
        "funding_valor": funding_valor,
        "etapas": etapas,
        "invalidacion": invalidacion,
        "invalidacion_fuente": invalidacion_fuente,
        "escenario_b": escenario_b,
        "resumen": resumen,
        "resultado": None,  # se sella más adelante, cuando se emita la próxima tesis
    }, velas_completas


def _cargar_predicciones_disco():
    if os.path.exists(RUTA_PREDICCIONES):
        try:
            with open(RUTA_PREDICCIONES, "r") as f:
                return json.load(f)
        except Exception:
            return []
    return []


def _guardar_predicciones_disco(lista):
    try:
        with open(RUTA_PREDICCIONES, "w") as f:
            json.dump(lista, f)
    except Exception:
        pass  # filesystem read-only u otro problema puntual -- no rompe el hilo


def _ejecutar_ciclo_generacion(forzar=False, bloquear=True):
    """
    Corre UN ciclo de generación (generar + sellar + guardar), protegido
    por _GENERACION_LOCK -- este lock es compartido entre el hilo de
    fondo y el disparo on-demand de abajo, así nunca corren dos
    generaciones en simultáneo (dos pedidos a Binance/Deribit a la vez
    por la misma tesis).

    bloquear=True (usado por el hilo de fondo): espera el lock si está
    ocupado -- no hay apuro, el hilo tiene las 5hs completas por delante.

    bloquear=False (usado por el disparo on-demand del endpoint): si el
    lock ya está tomado por otra generación en curso, NO espera, sale
    inmediatamente -- así un request de un usuario no se cuelga
    esperando que termine un ciclo ajeno.

    forzar=True: ignora el cooldown de 2 minutos (lo usa el hilo de
    fondo, que ya decide su propio ritmo con time.sleep). forzar=False
    respeta el cooldown -- evita que refreshes de 15s del dashboard
    disparen un intento de generación cada vez que la anterior está
    vencida pero la de ahora también falló.

    Devuelve True si efectivamente generó y guardó una tesis nueva.
    """
    global _PREDICCIONES, _ULTIMO_INTENTO_GENERACION, _ULTIMO_ERROR_GENERACION

    adquirido = _GENERACION_LOCK.acquire(blocking=bloquear)
    if not adquirido:
        return False

    try:
        if not forzar and (time.time() - _ULTIMO_INTENTO_GENERACION) < COOLDOWN_ON_DEMAND_SEGUNDOS:
            return False
        _ULTIMO_INTENTO_GENERACION = time.time()

        pred, velas_completas = _generar_prediccion()
        if not pred:
            return False

        ahora_ciclo = datetime.now(timezone.utc)
        with _PREDICCIONES_LOCK:
            _sellar_predicciones_pendientes_sin_lock(velas_completas, ahora_ciclo)
            _PREDICCIONES.insert(0, pred)
            _PREDICCIONES = _PREDICCIONES[:MAX_PREDICCIONES_GUARDADAS]
            _guardar_predicciones_disco(_PREDICCIONES)
        _ULTIMO_ERROR_GENERACION = None  # ciclo exitoso -- limpia cualquier error viejo
        return True
    except Exception as e:
        _ULTIMO_ERROR_GENERACION = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
        print(f"[predicciones] error generando tesis: {_ULTIMO_ERROR_GENERACION}")
        return False
    finally:
        _GENERACION_LOCK.release()


def _generar_si_corresponde():
    """
    Disparo ON-DEMAND: se llama al principio de /predicciones, en cada
    request real de un usuario. Si no hay ninguna tesis guardada, o la
    más reciente ya venció su intervalo de INTERVALO_PREDICCION_HORAS,
    dispara un ciclo de generación ahí mismo (sin bloquear si ya hay
    uno en curso -- ver _ejecutar_ciclo_generacion).

    POR QUÉ HACE FALTA ESTO (no solo el hilo de fondo): en el free tier
    de Render, el servicio se duerme por inactividad y el hilo de fondo
    muere con el proceso. Si nadie visita la app en horas, el hilo
    nunca llega a completar un ciclo, y el reintento de
    INTERVALO_REINTENTO_MINUTOS no sirve de nada porque el proceso ya
    no existe para reintentar. Enganchar la generación al propio
    request del usuario es lo que garantiza que, apenas alguien abre
    el dashboard (lo que sea que haya despertado el servicio), la tesis
    pendiente se genera en ese momento -- no depende de que el proceso
    haya seguido despierto solo, dormido, esperando su turno.

    FIX (episodio real): si la generación viene fallando (ban activo,
    Deribit caído, etc.), la tesis se queda "vencida" indefinidamente
    -- y CADA sesión abierta, cada 15s (el auto-refresh del dashboard),
    volvía a llamar acá. El cooldown interno de _ejecutar_ciclo_generacion
    ya evitaba pedidos duplicados, pero a 2 minutos de cooldown eso
    seguía siendo hasta 30 intentos/hora justo en el peor momento
    (varias sesiones reconectando por señal inestable + ban activo).
    Ahora, además de un cooldown más largo (10 min), se corta ACÁ MISMO
    si hay un ban activo -- ni siquiera intenta tomar el lock, cero
    trabajo de más mientras Binance ya está limitando la IP.
    """
    if _grupo_baneado("spot") or _grupo_baneado("futures"):
        return  # no sumar presión mientras el ban ya está activo

    with _PREDICCIONES_LOCK:
        hay_predicciones = len(_PREDICCIONES) > 0
        vencida = True
        if hay_predicciones:
            try:
                ultima = datetime.fromisoformat(_PREDICCIONES[0]["ts_emision"])
                vencida = (datetime.now(timezone.utc) - ultima).total_seconds() >= INTERVALO_PREDICCION_HORAS * 3600
            except Exception:
                vencida = True

    if hay_predicciones and not vencida:
        return  # la más reciente sigue vigente, nada que generar todavía

    _ejecutar_ciclo_generacion(forzar=False, bloquear=False)


def _hilo_generador_predicciones():
    """
    Best-effort mientras el proceso esté despierto: intenta un ciclo
    cada INTERVALO_PREDICCION_HORAS (o cada INTERVALO_REINTENTO_MINUTOS
    si el ciclo anterior falló). Complementa -- no reemplaza -- al
    disparo on-demand de _generar_si_corresponde: si el servicio se
    mantiene despierto por tráfico constante, este hilo solo mantiene
    el ritmo regular sin depender de que llegue un request justo cuando
    toca. Comparte _GENERACION_LOCK con el disparo on-demand, así nunca
    se pisan.

    LÍMITE HONESTO (mismo que contador_sesiones.json en main.py): el
    disco de Render free tier no es 100% persistente a largo plazo
    (puede perderse en un redeploy o al dormirse/despertar el
    servicio) -- por eso existe además el disparo on-demand, que no
    depende de que este hilo haya sobrevivido.
    """
    global _PREDICCIONES
    with _PREDICCIONES_LOCK:
        _PREDICCIONES = _cargar_predicciones_disco()

    while True:
        generado = _ejecutar_ciclo_generacion(forzar=True, bloquear=True)
        if generado:
            time.sleep(INTERVALO_PREDICCION_HORAS * 3600)
        else:
            time.sleep(INTERVALO_REINTENTO_MINUTOS * 60)


threading.Thread(target=_hilo_generador_predicciones, daemon=True).start()


@app.route("/predicciones")
def predicciones():
    _generar_si_corresponde()
    with _PREDICCIONES_LOCK:
        lista = list(_PREDICCIONES)
    return jsonify({
        "predicciones": lista,
        "intervalo_horas": INTERVALO_PREDICCION_HORAS,
        "ultimo_error_generacion": _ULTIMO_ERROR_GENERACION,
    })


@app.route("/predicciones/generar", methods=["POST"])
def generar_prediccion_manual():
    """
    Disparo MANUAL: pensado para un botón en el dashboard ("Generar
    análisis ahora"). A diferencia de _generar_si_corresponde (que solo
    actúa si la última tesis ya venció), esto intenta generar una nueva
    SIEMPRE que se llame -- pero sigue respetando el cooldown de 2
    minutos y el lock compartido con el hilo de fondo (forzar=False,
    bloquear=False), así un click accidental doble, o varias sesiones
    tocando el botón casi al mismo tiempo, no disparan pedidos
    simultáneos a Binance/Deribit.
    """
    generado = _ejecutar_ciclo_generacion(forzar=False, bloquear=False)

    if generado:
        return jsonify({"ok": True, "mensaje": "Tesis nueva generada."})

    segundos_cooldown_restantes = max(0, int(COOLDOWN_ON_DEMAND_SEGUNDOS - (time.time() - _ULTIMO_INTENTO_GENERACION)))

    if segundos_cooldown_restantes > 0:
        mensaje = f"Cooldown activo, esperá ~{segundos_cooldown_restantes}s e intentá de nuevo."
    else:
        mensaje = "Ya hay una generación en curso (disparada por otra sesión o por el hilo de fondo) o faltan datos del mercado este ciclo -- probá de nuevo en unos segundos."

    return jsonify({"ok": False, "mensaje": mensaje}), 429


@app.route("/ticker24hr")
def ticker24hr():
    if _grupo_baneado("spot"):
        body, status = _respuesta_ban_activo("spot")
        return jsonify(body), status

    symbol = request.args.get("symbol", "BTCUSDT")
    cache_key = f"ticker24hr:{symbol}"

    body, status = _get_con_cache(
        cache_key,
        lambda: _proxy_get(DOMINIOS_SPOT, "/api/v3/ticker/24hr", {"symbol": symbol}, grupo="spot"),
        ttl_segundos=TTL_RAPIDO,
    )
    return jsonify(body), status


@app.route("/klines")
def klines():
    if _grupo_baneado("spot"):
        body, status = _respuesta_ban_activo("spot")
        return jsonify(body), status

    symbol = request.args.get("symbol", "BTCUSDT")
    interval = request.args.get("interval", "5m")
    limit = request.args.get("limit", "100")
    cache_key = f"klines:{symbol}:{interval}:{limit}"

    body, status = _get_con_cache(
        cache_key,
        lambda: _proxy_get(
            DOMINIOS_SPOT, "/api/v3/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
            grupo="spot",
        ),
        ttl_segundos=TTL_RAPIDO,
    )
    return jsonify(body), status


@app.route("/depth")
def depth():
    """
    Order book SPOT vía REST (snapshot, no vivo). Para book EN VIVO
    real, ver /ws/depth más abajo.
    """
    if _grupo_baneado("spot"):
        body, status = _respuesta_ban_activo("spot")
        return jsonify(body), status

    symbol = request.args.get("symbol", "BTCUSDT")
    limit = min(int(request.args.get("limit", "20")), 50)
    cache_key = f"depth_spot:{symbol}:{limit}"

    body, status = _get_con_cache(
        cache_key,
        lambda: _proxy_get(DOMINIOS_SPOT, "/api/v3/depth", {"symbol": symbol, "limit": limit}, grupo="spot"),
        ttl_segundos=TTL_RAPIDO,
    )
    return jsonify(body), status


@app.route("/premiumIndex")
def premium_index():
    if _grupo_baneado("futures"):
        body, status = _respuesta_ban_activo("futures")
        return jsonify(body), status

    symbol = request.args.get("symbol", "BTCUSDT")
    cache_key = f"premiumIndex:{symbol}"

    body, status = _get_con_cache(
        cache_key,
        lambda: _proxy_get_simple(f"{DOMINIO_FUTURES}/fapi/v1/premiumIndex", {"symbol": symbol}, grupo="futures"),
        ttl_segundos=TTL_LENTO,
    )
    return jsonify(body), status


@app.route("/openInterest")
def open_interest():
    if _grupo_baneado("futures"):
        body, status = _respuesta_ban_activo("futures")
        return jsonify(body), status

    symbol = request.args.get("symbol", "BTCUSDT")
    cache_key = f"openInterest:{symbol}"

    body, status = _get_con_cache(
        cache_key,
        lambda: _proxy_get_simple(f"{DOMINIO_FUTURES}/fapi/v1/openInterest", {"symbol": symbol}, grupo="futures"),
        ttl_segundos=TTL_LENTO,
    )
    return jsonify(body), status


@app.route("/futures/depth")
def futures_depth():
    """
    Order book FUTUROS (USDT-M) vía REST (snapshot). Para book en
    vivo, ver /ws/depth.
    """
    if _grupo_baneado("futures"):
        body, status = _respuesta_ban_activo("futures")
        return jsonify(body), status

    symbol = request.args.get("symbol", "BTCUSDT")
    limit = min(int(request.args.get("limit", "20")), 50)
    cache_key = f"depth_futures:{symbol}:{limit}"

    body, status = _get_con_cache(
        cache_key,
        lambda: _proxy_get_simple(
            f"{DOMINIO_FUTURES}/fapi/v1/depth", {"symbol": symbol, "limit": limit}, grupo="futures"
        ),
        ttl_segundos=TTL_RAPIDO,
    )
    return jsonify(body), status


@app.route("/bybit/openInterest")
def bybit_open_interest():
    # Bybit tiene su propio sistema de rate-limit, independiente del
    # circuit breaker de Binance -- no lo pisamos con _BAN_HASTA.
    symbol = request.args.get("symbol", "BTCUSDT")
    interval_time = request.args.get("intervalTime", "5min")
    try:
        r = requests.get(
            f"{DOMINIO_BYBIT}/v5/market/open-interest",
            params={
                "category": "linear",
                "symbol": symbol,
                "intervalTime": interval_time,
            },
            timeout=8,
        )
        return jsonify(r.json()), r.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 502


@app.route("/")
def home():
    ahora = time.time()
    return jsonify({
        "status": "ok",
        "mensaje": "Proxy de Binance funcionando",
        "websocket": "/ws/depth?market=spot|futures",
        "bookmap": "/bookmap",
        "predicciones": "/predicciones",
        "ban_spot_activo": _grupo_baneado("spot"),
        "ban_spot_restante_segundos": max(0, int(_BAN_HASTA["spot"] - ahora)),
        "ban_futures_activo": _grupo_baneado("futures"),
        "ban_futures_restante_segundos": max(0, int(_BAN_HASTA["futures"] - ahora)),
    })


@app.route("/bookmap")
def bookmap():
    """
    Sirve el HTML del bookmap en vivo desde el mismo proxy -- así no
    hace falta un hosting nuevo ni aprender otra plataforma. El
    archivo vive en la carpeta static/ de este mismo repo (ver
    instrucciones de despliegue). Al servirse desde este mismo
    dominio, el HTML detecta el host solo (window.location.host) --
    no hace falta editar ninguna URL a mano.
    """
    return send_from_directory("static", "bookmap.html")


# ============================================================
# WEBSOCKET RELAY — order book EN VIVO (sub-segundo, no REST)
# ============================================================
#
# Objetivo: reemplazar el polling REST de /depth y /futures/depth
# para quien necesite book en vivo real, sin gastar weight de Binance
# ni arriesgar bans -1003. Los endpoints REST de arriba NO se tocan,
# siguen funcionando igual para quien ya los consume (ej. Streamlit).
#
# ARQUITECTURA:
#   1. Un hilo en background por mercado (spot, futures) mantiene UNA
#      conexión persistente al "partial book depth stream" de Binance
#      -- Binance empuja los primeros 20 niveles de cada lado cada
#      ~100ms, sin que el proxy tenga que pedir nada.
#   2. Cada mensaje que llega se guarda como "último estado conocido"
#      en memoria, normalizado al mismo formato bids/asks que ya usan
#      /depth y /futures/depth -- así el frontend no distingue si el
#      dato vino de REST o de WS.
#   3. Cualquier cliente (frontend JS, bookmap 3D) que se conecta a
#      /ws/depth recibe ese estado en un loop -- sin pedir nada, sin
#      rate-limit de su lado. Esto SOLUCIONA el techo real que tenías
#      con REST (Binance banea -1003 si pedís muy seguido); acá el
#      proxy pide UNA sola vez y reparte a cuantos clientes hagan falta.
#
# LÍMITE HONESTO: esto es "partial depth" (foto de los primeros 20
# niveles, no el book completo con miles de niveles vía diff+
# reconciliación de secuencia). Para lectura visual -- heatmap,
# bookmap 3D -- alcanza y sobra; el diff-depth completo solo aporta
# algo si necesitás reconstruir el book ENTERO, que no es el caso.
#
# NOTA: el WebSocket relay NO pasa por _BAN_HASTA porque es un stream
# push persistente, no polling -- Binance no lo cuenta contra el
# weight de peticiones REST. Si en algún momento migrás klines/ticker
# a un stream también (kline@interval, por ejemplo), ahí sí conviene
# unificar el criterio.

_ULTIMO_BOOK = {
    "spot": None,
    "futures": None,
}
_LOCK = threading.Lock()

URLS_WS_BINANCE = {
    "spot": "wss://stream.binance.com:9443/ws/btcusdt@depth20@100ms",
    "futures": "wss://fstream.binance.com/ws/btcusdt@depth20@100ms",
}


def _normalizar_mensaje(data):
    """
    Normaliza el mensaje crudo de Binance (spot o futures, con
    nombres de clave levemente distintos) al mismo formato que ya
    devuelven /depth y /futures/depth: {"bids": [...], "asks": [...]}.
    """
    bids = data.get("bids") or data.get("b") or []
    asks = data.get("asks") or data.get("a") or []
    return {
        "bids": bids,
        "asks": asks,
        "ts": int(time.time() * 1000),
    }


def _hilo_binance_ws(mercado):
    """
    Mantiene la conexión persistente a Binance para un mercado dado.
    Si se corta (red, restart del lado de Binance, deploy de Render,
    etc.), espera 3s y reconecta sola -- el hilo nunca muere en
    silencio, así que /ws/depth siempre tiene la mejor data disponible
    apenas la conexión vuelve.
    """
    url = URLS_WS_BINANCE[mercado]

    def _on_message(ws_conn, mensaje):
        try:
            data = json.loads(mensaje)
        except Exception:
            return
        normalizado = _normalizar_mensaje(data)
        with _LOCK:
            _ULTIMO_BOOK[mercado] = normalizado

    while True:
        try:
            wsapp = websocket.WebSocketApp(url, on_message=_on_message)
            wsapp.run_forever(ping_interval=20, ping_timeout=10)
        except Exception:
            pass
        time.sleep(3)  # pausa antes de reconectar -- evita loop agresivo si Binance está caído


def _iniciar_hilos_binance():
    for mercado in URLS_WS_BINANCE:
        hilo = threading.Thread(target=_hilo_binance_ws, args=(mercado,), daemon=True)
        hilo.start()


_iniciar_hilos_binance()


@sock.route("/ws/depth")
def ws_depth(ws):
    """
    Endpoint WebSocket para clientes externos (frontend JS, bookmap
    3D, o cualquier tercero autorizado).

    Uso: wss://<tu-proxy>.onrender.com/ws/depth?market=spot
         wss://<tu-proxy>.onrender.com/ws/depth?market=futures

    Empuja el último estado conocido del book ~7 veces por segundo
    mientras el cliente esté conectado. No hace falta que el cliente
    pida nada ni reintente -- si Binance todavía no mandó el primer
    mensaje, manda null hasta que llegue (arranque en frío del proxy).
    """
    mercado = request.args.get("market", "spot")
    if mercado not in _ULTIMO_BOOK:
        mercado = "spot"

    try:
        while True:
            with _LOCK:
                estado = _ULTIMO_BOOK[mercado]
            ws.send(json.dumps(estado))
            time.sleep(0.15)
    except Exception:
        # El cliente se desconectó (cerró pestaña, perdió red, etc.)
        # -- no es un error del servidor, solo termina esta conexión.
        return


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
