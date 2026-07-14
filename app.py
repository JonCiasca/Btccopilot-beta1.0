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
MAX_PREDICCIONES_GUARDADAS = 12  # ~2.5 días de historial
RUTA_PREDICCIONES = "predicciones.json"

_PREDICCIONES = []
_PREDICCIONES_LOCK = threading.Lock()


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


def _generar_prediccion():
    """
    Arma UNA tesis de mercado completa. Devuelve None si falta algún
    dato crítico (velas o ban activo) -- mejor no emitir nada a emitir
    una tesis fabricada con datos incompletos.
    """
    if _grupo_baneado("spot") or _grupo_baneado("futures"):
        return None

    ahora = datetime.now(timezone.utc)

    velas, status = _proxy_get(
        DOMINIOS_SPOT, "/api/v3/klines",
        {"symbol": "BTCUSDT", "interval": "15m", "limit": 100}, grupo="spot",
    )
    if not isinstance(velas, list) or len(velas) < 30:
        return None

    precio_actual = float(velas[-1][4])
    soportes, resistencias = _swing_niveles(velas)
    tendencia = _tendencia_sma(velas)

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

    def _agregar_etapas(candidatos):
        vistos = set()
        resultado = []
        for nivel, fuente in sorted(candidatos, key=lambda c: c[0]):
            clave = round(nivel / 50)  # evita 2 niveles casi pegados (ej. wall y swing a $30 de distancia)
            if clave in vistos:
                continue
            vistos.add(clave)
            resultado.append({"nivel": round(nivel, 1), "fuente": fuente})
            if len(resultado) >= 3:
                break
        return resultado

    if sesgo == "alcista":
        candidatos = [(r, "Imán resistencia (liquidez)") for r in resistencias if r > precio_actual]
        if call_wall and call_wall["strike"] > precio_actual:
            candidatos.append((call_wall["strike"], "Call Wall (OI opciones)"))
        if flip is not None and flip > precio_actual:
            candidatos.append((flip, "Flip Gamma (régimen)"))
        etapas = _agregar_etapas(candidatos)
        if soportes:
            invalidacion = round(soportes[0], 1)
            invalidacion_fuente = "Imán soporte reciente"

    elif sesgo == "bajista":
        candidatos = [(r, "Imán soporte (liquidez)") for r in soportes if r < precio_actual]
        if put_wall and put_wall["strike"] < precio_actual:
            candidatos.append((put_wall["strike"], "Put Wall (OI opciones)"))
        if flip is not None and flip < precio_actual:
            candidatos.append((flip, "Flip Gamma (régimen)"))
        # ordenamos de más cercano a más lejano igual (candidatos ya vienen < precio_actual)
        candidatos.sort(key=lambda c: -c[0])
        etapas = _agregar_etapas([(-n, f) for n, f in candidatos])
        etapas = [{"nivel": -e["nivel"], "fuente": e["fuente"]} for e in etapas]
        if resistencias:
            invalidacion = round(resistencias[0], 1)
            invalidacion_fuente = "Imán resistencia reciente"

    else:
        if resistencias:
            etapas.append({"nivel": round(resistencias[0], 1), "fuente": "Imán resistencia (liquidez)"})
        if soportes:
            etapas.append({"nivel": round(soportes[0], 1), "fuente": "Imán soporte (liquidez)"})

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
        "resumen": resumen,
    }


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


def _hilo_generador_predicciones():
    """
    Corre indefinidamente: genera una tesis, la guarda (memoria + disco),
    duerme INTERVALO_PREDICCION_HORAS y repite. Igual criterio de
    resiliencia que el hilo de WebSocket de depth más abajo: si algo
    falla en un ciclo, no mata el hilo, solo lo salta.

    LÍMITE HONESTO (mismo que contador_sesiones.json en main.py): el
    disco de Render free tier no es 100% persistente a largo plazo
    (puede perderse en un redeploy) -- alcanza para sobrevivir
    reinicios normales del proceso dentro de la misma instancia, no
    para historial permanente.
    """
    global _PREDICCIONES
    with _PREDICCIONES_LOCK:
        _PREDICCIONES = _cargar_predicciones_disco()

    while True:
        try:
            pred = _generar_prediccion()
            if pred:
                with _PREDICCIONES_LOCK:
                    _PREDICCIONES.insert(0, pred)
                    _PREDICCIONES = _PREDICCIONES[:MAX_PREDICCIONES_GUARDADAS]
                    _guardar_predicciones_disco(_PREDICCIONES)
        except Exception as e:
            print(f"[predicciones] error generando tesis: {e}")

        time.sleep(INTERVALO_PREDICCION_HORAS * 3600)


threading.Thread(target=_hilo_generador_predicciones, daemon=True).start()


@app.route("/predicciones")
def predicciones():
    with _PREDICCIONES_LOCK:
        lista = list(_PREDICCIONES)
    return jsonify({
        "predicciones": lista,
        "intervalo_horas": INTERVALO_PREDICCION_HORAS,
    })


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
