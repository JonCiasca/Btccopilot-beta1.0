"""
ws_hub.py — Hub de datos en tiempo real por WebSocket para BTC Copilot.

Qué hace
--------
En vez de que cada pedido del dashboard dispare una consulta REST a
Binance (con su límite de peso y riesgo de ban -1003), este módulo
mantiene UNA sola conexión WebSocket abierta contra Binance y va
actualizando una cache en memoria. Los endpoints del proxy (app.py en
Render) leen de esa cache y responden al instante, sin tocar Binance.

Costo de mantenimiento: cero extra. Corre en el mismo proceso del
proxy de Render, no necesita otro servicio ni base de datos.

Fuentes:
  - Spot  : wss://data-stream.binance.vision  (ticker 24h + klines)
            Es el dominio de SOLO datos de mercado de Binance: no
            requiere API key y no computa peso de API.
  - Futures: wss://fstream.binance.com  (markPrice -> funding rate)
  - Open Interest (Binance y Bybit): no existe stream WS público, se
    mantiene un poll REST suave (cada POLL_OI_SEGUNDOS) con cache.

Uso desde app.py (Flask):
    import ws_hub
    ws_hub.iniciar()            # una sola vez, al arrancar el proceso

    @app.route("/ticker24hr")
    def ticker24hr():
        d = ws_hub.get_ticker24hr()
        if d is not None:
            return jsonify(d)
        ...  # fallback REST actual (cache fría / recién arrancado)

    @app.route("/klines")
    def klines():
        filas = ws_hub.get_klines(request.args.get("interval", "15m"),
                                  int(request.args.get("limit", 100)))
        if filas is not None:
            return jsonify(filas)
        ...  # fallback REST actual

El formato devuelto es EXACTAMENTE el de la API REST de Binance
(mismos nombres de campos, misma lista de 12 columnas por vela), así
que main.py (Streamlit) no necesita NINGÚN cambio.
"""

import json
import os
import threading
import time
import traceback
from collections import deque

import requests

try:
    import websocket  # websocket-client
except ImportError as _e:  # mensaje claro si falta la dependencia
    raise ImportError(
        "Falta la librería 'websocket-client'. Agregar a requirements.txt: "
        "websocket-client==1.8.0"
    ) from _e

# ----------------------------------
# CONFIGURACIÓN
# ----------------------------------

SYMBOL = "btcusdt"

# Intervalos de velas que usa el dashboard (main.py). Si algún día se
# agrega una temporalidad nueva en el frontend, sumarla acá.
#
# FIX: faltaba "3m" -- se agregó en main.py para "Confluencia by
# JonFlowMDQ" (Setup 1: MTF 3m/5m/15m) pero nunca se sumó acá. Como
# "3m" no estaba en esta lista, el hub nunca lo seedeaba ni lo
# streameaba: get_klines("3m", ...) devolvía None SIEMPRE, y el
# endpoint /klines caía al fallback REST en TODOS los refreshes (cada
# 8-10s, indefinidamente) para ese timeframe puntual -- ese REST sin
# cache es lo que dispara los bans -1003 de peso, incluso con el
# dashboard casi sin uso, mientras la sesión (o una pestaña vieja
# olvidada) siga con el autorefresh corriendo de fondo.
INTERVALOS = ["1m", "3m", "5m", "15m", "1h", "4h", "1d"]

MAX_VELAS = 500          # velas guardadas por intervalo (el front pide <= 100)
POLL_OI_SEGUNDOS = 30    # poll suave de Open Interest (REST, cacheado)
WATCHDOG_SEGUNDOS = 60   # si no llega ningún mensaje en este tiempo, reconectar
SEED_LIMIT = 300         # velas históricas que se cargan al arrancar (REST, 1 vez)

SPOT_WS = "wss://data-stream.binance.vision/stream"
# Futures en formato de stream CRUDO (/ws/<stream>), NO combinado
# (/stream?streams=...): episodio real en Render -- el combinado
# conectaba pero no entregaba ni un mensaje (watchdog timeout cada
# 60s, funding siempre null), mientras que el relay del bookmap, que
# usa /ws/ crudo contra el mismo host, funcionaba perfecto. Mismo
# formato que el relay, entonces.
FUT_WS_RAW = f"wss://fstream.binance.com/ws/{SYMBOL}@markPrice@1s"

# Dominios REST para el seed inicial y el poll de OI (mismo criterio
# que ya usa app.py: probar varios por si alguno bloquea la IP).
SPOT_REST = ["https://data-api.binance.vision", "https://api.binance.com",
             "https://api1.binance.com", "https://api2.binance.com"]
FUT_REST = ["https://fapi.binance.com"]
BYBIT_REST = ["https://api.bybit.com"]
OKX_REST = ["https://www.okx.com"]

# ----------------------------------
# ESTADO (cache en memoria)
# ----------------------------------

_lock = threading.Lock()

_estado = {
    "ticker": None,            # dict estilo /api/v3/ticker/24hr
    "ticker_ts": 0,
    "funding": None,           # dict estilo /fapi/v1/premiumIndex
    "funding_ts": 0,
    "oi_binance": None,        # dict estilo /fapi/v1/openInterest
    "oi_binance_ts": 0,
    "oi_bybit": None,          # dict crudo de Bybit (retCode/result/list)
    "oi_bybit_ts": 0,
    "oi_okx": None,            # dict crudo de OKX (code/data)
    "oi_okx_ts": 0,
    "funding_bybit": None,     # funding de Bybit (respaldo de Binance)
    "funding_bybit_ts": 0,
    "klines": {},              # interval -> deque de listas de 12 campos
    "klines_seed_ok": {},      # interval -> bool
    "ws_spot_ok": False,
    "ws_fut_ok": False,
    "ultimo_msg_spot": 0,
    "ultimo_msg_fut": 0,
    "reconexiones_spot": 0,
    "reconexiones_fut": 0,
    "ultimo_error": None,
}

for _iv in INTERVALOS:
    _estado["klines"][_iv] = deque(maxlen=MAX_VELAS)
    _estado["klines_seed_ok"][_iv] = False

# PID del proceso donde efectivamente arrancaron los hilos. Se guarda
# el PID (y no un simple bool) por gunicorn con --preload / fork: el
# master importa el módulo (hilos arrancan en el master), después
# forkea los workers -- y LOS HILOS NO SOBREVIVEN AL FORK. El worker
# hereda el estado congelado (todo en cero) y un bool diría "ya
# arranqué" siendo mentira. Comparando el PID, cada worker detecta que
# él mismo nunca arrancó los hilos y los levanta de nuevo (ver el
# before_request en app.py).
_pid_arranque = None


# ----------------------------------
# HELPERS REST (seed + fallback)
# ----------------------------------

def _get_rest(dominios, path, params, timeout=10):
    """Prueba el mismo path en varios dominios y devuelve el primer JSON
    con status 200. Si ninguno responde 200, devuelve el último body
    JSON que se haya podido parsear (p. ej. el error -1003 de Binance,
    que viene con status 418/429) -- así el caller puede detectar y
    registrar un ban en vez de perder esa información."""
    ultimo_error = None
    ultimo_body = None
    for base in dominios:
        try:
            r = requests.get(base + path, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json(), None
            try:
                ultimo_body = r.json()
            except ValueError:
                pass
            ultimo_error = f"HTTP {r.status_code} en {base}"
        except Exception as e:
            ultimo_error = f"{base}: {e}"
    return ultimo_body, ultimo_error


def _seed_klines():
    """Carga histórica inicial de velas por REST, UNA vez por intervalo.

    Después de esto, las velas se mantienen al día solo con el stream:
    Binance no vuelve a recibir pedidos de velas nunca más mientras el
    proceso viva. Si algún intervalo falla (ban activo al arrancar,
    timeout en frío de Render, etc.), se reintenta suave cada 60s
    hasta completarlo -- mientras tanto get_klines devuelve None y el
    endpoint usa su fallback REST de siempre.

    FIX: esta función NO respetaba el circuit breaker de ban (a
    diferencia de _loop_oi, que sí chequea _BAN_CHECK). Con un ban
    "spot" activo al arrancar el proceso (típico después de un
    redeploy: Render reinicia, el hub arranca de cero y tiene que
    re-sembrar los 7 intervalos), este loop igual salía a pedir REST
    cada 60s SIN mirar si ya estábamos baneados -- y según el propio
    comentario de _refrescar_ban_estado_desde_disco más abajo en este
    archivo, insistir contra Binance durante un ban activo es
    justamente lo que lo extiende (un ban de ~15min se había alargado
    a ~2hs por esto mismo, en el poll de OI, antes de agregarle el
    chequeo ahí). Acá le faltaba el mismo chequeo. Ahora: si "spot"
    está baneado, no pide nada y espera a que se cumpla el ban antes
    de reintentar.
    """
    while True:
        pendientes = [iv for iv in INTERVALOS if not _estado["klines_seed_ok"][iv]]
        if not pendientes:
            return

        if _BAN_CHECK and _BAN_CHECK("spot"):
            # No insistir mientras el ban siga vigente -- cada intento
            # durante un ban puede extenderlo en vez de dejarlo enfriar.
            time.sleep(15)
            continue

        for iv in pendientes:
            datos, err = _get_rest(
                SPOT_REST, "/api/v3/klines",
                {"symbol": SYMBOL.upper(), "interval": iv, "limit": SEED_LIMIT},
            )
            if isinstance(datos, list) and datos:
                with _lock:
                    dq = _estado["klines"][iv]
                    dq.clear()
                    dq.extend(datos)
                    _estado["klines_seed_ok"][iv] = True
            else:
                with _lock:
                    _estado["ultimo_error"] = f"seed klines {iv}: {err}"
                if isinstance(datos, dict) and _BAN_REGISTRAR:
                    # Si esto fue un -1003, registrarlo en el circuit
                    # breaker para que TODO el proxy (y este mismo loop,
                    # en la próxima vuelta) lo respete de inmediato.
                    _BAN_REGISTRAR("spot", datos)
                if _BAN_CHECK and _BAN_CHECK("spot"):
                    # Ya sabemos que estamos baneados -- cortar acá esta
                    # vuelta en vez de seguir probando los intervalos
                    # pendientes que quedan (todos van a fallar igual).
                    break
        time.sleep(60)


# ----------------------------------
# PROCESAMIENTO DE MENSAJES WS
# ----------------------------------

def _procesar_ticker(d):
    """Mapea el evento @ticker del WS al formato del endpoint REST
    /api/v3/ticker/24hr, para que main.py no note la diferencia."""
    t = {
        "symbol": d["s"],
        "priceChange": d["p"],
        "priceChangePercent": d["P"],
        "weightedAvgPrice": d["w"],
        "prevClosePrice": d["x"],
        "lastPrice": d["c"],
        "lastQty": d["Q"],
        "bidPrice": d["b"],
        "bidQty": d["B"],
        "askPrice": d["a"],
        "askQty": d["A"],
        "openPrice": d["o"],
        "highPrice": d["h"],
        "lowPrice": d["l"],
        "volume": d["v"],
        "quoteVolume": d["q"],
        "openTime": d["O"],
        "closeTime": d["C"],
        "firstId": d["F"],
        "lastId": d["L"],
        "count": d["n"],
    }
    with _lock:
        _estado["ticker"] = t
        _estado["ticker_ts"] = time.time()


def _procesar_kline(d):
    """Actualiza (o agrega) la última vela del intervalo correspondiente.

    El evento k del WS trae los mismos 12 campos que una fila de
    /api/v3/klines, así que armamos la fila idéntica.
    """
    k = d["k"]
    iv = k["i"]
    if iv not in _estado["klines"]:
        return
    fila = [
        k["t"], k["o"], k["h"], k["l"], k["c"], k["v"],
        k["T"], k["q"], k["n"], k["V"], k["Q"], k["B"],
    ]
    with _lock:
        dq = _estado["klines"][iv]
        if dq and dq[-1][0] == fila[0]:
            dq[-1] = fila          # misma vela: se actualiza en vivo
        elif not dq or fila[0] > dq[-1][0]:
            dq.append(fila)        # vela nueva: se agrega
        # (si llegara una vela vieja fuera de orden, se ignora)


def _procesar_mark_price(d):
    """Mapea el evento markPrice del WS de futuros al formato del
    endpoint REST /fapi/v1/premiumIndex (para el funding rate)."""
    f = {
        "symbol": d["s"],
        "markPrice": d["p"],
        "indexPrice": d["i"],
        "estimatedSettlePrice": d.get("P", ""),
        "lastFundingRate": d["r"],
        "nextFundingTime": d["T"],
        "interestRate": d.get("R", ""),
        "time": d["E"],
    }
    with _lock:
        _estado["funding"] = f
        _estado["funding_ts"] = time.time()


# ----------------------------------
# LOOPS DE CONEXIÓN (con reconexión automática)
# ----------------------------------

def _loop_ws(nombre, full_url, on_data, clave_ok, clave_ts, clave_reconn):
    """Loop genérico: conecta, escucha, y si algo se corta espera y
    reconecta con backoff. Nunca lanza excepción hacia afuera.
    full_url ya viene armada (combinada para spot, cruda para futures)."""
    backoff = 1
    while True:
        try:
            ws = websocket.create_connection(
                full_url, timeout=WATCHDOG_SEGUNDOS, enable_multithread=True
            )
            with _lock:
                _estado[clave_ok] = True
                _estado[clave_ts] = time.time()
            backoff = 1
            while True:
                msg = ws.recv()  # timeout actúa de watchdog
                if not msg:
                    raise ConnectionError("mensaje vacío")
                with _lock:
                    _estado[clave_ts] = time.time()
                data = json.loads(msg)
                d = data.get("data", data)
                on_data(d)
        except Exception as e:
            with _lock:
                _estado[clave_ok] = False
                _estado[clave_reconn] += 1
                _estado["ultimo_error"] = f"{nombre}: {e}"
            try:
                ws.close()
            except Exception:
                pass
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)  # 1,2,4,...,60s máx.


def _on_spot(d):
    e = d.get("e")
    if e == "24hrTicker":
        _procesar_ticker(d)
    elif e == "kline":
        _procesar_kline(d)


def _on_fut(d):
    if d.get("e") == "markPriceUpdate":
        _procesar_mark_price(d)


_POLL_OI_BINANCE = True  # se define en iniciar() -- respeta el
                         # interruptor BINANCE_FUNDING_OI_ACTIVO de app.py
_BAN_CHECK = None      # callable(grupo)->bool: el _grupo_baneado de app.py
_BAN_REGISTRAR = None  # callable(grupo, body): el _registrar_si_es_ban de app.py


def _loop_oi():
    """Poll suave de Open Interest (no hay stream WS público para OI).

    2 pedidos livianos cada POLL_OI_SEGUNDOS en total, contra los ~8
    por minuto que generaba el dashboard refrescando. Con cache: si
    Binance/Bybit no responden, se sirve el último valor conocido.
    Si el OI de Binance está dado de baja (interruptor anti-ban de
    app.py), solo se pollea Bybit.
    """
    while True:
        # Binance Futures OI (solo si el interruptor lo permite Y no hay
        # un ban -1003 vigente -- respeta el circuit breaker de app.py:
        # pegarle a Binance durante un ban lo puede EXTENDER).
        if _POLL_OI_BINANCE and not (_BAN_CHECK and _BAN_CHECK("futures")):
            datos, err = _get_rest(FUT_REST, "/fapi/v1/openInterest",
                                   {"symbol": SYMBOL.upper()}, timeout=8)
            if isinstance(datos, dict) and "openInterest" in datos:
                with _lock:
                    _estado["oi_binance"] = datos
                    _estado["oi_binance_ts"] = time.time()
            elif isinstance(datos, dict) and _BAN_REGISTRAR:
                # si Binance devolvió -1003, registrarlo en el circuit
                # breaker de app.py para que TODO el proxy lo respete
                _BAN_REGISTRAR("futures", datos)

        # Funding por poll REST (respaldo del stream): episodio real en
        # Render -- el WS de futures conectaba pero no entregaba ni un
        # mensaje (en formato combinado Y crudo), con lo cual el
        # funding quedaba en null para siempre. El poll lo cubre; si el
        # stream algún día entrega, su dato (1 por segundo) es más
        # fresco y este poll no lo pisa (chequeo de edad de 5s).
        if _POLL_OI_BINANCE and not (_BAN_CHECK and _BAN_CHECK("futures")):
            datos, err = _get_rest(FUT_REST, "/fapi/v1/premiumIndex",
                                   {"symbol": SYMBOL.upper()}, timeout=8)
            if isinstance(datos, dict) and "lastFundingRate" in datos:
                with _lock:
                    if time.time() - _estado["funding_ts"] > 5:
                        _estado["funding"] = datos
                        _estado["funding_ts"] = time.time()
            elif isinstance(datos, dict) and _BAN_REGISTRAR:
                _BAN_REGISTRAR("futures", datos)
        # Bybit OI
        datos, err = _get_rest(
            BYBIT_REST, "/v5/market/open-interest",
            {"category": "linear", "symbol": SYMBOL.upper(),
             "intervalTime": "5min", "limit": 1},
            timeout=8,
        )
        if isinstance(datos, dict) and datos.get("retCode") == 0:
            with _lock:
                _estado["oi_bybit"] = datos
                _estado["oi_bybit_ts"] = time.time()
        # Funding de Bybit (perpetuo lineal) -- RESPALDO del funding de
        # Binance: Bybit no banea la IP de Render, así que este dato
        # está SIEMPRE, incluso con Binance en -1003. El funding de los
        # perpetuos BTC trackea muy parecido entre exchanges, sirve
        # perfectamente para leer el sesgo de posicionamiento.
        datos, err = _get_rest(
            BYBIT_REST, "/v5/market/tickers",
            {"category": "linear", "symbol": SYMBOL.upper()},
            timeout=8,
        )
        if isinstance(datos, dict) and datos.get("retCode") == 0:
            try:
                fila = datos["result"]["list"][0]
                if fila.get("fundingRate") not in (None, ""):
                    with _lock:
                        _estado["funding_bybit"] = fila
                        _estado["funding_bybit_ts"] = time.time()
            except (KeyError, IndexError, TypeError):
                pass

        # OKX OI (perpetuo BTC-USDT-SWAP) -- gratis, sin API key, y OKX
        # no bloquea la IP de Render. Tercera fuente para el agregado.
        datos, err = _get_rest(
            OKX_REST, "/api/v5/public/open-interest",
            {"instType": "SWAP", "instId": "BTC-USDT-SWAP"},
            timeout=8,
        )
        if isinstance(datos, dict) and datos.get("code") == "0":
            with _lock:
                _estado["oi_okx"] = datos
                _estado["oi_okx_ts"] = time.time()
        time.sleep(POLL_OI_SEGUNDOS)


# ----------------------------------
# API PÚBLICA (lo que usa app.py)
# ----------------------------------

def iniciar(poll_oi_binance=True, ban_check=None, ban_registrar=None):
    """Arranca los hilos del hub. Idempotente POR PROCESO: llamadas
    repetidas en el mismo proceso no duplican hilos, pero un proceso
    forkeado (gunicorn --preload) detecta por PID que él no los tiene
    y los arranca de nuevo. Por eso app.py la llama también desde un
    before_request: el primer request de cada worker garantiza el hub
    vivo en ESE worker.

    poll_oi_binance: pasarle BINANCE_FUNDING_OI_ACTIVO desde app.py.
    En False, el hub NO le pide Open Interest a Binance (el stream de
    funding por WS sigue igual: es push, no suma peso REST ni riesgo
    de ban -1003).
    ban_check / ban_registrar: los hooks del circuit breaker de app.py
    (_grupo_baneado / _registrar_si_es_ban) para que el poll de OI lo
    respete y lo alimente."""
    global _pid_arranque, _POLL_OI_BINANCE, _BAN_CHECK, _BAN_REGISTRAR
    if _pid_arranque == os.getpid():
        return
    _pid_arranque = os.getpid()
    _POLL_OI_BINANCE = bool(poll_oi_binance)
    _BAN_CHECK = ban_check
    _BAN_REGISTRAR = ban_registrar

    # Seed histórico en un hilo para no frenar el arranque del server
    threading.Thread(target=_seed_klines, daemon=True).start()

    streams_spot = [f"{SYMBOL}@ticker"] + [f"{SYMBOL}@kline_{iv}" for iv in INTERVALOS]
    url_spot = SPOT_WS + "?streams=" + "/".join(streams_spot)
    threading.Thread(
        target=_loop_ws,
        args=("ws_spot", url_spot, _on_spot,
              "ws_spot_ok", "ultimo_msg_spot", "reconexiones_spot"),
        daemon=True,
    ).start()

    threading.Thread(
        target=_loop_ws,
        args=("ws_fut", FUT_WS_RAW, _on_fut,
              "ws_fut_ok", "ultimo_msg_fut", "reconexiones_fut"),
        daemon=True,
    ).start()

    threading.Thread(target=_loop_oi, daemon=True).start()


def _fresco(ts, maximo):
    return ts > 0 and (time.time() - ts) <= maximo


def get_ticker24hr(max_edad=30):
    """Ticker 24h desde cache. None si la cache está fría (usar fallback REST)."""
    with _lock:
        if _estado["ticker"] and _fresco(_estado["ticker_ts"], max_edad):
            return dict(_estado["ticker"])
    return None


def get_klines(intervalo, limite=100):
    """Velas desde cache, formato idéntico a /api/v3/klines.
    None si el intervalo no está seedeado todavía (usar fallback REST)."""
    with _lock:
        if not _estado["klines_seed_ok"].get(intervalo):
            return None
        dq = _estado["klines"][intervalo]
        if not dq:
            return None
        return [list(f) for f in list(dq)[-limite:]]


def get_premium_index(max_edad=600):
    """Funding rate (premiumIndex) desde cache. None si está fría.

    max_edad=600 (10 min) a propósito: el funding se recalcula lento y
    casi no cambia minuto a minuto -- y el poll que lo alimenta se
    PAUSA durante un ban -1003 (circuit breaker). Con 60s de tolerancia
    (valor anterior), un ban de 15 min dejaba el funding 'frío' y
    volteaba el endpoint y la generación de tesis sin necesidad: el
    último valor conocido de hace unos minutos sigue siendo
    operativamente válido.

    MULTI-EXCHANGE: si el dato de Binance no está (ban largo, stream
    mudo), se sirve el funding de BYBIT mapeado al mismo formato --
    los perpetuos BTC trackean funding muy parecido entre exchanges,
    y con esto el dashboard y las tesis nunca se quedan sin el dato.
    El campo extra "fuente" dice de dónde salió."""
    with _lock:
        if _estado["funding"] and _fresco(_estado["funding_ts"], max_edad):
            d = dict(_estado["funding"])
            d.setdefault("fuente", "binance")
            return d
        fb = _estado["funding_bybit"]
        if fb and _fresco(_estado["funding_bybit_ts"], max_edad):
            return {
                "symbol": "BTCUSDT",
                "markPrice": fb.get("markPrice", ""),
                "indexPrice": fb.get("indexPrice", ""),
                "lastFundingRate": fb["fundingRate"],
                "nextFundingTime": int(fb.get("nextFundingTime") or 0),
                "time": int(time.time() * 1000),
                "fuente": "bybit",
            }
    return None


def get_open_interest(max_edad=300):
    """OI desde cache. None si está fría.
    Tolerancia de 5 min: el poll se pausa durante bans (ver
    get_premium_index) y un OI de hace unos minutos sigue siendo
    mejor que un panel caído.

    MULTI-EXCHANGE: si el OI de Binance no está (ban largo), se sirve
    el de BYBIT mapeado al mismo formato, con "fuente": "bybit".
    OJO: los valores absolutos difieren entre exchanges (Binance ~85k
    BTC, Bybit ~50k), así que al cambiar de fuente el 'Cambio OI' del
    dashboard puede mostrar UN salto puntual -- pasa solo al entrar o
    salir de un ban, y es preferible a mostrar N/D por horas."""
    with _lock:
        if _estado["oi_binance"] and _fresco(_estado["oi_binance_ts"], max_edad):
            d = dict(_estado["oi_binance"])
            d.setdefault("fuente", "binance")
            return d
        ob = _estado["oi_bybit"]
        if ob and _fresco(_estado["oi_bybit_ts"], max_edad):
            try:
                valor = ob["result"]["list"][0]["openInterest"]
                return {
                    "symbol": "BTCUSDT",
                    "openInterest": valor,
                    "time": int(time.time() * 1000),
                    "fuente": "bybit",
                }
            except (KeyError, IndexError, TypeError):
                pass
    return None


def get_bybit_open_interest(max_edad=300):
    """OI de Bybit (respuesta cruda v5) desde cache. None si está fría."""
    with _lock:
        if _estado["oi_bybit"] and _fresco(_estado["oi_bybit_ts"], max_edad):
            return dict(_estado["oi_bybit"])
    return None


def get_okx_open_interest(max_edad=300):
    """OI de OKX (respuesta cruda v5) desde cache. None si está fría."""
    with _lock:
        if _estado["oi_okx"] and _fresco(_estado["oi_okx_ts"], max_edad):
            return dict(_estado["oi_okx"])
    return None


def get_oi_agregado():
    """Open Interest combinado de las 3 fuentes, normalizado a BTC.

    Devuelve dict con el detalle por exchange (None donde falte dato)
    y el total de las fuentes disponibles. Pensado para el endpoint
    /oi/agregado del proxy: una sola vista "veraz" del OI, sin
    depender de un solo exchange ni de servicios pagos tipo Coinglass.
    """
    fuentes = {}

    d = get_open_interest()
    fuentes["binance"] = float(d["openInterest"]) if d and "openInterest" in d else None

    d = get_bybit_open_interest()
    try:
        fuentes["bybit"] = float(d["result"]["list"][0]["openInterest"]) if d else None
    except (KeyError, IndexError, TypeError, ValueError):
        fuentes["bybit"] = None

    d = get_okx_open_interest()
    try:
        # OKX: "oiCcy" ya viene expresado en moneda base (BTC)
        fuentes["okx"] = float(d["data"][0]["oiCcy"]) if d else None
    except (KeyError, IndexError, TypeError, ValueError):
        fuentes["okx"] = None

    disponibles = {k: v for k, v in fuentes.items() if v is not None}
    return {
        "unidad": "BTC",
        "por_exchange": fuentes,
        "total_btc": round(sum(disponibles.values()), 1) if disponibles else None,
        "fuentes_activas": sorted(disponibles.keys()),
    }


def estado():
    """Diagnóstico del hub, para un endpoint /ws-status en el proxy."""
    with _lock:
        return {
            "ws_spot_conectado": _estado["ws_spot_ok"],
            "ws_futures_conectado": _estado["ws_fut_ok"],
            "segundos_sin_msg_spot": round(time.time() - _estado["ultimo_msg_spot"], 1)
            if _estado["ultimo_msg_spot"] else None,
            "segundos_sin_msg_futures": round(time.time() - _estado["ultimo_msg_fut"], 1)
            if _estado["ultimo_msg_fut"] else None,
            "reconexiones_spot": _estado["reconexiones_spot"],
            "reconexiones_futures": _estado["reconexiones_fut"],
            "klines_seed": dict(_estado["klines_seed_ok"]),
            "velas_en_cache": {iv: len(_estado["klines"][iv]) for iv in INTERVALOS},
            "ticker_edad_seg": round(time.time() - _estado["ticker_ts"], 1)
            if _estado["ticker_ts"] else None,
            "funding_edad_seg": round(time.time() - _estado["funding_ts"], 1)
            if _estado["funding_ts"] else None,
            "oi_binance_edad_seg": round(time.time() - _estado["oi_binance_ts"], 1)
            if _estado["oi_binance_ts"] else None,
            "oi_bybit_edad_seg": round(time.time() - _estado["oi_bybit_ts"], 1)
            if _estado["oi_bybit_ts"] else None,
            "oi_okx_edad_seg": round(time.time() - _estado["oi_okx_ts"], 1)
            if _estado["oi_okx_ts"] else None,
            "funding_bybit_edad_seg": round(time.time() - _estado["funding_bybit_ts"], 1)
            if _estado["funding_bybit_ts"] else None,
            "ban_futures_activo_para_polls": bool(_BAN_CHECK and _BAN_CHECK("futures")),
            "ultimo_error": _estado["ultimo_error"],
        }
