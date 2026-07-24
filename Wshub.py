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
INTERVALOS = ["1m", "5m", "15m", "1h", "4h", "1d"]

MAX_VELAS = 500          # velas guardadas por intervalo (el front pide <= 100)
POLL_OI_SEGUNDOS = 30    # poll suave de Open Interest (REST, cacheado)
WATCHDOG_SEGUNDOS = 60   # si no llega ningún mensaje en este tiempo, reconectar
SEED_LIMIT = 300         # velas históricas que se cargan al arrancar (REST, 1 vez)

SPOT_WS = "wss://data-stream.binance.vision/stream"
FUT_WS = "wss://fstream.binance.com/stream"

# Dominios REST para el seed inicial y el poll de OI (mismo criterio
# que ya usa app.py: probar varios por si alguno bloquea la IP).
SPOT_REST = ["https://data-api.binance.vision", "https://api.binance.com",
             "https://api1.binance.com", "https://api2.binance.com"]
FUT_REST = ["https://fapi.binance.com"]
BYBIT_REST = ["https://api.bybit.com"]

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

_arrancado = False


# ----------------------------------
# HELPERS REST (seed + fallback)
# ----------------------------------

def _get_rest(dominios, path, params, timeout=10):
    """Prueba el mismo path en varios dominios y devuelve el primer JSON válido."""
    ultimo_error = None
    for base in dominios:
        try:
            r = requests.get(base + path, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json(), None
            ultimo_error = f"HTTP {r.status_code} en {base}"
        except Exception as e:
            ultimo_error = f"{base}: {e}"
    return None, ultimo_error


def _seed_klines():
    """Carga histórica inicial de velas por REST, UNA vez por intervalo.

    Después de esto, las velas se mantienen al día solo con el stream:
    Binance no vuelve a recibir pedidos de velas nunca más mientras el
    proceso viva. Si algún intervalo falla (ban activo al arrancar,
    timeout en frío de Render, etc.), se reintenta suave cada 60s
    hasta completarlo -- mientras tanto get_klines devuelve None y el
    endpoint usa su fallback REST de siempre.
    """
    while True:
        pendientes = [iv for iv in INTERVALOS if not _estado["klines_seed_ok"][iv]]
        if not pendientes:
            return
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

def _loop_ws(nombre, url, streams, on_data, clave_ok, clave_ts, clave_reconn):
    """Loop genérico: conecta, escucha, y si algo se corta espera y
    reconecta con backoff. Nunca lanza excepción hacia afuera."""
    backoff = 1
    while True:
        try:
            full = url + "?streams=" + "/".join(streams)
            ws = websocket.create_connection(
                full, timeout=WATCHDOG_SEGUNDOS, enable_multithread=True
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


def _loop_oi():
    """Poll suave de Open Interest (no hay stream WS público para OI).

    2 pedidos livianos cada POLL_OI_SEGUNDOS en total, contra los ~8
    por minuto que generaba el dashboard refrescando. Con cache: si
    Binance/Bybit no responden, se sirve el último valor conocido.
    Si el OI de Binance está dado de baja (interruptor anti-ban de
    app.py), solo se pollea Bybit.
    """
    while True:
        # Binance Futures OI (solo si el interruptor lo permite)
        if _POLL_OI_BINANCE:
            datos, err = _get_rest(FUT_REST, "/fapi/v1/openInterest",
                                   {"symbol": SYMBOL.upper()}, timeout=8)
            if isinstance(datos, dict) and "openInterest" in datos:
                with _lock:
                    _estado["oi_binance"] = datos
                    _estado["oi_binance_ts"] = time.time()
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
        time.sleep(POLL_OI_SEGUNDOS)


# ----------------------------------
# API PÚBLICA (lo que usa app.py)
# ----------------------------------

def iniciar(poll_oi_binance=True):
    """Arranca los hilos del hub. Llamar UNA vez al inicio del proceso.
    Es idempotente: llamadas repetidas no crean hilos duplicados.

    poll_oi_binance: pasarle BINANCE_FUNDING_OI_ACTIVO desde app.py.
    En False, el hub NO le pide Open Interest a Binance (el stream de
    funding por WS sigue igual: es push, no suma peso REST ni riesgo
    de ban -1003)."""
    global _arrancado, _POLL_OI_BINANCE
    if _arrancado:
        return
    _arrancado = True
    _POLL_OI_BINANCE = bool(poll_oi_binance)

    # Seed histórico en un hilo para no frenar el arranque del server
    threading.Thread(target=_seed_klines, daemon=True).start()

    streams_spot = [f"{SYMBOL}@ticker"] + [f"{SYMBOL}@kline_{iv}" for iv in INTERVALOS]
    threading.Thread(
        target=_loop_ws,
        args=("ws_spot", SPOT_WS, streams_spot, _on_spot,
              "ws_spot_ok", "ultimo_msg_spot", "reconexiones_spot"),
        daemon=True,
    ).start()

    threams_fut = [f"{SYMBOL}@markPrice@1s"]
    threading.Thread(
        target=_loop_ws,
        args=("ws_fut", FUT_WS, threams_fut, _on_fut,
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


def get_premium_index(max_edad=60):
    """Funding rate (premiumIndex) desde cache. None si está fría."""
    with _lock:
        if _estado["funding"] and _fresco(_estado["funding_ts"], max_edad):
            return dict(_estado["funding"])
    return None


def get_open_interest(max_edad=120):
    """OI de Binance Futures desde cache. None si está fría."""
    with _lock:
        if _estado["oi_binance"] and _fresco(_estado["oi_binance_ts"], max_edad):
            return dict(_estado["oi_binance"])
    return None


def get_bybit_open_interest(max_edad=120):
    """OI de Bybit (respuesta cruda v5) desde cache. None si está fría."""
    with _lock:
        if _estado["oi_bybit"] and _fresco(_estado["oi_bybit_ts"], max_edad):
            return dict(_estado["oi_bybit"])
    return None


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
            "ultimo_error": _estado["ultimo_error"],
        }
