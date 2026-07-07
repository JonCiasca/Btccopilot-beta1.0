from flask import Flask, request, jsonify
from flask_sock import Sock
import requests
import time
import threading
import json
import websocket  # librería websocket-client

app = Flask(__name__)
sock = Sock(app)

# ----------------------------------
# CACHE TTL CORTO (solo endpoints de profundidad REST)
# ----------------------------------
# Mitigación directa contra los bans -1003 de Binance: si dos sesiones
# (dos pestañas, o el ciclo de refresh de la app pisándose con una
# petición manual) piden /depth casi al mismo tiempo, la segunda
# reusa la respuesta cacheada en vez de generar un pedido nuevo a
# Binance. TTL corto a propósito (3s): no vuelve el dato viejo, solo
# absorbe ráfagas.
_CACHE_DEPTH = {}
CACHE_TTL_SEGUNDOS = 3


def _get_con_cache(cache_key, fetch_fn):
    ahora = time.time()
    entrada = _CACHE_DEPTH.get(cache_key)
    if entrada and (ahora - entrada[0]) < CACHE_TTL_SEGUNDOS:
        return entrada[1], entrada[2]
    body, status = fetch_fn()
    _CACHE_DEPTH[cache_key] = (ahora, body, status)
    return body, status

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


def _proxy_get(dominios, path, params):
    """Prueba cada dominio hasta que uno responda OK. Devuelve
    (json_body, status_code)."""
    ultimo_error = "sin detalle"
    for dominio in dominios:
        url = f"{dominio}{path}"
        try:
            r = requests.get(url, params=params, timeout=8)
            return r.json(), r.status_code
        except Exception as e:
            ultimo_error = str(e)
            continue
    return {"error": f"Todos los dominios fallaron: {ultimo_error}"}, 502


@app.route("/ticker24hr")
def ticker24hr():
    symbol = request.args.get("symbol", "BTCUSDT")
    body, status = _proxy_get(DOMINIOS_SPOT, "/api/v3/ticker/24hr", {"symbol": symbol})
    return jsonify(body), status


@app.route("/klines")
def klines():
    params = {
        "symbol": request.args.get("symbol", "BTCUSDT"),
        "interval": request.args.get("interval", "5m"),
        "limit": request.args.get("limit", "100"),
    }
    body, status = _proxy_get(DOMINIOS_SPOT, "/api/v3/klines", params)
    return jsonify(body), status


@app.route("/depth")
def depth():
    """
    Order book SPOT vía REST (snapshot, no vivo). Se mantiene igual
    que antes -- quien ya lo usa (ej. Streamlit con refresh de 15s)
    sigue funcionando sin cambios. Para book EN VIVO real, ver
    /ws/depth más abajo.
    """
    symbol = request.args.get("symbol", "BTCUSDT")
    limit = min(int(request.args.get("limit", "20")), 50)
    cache_key = f"spot:{symbol}:{limit}"

    body, status = _get_con_cache(
        cache_key,
        lambda: _proxy_get(DOMINIOS_SPOT, "/api/v3/depth", {"symbol": symbol, "limit": limit}),
    )
    return jsonify(body), status


@app.route("/premiumIndex")
def premium_index():
    symbol = request.args.get("symbol", "BTCUSDT")
    try:
        r = requests.get(
            f"{DOMINIO_FUTURES}/fapi/v1/premiumIndex",
            params={"symbol": symbol},
            timeout=8,
        )
        return jsonify(r.json()), r.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 502


@app.route("/openInterest")
def open_interest():
    symbol = request.args.get("symbol", "BTCUSDT")
    try:
        r = requests.get(
            f"{DOMINIO_FUTURES}/fapi/v1/openInterest",
            params={"symbol": symbol},
            timeout=8,
        )
        return jsonify(r.json()), r.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 502


@app.route("/futures/depth")
def futures_depth():
    """
    Order book FUTUROS (USDT-M) vía REST (snapshot). Sin cambios --
    mismo comportamiento que antes. Para book en vivo, ver /ws/depth.
    """
    symbol = request.args.get("symbol", "BTCUSDT")
    limit = min(int(request.args.get("limit", "20")), 50)
    cache_key = f"futures:{symbol}:{limit}"

    def _fetch():
        try:
            r = requests.get(
                f"{DOMINIO_FUTURES}/fapi/v1/depth",
                params={"symbol": symbol, "limit": limit},
                timeout=8,
            )
            return r.json(), r.status_code
        except Exception as e:
            return {"error": str(e)}, 502

    body, status = _get_con_cache(cache_key, _fetch)
    return jsonify(body), status


@app.route("/bybit/openInterest")
def bybit_open_interest():
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
    return jsonify({
        "status": "ok",
        "mensaje": "Proxy de Binance funcionando",
        "websocket": "/ws/depth?market=spot|futures",
    })


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
# A VALIDAR EN EL PRIMER TEST (por eso el cliente HTML de prueba
# aparte, antes de construir nada más encima): el partial depth
# stream de SPOT llega con claves "bids"/"asks" (igual que el REST).
# El de FUTURES puede llegar con "b"/"a" en vez de "bids"/"asks"
# según la versión del stream -- por eso _normalizar_mensaje() ya
# contempla ambos nombres. Si al conectar ves el book de futures
# vacío, es la primera pista a revisar.

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
    Ver nota "A VALIDAR" más arriba.
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
