from flask import Flask, request, jsonify, send_from_directory
from flask_sock import Sock
import requests
import time
import threading
import json
import re
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
    return jsonify({
        "status": "ok",
        "mensaje": "Proxy de Binance funcionando",
        "websocket": "/ws/depth?market=spot|futures",
        "bookmap": "/bookmap",
        "ban_spot_activo": _grupo_baneado("spot"),
        "ban_futures_activo": _grupo_baneado("futures"),
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
