from flask import Flask, request, jsonify
import requests
import time
import threading
import re

app = Flask(__name__)

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
TTL_LENTO = 14


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
        "ban_spot_activo": _grupo_baneado("spot"),
        "ban_spot_restante_segundos": max(0, int(_BAN_HASTA["spot"] - ahora)),
        "ban_futures_activo": _grupo_baneado("futures"),
        "ban_futures_restante_segundos": max(0, int(_BAN_HASTA["futures"] - ahora)),
    })


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
