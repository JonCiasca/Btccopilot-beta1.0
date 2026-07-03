from flask import Flask, request, jsonify
import requests
import time

app = Flask(__name__)

# ----------------------------------
# CACHE TTL CORTO (solo endpoints de profundidad)
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
    Order book SPOT. Reusa el mismo mecanismo multi-dominio que klines/
    ticker (Binance spot bloquea la IP de Streamlit Cloud, no la del
    proxy). limit acepta los valores nativos de Binance: 5, 10, 20, 50,
    100, 500, 1000, 5000 — capado acá a 50 como techo de seguridad
    (más que eso no suma resolución real al heatmap del dashboard, y
    solo aumenta el weight consumido).
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
    Order book FUTUROS (USDT-M). Mismo formato de respuesta que /depth
    (bids/asks), pero contra fapi.binance.com. limit capado a 50: en
    Binance Futures el weight de /fapi/v1/depth salta de 2 a 5 al pasar
    de 50 a 100 — este endpoint es el que veníamos baneando (-1003),
    así que el techo acá es más importante que en spot.
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
    return jsonify({"status": "ok", "mensaje": "Proxy de Binance funcionando"})


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
