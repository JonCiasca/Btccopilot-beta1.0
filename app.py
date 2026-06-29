from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

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
