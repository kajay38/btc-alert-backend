from fastapi import FastAPI
import requests

app = FastAPI()

DELTA_URL = "https://api.delta.exchange/v2/tickers"

@app.get("/")
def home():
    return {"status": "BTC Alert Backend Running"}

@app.get("/price")
def get_price():
    try:
        res = requests.get(DELTA_URL, timeout=10)
        res.raise_for_status()
        data = res.json()

        for item in data.get("result", []):
            if item.get("symbol") == "BTCUSD_PERP":
                price = (
                    item.get("mark_price")
                    or item.get("last_price")
                    or item.get("index_price")
                )

                return {
                    "symbol": "BTCUSD_PERP",
                    "price": float(price)
                }

        return {"error": "BTCUSD_PERP not found"}

    except Exception as e:
        return {"error": str(e)}

