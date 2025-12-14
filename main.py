from fastapi import FastAPI
import requests

app = FastAPI()

# BTCUSD Perpetual Futures (Delta Exchange)
PRODUCT_ID = 27
TICKER_URL = f"https://api.delta.exchange/v2/tickers/{PRODUCT_ID}"

@app.get("/")
def home():
    return {"status": "BTCUSD Backend Running"}

@app.get("/price")
def get_price():
    try:
        r = requests.get(TICKER_URL, timeout=10)
        r.raise_for_status()
        data = r.json()

        result = data.get("result", {})

        price = (
            result.get("mark_price")
            or result.get("last_price")
            or result.get("index_price")
        )

        if not price:
            return {"error": "Price not available", "raw": result}

        return {
            "product_id": PRODUCT_ID,
            "symbol": result.get("symbol"),
            "price": float(price)
        }

    except Exception as e:
        return {"error": str(e)}


