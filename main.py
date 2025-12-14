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
            symbol = item.get("symbol", "")

            # Perpetual BTC contract
            if "BTC" in symbol and "USD" in symbol:
                price = (
                    item.get("mark_price")
                    or item.get("last_price")
                    or item.get("index_price")
                )

                if price:
                    return {
                        "symbol": symbol,
                        "price": float(price)
                    }

        return {"error": "BTC price not found"}

    except Exception as e:
        return {"error": str(e)}


