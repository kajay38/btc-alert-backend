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
        response = requests.get(DELTA_URL, timeout=10)
        response.raise_for_status()
        json_data = response.json()

        if "result" not in json_data:
            return {"error": "Invalid API response", "raw": json_data}

        for item in json_data["result"]:
            if item.get("symbol") == "BTCUSD":
                return {
                    "symbol": "BTCUSD",
                    "price": float(item.get("last_price", 0))
                }

        return {"error": "BTCUSD not found"}

    except Exception as e:
        return {"error": str(e)}

