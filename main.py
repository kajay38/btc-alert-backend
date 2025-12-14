from fastapi import FastAPI
import requests

app = FastAPI()

TICKERS_URL = "https://api.delta.exchange/v2/tickers"

@app.get("/")
def home():
    return {"status": "BTC Alert Backend Running"}

@app.get("/price")
def get_price():
    try:
        r = requests.get(TICKERS_URL, timeout=10)
        r.raise_for_status()
        data = r.json()

        for item in data.get("result", []):
            contract = item.get("contract_type")
            underlying = item.get("underlying_asset", {})

            # ✅ BTC Perpetual Future
            if contract == "perpetual" and underlying.get("symbol") == "BTC":
                price = (
                    item.get("mark_price")
                    or item.get("last_price")
                    or item.get("index_price")
                )

                if price:
                    return {
                        "symbol": item.get("symbol"),
                        "price": float(price)
                    }

        return {"error": "BTC perpetual contract not found"}

    except Exception as e:
        return {"error": str(e)}
