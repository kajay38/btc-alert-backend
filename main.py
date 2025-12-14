from fastapi import FastAPI
import requests

app = FastAPI()

PRODUCT_ID = 27
TICKER_URL = f"https://api.delta.exchange/v2/tickers/{PRODUCT_ID}"

# ---- ALERT LEVELS (static for now) ----
SELL_ALERT = 90200
BUY_ALERT = 89800

@app.get("/")
def home():
    return {"status": "BTCUSD Alert Backend Running"}

@app.get("/price")
def get_price():
    r = requests.get(TICKER_URL, timeout=10)
    r.raise_for_status()
    data = r.json()["result"]

    price = (
        data.get("mark_price")
        or data.get("last_price")
        or data.get("index_price")
    )

    return {
        "product_id": PRODUCT_ID,
        "symbol": data.get("symbol"),
        "price": float(price)
    }

@app.get("/check-alert")
def check_alert():
    r = requests.get(TICKER_URL, timeout=10)
    r.raise_for_status()
    data = r.json()["result"]

    price = float(
        data.get("mark_price")
        or data.get("last_price")
        or data.get("index_price")
    )

    if price >= SELL_ALERT:
        return {
            "alert": "SELL ZONE HIT",
            "price": price,
            "level": SELL_ALERT
        }

    if price <= BUY_ALERT:
        return {
            "alert": "BUY ZONE HIT",
            "price": price,
            "level": BUY_ALERT
        }

    return {
        "alert": "NO ALERT",
        "price": price
    }

