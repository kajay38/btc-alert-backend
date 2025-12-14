from fastapi import FastAPI
import requests

app = FastAPI()

BASE_URL = "https://api.delta.exchange/v2/tickers"

# ---- Instruments Configuration ----
INSTRUMENTS = {
    "BTC": {
        "product_id": 27,
        "sell": 90200,
        "buy": 89800
    },
    "ETH": {
        "product_id": 3136,
        "sell": 5100,
        "buy": 4900
    },
    "SOL": {
        "symbol": "SOL",
        "sell": 220,
        "buy": 200
    }
}

@app.get("/")
def home():
    return {"status": "Multi-Asset Alert Backend Running"}

# ---- Helper: Get price by product_id ----
def get_price_by_product(product_id):
    r = requests.get(f"{BASE_URL}/{product_id}", timeout=10)
    r.raise_for_status()
    data = r.json()["result"]

    price = (
        data.get("mark_price")
        or data.get("last_price")
        or data.get("index_price")
    )

    return float(price), data.get("symbol")

# ---- Helper: Find SOL perpetual dynamically ----
def get_sol_price():
    r = requests.get(BASE_URL, timeout=10)
    r.raise_for_status()

    for item in r.json().get("result", []):
        if (
            item.get("contract_type") == "perpetual"
            and item.get("underlying_asset", {}).get("symbol") == "SOL"
        ):
            price = (
                item.get("mark_price")
                or item.get("last_price")
                or item.get("index_price")
            )
            return float(price), item.get("symbol")

    return None, None

@app.get("/prices")
def get_all_prices():
    results = {}

    # BTC & ETH
    for name, cfg in INSTRUMENTS.items():
        if "product_id" in cfg:
            price, symbol = get_price_by_product(cfg["product_id"])
            results[name] = {"symbol": symbol, "price": price}

    # SOL
    sol_price, sol_symbol = get_sol_price()
    results["SOL"] = {"symbol": sol_symbol, "price": sol_price}

    return results

@app.get("/check-alerts")
def check_alerts():
    alerts = []

    # BTC & ETH
    for name, cfg in INSTRUMENTS.items():
        if "product_id" in cfg:
            price, symbol = get_price_by_product(cfg["product_id"])

            if price >= cfg["sell"]:
                alerts.append(f"{name} SELL zone hit @ {price}")

            elif price <= cfg["buy"]:
                alerts.append(f"{name} BUY zone hit @ {price}")

    # SOL
    sol_price, sol_symbol = get_sol_price()
    sol_cfg = INSTRUMENTS["SOL"]

    if sol_price:
        if sol_price >= sol_cfg["sell"]:
            alerts.append(f"SOL SELL zone hit @ {sol_price}")
        elif sol_price <= sol_cfg["buy"]:
            alerts.append(f"SOL BUY zone hit @ {sol_price}")

    return {
        "alerts": alerts if alerts else ["NO ALERT"],
    }
