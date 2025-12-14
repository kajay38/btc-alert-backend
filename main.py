from fastapi import FastAPI
import requests

app = FastAPI()

BASE_URL = "https://api.delta.exchange/v2/tickers"

INSTRUMENTS = {
    "BTC": {"product_id": 27, "sell": 90200, "buy": 89800},
    "ETH": {"product_id": 3136, "sell": 5100, "buy": 4900},
    "SOL": {"sell": 220, "buy": 200},
}

@app.get("/")
def home():
    return {"status": "Multi-Asset Alert Backend Running"}

def safe_price(value):
    try:
        return float(value)
    except:
        return None

def get_price_by_product(product_id):
    r = requests.get(f"{BASE_URL}/{product_id}", timeout=10)
    r.raise_for_status()
    data = r.json().get("result", {})

    price = safe_price(
        data.get("mark_price")
        or data.get("last_price")
        or data.get("index_price")
    )
    return price, data.get("symbol")

def get_sol_price():
    r = requests.get(BASE_URL, timeout=10)
    r.raise_for_status()

    for item in r.json().get("result", []):
        if (
            item.get("contract_type") == "perpetual"
            and item.get("underlying_asset", {}).get("symbol") == "SOL"
        ):
            price = safe_price(
                item.get("mark_price")
                or item.get("last_price")
                or item.get("index_price")
            )
            return price, item.get("symbol")
    return None, None

@app.get("/prices")
def prices():
    output = {}

    for name, cfg in INSTRUMENTS.items():
        if "product_id" in cfg:
            price, symbol = get_price_by_product(cfg["product_id"])
            output[name] = {"symbol": symbol, "price": price}

    sol_price, sol_symbol = get_sol_price()
    output["SOL"] = {"symbol": sol_symbol, "price": sol_price}

    return output

@app.get("/check-alerts")
def check_alerts():
    alerts = []

    for name, cfg in INSTRUMENTS.items():
        if "product_id" in cfg:
            price, _ = get_price_by_product(cfg["product_id"])
            if price is None:
                continue

            if price >= cfg["sell"]:
                alerts.append(f"{name} SELL zone @ {price}")
            elif price <= cfg["buy"]:
                alerts.append(f"{name} BUY zone @ {price}")

    sol_price, _ = get_sol_price()
    if sol_price is not None:
        if sol_price >= INSTRUMENTS["SOL"]["sell"]:
            alerts.append(f"SOL SELL zone @ {sol_price}")
        elif sol_price <= INSTRUMENTS["SOL"]["buy"]:
            alerts.append(f"SOL BUY zone @ {sol_price}")

    return {"alerts": alerts or ["NO ALERT"]}
