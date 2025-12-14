from fastapi import FastAPI
import requests

app = FastAPI()

@app.get("/")
def home():
    return {"status": "BTC Alert Backend Running"}

@app.get("/price")
def get_price():
    url = "https://api.delta.exchange/v2/tickers/BTCUSD"
    data = requests.get(url).json()
    return {
        "price": data["result"]["last_price"]
    }
