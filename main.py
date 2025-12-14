@app.get("/price/alt")
def get_price_alternative():
    """Alternative implementation using product-specific endpoint"""
    try:
        # Try different possible symbols
        symbols_to_try = [
            "BTCUSD", 
            "BTC-PERP", 
            "BTCUSD-PERP",
            "BTCUSDT",
            "BTCUSDT-PERP"
        ]
        
        for symbol in symbols_to_try:
            url = f"https://api.delta.exchange/v2/tickers/{symbol}"
            try:
                res = requests.get(url, timeout=5)
                if res.status_code == 200:
                    data = res.json()
                    if data.get("result"):
                        item = data["result"]
                        price = (
                            item.get("mark_price")
                            or item.get("last_price")
                            or item.get("index_price")
                        )
                        if price:
                            return {
                                "symbol": symbol,
                                "price": float(price),
                                "source": "Delta Exchange"
                            }
            except:
                continue
        
        # Fallback to Binance or other exchange
        binance_res = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT", timeout=5)
        if binance_res.status_code == 200:
            binance_data = binance_res.json()
            return {
                "symbol": "BTCUSDT",
                "price": float(binance_data["price"]),
                "source": "Binance (fallback)",
                "note": "Delta Exchange symbol not found, using Binance"
            }
        
        return {"error": "Could not fetch BTC price from any source"}
        
    except Exception as e:
        return {"error": str(e)}
