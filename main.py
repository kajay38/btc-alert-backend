from fastapi import FastAPI
import requests
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
        
        # Debug: Print the response structure
        print(f"Response status: {res.status_code}")
        print(f"Number of results: {len(data.get('result', []))}")
        
        # Look for BTC symbols (try different variations)
        btc_symbols = []
        for item in data.get("result", []):
            symbol = item.get("symbol", "")
            if "BTC" in symbol and ("USD" in symbol or "PERP" in symbol):
                btc_symbols.append({
                    "symbol": symbol,
                    "mark_price": item.get("mark_price"),
                    "last_price": item.get("last_price"),
                    "index_price": item.get("index_price")
                })
        
        print(f"Found BTC symbols: {btc_symbols}")
        
        # Try to find BTCUSD_PERP specifically
        for item in data.get("result", []):
            symbol = item.get("symbol")
            if symbol == "BTCUSD_PERP":
                price = (
                    item.get("mark_price")
                    or item.get("last_price")
                    or item.get("index_price")
                )
                
                if price:
                    return {
                        "symbol": "BTCUSD_PERP",
                        "price": float(price),
                        "source": "Delta Exchange"
                    }
        
        # If BTCUSD_PERP not found, try to find any BTC perpetual
        for item in data.get("result", []):
            symbol = item.get("symbol", "")
            if "BTC" in symbol and "PERP" in symbol:
                price = (
                    item.get("mark_price")
                    or item.get("last_price")
                    or item.get("index_price")
                )
                
                if price:
                    return {
                        "symbol": symbol,
                        "price": float(price),
                        "source": "Delta Exchange",
                        "note": "Using alternative BTC perpetual symbol"
                    }
        
        # If no BTC perpetual found, try any BTCUSD pair
        for item in data.get("result", []):
            symbol = item.get("symbol", "")
            if "BTC" in symbol and "USD" in symbol:
                price = (
                    item.get("mark_price")
                    or item.get("last_price")
                    or item.get("index_price")
                )
                
                if price:
                    return {
                        "symbol": symbol,
                        "price": float(price),
                        "source": "Delta Exchange",
                        "note": "Using BTCUSD spot/futures symbol"
                    }
        
        return {
            "error": "BTCUSD_PERP not found",
            "available_symbols": btc_symbols[:10],  # Return first 10 BTC symbols for debugging
            "total_results": len(data.get("result", []))
        }

    except requests.exceptions.Timeout:
        return {"error": "Request timeout"}
    except requests.exceptions.HTTPError as e:
        return {"error": f"HTTP error: {e}"}
    except requests.exceptions.RequestException as e:
        return {"error": f"Request failed: {e}"}
    except Exception as e:
        return {"error": f"Unexpected error: {str(e)}"}

@app.get("/debug/tickers")
def debug_tickers():
    """Endpoint to debug and see all available tickers"""
    try:
        res = requests.get(DELTA_URL, timeout=10)
        res.raise_for_status()
        data = res.json()
        
        # Extract all BTC-related symbols
        btc_tickers = []
        for item in data.get("result", []):
            symbol = item.get("symbol", "")
            if "BTC" in symbol:
                btc_tickers.append({
                    "symbol": symbol,
                    "mark_price": item.get("mark_price"),
                    "last_price": item.get("last_price"),
                    "index_price": item.get("index_price"),
                    "product_id": item.get("product_id")
                })
        
        return {
            "total_tickers": len(data.get("result", [])),
            "btc_tickers": btc_tickers[:20],  # Limit to 20 for readability
            "all_tickers_count": len(data.get("result", []))
        }
        
    except Exception as e:
        return {"error": str(e)}
