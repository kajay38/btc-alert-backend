from fastapi import FastAPI
import requests
import logging
from typing import Dict, Any

# Set up logging for better error visibility
logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="BTC Perpetual Price API",
    description="A backend service to retrieve the current price of the BTC perpetual future from Delta Exchange."
)

TICKERS_URL = "https://api.delta.exchange/v2/tickers"

@app.get("/", summary="Check API status")
def home():
    """Returns a simple status message to confirm the backend is running."""
    return {"status": "BTC Alert Backend Running"}

@app.get("/tickers", summary="Get Raw Ticker Data")
def get_tickers():
    """
    Fetches and returns the raw ticker data from the external API for debugging purposes.
    You can check this endpoint to confirm if the contract structure or symbol has changed.
    """
    try:
        logging.info(f"Fetching raw data from: {TICKERS_URL}")
        r = requests.get(TICKERS_URL, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as req_e:
        logging.error(f"HTTP/Network Error fetching raw tickers: {req_e}")
        return {"error": f"Failed to connect to external API: {req_e}"}
    except Exception as e:
        logging.error(f"An unexpected error occurred fetching raw tickers: {e}")
        return {"error": f"An internal error occurred: {e}"}

@app.get("/price", summary="Get BTC Perpetual Price", response_model=Dict[str, Any])
def get_price():
    """
    Fetches ticker data, looks for the BTC perpetual future, and returns its price.
    It prioritizes mark_price, then last_price, then index_price.
    """
    try:
        # 1. Fetch data from the external API
        logging.info(f"Fetching data from: {TICKERS_URL}")
        r = requests.get(TICKERS_URL, timeout=10)
        r.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        data = r.json()

        # 2. Iterate through results to find the BTC Perpetual Future
        for item in data.get("result", []):
            contract = item.get("contract_type")
            underlying = item.get("underlying_asset", {})

            # FIX: Changed "perpetual" to the correct "perpetual_futures" based on raw data inspection.
            if contract == "perpetual_futures" and underlying.get("symbol") == "BTC":
                # Get the price, prioritizing mark_price, then last_price, then index_price
                price_str = (
                    item.get("mark_price")
                    or item.get("last_price")
                    or item.get("index_price")
                )

                if price_str:
                    # Convert the found price string to a float
                    price = float(price_str)
                    logging.info(f"Found BTC perpetual price: {price}")
                    return {
                        "symbol": item.get("symbol"),
                        "price": price
                    }

        # 3. Handle case where contract is not found
        logging.warning("BTC perpetual contract not found in the ticker list.")
        # Revert to the original error message now that the contract type is fixed
        return {"error": "BTC perpetual contract not found"}

    except requests.exceptions.RequestException as req_e:
        # Handle network/HTTP errors
        logging.error(f"HTTP/Network Error: {req_e}")
        return {"error": f"Failed to connect to external API: {req_e}"}
    except Exception as e:
        # Handle other unexpected errors (e.g., JSON parsing, type conversion)
        logging.error(f"An unexpected error occurred: {e}")
        return {"error": f"An internal error occurred: {e}"}

# Note on running: To run this file, you would typically save it as main.py
# and execute 'uvicorn main:app --reload' in your terminal, after installing
# fastapi and uvicorn: 'pip install fastapi uvicorn requests'
