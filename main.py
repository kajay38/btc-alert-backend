import json
import asyncio
import os
from datetime import datetime
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any, Optional

# --- 1. Dependencies and Setup ---
# Load environment variables (API_KEY, API_SECRET) from .env file
load_dotenv()

# Initialize FastAPI App
app = FastAPI(title="Delta Exchange Trading Backend")

# Add CORS for Flutter Web/Mobile development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins during development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Delta Exchange API Details
DELTA_BASE_URL = "https://api.india.delta.exchange"
DELTA_WS_URL = "wss://stream.india.delta.exchange/v2/websocket"
API_KEY = os.getenv("DELTA_API_KEY")
API_SECRET = os.getenv("DELTA_API_SECRET")

# In-memory storage for the latest tick price
latest_ticks: Dict[str, Dict[str, Any]] = {}
is_ws_connected: bool = False

# --- 2. Delta Exchange API Client Mock/Placeholder ---

# NOTE: For real trading, you MUST use Delta's official SDK or a proper signed REST API client.
# This client is a placeholder for sending market/limit orders.
def send_delta_order(symbol: str, side: str, size: int, order_type: str, price: Optional[float] = None) -> Dict[str, Any]:
    """
    Sends a mock order to Delta Exchange. 
    In a real app, this would involve HMAC signing and a REST API call.
    """
    if not API_KEY or not API_SECRET:
        raise ValueError("API credentials not configured.")
    
    # Placeholder for actual API integration (e.g., using 'requests' with signing)
    print(f"\n--- 🚨 TRADING ORDER SENT (MOCK) ---")
    print(f"SYMBOL: {symbol}, SIDE: {side}, SIZE: {size}, TYPE: {order_type}")
    if price:
        print(f"PRICE: {price}")
    print("-----------------------------------")
    
    # Mock response
    return {
        "success": True,
        "order_id": f"mock_{datetime.now().timestamp()}",
        "symbol": symbol,
        "status": "placed",
        "mock_response": "Actual REST call would be here."
    }

# --- 3. WebSocket Connection and Ticker Stream ---

async def connect_and_stream_delta(symbols: List[str]):
    """Connects to Delta WS and streams tickers into latest_ticks."""
    global is_ws_connected
    
    # Example subscription message for L2 order book updates (fastest)
    subscribe_msg = {
        "type": "subscribe",
        "payload": {
            "channels": [
                {
                    "name": "l2_updates",
                    "symbols": symbols
                }
            ]
        }
    }
    
    while True:
        try:
            print("Attempting to connect to Delta Exchange WebSocket...")
            # Using 'websockets' library is preferred for asyncio environments
            # For simplicity, we assume an external process/library handles the raw WS connection
            
            # --- MOCK WS SIMULATION ---
            for symbol in symbols:
                # Simulate receiving a fast tick (every 100ms)
                current_price = latest_ticks.get(symbol, {}).get("price", 50000.0)
                change = (asyncio.get_event_loop().time() % 2) * 2 - 1  # Simulate price change
                new_price = round(current_price + change * 0.5, 2)
                
                latest_ticks[symbol] = {
                    "symbol": symbol,
                    "price": new_price,
                    "timestamp": datetime.now().isoformat(),
                    # Add more fields based on real L2 update: bid/ask, size, etc.
                    "bid": round(new_price - 1.0, 2),
                    "ask": round(new_price + 1.0, 2),
                }
            
            is_ws_connected = True
            await asyncio.sleep(1) # Simulate 1 second data pull/refresh interval
            # --------------------------

        except Exception as e:
            print(f"Delta WS Error: {e}. Reconnecting in 5 seconds...")
            is_ws_connected = False
            await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event():
    # Start the background task to stream Delta data
    # IMPORTANT: Ensure 'BTCUSD', 'ETHUSD', etc. are initialized in latest_ticks 
    # before the streaming starts.
    initial_symbols = ["BTCUSD", "ETHUSD", "ETHUSDT", "SOLUSD", "XRPUSD"] # Example symbols
    for sym in initial_symbols:
         latest_ticks[sym] = {"symbol": sym, "price": 50000.0, "timestamp": datetime.now().isoformat(), "bid": 49999.0, "ask": 50001.0}
    
    # Run the streaming function in the background
    asyncio.create_task(connect_and_stream_delta(initial_symbols))
    print("FastAPI backend started. Starting Delta WS task...")

# --- 4. FastAPI Endpoints ---

# 4.1. WebSocket Endpoint for Flutter Client (Tick-by-tick Price Stream)
@app.websocket("/ws/tickers")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("Flutter client connected to Ticker WS.")
    
    try:
        while True:
            # Send the entire dictionary of latest ticks to the client
            # This handles the "tick-by-tick price bhi aay" requirement
            await websocket.send_json({"ticks": latest_ticks})
            await asyncio.sleep(0.1) # Send update every 100 milliseconds
            
    except Exception as e:
        print(f"Flutter WS connection closed: {e}")
    finally:
        print("Flutter client disconnected from Ticker WS.")

# 4.2. REST Endpoint for Trading Orders (Strategy Execution)
@app.post("/api/trade")
async def create_trade(
    symbol: str, 
    side: str, 
    size: int, 
    order_type: str = "market", 
    price: Optional[float] = None
):
    """
    Receives trading commands from the Flutter app (e.g., when a strategy alert triggers).
    """
    try:
        if symbol not in latest_ticks:
            raise HTTPException(status_code=400, detail="Invalid symbol.")
            
        if side.upper() not in ["BUY", "SELL"]:
            raise HTTPException(status_code=400, detail="Side must be BUY or SELL.")
            
        if size <= 0:
            raise HTTPException(status_code=400, detail="Size must be positive.")
            
        # Execute the trade (MOCK)
        result = send_delta_order(symbol.upper(), side.upper(), size, order_type.lower(), price)
        
        return {"status": "success", "data": result}
        
    except ValueError as e:
        raise HTTPException(status_code=500, detail=f"Configuration Error: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Trading Error: {e}")

# 4.3. Status Check Endpoint (Optional)
@app.get("/status")
def get_status():
    return {
        "status": "up",
        "delta_ws_status": "connected" if is_ws_connected else "disconnected (Mocking)",
        "last_updated_time": datetime.now().isoformat(),
        "latest_prices": {sym: data.get("price") for sym, data in latest_ticks.items()}
    }

# To run the backend:
# 1. Create a .env file with DELTA_API_KEY=YOUR_KEY and DELTA_API_SECRET=YOUR_SECRET
# 2. Run in terminal from the 'backend' folder: uvicorn main:app --reload --host 0.0.0.0 --port 8000
