import asyncio
import json
from datetime import datetime
from typing import Dict, Any

import websockets
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware

# ===============================
# APP SETUP
# ===============================
app = FastAPI(title="Delta WS Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===============================
# GLOBAL STATE
# ===============================
DELTA_WS_URL = "wss://socket.delta.exchange"
SYMBOLS = ["BTCUSD", "ETHUSD", "SOLUSD"]  # ✅ Added ETH and SOL

latest_ticks: Dict[str, Dict[str, Any]] = {}

# Initialize all symbols
for symbol in SYMBOLS:
    latest_ticks[symbol] = {
        "symbol": symbol,
        "price": 0.0,
        "timestamp": datetime.utcnow().isoformat(),
    }

is_connected = False


# ===============================
# ROOT (Railway health check)
# ===============================
@app.get("/")
def home():
    return {
        "status": "ok",
        "service": "Delta WebSocket Backend",
        "symbols": SYMBOLS,
    }


# ===============================
# STATUS API
# ===============================
@app.get("/status")
def status():
    return {
        "status": "up",
        "delta_ws": "connected" if is_connected else "disconnected",
        "ticks": latest_ticks,
        "time": datetime.utcnow().isoformat(),
    }


# ===============================
# DELTA WEBSOCKET LISTENER
# ===============================
async def delta_ws_listener():
    global is_connected

    while True:
        try:
            print("🔄 Connecting to Delta WebSocket...")
            async with websockets.connect(
                DELTA_WS_URL,
                ping_interval=20,
                ping_timeout=20,
            ) as ws:

                # ✅ Subscribe to multiple symbols
                subscribe_msg = {
                    "type": "subscribe",
                    "payload": {
                        "channels": [
                            {
                                "name": "v2/ticker",
                                "symbols": SYMBOLS,  # Subscribe to all symbols
                            }
                        ]
                    },
                }

                await ws.send(json.dumps(subscribe_msg))
                print(f"📤 Sent subscription request for {', '.join(SYMBOLS)}")

                async for msg in ws:
                    await asyncio.sleep(0)  # prevent event-loop blocking
                    data = json.loads(msg)

                    # Handle subscription confirmation
                    if data.get("type") == "subscriptions":
                        is_connected = True
                        print(f"✅ Subscription confirmed: {data}")
                        continue

                    # ✅ Process ticker data for any subscribed symbol
                    symbol = data.get("symbol")
                    if symbol in SYMBOLS:
                        # Try different price fields in order of preference
                        raw_price = (
                            data.get("mark_price")      # Primary: mark price
                            or data.get("spot_price")   # Fallback: spot price
                            or data.get("close")        # Fallback: close price
                        )

                        if raw_price:
                            price_float = float(raw_price)
                            latest_ticks[symbol] = {
                                "symbol": symbol,
                                "price": price_float,
                                "timestamp": datetime.utcnow().isoformat(),
                                "open": float(data.get("open", 0)),
                                "high": float(data.get("high", 0)),
                                "low": float(data.get("low", 0)),
                                "close": float(data.get("close", 0)),
                                "volume": data.get("volume", 0),
                                "mark_change_24h": data.get("mark_change_24h", "0"),
                            }
                            print(f"📊 {symbol}: ${price_float:,.2f}")

        except websockets.exceptions.ConnectionClosed as e:
            is_connected = False
            print(f"❌ WebSocket connection closed: {e}")
            print("🔁 Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

        except Exception as e:
            is_connected = False
            print(f"❌ Delta WS error: {type(e).__name__} - {e}")
            print("🔁 Reconnecting in 5 seconds...")
            await asyncio.sleep(5)


# ===============================
# STARTUP EVENT (Railway-Safe)
# ===============================
@app.on_event("startup")
async def startup():
    print("🚀 Backend starting...")

    async def start_ws():
        # ⏳ Delay so Railway health check passes
        await asyncio.sleep(3)
        await delta_ws_listener()

    asyncio.create_task(start_ws())


# ===============================
# FLUTTER WEBSOCKET
# ===============================
@app.websocket("/ws/tickers")
async def flutter_ws(ws: WebSocket):
    await ws.accept()
    print("📱 Flutter client connected")
    try:
        while True:
            await ws.send_json({"ticks": latest_ticks})
            await asyncio.sleep(0.5)  # 500ms update
    except Exception as e:
        print(f"📱 Flutter client disconnected: {e}")


# ===============================
# INDIVIDUAL SYMBOL ENDPOINTS (Optional)
# ===============================
@app.get("/ticker/{symbol}")
def get_ticker(symbol: str):
    """Get ticker data for a specific symbol"""
    symbol = symbol.upper()
    if symbol in latest_ticks:
        return {
            "success": True,
            "data": latest_ticks[symbol]
        }
    return {
        "success": False,
        "error": f"Symbol {symbol} not found. Available: {', '.join(SYMBOLS)}"
    }


@app.get("/tickers")
def get_all_tickers():
    """Get all ticker data"""
    return {
        "success": True,
        "data": latest_ticks,
        "count": len(latest_ticks)
    }
