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
SYMBOL = "BTCUSD"

latest_ticks: Dict[str, Dict[str, Any]] = {
    SYMBOL: {
        "symbol": SYMBOL,
        "price": 0.0,
        "timestamp": datetime.utcnow().isoformat(),
    }
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
        "symbol": SYMBOL,
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

                # ✅ FIXED: Use correct channel name "v2/ticker"
                subscribe_msg = {
                    "type": "subscribe",
                    "payload": {
                        "channels": [
                            {
                                "name": "v2/ticker",  # Changed from "ticker" to "v2/ticker"
                                "symbols": [SYMBOL],
                            }
                        ]
                    },
                }

                await ws.send(json.dumps(subscribe_msg))
                print(f"📤 Sent subscription request for {SYMBOL}")

                async for msg in ws:
                    await asyncio.sleep(0)  # prevent event-loop blocking
                    data = json.loads(msg)

                    # Handle subscription confirmation
                    if data.get("type") == "subscriptions":
                        is_connected = True
                        print(f"✅ Subscription confirmed: {data}")
                        continue

                    # ✅ FIXED: Correct ticker data parsing
                    # Delta sends ticker data directly at root level, not nested in "data"
                    if data.get("symbol") == SYMBOL:
                        # Try different price fields in order of preference
                        raw_price = (
                            data.get("mark_price")      # Primary: mark price
                            or data.get("spot_price")   # Fallback: spot price
                            or data.get("close")        # Fallback: close price
                        )

                        if raw_price:
                            price_float = float(raw_price)
                            latest_ticks[SYMBOL] = {
                                "symbol": SYMBOL,
                                "price": price_float,
                                "timestamp": datetime.utcnow().isoformat(),
                                "open": float(data.get("open", 0)),
                                "high": float(data.get("high", 0)),
                                "low": float(data.get("low", 0)),
                                "close": float(data.get("close", 0)),
                                "volume": data.get("volume", 0),
                            }
                            print(f"📊 {SYMBOL}: ${price_float:,.2f}")

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
