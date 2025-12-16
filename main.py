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

                subscribe_msg = {
                    "type": "subscribe",
                    "payload": {
                        "channels": [
                            {
                                "name": "ticker",
                                "symbols": [SYMBOL],
                            }
                        ]
                    },
                }

                await ws.send(json.dumps(subscribe_msg))
                is_connected = True
                print("✅ Connected to Delta WS")

                async for msg in ws:
                    await asyncio.sleep(0)  # 🔑 prevent event-loop blocking
                    data = json.loads(msg)

                    # ✅ Robust ticker parsing
                    if data.get("type") == "ticker" and "data" in data:
                        ticker = data["data"]

                        if ticker.get("symbol") == SYMBOL:
                            raw_price = (
                                ticker.get("mark_price")
                                or ticker.get("close")
                                or ticker.get("index_price")
                            )

                            if raw_price:
                                latest_ticks[SYMBOL] = {
                                    "symbol": SYMBOL,
                                    "price": float(raw_price),
                                    "timestamp": datetime.utcnow().isoformat(),
                                }

        except Exception as e:
            is_connected = False
            print("❌ Delta WS error:", e)
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
    try:
        while True:
            await ws.send_json({"ticks": latest_ticks})
            await asyncio.sleep(0.5)  # 500ms update
    except:
        pass
