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
app = FastAPI(title="Delta WS Backend (LTP Mode)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===============================
# CONFIG
# ===============================
DELTA_WS_URL = "wss://socket.delta.exchange"
SYMBOLS = ["BTCUSD", "ETHUSD", "SOLUSD"]

# ===============================
# GLOBAL STATE
# ===============================
latest_ticks: Dict[str, Dict[str, Any]] = {}
is_connected = False

# Initialize symbols
for symbol in SYMBOLS:
    latest_ticks[symbol] = {
        "symbol": symbol,
        "price": 0.0,        # LTP (main price)
        "ltp": 0.0,
        "mark_price": 0.0,
        "spot_price": 0.0,
        "timestamp": datetime.utcnow().isoformat(),
    }

# ===============================
# HEALTH CHECK
# ===============================
@app.get("/")
def home():
    return {
        "status": "ok",
        "service": "Delta WebSocket Backend",
        "mode": "LTP (Delta App Match)",
        "symbols": SYMBOLS,
    }

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
                                "name": "v2/ticker",
                                "symbols": SYMBOLS,
                            }
                        ]
                    },
                }

                await ws.send(json.dumps(subscribe_msg))
                print(f"📤 Subscribed: {', '.join(SYMBOLS)}")

                async for msg in ws:
                    data = json.loads(msg)

                    # Subscription confirmation
                    if data.get("type") == "subscriptions":
                        is_connected = True
                        print("✅ Delta WS subscribed")
                        continue

                    symbol = data.get("symbol")
                    if symbol not in SYMBOLS:
                        continue

                    # ===============================
                    # 🔥 PRICE LOGIC (DELTA APP STYLE)
                    # ===============================
                    ltp = data.get("close")          # ✅ LAST TRADED PRICE
                    spot = data.get("spot_price")
                    mark = data.get("mark_price")

                    raw_price = ltp or spot or mark
                    if not raw_price:
                        continue

                    latest_ticks[symbol] = {
                        "symbol": symbol,

                        # 🔥 MAIN PRICE (Flutter uses this)
                        "price": float(raw_price),
                        "ltp": float(ltp) if ltp else None,

                        # 🛡 Safety / info
                        "mark_price": float(mark) if mark else None,
                        "spot_price": float(spot) if spot else None,

                        # 📊 Market data
                        "open": float(data.get("open", 0)),
                        "high": float(data.get("high", 0)),
                        "low": float(data.get("low", 0)),
                        "close": float(ltp) if ltp else 0,
                        "volume": data.get("volume", 0),

                        # ⏱ Time
                        "timestamp": datetime.utcnow().isoformat(),
                    }

                    print(f"📊 {symbol} LTP: ${float(raw_price):,.2f}")

        except websockets.exceptions.ConnectionClosed as e:
            is_connected = False
            print(f"❌ WS closed: {e}")
            print("🔁 Reconnecting in 5s...")
            await asyncio.sleep(5)

        except Exception as e:
            is_connected = False
            print(f"❌ WS error: {type(e).__name__} - {e}")
            print("🔁 Reconnecting in 5s...")
            await asyncio.sleep(5)

# ===============================
# STARTUP (RAILWAY SAFE)
# ===============================
@app.on_event("startup")
async def startup():
    print("🚀 Backend starting...")
    async def start_ws():
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
            await asyncio.sleep(0.3)  # 🔥 faster update (300ms)
    except Exception as e:
        print(f"📱 Flutter disconnected: {e}")

# ===============================
# REST ENDPOINTS (OPTIONAL)
# ===============================
@app.get("/ticker/{symbol}")
def get_ticker(symbol: str):
    symbol = symbol.upper()
    if symbol in latest_ticks:
        return {"success": True, "data": latest_ticks[symbol]}
    return {"success": False, "error": "Symbol not found"}

@app.get("/tickers")
def get_all_tickers():
    return {
        "success": True,
        "data": latest_ticks,
        "count": len(latest_ticks),
    }
