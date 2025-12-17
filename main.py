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
app = FastAPI(title="Delta WS Backend - Ultra Realtime")

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

for s in SYMBOLS:
    latest_ticks[s] = {
        "symbol": s,
        "price": 0.0,          # display price (smooth)
        "ltp": 0.0,            # last traded price
        "bid": 0.0,
        "ask": 0.0,
        "mark_price": 0.0,
        "timestamp": datetime.utcnow().isoformat(),
    }

# ===============================
# HEALTH
# ===============================
@app.get("/")
def home():
    return {"status": "ok", "mode": "Delta Style Ultra Realtime"}

# ===============================
# DELTA WS LISTENER (NO DELAY)
# ===============================
async def delta_ws_listener():
    global is_connected

    while True:
        try:
            print("🔄 Connecting Delta WS...")
            async with websockets.connect(
                DELTA_WS_URL,
                ping_interval=15,
                ping_timeout=15,
            ) as ws:

                sub = {
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

                await ws.send(json.dumps(sub))
                print("✅ Subscribed to Delta ticker")

                async for msg in ws:
                    data = json.loads(msg)

                    if data.get("type") == "subscriptions":
                        is_connected = True
                        continue

                    symbol = data.get("symbol")
                    if symbol not in SYMBOLS:
                        continue

                    # ---------- RAW DATA ----------
                    ltp = data.get("close")
                    bid = data.get("best_bid")
                    ask = data.get("best_ask")
                    mark = data.get("mark_price")

                    # ---------- DISPLAY PRICE (SMOOTH) ----------
                    # Delta-style: mid price if bid/ask available
                    if bid and ask:
                        display_price = (float(bid) + float(ask)) / 2
                    else:
                        display_price = ltp or mark

                    if not display_price:
                        continue

                    latest_ticks[symbol] = {
                        "symbol": symbol,

                        # 🔥 UI PRICE (moves continuously)
                        "price": float(display_price),

                        # 🎯 REAL TRADE PRICE
                        "ltp": float(ltp) if ltp else None,

                        # 📊 ORDER BOOK
                        "bid": float(bid) if bid else None,
                        "ask": float(ask) if ask else None,

                        # 🛡 SAFETY
                        "mark_price": float(mark) if mark else None,

                        "timestamp": datetime.utcnow().isoformat(),
                    }

        except Exception as e:
            is_connected = False
            print(f"❌ Delta WS error: {e}")
            await asyncio.sleep(3)

# ===============================
# STARTUP
# ===============================
@app.on_event("startup")
async def startup():
    print("🚀 Backend starting (Ultra Realtime)...")
    asyncio.create_task(delta_ws_listener())

# ===============================
# FLUTTER WS (INSTANT PUSH)
# ===============================
@app.websocket("/ws/tickers")
async def flutter_ws(ws: WebSocket):
    await ws.accept()
    print("📱 Flutter connected")

    try:
        while True:
            # ❌ NO sleep → push immediately
            await ws.send_json({"ticks": latest_ticks})
            await asyncio.sleep(0.05)  # ~20 FPS (smooth like Delta)
    except Exception as e:
        print(f"📱 Flutter disconnected: {e}")

# ===============================
# REST (OPTIONAL)
# ===============================
@app.get("/ticker/{symbol}")
def get_ticker(symbol: str):
    symbol = symbol.upper()
    return latest_ticks.get(symbol, {})
