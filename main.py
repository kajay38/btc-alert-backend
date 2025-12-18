import asyncio
import json
from datetime import datetime
from typing import Dict, Any, List

import websockets
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware

# ===============================
# APP SETUP
# ===============================
app = FastAPI(title="Delta Public Market WS Backend")

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
latest_trades: Dict[str, List[Dict[str, Any]]] = {}
is_connected = False

MAX_TRADES = 20  # last 20 trades per symbol

for s in SYMBOLS:
    latest_ticks[s] = {
        "symbol": s,
        "price": 0.0,       # Smooth (UI)
        "ltp": 0.0,         # Last trade
        "mark_price": 0.0,
        "bid": 0.0,
        "ask": 0.0,
        "timestamp": datetime.utcnow().isoformat(),
    }
    latest_trades[s] = []

# ===============================
# HEALTH CHECK
# ===============================
@app.get("/")
def home():
    return {
        "status": "ok",
        "service": "Delta Public Market WS",
        "symbols": SYMBOLS,
    }

# ===============================
# DELTA WS LISTENER
# ===============================
async def delta_ws_listener():
    global is_connected

    while True:
        try:
            print("🔄 Connecting to Delta WS...")
            async with websockets.connect(
                DELTA_WS_URL,
                ping_interval=15,
                ping_timeout=15,
            ) as ws:

                subscribe_msg = {
                    "type": "subscribe",
                    "payload": {
                        "channels": [
                            {
                                "name": "v2/ticker",
                                "symbols": SYMBOLS,
                            },
                            {
                                "name": "v2/trades",
                                "symbols": SYMBOLS,
                            },
                        ]
                    },
                }

                await ws.send(json.dumps(subscribe_msg))
                print("✅ Subscribed: ticker + trades")

                async for msg in ws:
                    data = json.loads(msg)

                    if data.get("type") == "subscriptions":
                        is_connected = True
                        continue

                    symbol = data.get("symbol")
                    if symbol not in SYMBOLS:
                        continue

                    # ===============================
                    # TICKER DATA
                    # ===============================
                    if data.get("channel") == "v2/ticker":
                        ltp = data.get("close")
                        bid = data.get("best_bid")
                        ask = data.get("best_ask")
                        mark = data.get("mark_price")

                        # Smooth price for UI
                        if bid and ask:
                            smooth = (float(bid) + float(ask)) / 2
                        else:
                            smooth = ltp or mark

                        if not smooth:
                            continue

                        latest_ticks[symbol] = {
                            "symbol": symbol,
                            "price": float(smooth),
                            "ltp": float(ltp) if ltp else None,
                            "mark_price": float(mark) if mark else None,
                            "bid": float(bid) if bid else None,
                            "ask": float(ask) if ask else None,
                            "timestamp": datetime.utcnow().isoformat(),
                        }

                    # ===============================
                    # TRADES DATA (REAL BUY / SELL)
                    # ===============================
                    if data.get("channel") == "v2/trades":
                        trade = {
                            "price": float(data.get("price", 0)),
                            "size": float(data.get("size", 0)),
                            "side": data.get("side"),  # buy / sell
                            "timestamp": data.get("timestamp"),
                        }

                        latest_trades[symbol].insert(0, trade)

                        if len(latest_trades[symbol]) > MAX_TRADES:
                            latest_trades[symbol].pop()

        except Exception as e:
            is_connected = False
            print(f"❌ Delta WS error: {e}")
            print("🔁 Reconnecting in 3s...")
            await asyncio.sleep(3)

# ===============================
# STARTUP
# ===============================
@app.on_event("startup")
async def startup():
    print("🚀 Backend starting (Public Market Data)...")
    asyncio.create_task(delta_ws_listener())

# ===============================
# FLUTTER WEBSOCKET
# ===============================
@app.websocket("/ws/market")
async def flutter_market_ws(ws: WebSocket):
    await ws.accept()
    print("📱 Flutter connected (market WS)")

    try:
        while True:
            await ws.send_json({
                "ticks": latest_ticks,
                "trades": latest_trades,
            })
            await asyncio.sleep(0.1)  # 🔥 fast push (Delta-like)
    except Exception as e:
        print(f"📱 Flutter disconnected: {e}")

# ===============================
# OPTIONAL REST
# ===============================
@app.get("/market/{symbol}")
def get_market(symbol: str):
    symbol = symbol.upper()
    return {
        "tick": latest_ticks.get(symbol),
        "trades": latest_trades.get(symbol, []),
    }
