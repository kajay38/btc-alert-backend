import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Set

import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

# ===============================
# LOGGING
# ===============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("DELTA")

# ===============================
# CONFIG (SOL Removed)
# ===============================
DELTA_WS_URL = "wss://socket.delta.exchange"
SYMBOLS = ["BTCUSD", "ETHUSD"]  # SOL removed as requested
MAX_TRADES = 20

# ===============================
# GLOBAL STATE
# ===============================
latest_ticks: Dict[str, Dict[str, Any]] = {
    s: {
        "symbol": s,
        "price": None,
        "ltp": None,
        "mark_price": None,
        "spot_price": None,
        "bid": None,
        "ask": None,
        "timestamp": None,
    }
    for s in SYMBOLS
}

latest_trades: Dict[str, List[Dict[str, Any]]] = {s: [] for s in SYMBOLS}
active_clients: Set[WebSocket] = set()
is_delta_connected = False

# ===============================
# BROADCAST
# ===============================
async def broadcast():
    if not active_clients:
        return

    payload = {
        "ticks": latest_ticks,
        "trades": latest_trades,
        "status": "connected" if is_delta_connected else "reconnecting",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    disconnected = set()
    for ws in active_clients:
        try:
            await ws.send_json(payload)
        except Exception:
            disconnected.add(ws)

    for ws in disconnected:
        active_clients.discard(ws)

# ===============================
# DELTA WS LISTENER
# ===============================
async def delta_ws_listener():
    global is_delta_connected

    while True:
        try:
            async with websockets.connect(DELTA_WS_URL, ping_interval=20, ping_timeout=10) as ws:
                subscribe_msg = {
                    "type": "subscribe",
                    "payload": {
                        "channels": [
                            {"name": "v2/ticker", "symbols": SYMBOLS},
                            {"name": "all_trades", "symbols": SYMBOLS},
                        ]
                    },
                }
                await ws.send(json.dumps(subscribe_msg))
                
                async for message in ws:
                    try:
                        msg = json.loads(message)
                        msg_type = msg.get("type")

                        if msg_type == "subscriptions":
                            is_delta_connected = True
                            continue

                        symbol = msg.get("symbol")
                        if not symbol or symbol not in SYMBOLS:
                            continue

                        if msg_type == "v2/ticker":
                            quotes = msg.get("quotes", {})
                            best_bid = quotes.get("best_bid")
                            best_ask = quotes.get("best_ask")
                            close_price = msg.get("close")

                            # Simple average for price
                            price = None
                            if best_bid and best_ask:
                                price = (float(best_bid) + float(best_ask)) / 2
                            elif close_price:
                                price = float(close_price)

                            latest_ticks[symbol] = {
                                "symbol": symbol,
                                "price": price,
                                "ltp": float(close_price) if close_price else None,
                                "mark_price": float(msg.get("mark_price")) if msg.get("mark_price") else None,
                                "bid": float(best_bid) if best_bid else None,
                                "ask": float(best_ask) if best_ask else None,
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                            }
                            await broadcast()

                        elif msg_type == "all_trades":
                            trade = {
                                "price": float(msg.get("price", 0)),
                                "size": msg.get("size"),
                                "side": msg.get("buyer_role"),
                                "timestamp": msg.get("timestamp"),
                            }
                            latest_trades[symbol].insert(0, trade)
                            latest_trades[symbol] = latest_trades[symbol][:MAX_TRADES]
                            await broadcast()

                    except Exception as e:
                        logger.error(f"Error: {e}")

        except Exception as e:
            is_delta_connected = False
            await asyncio.sleep(5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(delta_ws_listener())
    yield
    task.cancel()

app = FastAPI(title="Delta Market Pro", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/")
async def health():
    return {"status": "online", "delta": is_delta_connected}

@app.websocket("/ws/market")
async def ws_market(websocket: WebSocket):
    await websocket.accept()
    active_clients.add(websocket)
    try:
        while True: await websocket.receive_text()
    except WebSocketDisconnect:
        active_clients.discard(websocket)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
