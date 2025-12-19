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
# CONFIG
# ===============================
DELTA_WS_URL = "wss://socket.india.delta.exchange"
SYMBOLS = ["BTCUSD", "ETHUSD"]
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
# BROADCAST TO FLUTTER CLIENTS
# ===============================
async def broadcast():
    if not active_clients:
        return

    payload = {
        "status": "connected" if is_delta_connected else "reconnecting",
        "ticks": latest_ticks,
        "trades": latest_trades,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    dead_clients = set()
    for ws in active_clients:
        try:
            await ws.send_json(payload)
        except Exception:
            dead_clients.add(ws)

    for ws in dead_clients:
        active_clients.discard(ws)

# ===============================
# DELTA WEBSOCKET LISTENER
# ===============================
async def delta_ws_listener():
    global is_delta_connected

    while True:
        try:
            logger.info("🔄 Connecting to Delta WebSocket...")
            async with websockets.connect(
                DELTA_WS_URL,
                ping_interval=20,
                ping_timeout=10,
            ) as ws:

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
                is_delta_connected = True
                logger.info(f"✅ Subscribed to {SYMBOLS}")

                async for message in ws:
                    await asyncio.sleep(0)  # prevent event-loop block
                    msg = json.loads(message)

                    msg_type = msg.get("type")
                    symbol = msg.get("symbol")

                    if not symbol or symbol not in SYMBOLS:
                        continue

                    # ======================
                    # TICKER UPDATE
                    # ======================
                    if msg_type == "v2/ticker":
                        bid = msg.get("quotes", {}).get("best_bid")
                        ask = msg.get("quotes", {}).get("best_ask")

                        close_price = msg.get("close")
                        mark_price = msg.get("mark_price")
                        spot_price = msg.get("spot_price")

                        # 🔥 Price priority logic
                        price = None
                        if bid and ask:
                            price = (float(bid) + float(ask)) / 2
                        elif mark_price:
                            price = float(mark_price)
                        elif close_price:
                            price = float(close_price)

                        latest_ticks[symbol] = {
                            "symbol": symbol,
                            "price": price,
                            "ltp": float(close_price) if close_price else None,
                            "mark_price": float(mark_price) if mark_price else None,
                            "spot_price": float(spot_price) if spot_price else None,
                            "bid": float(bid) if bid else None,
                            "ask": float(ask) if ask else None,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }

                        logger.info(f"📊 {symbol} → {price}")
                        await broadcast()

                    # ======================
                    # TRADE UPDATE
                    # ======================
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
            is_delta_connected = False
            logger.error(f"❌ Delta WS error: {e}")
            await asyncio.sleep(5)

# ===============================
# FASTAPI LIFESPAN (Railway Safe)
# ===============================
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(delta_ws_listener())
    yield
    task.cancel()

app = FastAPI(title="Delta Market Pro", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===============================
# HEALTH CHECK
# ===============================
@app.get("/")
async def health():
    return {
        "status": "online",
        "delta_connected": is_delta_connected,
        "symbols": SYMBOLS,
        "ticks": latest_ticks,
    }

# ===============================
# FLUTTER WEBSOCKET
# ===============================
@app.websocket("/ws/market")
async def ws_market(websocket: WebSocket):
    await websocket.accept()
    active_clients.add(websocket)
    logger.info(f"📱 Client connected ({len(active_clients)})")

    try:
        while True:
            await websocket.receive_text()  # keep alive
    except WebSocketDisconnect:
        active_clients.discard(websocket)
        logger.info(f"📱 Client disconnected ({len(active_clients)})")

# ===============================
# LOCAL RUN
# ===============================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
