import asyncio
import json
import os
import base64
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Set
from contextlib import asynccontextmanager

import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ===============================
# LOGGING
# ===============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("DELTA-BACKEND")

# ===============================
# CONFIG
# ===============================
DELTA_WS_URL = "wss://socket.india.delta.exchange"
SYMBOLS = ["BTCUSD", "ETHUSD"]

# ===============================
# GLOBAL STATE
# ===============================
latest_ticks: Dict[str, Dict[str, Any]] = {
    s: {
        "symbol": s,
        "price": None,        # calculated display price
        "mark_price": None,
        "ltp": None,
        "bid": None,
        "ask": None,
        "timestamp": None,
    }
    for s in SYMBOLS
}

active_clients: Set[WebSocket] = set()
is_delta_connected = False

# ===============================
# FIRESTORE (OPTIONAL / SAFE)
# ===============================
db = None

def init_firestore():
    global db
    try:
        key_b64 = os.environ.get("FIREBASE_KEY_BASE64")
        if not key_b64:
            logger.warning("⚠️ FIREBASE_KEY_BASE64 not set → Firestore disabled")
            return

        import firebase_admin
        from firebase_admin import credentials, firestore

        key_json = base64.b64decode(key_b64).decode("utf-8")
        cred = credentials.Certificate(json.loads(key_json))
        firebase_admin.initialize_app(cred)
        db = firestore.client()
        logger.info("🔥 Firestore connected")

    except Exception as e:
        logger.error(f"❌ Firestore init failed: {e}")
        db = None

# ===============================
# ALERT MODEL (FOR LATER USE)
# ===============================
class AlertCreate(BaseModel):
    symbol: str
    type: str                 # above | below | range
    from_price: float
    to_price: Optional[float] = None
    note: str

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
                            {"name": "v2/ticker", "symbols": SYMBOLS}
                        ]
                    },
                }

                await ws.send(json.dumps(subscribe_msg))
                is_delta_connected = True
                logger.info("✅ Delta WS connected")

                async for msg in ws:
                    await asyncio.sleep(0)
                    data = json.loads(msg)

                    symbol = data.get("symbol")
                    if symbol not in SYMBOLS:
                        continue

                    # ---------------------------
                    # RAW FIELDS FROM DELTA
                    # ---------------------------
                    mark_price = data.get("mark_price")
                    ltp = data.get("close")           # last traded price
                    quotes = data.get("quotes") or {}
                    bid = quotes.get("best_bid")
                    ask = quotes.get("best_ask")

                    # ---------------------------
                    # CALCULATED DISPLAY PRICE
                    # ---------------------------
                    display_price = None
                    if bid and ask:
                        display_price = (float(bid) + float(ask)) / 2
                    elif mark_price:
                        display_price = float(mark_price)
                    elif ltp:
                        display_price = float(ltp)

                    # ---------------------------
                    # UPDATE GLOBAL STATE
                    # ---------------------------
                    latest_ticks[symbol] = {
                        "symbol": symbol,
                        "price": display_price,
                        "mark_price": float(mark_price) if mark_price else None,
                        "ltp": float(ltp) if ltp else None,
                        "bid": float(bid) if bid else None,
                        "ask": float(ask) if ask else None,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }

                    logger.info(
                        f"📊 {symbol} | price={display_price} "
                        f"mark={mark_price} ltp={ltp} bid/ask={bid}/{ask}"
                    )

                    await broadcast()

        except Exception as e:
            is_delta_connected = False
            logger.error(f"❌ Delta WS error: {e}")
            await asyncio.sleep(5)

# ===============================
# BROADCAST TO FLUTTER CLIENTS
# ===============================
async def broadcast():
    if not active_clients:
        return

    payload = {
        "status": "connected" if is_delta_connected else "reconnecting",
        "ticks": latest_ticks,
        "time": datetime.now(timezone.utc).isoformat(),
    }

    dead = set()
    for ws in active_clients:
        try:
            await ws.send_json(payload)
        except Exception:
            dead.add(ws)

    for ws in dead:
        active_clients.discard(ws)

# ===============================
# FASTAPI APP (RAILWAY SAFE)
# ===============================
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_firestore()

    async def start_ws():
        await asyncio.sleep(3)  # Railway health check safe
        await delta_ws_listener()

    task = asyncio.create_task(start_ws())
    yield
    task.cancel()

app = FastAPI(title="BTC Delta Market Backend", lifespan=lifespan)

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
def root():
    return {
        "status": "up",
        "delta_ws": "connected" if is_delta_connected else "disconnected",
        "symbols": SYMBOLS,
        "sample": latest_ticks,
    }

# ===============================
# FLUTTER WEBSOCKET
# ===============================
@app.websocket("/ws/market")
async def ws_market(ws: WebSocket):
    await ws.accept()
    active_clients.add(ws)
    logger.info(f"📱 Flutter client connected ({len(active_clients)})")

    try:
        while True:
            await ws.receive_text()  # keep alive
    except WebSocketDisconnect:
        active_clients.discard(ws)
        logger.info(f"📱 Flutter client disconnected ({len(active_clients)})")

# ===============================
# LOCAL RUN
# ===============================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
