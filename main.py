import asyncio
import json
import os
import base64
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Set
from contextlib import asynccontextmanager

import websockets
import firebase_admin
from firebase_admin import credentials, firestore
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ===============================
# LOGGING
# ===============================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BACKEND")

# ===============================
# CONFIG
# ===============================
DELTA_WS_URL = "wss://socket.india.delta.exchange"
SYMBOLS = ["BTCUSD"]

# ===============================
# GLOBAL STATE
# ===============================
latest_ticks: Dict[str, Dict[str, Any]] = {
    s: {
        "symbol": s,
        "price": None,
        "timestamp": None,
    }
    for s in SYMBOLS
}

active_clients: Set[WebSocket] = set()
is_delta_connected = False

# ===============================
# FIRESTORE INIT
# ===============================
firebase_app = None
db = None

def init_firestore():
    global firebase_app, db
    if firebase_app:
        return

    key_b64 = os.environ.get("FIREBASE_KEY_BASE64")
    if not key_b64:
        raise RuntimeError("FIREBASE_KEY_BASE64 not set")

    key_json = base64.b64decode(key_b64).decode("utf-8")
    cred = credentials.Certificate(json.loads(key_json))
    firebase_app = firebase_admin.initialize_app(cred)
    db = firestore.client()
    logger.info("🔥 Firestore connected")

# ===============================
# ALERT MODEL
# ===============================
class AlertCreate(BaseModel):
    symbol: str
    type: str                 # above | below | range
    from_price: float
    to_price: Optional[float] = None
    note: str

# ===============================
# DELTA WEBSOCKET
# ===============================
async def delta_ws_listener():
    global is_delta_connected

    while True:
        try:
            logger.info("🔄 Connecting to Delta WS...")
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

                    raw_price = (
                        data.get("mark_price")
                        or data.get("spot_price")
                        or data.get("close")
                    )

                    if raw_price:
                        latest_ticks[symbol] = {
                            "symbol": symbol,
                            "price": float(raw_price),
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }

                        await broadcast()

        except Exception as e:
            is_delta_connected = False
            logger.error(f"❌ Delta WS error: {e}")
            await asyncio.sleep(5)

# ===============================
# BROADCAST TO FLUTTER
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
# FASTAPI APP (Railway-Safe)
# ===============================
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_firestore()
    task = asyncio.create_task(delta_ws_listener())
    yield
    task.cancel()

app = FastAPI(title="Delta Alerts Backend", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===============================
# HEALTH
# ===============================
@app.get("/")
def root():
    return {
        "status": "up",
        "delta_ws": "connected" if is_delta_connected else "disconnected",
        "symbols": SYMBOLS,
    }

# ===============================
# ALERTS APIs
# ===============================
@app.post("/alerts")
def create_alert(alert: AlertCreate):
    doc = {
        "symbol": alert.symbol,
        "type": alert.type,
        "from_price": alert.from_price,
        "to_price": alert.to_price,
        "note": alert.note,
        "active": True,
        "triggered": False,
        "created_at": datetime.utcnow(),
    }
    ref = db.collection("alerts").add(doc)
    return {"status": "saved", "id": ref[1].id}

@app.get("/alerts")
def read_alerts():
    alerts = []
    docs = db.collection("alerts").order_by(
        "created_at", direction=firestore.Query.DESCENDING
    ).stream()

    for d in d
