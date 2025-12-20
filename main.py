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

# ✅ NEW IMPORTS
from analysis_worker import AnalysisWorker

# ===============================
# LOGGING (Backend monitoring ke liye)
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
# GLOBAL STATE (Saara data yahan store hoga)
# ===============================
latest_ticks: Dict[str, Dict[str, Any]] = {
    s: {
        "symbol": s,
        "price": None,
        "mark_price": None,
        "ltp": None,
        "open_24h": None,
        "high_24h": None,
        "low_24h": None,
        "volume_24h": None,
        "change_percent": 0.0,
        "bid": None,
        "ask": None,
        "spread": None,
        "timestamp": None,
    }
    for s in SYMBOLS
}

active_clients: Set[WebSocket] = set()
is_delta_connected = False

# ✅ NEW: Analysis Worker Instance
analysis_worker = AnalysisWorker(
    symbols=SYMBOLS,
    resolution="1h",      # 1 hour timeframe
    update_interval=300   # Update every 5 minutes
)

# ===============================
# FIRESTORE (Agar use karna ho toh)
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

                    # RAW DATA EXTRACTION
                    mark_price = data.get("mark_price")
                    ltp = data.get("close")
                    open_24h = data.get("open")
                    high_24h = data.get("high")
                    low_24h = data.get("low")
                    volume_24h = data.get("volume")
                    
                    quotes = data.get("quotes") or {}
                    bid = quotes.get("best_bid")
                    ask = quotes.get("best_ask")

                    # CALCULATIONS
                    display_price = None
                    if ltp:
                        display_price = float(ltp)
                    elif bid and ask:
                        display_price = (float(bid) + float(ask)) / 2
                    elif mark_price:
                        display_price = float(mark_price)
                    else:
                        display_price = 0.0
                    
                    change_pct = 0.0
                    if open_24h and ltp:
                        try:
                            o = float(open_24h)
                            c = float(ltp)
                            if o > 0:
                                change_pct = ((c - o) / o) * 100
                        except (ValueError, ZeroDivisionError):
                            change_pct = 0.0

                    spread = 0.0
                    if bid and ask:
                        try:
                            spread = float(ask) - float(bid)
                        except ValueError:
                            spread = 0.0

                    # UPDATE GLOBAL STATE
                    latest_ticks[symbol] = {
                        "symbol": symbol,
                        "price": round(display_price, 2) if display_price else None,
                        "mark_price": float(mark_price) if mark_price else None,
                        "ltp": float(ltp) if ltp else None,
                        "open_24h": float(open_24h) if open_24h else None,
                        "high_24h": float(high_24h) if high_24h else None,
                        "low_24h": float(low_24h) if low_24h else None,
                        "volume_24h": float(volume_24h) if volume_24h else None,
                        "change_percent": round(change_pct, 2),
                        "bid": float(bid) if bid else None,
                        "ask": float(ask) if ask else None,
                        "spread": round(spread, 4) if spread else None,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }

                    await broadcast()

        except websockets.exceptions.ConnectionClosed:
            is_delta_connected = False
            logger.warning("⚠️ Delta WS connection closed, reconnecting...")
            await asyncio.sleep(5)
        except Exception as e:
            is_delta_connected = False
            logger.error(f"❌ Delta WS error: {e}")
            await asyncio.sleep(5)

# ===============================
# BROADCAST TO CLIENTS
# ===============================
async def broadcast():
    if not active_clients:
        return

    # ✅ NEW: Include indicators data in broadcast
    indicators_data = analysis_worker.get_all_indicators()

    payload = {
        "status": "connected" if is_delta_connected else "reconnecting",
        "ticks": latest_ticks,
        "indicators": indicators_data,  # ✅ EMA/MA data added
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
# FASTAPI SETUP
# ===============================
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_firestore()
    
    # Start Delta WebSocket
    async def start_ws():
        await asyncio.sleep(2) 
        await delta_ws_listener()
    ws_task = asyncio.create_task(start_ws())
    
    # ✅ NEW: Start Analysis Worker
    analysis_task = asyncio.create_task(analysis_worker.start())
    
    yield
    
    # Cleanup
    ws_task.cancel()
    analysis_task.cancel()
    analysis_worker.stop()

app = FastAPI(title="Delta Advanced Market API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {
        "status": "up",
        "symbols": SYMBOLS,
        "data": latest_ticks,
        "indicators": analysis_worker.get_all_indicators(),  # ✅ NEW
    }

# ✅ NEW: Dedicated endpoint for indicators
@app.get("/indicators")
def get_indicators():
    """Get EMA and MA indicators for all symbols"""
    return {
        "status": "success",
        "data": analysis_worker.get_all_indicators(),
    }

# ✅ NEW: Get indicators for specific symbol
@app.get("/indicators/{symbol}")
def get_symbol_indicators(symbol: str):
    """Get EMA and MA indicators for a specific symbol"""
    if symbol not in SYMBOLS:
        return {"status": "error", "message": f"Symbol {symbol} not found"}
    
    data = analysis_worker.get_indicators(symbol)
    if not data:
        return {"status": "error", "message": "Indicators not yet calculated"}
    
    return {
        "status": "success",
        "data": data,
    }

@app.websocket("/ws/market")
async def ws_market(ws: WebSocket):
    await ws.accept()
    active_clients.add(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        active_clients.discard(ws)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
