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

# ✅ Import with error handling
try:
    from analysis_worker import AnalysisWorker
    ANALYSIS_ENABLED = True
except ImportError as e:
    logging.warning(f"⚠️ Analysis worker disabled: {e}")
    ANALYSIS_ENABLED = False

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

# ✅ Railway PORT support
PORT = int(os.environ.get("PORT", 8000))

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

# ✅ Analysis Worker (with error handling)
analysis_worker = None
if ANALYSIS_ENABLED:
    try:
        analysis_worker = AnalysisWorker(
            symbols=SYMBOLS,
            resolution="1h",
            update_interval=300  # 5 minutes
        )
        logger.info("✅ Analysis worker initialized")
    except Exception as e:
        logger.error(f"❌ Failed to initialize analysis worker: {e}")
        ANALYSIS_ENABLED = False

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
    
    retry_count = 0
    max_retries = 5

    while retry_count < max_retries:
        try:
            logger.info(f"🔄 Connecting to Delta WebSocket (attempt {retry_count + 1}/{max_retries})...")
            
            async with websockets.connect(
                DELTA_WS_URL,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10,
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
                retry_count = 0  # Reset on successful connection
                logger.info("✅ Delta WS connected")

                async for msg in ws:
                    try:
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
                        
                    except json.JSONDecodeError:
                        logger.warning("⚠️ Invalid JSON received from Delta WS")
                        continue
                    except Exception as e:
                        logger.error(f"❌ Error processing message: {e}")
                        continue

        except websockets.exceptions.ConnectionClosed:
            is_delta_connected = False
            retry_count += 1
            logger.warning(f"⚠️ Delta WS connection closed, retrying in 5s...")
            await asyncio.sleep(5)
            
        except Exception as e:
            is_delta_connected = False
            retry_count += 1
            logger.error(f"❌ Delta WS error: {e}")
            await asyncio.sleep(5)
    
    logger.error("❌ Max retries reached. WebSocket listener stopped.")

# ===============================
# BROADCAST TO CLIENTS
# ===============================
async def broadcast():
    if not active_clients:
        return

    # Get indicators data if available
    indicators_data = {}
    if ANALYSIS_ENABLED and analysis_worker:
        try:
            indicators_data = analysis_worker.get_all_indicators()
        except Exception as e:
            logger.error(f"❌ Error getting indicators: {e}")

    payload = {
        "status": "connected" if is_delta_connected else "reconnecting",
        "ticks": latest_ticks,
        "indicators": indicators_data,
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
    """Application lifespan manager"""
    logger.info("🚀 Starting application...")
    
    # Initialize Firestore
    init_firestore()
    
    # Start Delta WebSocket
    ws_task = asyncio.create_task(delta_ws_listener())
    
    # Start Analysis Worker (if enabled)
    analysis_task = None
    if ANALYSIS_ENABLED and analysis_worker:
        try:
            analysis_task = asyncio.create_task(analysis_worker.start())
            logger.info("✅ Analysis worker started")
        except Exception as e:
            logger.error(f"❌ Failed to start analysis worker: {e}")
    
    logger.info(f"✅ Application started on port {PORT}")
    
    yield
    
    # Cleanup
    logger.info("🛑 Shutting down application...")
    ws_task.cancel()
    if analysis_task:
        analysis_task.cancel()
    if analysis_worker:
        analysis_worker.stop()
    logger.info("✅ Application stopped")

app = FastAPI(
    title="Delta Advanced Market API",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===============================
# HEALTH CHECK
# ===============================
@app.get("/health")
def health_check():
    """Health check endpoint for Railway"""
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "websocket_connected": is_delta_connected,
        "analysis_enabled": ANALYSIS_ENABLED,
    }

# ===============================
# ROOT ENDPOINT
# ===============================
@app.get("/")
def root():
    indicators_data = {}
    if ANALYSIS_ENABLED and analysis_worker:
        try:
            indicators_data = analysis_worker.get_all_indicators()
        except Exception as e:
            logger.error(f"Error getting indicators: {e}")
    
    return {
        "status": "up",
        "symbols": SYMBOLS,
        "data": latest_ticks,
        "indicators": indicators_data,
        "websocket_connected": is_delta_connected,
        "analysis_enabled": ANALYSIS_ENABLED,
    }

# ===============================
# INDICATORS ENDPOINTS
# ===============================
@app.get("/indicators")
def get_indicators():
    """Get EMA and MA indicators for all symbols"""
    if not ANALYSIS_ENABLED or not analysis_worker:
        return {
            "status": "disabled",
            "message": "Analysis worker is not enabled"
        }
    
    try:
        return {
            "status": "success",
            "data": analysis_worker.get_all_indicators(),
        }
    except Exception as e:
        logger.error(f"Error in /indicators: {e}")
        return {
            "status": "error",
            "message": str(e)
        }

@app.get("/indicators/{symbol}")
def get_symbol_indicators(symbol: str):
    """Get EMA and MA indicators for a specific symbol"""
    if not ANALYSIS_ENABLED or not analysis_worker:
        return {
            "status": "disabled",
            "message": "Analysis worker is not enabled"
        }
    
    if symbol not in SYMBOLS:
        return {"status": "error", "message": f"Symbol {symbol} not found"}
    
    try:
        data = analysis_worker.get_indicators(symbol)
        if not data:
            return {"status": "error", "message": "Indicators not yet calculated"}
        
        return {
            "status": "success",
            "data": data,
        }
    except Exception as e:
        logger.error(f"Error in /indicators/{symbol}: {e}")
        return {
            "status": "error",
            "message": str(e)
        }

# ===============================
# WEBSOCKET ENDPOINT
# ===============================
@app.websocket("/ws/market")
async def ws_market(ws: WebSocket):
    await ws.accept()
    active_clients.add(ws)
    logger.info(f"✅ Client connected. Total clients: {len(active_clients)}")
    
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        active_clients.discard(ws)
        logger.info(f"❌ Client disconnected. Total clients: {len(active_clients)}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        active_clients.discard(ws)

# ===============================
# RUN SERVER
# ===============================
if __name__ == "__main__":
    import uvicorn
    
    logger.info(f"🚀 Starting server on 0.0.0.0:{PORT}")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=PORT,
        log_level="info",
        access_log=True,
    )
