import asyncio
import json
import os
import base64
import logging
import signal
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
# GLOBAL STATE (Enhanced with today + previous day data)
# ===============================
latest_ticks: Dict[str, Dict[str, Any]] = {
    s: {
        "symbol": s,
        "price": None,
        "mark_price": None,
        "ltp": None,
        
        # TODAY'S DATA
        "today": {
            "open": None,
            "high": None,
            "low": None,
            "close": None,
            "volume": None,
            "change_value": None,
            "change_percent": None,
        },
        
        # PREVIOUS DAY'S DATA
        "previous_day": {
            "open": None,
            "high": None,
            "low": None,
            "close": None,
            "volume": None,
        },
        
        # ORDER BOOK
        "bid": None,
        "ask": None,
        "spread": None,
        
        "timestamp": None,
    }
    for s in SYMBOLS
}

# Store previous day's close for comparison
previous_day_close: Dict[str, float] = {s: None for s in SYMBOLS}

active_clients: Set[WebSocket] = set()
is_delta_connected = False
shutdown_event = asyncio.Event()

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
    """Initialize Firestore connection"""
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
# HELPER: Store Previous Day Data
# ===============================
def store_previous_day_data(symbol: str, data: dict):
    """
    Store previous day's data at day rollover (00:00 UTC)
    Call this function when you detect a new day
    """
    global previous_day_close
    
    if latest_ticks[symbol]["today"]["close"]:
        previous_day_close[symbol] = latest_ticks[symbol]["today"]["close"]
        
        latest_ticks[symbol]["previous_day"] = {
            "open": latest_ticks[symbol]["today"]["open"],
            "high": latest_ticks[symbol]["today"]["high"],
            "low": latest_ticks[symbol]["today"]["low"],
            "close": latest_ticks[symbol]["today"]["close"],
            "volume": latest_ticks[symbol]["today"]["volume"],
        }
        
        logger.info(f"📅 Stored previous day data for {symbol}")

# ===============================
# DELTA WEBSOCKET LISTENER (ENHANCED)
# ===============================
async def delta_ws_listener():
    """
    Connects to Delta Exchange WebSocket and streams market data.
    Now tracks both today's and previous day's data.
    """
    global is_delta_connected
    
    reconnect_delay = 5  # seconds
    current_day = datetime.now(timezone.utc).day
    
    while not shutdown_event.is_set():
        try:
            logger.info(f"🔄 Connecting to Delta WebSocket...")
            
            async with websockets.connect(
                DELTA_WS_URL,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10,
            ) as ws:

                # Subscribe to ticker channels
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
                logger.info("✅ Delta WS connected and subscribed")

                # Listen for messages
                async for msg in ws:
                    if shutdown_event.is_set():
                        break
                        
                    try:
                        data = json.loads(msg)
                        symbol = data.get("symbol")
                        
                        if symbol not in SYMBOLS:
                            continue

                        # Check for day rollover
                        now = datetime.now(timezone.utc)
                        if now.day != current_day:
                            for sym in SYMBOLS:
                                store_previous_day_data(sym, data)
                            current_day = now.day

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
                        
                        # Calculate change (absolute and percentage)
                        change_value = 0.0
                        change_pct = 0.0
                        if open_24h and ltp:
                            try:
                                o = float(open_24h)
                                c = float(ltp)
                                if o > 0:
                                    change_value = c - o
                                    change_pct = (change_value / o) * 100
                            except (ValueError, ZeroDivisionError):
                                change_value = 0.0
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
                            
                            # TODAY'S DATA
                            "today": {
                                "open": float(open_24h) if open_24h else None,
                                "high": float(high_24h) if high_24h else None,
                                "low": float(low_24h) if low_24h else None,
                                "close": float(ltp) if ltp else None,
                                "volume": float(volume_24h) if volume_24h else None,
                                "change_value": round(change_value, 2),
                                "change_percent": round(change_pct, 2),
                            },
                            
                            # PREVIOUS DAY'S DATA (preserved from last rollover)
                            "previous_day": latest_ticks[symbol]["previous_day"],
                            
                            # ORDER BOOK
                            "bid": float(bid) if bid else None,
                            "ask": float(ask) if ask else None,
                            "spread": round(spread, 4) if spread else None,
                            
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }

                        # Broadcast to all connected clients
                        await broadcast()
                        
                    except json.JSONDecodeError:
                        logger.warning("⚠️ Invalid JSON received from Delta WS")
                        continue
                    except Exception as e:
                        logger.error(f"❌ Error processing message: {e}")
                        continue

        except websockets.exceptions.ConnectionClosed as e:
            is_delta_connected = False
            logger.warning(f"⚠️ Delta WS connection closed: {e}")
            
        except websockets.exceptions.WebSocketException as e:
            is_delta_connected = False
            logger.error(f"❌ Delta WS error: {e}")
            
        except Exception as e:
            is_delta_connected = False
            logger.error(f"❌ Unexpected error in Delta WS: {e}")
        
        # Wait before reconnecting (unless shutting down)
        if not shutdown_event.is_set():
            logger.info(f"🔄 Reconnecting in {reconnect_delay} seconds...")
            try:
                await asyncio.wait_for(
                    shutdown_event.wait(), 
                    timeout=reconnect_delay
                )
            except asyncio.TimeoutError:
                pass  # Continue to reconnect
    
    logger.info("🛑 Delta WS listener stopped")

# ===============================
# BROADCAST TO CLIENTS
# ===============================
async def broadcast():
    """Broadcast market data to all connected WebSocket clients"""
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
        except Exception as e:
            logger.warning(f"⚠️ Failed to send to client: {e}")
            dead.add(ws)

    # Remove dead connections
    for ws in dead:
        active_clients.discard(ws)

# ===============================
# GRACEFUL SHUTDOWN HANDLER
# ===============================
def handle_shutdown(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"🛑 Received shutdown signal: {signum}")
    shutdown_event.set()

# Register signal handlers
signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

# ===============================
# FASTAPI SETUP (FIXED LIFESPAN)
# ===============================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager with proper cleanup"""
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
    
    # ===============================
    # CLEANUP ON SHUTDOWN
    # ===============================
    logger.info("🛑 Shutting down application...")
    
    # Signal shutdown to all tasks
    shutdown_event.set()
    
    # Cancel WebSocket task
    if ws_task and not ws_task.done():
        ws_task.cancel()
        try:
            await asyncio.wait_for(ws_task, timeout=5.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            logger.info("✅ WebSocket task cancelled")
    
    # Cancel Analysis task
    if analysis_task and not analysis_task.done():
        analysis_task.cancel()
        try:
            await asyncio.wait_for(analysis_task, timeout=5.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            logger.info("✅ Analysis task cancelled")
    
    # Stop analysis worker
    if analysis_worker:
        try:
            analysis_worker.stop()
            logger.info("✅ Analysis worker stopped")
        except Exception as e:
            logger.error(f"❌ Error stopping analysis worker: {e}")
    
    # Close all WebSocket connections
    for ws in list(active_clients):
        try:
            await ws.close()
        except Exception:
            pass
    active_clients.clear()
    
    logger.info("✅ Application stopped gracefully")

app = FastAPI(
    title="Delta Advanced Market API",
    description="Real-time market data from Delta Exchange with technical indicators",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===============================
# HEALTH CHECK
# ===============================
@app.get("/health")
def health_check():
    """Health check endpoint for Railway and monitoring"""
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "websocket_connected": is_delta_connected,
        "analysis_enabled": ANALYSIS_ENABLED,
        "active_clients": len(active_clients),
        "symbols": SYMBOLS,
    }

# ===============================
# ROOT ENDPOINT
# ===============================
@app.get("/")
def root():
    """Get current market data and indicators"""
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
        "active_clients": len(active_clients),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

# ===============================
# MARKET DATA ENDPOINTS
# ===============================
@app.get("/market/{symbol}")
def get_market_data(symbol: str):
    """Get market data for a specific symbol"""
    symbol = symbol.upper()
    
    if symbol not in SYMBOLS:
        return {
            "status": "error",
            "message": f"Symbol {symbol} not found. Available symbols: {', '.join(SYMBOLS)}"
        }
    
    return {
        "status": "success",
        "data": latest_ticks.get(symbol),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

# ===============================
# NEW: COMPARISON ENDPOINT
# ===============================
@app.get("/market/{symbol}/comparison")
def get_comparison(symbol: str):
    """Compare today's data with previous day"""
    symbol = symbol.upper()
    
    if symbol not in SYMBOLS:
        return {
            "status": "error",
            "message": f"Symbol {symbol} not found. Available symbols: {', '.join(SYMBOLS)}"
        }
    
    data = latest_ticks.get(symbol)
    today = data["today"]
    prev = data["previous_day"]
    
    # Calculate day-over-day changes
    comparison = {}
    if prev["close"] and today["close"]:
        comparison["close_change"] = round(today["close"] - prev["close"], 2)
        comparison["close_change_percent"] = round(
            ((today["close"] - prev["close"]) / prev["close"]) * 100, 2
        )
    
    if prev["high"] and today["high"]:
        comparison["high_change"] = round(today["high"] - prev["high"], 2)
    
    if prev["low"] and today["low"]:
        comparison["low_change"] = round(today["low"] - prev["low"], 2)
    
    if prev["volume"] and today["volume"]:
        comparison["volume_change_percent"] = round(
            ((today["volume"] - prev["volume"]) / prev["volume"]) * 100, 2
        )
    
    return {
        "status": "success",
        "symbol": symbol,
        "today": today,
        "previous_day": prev,
        "comparison": comparison,
        "timestamp": datetime.now(timezone.utc).isoformat(),
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
            "timestamp": datetime.now(timezone.utc).isoformat(),
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
    symbol = symbol.upper()
    
    if not ANALYSIS_ENABLED or not analysis_worker:
        return {
            "status": "disabled",
            "message": "Analysis worker is not enabled"
        }
    
    if symbol not in SYMBOLS:
        return {
            "status": "error",
            "message": f"Symbol {symbol} not found. Available symbols: {', '.join(SYMBOLS)}"
        }
    
    try:
        data = analysis_worker.get_indicators(symbol)
        if not data:
            return {
                "status": "error",
                "message": "Indicators not yet calculated. Please wait a few moments."
            }
        
        return {
            "status": "success",
            "data": data,
            "timestamp": datetime.now(timezone.utc).isoformat(),
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
    """WebSocket endpoint for real-time market data streaming"""
    await ws.accept()
    active_clients.add(ws)
    logger.info(f"✅ Client connected. Total clients: {len(active_clients)}")
    
    try:
        # Send initial data immediately
        await broadcast()
        
        # Keep connection alive and listen for client messages
        while True:
            try:
                # Wait for client messages (ping/pong or commands)
                await asyncio.wait_for(ws.receive_text(), timeout=30.0)
            except asyncio.TimeoutError:
                # Send ping to keep connection alive
                try:
                    await ws.send_json({"type": "ping"})
                except Exception:
                    break
                    
    except WebSocketDisconnect:
        logger.info(f"❌ Client disconnected gracefully")
    except Exception as e:
        logger.error(f"❌ WebSocket error: {e}")
    finally:
        active_clients.discard(ws)
        logger.info(f"📊 Total clients: {len(active_clients)}")

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
        timeout_keep_alive=75,
    )
