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
PORT = int(os.environ.get("PORT", 8000))

# ===============================
# GLOBAL STATE
# ===============================
latest_ticks: Dict[str, Dict[str, Any]] = {
    s: {
        "symbol": s,
        "price": None,
        "mark_price": None,
        "ltp": None,
        "today": {"open": None, "high": None, "low": None, "close": None, "volume": None, "change_value": None, "change_percent": None},
        "previous_day": {"open": None, "high": None, "low": None, "close": None, "volume": None},
        "bid": None, "ask": None, "spread": None, "timestamp": None,
    }
    for s in SYMBOLS
}

active_clients: Set[WebSocket] = set()
is_delta_connected = False
shutdown_event = asyncio.Event()

# ✅ Analysis Worker
analysis_worker = None
if ANALYSIS_ENABLED:
    try:
        analysis_worker = AnalysisWorker(symbols=SYMBOLS, resolution="1h", update_interval=300)
        logger.info("✅ Analysis worker initialized")
    except Exception as e:
        logger.error(f"❌ Failed to initialize analysis worker: {e}")
        ANALYSIS_ENABLED = False

# ===============================
# DELTA WEBSOCKET LISTENER
# ===============================
async def delta_ws_listener():
    global is_delta_connected
    reconnect_delay = 5
    current_day = datetime.now(timezone.utc).day
    
    while not shutdown_event.is_set():
        try:
            async with websockets.connect(DELTA_WS_URL, ping_interval=20) as ws:
                subscribe_msg = {"type": "subscribe", "payload": {"channels": [{"name": "v2/ticker", "symbols": SYMBOLS}]}}
                await ws.send(json.dumps(subscribe_msg))
                is_delta_connected = True

                async for msg in ws:
                    if shutdown_event.is_set(): break
                    data = json.loads(msg)
                    symbol = data.get("symbol")
                    if symbol not in SYMBOLS: continue

                    now = datetime.now(timezone.utc)
                    if now.day != current_day:
                        # Day rollover logic...
                        current_day = now.day

                    # Extract data and update latest_ticks...
                    # (Mapping code from previous versions goes here)
                    ltp = data.get("close")
                    mark_price = data.get("mark_price")
                    open_24h = data.get("open")
                    
                    latest_ticks[symbol]["ltp"] = float(ltp) if ltp else None
                    latest_ticks[symbol]["mark_price"] = float(mark_price) if mark_price else None
                    latest_ticks[symbol]["today"]["open"] = float(open_24h) if open_24h else None
                    # ... update other fields ...

                    await broadcast()
        except Exception as e:
            is_delta_connected = False
            await asyncio.sleep(reconnect_delay)

# ===============================
# BROADCAST
# ===============================
async def broadcast():
    if not active_clients: return
    indicators_data = {}
    if ANALYSIS_ENABLED and analysis_worker:
        indicators_data = analysis_worker.get_all_indicators()

    payload = {
        "status": "connected" if is_delta_connected else "reconnecting",
        "ticks": latest_ticks,
        "indicators": indicators_data,
        "time": datetime.now(timezone.utc).isoformat(),
    }
    dead = set()
    for ws in active_clients:
        try: await ws.send_json(payload)
        except: dead.add(ws)
    for ws in dead: active_clients.discard(ws)

# ===============================
# WEBSOCKET ENDPOINT (UPDATED)
# ===============================
@app.websocket("/ws/market")
async def ws_market(ws: WebSocket):
    await ws.accept()
    active_clients.add(ws)
    
    try:
        await broadcast()
        while True:
            # Wait for client messages with timeout
            raw_msg = await asyncio.wait_for(ws.receive_text(), timeout=30.0)
            try:
                msg_data = json.loads(raw_msg)
                
                # Handle resolution change request
                if msg_data.get("type") == "change_resolution":
                    new_res = msg_data.get("payload")
                    if new_res and ANALYSIS_ENABLED and analysis_worker:
                        logger.info(f"⚡ Client requested timeframe change: {new_res}")
                        # Update worker resolution
                        analysis_worker.resolution = new_res
                        # Trigger an immediate update task
                        asyncio.create_task(analysis_worker.update_all_indicators())
                        
                elif msg_data.get("type") == "ping":
                    await ws.send_json({"type": "pong"})
                    
            except json.JSONDecodeError:
                continue
    except (WebSocketDisconnect, asyncio.TimeoutError):
        pass
    finally:
        active_clients.discard(ws)

# ===============================
# FASTAPI LIFESPAN & APP
# ===============================
@asynccontextmanager
async def lifespan(app: FastAPI):
    ws_task = asyncio.create_task(delta_ws_listener())
    analysis_task = None
    if ANALYSIS_ENABLED and analysis_worker:
        analysis_task = asyncio.create_task(analysis_worker.start())
    yield
    shutdown_event.set()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/")
def root(): return {"status": "up"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
