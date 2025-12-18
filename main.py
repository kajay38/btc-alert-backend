import asyncio
import json
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Set

import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# ===============================
# LOGGING SETUP
# ===============================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DeltaWS")

# ===============================
# CONFIG & STATE
# ===============================
DELTA_WS_URL = "wss://socket.delta.exchange"
SYMBOLS = ["BTCUSD", "ETHUSD", "SOLUSD"]
MAX_TRADES = 20

# Global stores
latest_ticks: Dict[str, Dict[str, Any]] = {s: {"symbol": s, "price": None} for s in SYMBOLS}
latest_trades: Dict[str, List[Dict[str, Any]]] = {s: [] for s in SYMBOLS}
active_connections: Set[WebSocket] = set()

# ===============================
# BROADCAST LOGIC
# ===============================
async def broadcast_update():
    """Sends the latest data to all connected Flutter clients immediately."""
    if not active_connections:
        return
        
    payload = {
        "ticks": latest_ticks,
        "trades": latest_trades,
        "status": "connected",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    # Create tasks for all connections to send in parallel
    disconnected = set()
    for ws in active_connections:
        try:
            await ws.send_json(payload)
        except Exception:
            disconnected.add(ws)
            
    for ws in disconnected:
        active_connections.remove(ws)

# ===============================
# DELTA WEBSOCKET LISTENER
# ===============================
async def delta_ws_listener():
    while True:
        try:
            async with websockets.connect(DELTA_WS_URL) as ws:
                subscribe_msg = {
                    "type": "subscribe",
                    "payload": {"channels": [{"name": "v2/ticker", "symbols": SYMBOLS}]}
                }
                await ws.send(json.dumps(subscribe_msg))

                async for message in ws:
                    msg = json.loads(message)
                    symbol = msg.get("symbol")
                    
                    if msg.get("type") == "v2/ticker" and symbol in SYMBOLS:
                        # Update logic
                        ltp = msg.get("close")
                        quotes = msg.get("quotes", {})
                        bid = quotes.get("best_bid")
                        ask = quotes.get("best_ask")

                        latest_ticks[symbol] = {
                            "symbol": symbol,
                            "price": (float(bid) + float(ask))/2 if bid and ask else float(ltp or 0),
                            "ltp": float(ltp) if ltp else None,
                            "bid": float(bid) if bid else None,
                            "ask": float(ask) if ask else None,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }
                        
                        # ✅ TICK-BY-TICK: Jaise hi update aaya, broadcast karo
                        await broadcast_update()

        except Exception as e:
            logger.error(f"Delta WS Error: {e}")
            await asyncio.sleep(2)

# ===============================
# FASTAPI
# ===============================
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(delta_ws_listener())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"])

@app.websocket("/ws/market")
async def market_data_stream(websocket: WebSocket):
    await websocket.accept()
    active_connections.add(websocket)
    try:
        while True:
            # Just keep the connection alive, broadcast_update handles the sending
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_connections.remove(websocket)
