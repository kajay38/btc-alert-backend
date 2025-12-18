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
# LOGGING SETUP (Logging ko configure karein)
# ===============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DeltaWS")

# ===============================
# CONFIG & STATE (Initial Config)
# ===============================
DELTA_WS_URL = "wss://socket.delta.exchange"
SYMBOLS = ["BTCUSD", "ETHUSD", "SOLUSD"]
MAX_TRADES = 20

# Global stores (Price aur Trades ka data store karne ke liye)
latest_ticks: Dict[str, Dict[str, Any]] = {
    s: {
        "symbol": s,
        "price": None,
        "ltp": None,
        "mark_price": None,
        "bid": None,
        "ask": None,
        "timestamp": None
    } 
    for s in SYMBOLS
}
latest_trades: Dict[str, List[Dict[str, Any]]] = {s: [] for s in SYMBOLS}
active_connections: Set[WebSocket] = set() # Flutter clients ki list
is_delta_connected = False

# ===============================
# BROADCAST LOGIC (Tick-by-Tick Push)
# ===============================
async def broadcast_update():
    """
    Jaise hi Delta se naya data aaye, use turant saare Flutter clients ko bhej dega.
    """
    if not active_connections:
        return
        
    payload = {
        "ticks": latest_ticks,
        "trades": latest_trades,
        "status": "connected" if is_delta_connected else "reconnecting",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    # Saare connected clients ko parallel mein data bhejein
    disconnected = set()
    for ws in active_connections:
        try:
            await ws.send_json(payload)
        except Exception:
            disconnected.add(ws)
            
    # Purane/Toote huye connections ko remove karein
    for ws in disconnected:
        active_connections.remove(ws)

# ===============================
# DELTA WEBSOCKET LISTENER
# ===============================
async def delta_ws_listener():
    """
    Delta Exchange se connect hota hai aur live market data fetch karta hai.
    """
    global is_delta_connected
    
    while True:
        try:
            logger.info(f"🔄 Connecting to Delta: {DELTA_WS_URL}")
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
                            {"name": "all_trades", "symbols": SYMBOLS}
                        ]
                    }
                }
                
                await ws.send(json.dumps(subscribe_msg))
                logger.info(f"📡 Subscription sent for: {SYMBOLS}")

                async for message in ws:
                    try:
                        msg = json.loads(message)
                        
                        if msg.get("type") == "subscriptions":
                            is_delta_connected = True
                            logger.info("✅ Delta subscription ACTIVE")
                            continue

                        msg_type = msg.get("type")
                        symbol = msg.get("symbol")

                        if not symbol or symbol not in SYMBOLS:
                            continue

                        # Handle Ticker Update
                        if msg_type == "v2/ticker":
                            ltp = msg.get("close")
                            mark = msg.get("mark_price")
                            quotes = msg.get("quotes", {})
                            bid = quotes.get("best_bid") if quotes else None
                            ask = quotes.get("best_ask") if quotes else None

                            # Mid price calculate karein
                            price = (float(bid) + float(ask)) / 2 if bid and ask else float(ltp or mark or 0)

                            latest_ticks[symbol] = {
                                "symbol": symbol,
                                "price": price,
                                "ltp": float(ltp) if ltp else None,
                                "mark_price": float(mark) if mark else None,
                                "bid": float(bid) if bid else None,
                                "ask": float(ask) if ask else None,
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                            }
                            
                            # ✅ TICK-BY-TICK BROADCAST: Jaise hi update aaye, push karein
                            await broadcast_update()

                        # Handle Trades
                        elif msg_type == "all_trades":
                            trade = {
                                "price": float(msg.get("price", 0)),
                                "size": msg.get("size"),
                                "side": msg.get("buyer_role"),
                                "timestamp": msg.get("timestamp"),
                            }
                            latest_trades[symbol].insert(0, trade)
                            latest_trades[symbol] = latest_trades[symbol][:MAX_TRADES]
                            await broadcast_update()

                    except Exception as e:
                        logger.error(f"❌ Msg Processing Error: {e}")

        except Exception as e:
            is_delta_connected = False
            logger.error(f"❌ Connection Error: {e}")
            await asyncio.sleep(5)

# ===============================
# FASTAPI APPLICATION
# ===============================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Background task start karein
    task = asyncio.create_task(delta_ws_listener())
    logger.info("🚀 Delta Listener Started")
    yield
    task.cancel()
    logger.info("🛑 Delta Listener Stopped")

app = FastAPI(title="Delta Market Pro", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def health():
    return {"status": "online", "delta_connected": is_delta_connected}

# ===============================
# FLUTTER WEBSOCKET ENDPOINT
# ===============================
@app.websocket("/ws/market")
async def market_data_stream(websocket: WebSocket):
    await websocket.accept()
    active_connections.add(websocket)
    logger.info(f"📱 New Flutter Client connected. Total: {len(active_connections)}")
    
    try:
        while True:
            # Client se message receive karein (connection alive rakhne ke liye)
            # Lekin broadcasting `broadcast_update` function handle karta hai
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_connections.remove(websocket)
        logger.info(f"📱 Client disconnected. Remaining: {len(active_connections)}")
    except Exception as e:
        if websocket in active_connections:
            active_connections.remove(websocket)
        logger.error(f"📱 WS Error: {e}")

# ===============================
# RUN SERVER
# ===============================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
