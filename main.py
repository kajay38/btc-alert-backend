import asyncio
import json
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Dict, Any, List

import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DeltaWS")

# ===============================
# CONFIG & STATE
# ===============================
DELTA_WS_URL = "wss://socket.delta.exchange"
SYMBOLS = ["BTCUSD", "ETHUSD", "SOLUSD"]
MAX_TRADES = 20

# Global stores
latest_ticks: Dict[str, Dict[str, Any]] = {
    s: {"symbol": s, "price": None, "ltp": None, "mark_price": None, "bid": None, "ask": None, "timestamp": None} 
    for s in SYMBOLS
}
latest_trades: Dict[str, List[Dict[str, Any]]] = {s: [] for s in SYMBOLS}
is_delta_connected = False

# ===============================
# DELTA WS LOGIC
# ===============================
async def delta_ws_listener():
    global is_delta_connected
    
    while True:
        try:
            logger.info("🔄 Connecting to Delta Exchange WebSocket...")
            async with websockets.connect(
                DELTA_WS_URL,
                ping_interval=20,
                ping_timeout=20,
            ) as ws:
                
                subscribe_msg = {
                    "type": "subscribe",
                    "payload": {
                        "channels": [
                            {"name": "v2/ticker", "symbols": SYMBOLS},
                            {"name": "v2/trades", "symbols": SYMBOLS},
                        ]
                    },
                }
                await ws.send(json.dumps(subscribe_msg))
                logger.info("✅ Subscribed to Delta Channels")

                async for message in ws:
                    msg = json.loads(message)
                    
                    if msg.get("type") == "subscriptions":
                        is_delta_connected = True
                        continue

                    channel = msg.get("channel")
                    symbol = msg.get("symbol")
                    data = msg.get("data")

                    if not symbol or symbol not in SYMBOLS or not data:
                        continue

                    # --- TICKER UPDATE ---
                    if channel == "v2/ticker":
                        ltp = data.get("close")
                        bid = data.get("best_bid")
                        ask = data.get("best_ask")
                        mark = data.get("mark_price")

                        # Mid-price calculation for smooth UI
                        try:
                            if bid and ask:
                                price = (float(bid) + float(ask)) / 2
                            else:
                                price = float(ltp or mark or 0)
                        except (ValueError, TypeError):
                            price = 0

                        latest_ticks[symbol] = {
                            "symbol": symbol,
                            "price": price,
                            "ltp": float(ltp) if ltp else None,
                            "mark_price": float(mark) if mark else None,
                            "bid": float(bid) if bid else None,
                            "ask": float(ask) if ask else None,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }

                    # --- TRADES UPDATE ---
                    elif channel == "v2/trades":
                        trade = {
                            "price": data.get("price"),
                            "size": data.get("size"),
                            "side": data.get("side"),
                            "timestamp": data.get("timestamp"),
                        }
                        # Atomic update to list
                        trades_list = latest_trades[symbol]
                        trades_list.insert(0, trade)
                        latest_trades[symbol] = trades_list[:MAX_TRADES]

        except Exception as e:
            is_delta_connected = False
            logger.error(f"❌ Delta WS Connection Lost: {e}")
            await asyncio.sleep(5) # Wait before reconnecting

# ===============================
# FASTAPI LIFESPAN & APP
# ===============================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Start background task
    task = asyncio.create_task(delta_ws_listener())
    yield
    # Shutdown: Clean up
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("Background task stopped")

app = FastAPI(title="Delta Market Pro", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===============================
# ENDPOINTS
# ===============================
@app.get("/")
async def health_check():
    return {
        "status": "online" if is_delta_connected else "reconnecting",
        "symbols": SYMBOLS,
        "server_time": datetime.now(timezone.utc).isoformat()
    }

@app.websocket("/ws/market")
async def market_data_stream(websocket: WebSocket):
    await websocket.accept()
    logger.info(f"📱 Client connected: {websocket.client}")
    
    try:
        while True:
            # Send current snapshot of all symbols
            payload = {
                "type": "market_update",
                "ticks": latest_ticks,
                "trades": latest_trades,
                "status": "connected" if is_delta_connected else "connecting"
            }
            await websocket.send_json(payload)
            # 100ms update rate is fine for mobile, 
            # adjust if battery consumption is too high on Flutter
            await asyncio.sleep(0.1) 
            
    except WebSocketDisconnect:
        logger.info("📱 Client disconnected")
    except Exception as e:
        logger.error(f"📱 WebSocket Error: {e}")

@app.get("/market/{symbol}")
async def get_symbol_data(symbol: str):
    s = symbol.upper()
    if s not in SYMBOLS:
        return {"error": "Invalid symbol"}
    return {
        "tick": latest_ticks.get(s),
        "recent_trades": latest_trades.get(s, [])
    }
