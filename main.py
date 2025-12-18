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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DeltaWS")

# ===============================
# CONFIG & STATE
# ===============================
DELTA_WS_URL = "wss://socket.delta.exchange"
SYMBOLS = ["BTCUSD", "ETHUSD", "SOLUSD"]
MAX_TRADES = 20

# Global stores
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
active_connections: Set[WebSocket] = set()
is_delta_connected = False

# ===============================
# BROADCAST LOGIC
# ===============================
async def broadcast_update():
    """
    Broadcasts real-time updates to all connected Flutter clients
    """
    if not active_connections:
        return
        
    payload = {
        "ticks": latest_ticks,
        "trades": latest_trades,
        "status": "connected" if is_delta_connected else "reconnecting",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    disconnected = set()
    for ws in active_connections:
        try:
            await ws.send_json(payload)
        except Exception as e:
            logger.error(f"Broadcast error: {e}")
            disconnected.add(ws)
            
    # Remove disconnected clients
    for ws in disconnected:
        active_connections.discard(ws)

# ===============================
# DELTA WEBSOCKET LISTENER
# ===============================
async def delta_ws_listener():
    """
    Connects to Delta Exchange WebSocket and processes market data
    """
    global is_delta_connected
    
    while True:
        try:
            logger.info(f"Connecting to Delta: {DELTA_WS_URL}")
            async with websockets.connect(
                DELTA_WS_URL,
                ping_interval=20,
                ping_timeout=10,
            ) as ws:
                
                # Subscribe to channels
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
                logger.info(f"Subscription sent for: {SYMBOLS}")

                async for message in ws:
                    try:
                        msg = json.loads(message)
                        
                        # Handle subscription confirmation
                        if msg.get("type") == "subscriptions":
                            is_delta_connected = True
                            logger.info("Delta subscription ACTIVE")
                            continue

                        msg_type = msg.get("type")
                        symbol = msg.get("symbol")

                        if not symbol or symbol not in SYMBOLS:
                            continue

                        # ===== HANDLE TICKER UPDATE =====
                        if msg_type == "v2/ticker":
                            # Extract price data from ticker message
                            close_price = msg.get("close")  # Last traded price
                            mark_price = msg.get("mark_price")  # Mark price
                            spot_price = msg.get("spot_price")  # Spot price
                            
                            # Extract quotes (bid/ask)
                            quotes = msg.get("quotes", {})
                            best_bid = quotes.get("best_bid") if quotes else None
                            best_ask = quotes.get("best_ask") if quotes else None

                            # Calculate display price with proper fallback logic
                            try:
                                if best_bid and best_ask:
                                    # Mid price from bid/ask
                                    price = (float(best_bid) + float(best_ask)) / 2
                                elif close_price:
                                    # Use last traded price
                                    price = float(close_price)
                                elif mark_price:
                                    # Use mark price
                                    price = float(mark_price)
                                elif spot_price:
                                    # Use spot price as last resort
                                    price = float(spot_price)
                                else:
                                    price = None
                                    logger.warning(f"{symbol}: No valid price data available")
                            except (ValueError, TypeError) as e:
                                logger.error(f"{symbol}: Price conversion error - {e}")
                                price = None

                            # Update ticker data
                            latest_ticks[symbol] = {
                                "symbol": symbol,
                                "price": price,
                                "ltp": float(close_price) if close_price else None,
                                "mark_price": float(mark_price) if mark_price else None,
                                "spot_price": float(spot_price) if spot_price else None,
                                "bid": float(best_bid) if best_bid else None,
                                "ask": float(best_ask) if best_ask else None,
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                            }
                            
                            # Log price update for debugging
                            logger.info(
                                f"{symbol} - Price: {price}, "
                                f"LTP: {close_price}, Mark: {mark_price}, "
                                f"Bid: {best_bid}, Ask: {best_ask}"
                            )
                            
                            # Broadcast update immediately
                            await broadcast_update()

                        # ===== HANDLE TRADES =====
                        elif msg_type == "all_trades":
                            trade = {
                                "price": float(msg.get("price", 0)),
                                "size": msg.get("size"),
                                "side": msg.get("buyer_role"),  # "maker" or "taker"
                                "timestamp": msg.get("timestamp"),
                            }
                            latest_trades[symbol].insert(0, trade)
                            latest_trades[symbol] = latest_trades[symbol][:MAX_TRADES]
                            
                            logger.debug(f"{symbol} - New trade: {trade['price']} x {trade['size']}")
                            await broadcast_update()

                        # ===== HANDLE TRADES SNAPSHOT =====
                        elif msg_type == "all_trades_snapshot":
                            trades_list = msg.get("trades", [])
                            formatted_trades = [
                                {
                                    "price": float(t.get("price", 0)),
                                    "size": t.get("size"),
                                    "side": t.get("buyer_role"),
                                    "timestamp": t.get("timestamp"),
                                }
                                for t in trades_list[:MAX_TRADES]
                            ]
                            latest_trades[symbol] = formatted_trades
                            logger.info(f"{symbol} - Received {len(formatted_trades)} trades in snapshot")
                            await broadcast_update()

                    except json.JSONDecodeError as e:
                        logger.error(f"JSON decode error: {e}")
                    except Exception as e:
                        logger.error(f"Message processing error: {e}", exc_info=True)

        except websockets.exceptions.WebSocketException as e:
            is_delta_connected = False
            logger.error(f"WebSocket connection error: {e}")
            await asyncio.sleep(5)
        except Exception as e:
            is_delta_connected = False
            logger.error(f"Unexpected error: {e}", exc_info=True)
            await asyncio.sleep(5)

# ===============================
# FASTAPI APPLICATION
# ===============================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start background task
    task = asyncio.create_task(delta_ws_listener())
    logger.info("Delta Listener Started")
    yield
    task.cancel()
    logger.info("Delta Listener Stopped")

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
    return {
        "status": "online",
        "delta_connected": is_delta_connected,
        "active_clients": len(active_connections),
        "symbols": SYMBOLS
    }

@app.get("/api/tickers")
async def get_tickers():
    """REST endpoint to get current ticker data"""
    return {
        "ticks": latest_ticks,
        "status": "connected" if is_delta_connected else "disconnected",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/api/trades/{symbol}")
async def get_trades(symbol: str):
    """REST endpoint to get recent trades for a symbol"""
    if symbol not in SYMBOLS:
        return {"error": "Invalid symbol"}
    return {
        "symbol": symbol,
        "trades": latest_trades.get(symbol, []),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

# ===============================
# FLUTTER WEBSOCKET ENDPOINT
# ===============================
@app.websocket("/ws/market")
async def market_data_stream(websocket: WebSocket):
    await websocket.accept()
    active_connections.add(websocket)
    logger.info(f"New Flutter client connected. Total: {len(active_connections)}")
    
    # Send initial data immediately
    try:
        initial_payload = {
            "ticks": latest_ticks,
            "trades": latest_trades,
            "status": "connected" if is_delta_connected else "reconnecting",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        await websocket.send_json(initial_payload)
    except Exception as e:
        logger.error(f"Error sending initial data: {e}")
    
    try:
        while True:
            # Keep connection alive by receiving ping/pong messages
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_connections.discard(websocket)
        logger.info(f"Client disconnected. Remaining: {len(active_connections)}")
    except Exception as e:
        active_connections.discard(websocket)
        logger.error(f"WebSocket error: {e}")

# ===============================
# RUN SERVER
# ===============================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
