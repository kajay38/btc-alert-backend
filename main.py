import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Set

import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

# ===============================
# LOGGING
# ===============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("DELTA")

# ===============================
# CONFIG
# ===============================
DELTA_WS_URL = "wss://socket.delta.exchange"
SYMBOLS = ["BTCUSD", "ETHUSD", "SOLUSD"]
MAX_TRADES = 20

# ===============================
# GLOBAL STATE
# ===============================
latest_ticks: Dict[str, Dict[str, Any]] = {
    s: {
        "symbol": s,
        "price": None,       # smooth (UI)
        "ltp": None,         # last traded price
        "mark_price": None,
        "spot_price": None,
        "bid": None,
        "ask": None,
        "timestamp": None,
    }
    for s in SYMBOLS
}

latest_trades: Dict[str, List[Dict[str, Any]]] = {s: [] for s in SYMBOLS}
active_clients: Set[WebSocket] = set()
is_delta_connected = False

# ===============================
# BROADCAST TO FLUTTER
# ===============================
async def broadcast():
    """Broadcast updates to all connected Flutter clients"""
    if not active_clients:
        return

    payload = {
        "ticks": latest_ticks,
        "trades": latest_trades,
        "status": "connected" if is_delta_connected else "reconnecting",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    disconnected = set()
    for ws in active_clients:
        try:
            await ws.send_json(payload)
        except Exception as e:
            logger.error(f"Broadcast error: {e}")
            disconnected.add(ws)

    for ws in disconnected:
        active_clients.discard(ws)

# ===============================
# DELTA WS LISTENER
# ===============================
async def delta_ws_listener():
    """Connect to Delta Exchange WebSocket and process market data"""
    global is_delta_connected

    while True:
        try:
            logger.info(f"Connecting to {DELTA_WS_URL}...")
            async with websockets.connect(
                DELTA_WS_URL,
                ping_interval=20,
                ping_timeout=10,
            ) as ws:

                # Subscribe to ticker and trades channels
                subscribe_msg = {
                    "type": "subscribe",
                    "payload": {
                        "channels": [
                            {"name": "v2/ticker", "symbols": SYMBOLS},
                            {"name": "all_trades", "symbols": SYMBOLS},
                        ]
                    },
                }

                await ws.send(json.dumps(subscribe_msg))
                logger.info(f"Subscribed to v2/ticker + all_trades for {SYMBOLS}")

                async for message in ws:
                    try:
                        msg = json.loads(message)
                        msg_type = msg.get("type")

                        # Handle subscription confirmation
                        if msg_type == "subscriptions":
                            is_delta_connected = True
                            logger.info("✅ Delta subscription ACTIVE")
                            logger.info(f"Subscribed channels: {msg.get('channels')}")
                            continue

                        # Get symbol from message
                        symbol = msg.get("symbol")
                        if not symbol or symbol not in SYMBOLS:
                            continue

                        # ===============================
                        # HANDLE v2/ticker
                        # ===============================
                        if msg_type == "v2/ticker":
                            # Extract price fields directly from message root
                            close_price = msg.get("close")
                            mark_price = msg.get("mark_price")
                            spot_price = msg.get("spot_price")
                            
                            # Extract quotes (bid/ask) from quotes object
                            quotes = msg.get("quotes", {})
                            best_bid = quotes.get("best_bid") if quotes else None
                            best_ask = quotes.get("best_ask") if quotes else None

                            # Calculate smooth display price
                            try:
                                if best_bid and best_ask:
                                    price = (float(best_bid) + float(best_ask)) / 2
                                elif close_price:
                                    price = float(close_price)
                                elif mark_price:
                                    price = float(mark_price)
                                elif spot_price:
                                    price = float(spot_price)
                                else:
                                    price = None
                                    logger.warning(f"{symbol}: No valid price data")
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

                            logger.info(
                                f"📊 {symbol} | Price: {price} | "
                                f"LTP: {close_price} | Mark: {mark_price} | "
                                f"Bid: {best_bid} | Ask: {best_ask}"
                            )

                            await broadcast()

                        # ===============================
                        # HANDLE all_trades
                        # ===============================
                        elif msg_type == "all_trades":
                            trade = {
                                "price": float(msg.get("price", 0)),
                                "size": msg.get("size"),
                                "side": msg.get("buyer_role"),  # "maker" or "taker"
                                "timestamp": msg.get("timestamp"),
                            }

                            latest_trades[symbol].insert(0, trade)
                            latest_trades[symbol] = latest_trades[symbol][:MAX_TRADES]

                            logger.debug(
                                f"💹 {symbol} Trade | "
                                f"Price: {trade['price']} | Size: {trade['size']}"
                            )

                            await broadcast()

                        # ===============================
                        # HANDLE all_trades_snapshot
                        # ===============================
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
                            logger.info(
                                f"📸 {symbol} | Received {len(formatted_trades)} trades snapshot"
                            )
                            await broadcast()

                    except json.JSONDecodeError as e:
                        logger.error(f"❌ JSON decode error: {e}")
                    except Exception as e:
                        logger.error(f"❌ Message processing error: {e}", exc_info=True)

        except websockets.exceptions.WebSocketException as e:
            is_delta_connected = False
            logger.error(f"❌ WebSocket error: {e}")
            await asyncio.sleep(5)
        except Exception as e:
            is_delta_connected = False
            logger.error(f"❌ Unexpected error: {e}", exc_info=True)
            await asyncio.sleep(5)

# ===============================
# FASTAPI LIFESPAN
# ===============================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    task = asyncio.create_task(delta_ws_listener())
    logger.info("🚀 Delta Listener Started")
    yield
    task.cancel()
    logger.info("🛑 Delta Listener Stopped")

# ===============================
# FASTAPI APP
# ===============================
app = FastAPI(title="Delta Market Pro", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===============================
# REST ENDPOINTS
# ===============================
@app.get("/")
async def health():
    """Health check endpoint"""
    return {
        "status": "online",
        "delta_connected": is_delta_connected,
        "active_clients": len(active_clients),
        "symbols": SYMBOLS,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

@app.get("/api/tickers")
async def get_tickers():
    """Get current ticker data for all symbols"""
    return {
        "ticks": latest_ticks,
        "status": "connected" if is_delta_connected else "disconnected",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

@app.get("/api/trades/{symbol}")
async def get_trades(symbol: str):
    """Get recent trades for a specific symbol"""
    symbol = symbol.upper()
    if symbol not in SYMBOLS:
        return {"error": f"Invalid symbol. Available: {SYMBOLS}"}
    return {
        "symbol": symbol,
        "trades": latest_trades[symbol],
        "count": len(latest_trades[symbol]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

# ===============================
# FLUTTER WEBSOCKET
# ===============================
@app.websocket("/ws/market")
async def ws_market(websocket: WebSocket):
    """WebSocket endpoint for Flutter clients"""
    await websocket.accept()
    active_clients.add(websocket)
    logger.info(f"📱 Flutter connected | Total clients: {len(active_clients)}")

    # Send initial snapshot immediately
    try:
        initial_data = {
            "ticks": latest_ticks,
            "trades": latest_trades,
            "status": "connected" if is_delta_connected else "reconnecting",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await websocket.send_json(initial_data)
        logger.info(f"📤 Sent initial snapshot to client")
    except Exception as e:
        logger.error(f"❌ Error sending initial data: {e}")

    try:
        while True:
            # Keep connection alive by receiving messages
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_clients.discard(websocket)
        logger.info(f"📱 Flutter disconnected | Remaining: {len(active_clients)}")
    except Exception as e:
        active_clients.discard(websocket)
        logger.error(f"❌ WebSocket error: {e}")

# ===============================
# RUN SERVER
# ===============================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
