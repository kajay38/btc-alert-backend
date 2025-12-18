import asyncio
import json
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Dict, Any, List

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
is_delta_connected = False

# ===============================
# DELTA WEBSOCKET LISTENER
# ===============================
async def delta_ws_listener():
    """
    Connects to Delta Exchange WebSocket and handles ticker and trades data.
    """
    global is_delta_connected
    
    while True:
        try:
            logger.info(f"🔄 Connecting to Delta Exchange: {DELTA_WS_URL}")
            async with websockets.connect(
                DELTA_WS_URL,
                ping_interval=20,
                ping_timeout=10,
            ) as ws:
                
                # ✅ CORRECTED: Subscription payload with proper channel names
                subscribe_msg = {
                    "type": "subscribe",
                    "payload": {
                        "channels": [
                            {
                                "name": "v2/ticker",  # Correct channel name
                                "symbols": SYMBOLS
                            },
                            {
                                "name": "all_trades",  # Correct channel name
                                "symbols": SYMBOLS
                            }
                        ]
                    }
                }
                
                await ws.send(json.dumps(subscribe_msg))
                logger.info(f"📡 Subscription request sent for: {SYMBOLS}")

                async for message in ws:
                    try:
                        msg = json.loads(message)
                        
                        # Debug logging (enable if needed)
                        # logger.info(f"📥 RAW: {msg}")

                        # Handle subscription confirmation
                        if msg.get("type") == "subscriptions":
                            is_delta_connected = True
                            logger.info("✅ Delta subscription ACTIVE")
                            logger.info(f"📋 Subscribed channels: {msg.get('channels')}")
                            continue

                        msg_type = msg.get("type")
                        symbol = msg.get("symbol")

                        # Validate message
                        if not symbol or symbol not in SYMBOLS:
                            continue

                        # ===============================
                        # HANDLE TICKER DATA
                        # ===============================
                        if msg_type == "v2/ticker":
                            try:
                                # Extract price data
                                ltp = msg.get("close")
                                mark = msg.get("mark_price")
                                spot = msg.get("spot_price")
                                
                                # Extract quotes (bid/ask)
                                quotes = msg.get("quotes", {})
                                bid = quotes.get("best_bid") if quotes else None
                                ask = quotes.get("best_ask") if quotes else None

                                # Calculate mid price
                                if bid and ask:
                                    try:
                                        price = (float(bid) + float(ask)) / 2
                                    except (ValueError, TypeError):
                                        price = None
                                else:
                                    # Fallback to LTP or mark price
                                    price = float(ltp or mark or spot or 0)
                                    if price <= 0:
                                        price = None

                                # Update ticker data
                                latest_ticks[symbol] = {
                                    "symbol": symbol,
                                    "price": price,
                                    "ltp": float(ltp) if ltp else None,
                                    "mark_price": float(mark) if mark else None,
                                    "bid": float(bid) if bid else None,
                                    "ask": float(ask) if ask else None,
                                    "timestamp": datetime.now(timezone.utc).isoformat(),
                                }
                                
                                logger.debug(f"📊 {symbol} Ticker: Price={price}, LTP={ltp}, Mark={mark}")

                            except Exception as e:
                                logger.error(f"❌ Error processing ticker for {symbol}: {e}")

                        # ===============================
                        # HANDLE TRADES DATA
                        # ===============================
                        elif msg_type == "all_trades":
                            try:
                                trade = {
                                    "price": float(msg.get("price", 0)),
                                    "size": msg.get("size"),
                                    "side": msg.get("buyer_role"),  # "maker" or "taker"
                                    "timestamp": msg.get("timestamp"),
                                }
                                
                                # Add to trades list (newest first)
                                latest_trades[symbol].insert(0, trade)
                                latest_trades[symbol] = latest_trades[symbol][:MAX_TRADES]
                                
                                logger.debug(f"💹 {symbol} Trade: {trade['price']} @ {trade['size']}")

                            except Exception as e:
                                logger.error(f"❌ Error processing trade for {symbol}: {e}")

                        # ===============================
                        # HANDLE TRADES SNAPSHOT
                        # ===============================
                        elif msg_type == "all_trades_snapshot":
                            try:
                                trades_list = msg.get("trades", [])
                                logger.info(f"📸 Received {len(trades_list)} trades snapshot for {symbol}")
                                
                                # Process snapshot trades
                                for trade_data in trades_list[:MAX_TRADES]:
                                    trade = {
                                        "price": float(trade_data.get("price", 0)),
                                        "size": trade_data.get("size"),
                                        "side": trade_data.get("buyer_role"),
                                        "timestamp": trade_data.get("timestamp"),
                                    }
                                    latest_trades[symbol].append(trade)
                                
                                # Keep only MAX_TRADES
                                latest_trades[symbol] = latest_trades[symbol][:MAX_TRADES]

                            except Exception as e:
                                logger.error(f"❌ Error processing trades snapshot for {symbol}: {e}")

                    except json.JSONDecodeError as e:
                        logger.error(f"❌ JSON decode error: {e}")
                    except Exception as e:
                        logger.error(f"❌ Error processing message: {e}")

        except websockets.exceptions.WebSocketException as e:
            is_delta_connected = False
            logger.error(f"❌ WebSocket error: {e}")
            await asyncio.sleep(5)
        except Exception as e:
            is_delta_connected = False
            logger.error(f"❌ Unexpected error: {e}")
            await asyncio.sleep(5)

# ===============================
# FASTAPI APPLICATION
# ===============================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the lifecycle of the WebSocket listener task.
    """
    task = asyncio.create_task(delta_ws_listener())
    logger.info("🚀 Delta WebSocket listener started")
    yield
    task.cancel()
    logger.info("🛑 Delta WebSocket listener stopped")

app = FastAPI(
    title="Delta Market Pro",
    description="Real-time market data from Delta Exchange",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===============================
# API ENDPOINTS
# ===============================
@app.get("/")
async def health():
    """
    Health check endpoint with connection status.
    """
    return {
        "status": "online" if is_delta_connected else "reconnecting",
        "symbols": SYMBOLS,
        "delta_connected": is_delta_connected,
        "active_data": {
            s: latest_ticks[s]["price"] is not None 
            for s in SYMBOLS
        },
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/market/{symbol}")
async def get_symbol_data(symbol: str):
    """
    Get market data for a specific symbol.
    """
    s = symbol.upper()
    
    if s not in SYMBOLS:
        return {
            "error": "Symbol not found",
            "available_symbols": SYMBOLS
        }
    
    return {
        "tick": latest_ticks.get(s),
        "trades": latest_trades.get(s, []),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.websocket("/ws/market")
async def market_data_stream(websocket: WebSocket):
    """
    WebSocket endpoint for streaming market data to Flutter clients.
    """
    await websocket.accept()
    client_info = f"{websocket.client.host}:{websocket.client.port}"
    logger.info(f"📱 Flutter client connected: {client_info}")
    
    try:
        while True:
            # Send market data to client
            await websocket.send_json({
                "ticks": latest_ticks,
                "trades": latest_trades,
                "status": "connected" if is_delta_connected else "connecting",
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            
            # 200ms update interval
            await asyncio.sleep(0.2)
            
    except WebSocketDisconnect:
        logger.info(f"📱 Flutter client disconnected: {client_info}")
    except Exception as e:
        logger.error(f"📱 WebSocket error for {client_info}: {e}")

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
