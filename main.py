import asyncio
import json
import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Set
from contextlib import asynccontextmanager
import aiohttp

import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

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
# CONFIG - DELTA EXCHANGE INDIA
# ===============================
DELTA_WS_URL = "wss://socket.delta.exchange"
DELTA_REST_URL = "https://api.india.delta.exchange"

SYMBOLS = [
    "BTCUSD", "ETHUSD", "SOLUSD", "BNBUSD", "XRPUSD",
    "ADAUSD", "DOGEUSD", "DOTUSD", "LTCUSD", "MATICUSD",
]

# ✅ Feature flags
ENABLE_ORDER_BOOK = True  # Set to False to disable order book subscription
FETCH_HISTORICAL_DATA = True  # Fetch previous day data on startup

# ✅ CORRECTED Trading Configuration
TRADING_CONFIG = {
    "min_order_size": {s: 1 for s in SYMBOLS},
    "tick_size": {
        "BTCUSD": 0.5, "ETHUSD": 0.05, "SOLUSD": 0.0001,
        "BNBUSD": 0.001, "XRPUSD": 0.0001, "ADAUSD": 0.00001,
        "DOGEUSD": 0.000001, "DOTUSD": 0.001, "LTCUSD": 0.01, "MATICUSD": 0.0001,
    },
    "contract_values": {
        "BTCUSD": 0.001, "ETHUSD": 0.01, "SOLUSD": 1, "BNBUSD": 0.1, "XRPUSD": 1,
        "ADAUSD": 1, "DOGEUSD": 100, "DOTUSD": 1, "LTCUSD": 0.1, "MATICUSD": 1,
    },
    "leverage_options": [1, 2, 3, 5, 10, 20, 50, 100],
    "supported_order_types": ["limit_order", "market_order", "stop_market_order", "stop_limit_order"],
}

PORT = int(os.environ.get("PORT", 8000))

# ===============================
# GLOBAL STATE
# ===============================
latest_ticks: Dict[str, Dict[str, Any]] = {
    s: {
        "symbol": s, "price": None, "mark_price": None, "ltp": None,
        "today": {"open": None, "high": None, "low": None, "close": None, "volume": None, "change_value": None, "change_percent": None},
        "previous_day": {"open": None, "high": None, "low": None, "close": None, "volume": None},
        "order_book": {"bids": [], "asks": [], "spread": None, "depth": 0},
        "bid": None, "ask": None, "spread": None,
        "volatility": {"24h_high": None, "24h_low": None, "atr_14": None, "current_range": None},
        "liquidity": {"bid_volume": 0, "ask_volume": 0, "total_volume": 0},
        "timestamp": None, "last_update": None,
    }
    for s in SYMBOLS
}

trading_signals: Dict[str, Dict[str, Any]] = {
    s: {"signal": "NEUTRAL", "confidence": 0, "reasons": [], "last_signal_time": None}
    for s in SYMBOLS
}

user_portfolio = {
    "balance": 0.0, "available_balance": 0.0, "positions": [],
    "open_orders": [], "total_pnl": 0.0, "day_pnl": 0.0,
}

active_clients: Set[WebSocket] = set()
is_delta_connected = False
shutdown_event = asyncio.Event()

analysis_worker = None
if ANALYSIS_ENABLED:
    try:
        analysis_worker = AnalysisWorker(symbols=SYMBOLS, resolution="1h", update_interval=300)
        logger.info("✅ Analysis worker initialized")
    except Exception as e:
        logger.error(f"❌ Failed to initialize analysis worker: {e}")
        ANALYSIS_ENABLED = False

# ===============================
# REST API FUNCTIONS
# ===============================
async def fetch_previous_day_data():
    """Fetch previous day OHLCV data for all symbols"""
    logger.info("📊 Fetching previous day data...")
    
    # Calculate timestamps for yesterday
    now = datetime.now(timezone.utc)
    yesterday_start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_end = yesterday_start + timedelta(days=1)
    
    start_ts = int(yesterday_start.timestamp())
    end_ts = int(yesterday_end.timestamp())
    
    async with aiohttp.ClientSession() as session:
        for symbol in SYMBOLS:
            try:
                url = f"{DELTA_REST_URL}/v2/history/candles"
                params = {
                    "resolution": "1d",
                    "symbol": symbol,
                    "start": start_ts,
                    "end": end_ts
                }
                
                async with session.get(url, params=params, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        result = data.get("result", [])
                        
                        if result and len(result) > 0:
                            candle = result[-1]  # Get last candle (yesterday)
                            
                            latest_ticks[symbol]["previous_day"] = {
                                "open": candle.get("open"),
                                "high": candle.get("high"),
                                "low": candle.get("low"),
                                "close": candle.get("close"),
                                "volume": candle.get("volume"),
                            }
                            logger.info(f"✅ Loaded previous day data for {symbol}")
                    else:
                        logger.warning(f"⚠️ Failed to fetch data for {symbol}: {response.status}")
                        
            except Exception as e:
                logger.error(f"❌ Error fetching data for {symbol}: {e}")
    
    logger.info("✅ Previous day data loaded")

# ===============================
# HELPER FUNCTIONS
# ===============================
def calculate_position_size(symbol: str, risk_amount: float, stop_loss_percent: float) -> int:
    """Calculate position size in contracts"""
    current_price = latest_ticks[symbol].get("price")
    if not current_price or current_price == 0:
        return 1
    
    contract_value = TRADING_CONFIG["contract_values"].get(symbol, 1)
    risk_per_contract = contract_value * current_price * (stop_loss_percent / 100)
    
    if risk_per_contract == 0:
        return 1
    
    num_contracts = risk_amount / risk_per_contract
    return max(1, round(num_contracts))

def generate_trading_signal(symbol: str, indicators: dict) -> dict:
    """Generate trading signal based on indicators"""
    if not indicators:
        return {"signal": "NEUTRAL", "confidence": 0, "reasons": [], "reason": "No data"}
    
    ema_9 = indicators.get("ema_9")
    ema_20 = indicators.get("ema_20")
    ema_50 = indicators.get("ema_50")
    current_price = latest_ticks[symbol].get("price")
    
    if not all([ema_9, ema_20, ema_50, current_price]):
        return {"signal": "NEUTRAL", "confidence": 0, "reasons": [], "reason": "Incomplete data"}
    
    signal_score = 0
    reasons = []
    
    if ema_9 > ema_20 > ema_50:
        signal_score += 30
        reasons.append("EMA Bullish Alignment")
    elif ema_9 < ema_20 < ema_50:
        signal_score -= 30
        reasons.append("EMA Bearish Alignment")
    
    if current_price > ema_20:
        signal_score += 20
        reasons.append("Price above EMA20")
    else:
        signal_score -= 20
        reasons.append("Price below EMA20")
    
    if current_price > ema_50:
        signal_score += 20
        reasons.append("Price above EMA50")
    else:
        signal_score -= 20
        reasons.append("Price below EMA50")
    
    signal = "BUY" if signal_score >= 50 else "SELL" if signal_score <= -50 else "NEUTRAL"
    
    return {
        "signal": signal,
        "confidence": min(abs(signal_score), 100),
        "reasons": reasons,
        "score": signal_score,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

# ===============================
# WEBSOCKET LISTENER (ENHANCED)
# ===============================
async def delta_ws_listener():
    """Connect to Delta Exchange WebSocket with order book support"""
    global is_delta_connected
    
    reconnect_delay = 5
    max_reconnect_delay = 60
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

                # ✅ Build subscription channels
                channels = [{"name": "v2/ticker", "symbols": SYMBOLS}]
                
                # ✅ Add order book channel if enabled
                if ENABLE_ORDER_BOOK:
                    channels.append({"name": "l2_orderbook", "symbols": SYMBOLS})
                    logger.info("📖 Order book subscription enabled")

                subscribe_msg = {
                    "type": "subscribe",
                    "payload": {"channels": channels}
                }

                await ws.send(json.dumps(subscribe_msg))
                logger.info(f"📤 Subscribed to {len(SYMBOLS)} symbols")
                
                is_delta_connected = True
                reconnect_delay = 5
                logger.info("✅ Delta WS connected")

                message_count = 0
                async for msg in ws:
                    if shutdown_event.is_set():
                        break
                        
                    try:
                        data = json.loads(msg)
                        message_count += 1
                        
                        # Log first few messages
                        if message_count <= 3:
                            logger.info(f"📨 Message #{message_count}: {json.dumps(data)[:150]}...")
                        
                        msg_type = data.get("type")
                        
                        # Handle subscription confirmation
                        if msg_type == "subscriptions":
                            logger.info(f"✅ Subscription confirmed")
                            continue
                        
                        # ✅ Handle order book data
                        if msg_type == "l2_orderbook":
                            await _process_orderbook_data(data)
                            continue
                        
                        # ✅ Process ticker data (no "type" field in ticker messages)
                        symbol = data.get("symbol")
                        
                        if symbol and symbol in SYMBOLS:
                            if message_count <= 15:
                                logger.info(f"✅ Processing ticker for {symbol}")
                            
                            # Day rollover check
                            now = datetime.now(timezone.utc)
                            if now.day != current_day:
                                for sym in SYMBOLS:
                                    if latest_ticks[sym]["today"]["close"]:
                                        latest_ticks[sym]["previous_day"] = latest_ticks[sym]["today"].copy()
                                        latest_ticks[sym]["today"] = {
                                            "open": latest_ticks[sym]["price"],
                                            "high": latest_ticks[sym]["price"],
                                            "low": latest_ticks[sym]["price"],
                                            "close": latest_ticks[sym]["price"],
                                            "volume": 0, "change_value": 0, "change_percent": 0,
                                        }
                                current_day = now.day
                                logger.info("📅 New trading day")

                            await _process_ticker_data(symbol, data)
                            
                            if ANALYSIS_ENABLED and analysis_worker:
                                try:
                                    indicators = analysis_worker.get_indicators(symbol)
                                    if indicators:
                                        signal = generate_trading_signal(symbol, indicators.get("indicators", {}))
                                        if signal["signal"] != "NEUTRAL":
                                            trading_signals[symbol] = {
                                                **signal,
                                                "symbol": symbol,
                                                "current_price": latest_ticks[symbol]["price"],
                                            }
                                except Exception:
                                    pass
                            
                            await broadcast()
                        
                    except json.JSONDecodeError:
                        logger.warning("⚠️ Invalid JSON")
                        continue
                    except Exception as e:
                        logger.error(f"❌ Error processing: {e}")
                        continue

        except Exception as e:
            is_delta_connected = False
            logger.error(f"❌ WS error: {e}")
        
        if not shutdown_event.is_set():
            logger.info(f"🔄 Reconnecting in {reconnect_delay}s...")
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=reconnect_delay)
            except asyncio.TimeoutError:
                pass
            reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
    
    logger.info("🛑 WS listener stopped")


async def _process_ticker_data(symbol: str, data: dict):
    """Process ticker data"""
    mark_price = data.get("mark_price")
    ltp = data.get("close")
    open_24h = data.get("open")
    high_24h = data.get("high")
    low_24h = data.get("low")
    volume_24h = data.get("volume")
    
    quotes = data.get("quotes") or {}
    bid = quotes.get("best_bid")
    ask = quotes.get("best_ask")
    bid_size = quotes.get("best_bid_size")
    ask_size = quotes.get("best_ask_size")

    display_price = None
    if ltp:
        display_price = float(ltp)
    elif bid and ask:
        display_price = (float(bid) + float(ask)) / 2
    elif mark_price:
        display_price = float(mark_price)
    else:
        display_price = 0.0
    
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
            pass

    spread = 0.0
    if bid and ask:
        try:
            spread = float(ask) - float(bid)
        except ValueError:
            pass

    latest_ticks[symbol].update({
        "symbol": symbol,
        "price": round(display_price, 2) if display_price else None,
        "mark_price": float(mark_price) if mark_price else None,
        "ltp": float(ltp) if ltp else None,
        "today": {
            "open": float(open_24h) if open_24h else None,
            "high": float(high_24h) if high_24h else None,
            "low": float(low_24h) if low_24h else None,
            "close": float(ltp) if ltp else None,
            "volume": float(volume_24h) if volume_24h else None,
            "change_value": round(change_value, 2),
            "change_percent": round(change_pct, 2),
        },
        "bid": float(bid) if bid else None,
        "ask": float(ask) if ask else None,
        "spread": round(spread, 4) if spread else None,
        "liquidity": {
            "bid_volume": float(bid_size) if bid_size else 0,
            "ask_volume": float(ask_size) if ask_size else 0,
            "total_volume": float(volume_24h) if volume_24h else 0,
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "last_update": datetime.now(timezone.utc).timestamp(),
    })


async def _process_orderbook_data(data: dict):
    """Process L2 order book data"""
    symbol = data.get("symbol")
    if not symbol or symbol not in SYMBOLS:
        return
    
    buy_orders = data.get("buy", [])
    sell_orders = data.get("sell", [])
    
    # Convert to standard format
    bids = [[float(order["limit_price"]), float(order["size"])] for order in buy_orders[:10]]
    asks = [[float(order["limit_price"]), float(order["size"])] for order in sell_orders[:10]]
    
    # Calculate total depth
    total_bid_volume = sum(order["size"] for order in buy_orders)
    total_ask_volume = sum(order["size"] for order in sell_orders)
    
    # Calculate spread
    spread = None
    if bids and asks:
        spread = asks[0][0] - bids[0][0]
    
    latest_ticks[symbol]["order_book"] = {
        "bids": bids,
        "asks": asks,
        "spread": round(spread, 4) if spread else None,
        "depth": len(buy_orders) + len(sell_orders),
    }
    
    # Update liquidity data
    latest_ticks[symbol]["liquidity"].update({
        "bid_volume": total_bid_volume,
        "ask_volume": total_ask_volume,
    })

# ===============================
# BROADCAST
# ===============================
async def broadcast():
    """Broadcast to all clients"""
    if not active_clients:
        return

    indicators_data = {}
    if ANALYSIS_ENABLED and analysis_worker:
        try:
            indicators_data = analysis_worker.get_all_indicators()
        except Exception:
            pass

    payload = {
        "status": "connected" if is_delta_connected else "reconnecting",
        "ticks": latest_ticks,
        "indicators": indicators_data,
        "signals": trading_signals,
        "config": TRADING_CONFIG,
        "portfolio": user_portfolio,
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
# FASTAPI
# ===============================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Starting application...")
    
    # ✅ Fetch historical data on startup
    if FETCH_HISTORICAL_DATA:
        await fetch_previous_day_data()
    
    ws_task = asyncio.create_task(delta_ws_listener())
    
    analysis_task = None
    if ANALYSIS_ENABLED and analysis_worker:
        try:
            analysis_task = asyncio.create_task(analysis_worker.start())
            logger.info("✅ Analysis worker started")
        except Exception as e:
            logger.error(f"❌ Analysis worker failed: {e}")
    
    logger.info(f"✅ Application started on port {PORT}")
    
    yield
    
    logger.info("🛑 Shutting down...")
    shutdown_event.set()
    
    if ws_task and not ws_task.done():
        ws_task.cancel()
        try:
            await ws_task
        except asyncio.CancelledError:
            pass
    
    if analysis_task and not analysis_task.done():
        analysis_task.cancel()
        try:
            await analysis_task
        except asyncio.CancelledError:
            pass
    
    if analysis_worker:
        try:
            analysis_worker.stop()
        except Exception:
            pass
    
    for ws in list(active_clients):
        try:
            await ws.close()
        except Exception:
            pass
    active_clients.clear()
    
    logger.info("✅ Stopped gracefully")

app = FastAPI(
    title="Delta India Trading API",
    description="Real-time trading data from Delta Exchange India",
    version="2.1.0",
    lifespan=lifespan,
)

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
@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "exchange": "Delta Exchange India",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "websocket_connected": is_delta_connected,
        "analysis_enabled": ANALYSIS_ENABLED,
        "order_book_enabled": ENABLE_ORDER_BOOK,
        "active_clients": len(active_clients),
        "symbols": SYMBOLS,
    }

@app.get("/trading/config")
def get_trading_config():
    return {
        "status": "success",
        "config": TRADING_CONFIG,
        "features": {
            "order_book": ENABLE_ORDER_BOOK,
            "historical_data": FETCH_HISTORICAL_DATA,
            "analysis": ANALYSIS_ENABLED,
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

@app.get("/trading/calculator")
def trading_calculator(
    symbol: str,
    risk_amount: float = 100.0,
    stop_loss_percent: float = 2.0,
    take_profit_percent: float = 4.0,
    leverage: int = 1
):
    symbol = symbol.upper()
    if symbol not in SYMBOLS:
        return {"status": "error", "message": f"Symbol {symbol} not supported"}
    
    current_price = latest_ticks[symbol].get("price", 0)
    if not current_price:
        return {"status": "error", "message": "No price data available"}
    
    num_contracts = calculate_position_size(symbol, risk_amount, stop_loss_percent)
    contract_value = TRADING_CONFIG["contract_values"].get(symbol, 1)
    position_value = num_contracts * contract_value * current_price
    
    stop_loss_price = current_price * (1 - stop_loss_percent/100)
    take_profit_price = current_price * (1 + take_profit_percent/100)
    risk_reward_ratio = take_profit_percent / stop_loss_percent
    
    margin_required = position_value / leverage
    
    if leverage > 1:
        liquidation_buffer = (1 / leverage) * 0.9
        liquidation_price = current_price * (1 - liquidation_buffer)
    else:
        liquidation_price = None
    
    potential_loss = num_contracts * contract_value * (current_price - stop_loss_price)
    potential_profit = num_contracts * contract_value * (take_profit_price - current_price)
    
    return {
        "status": "success",
        "symbol": symbol,
        "current_price": current_price,
        "calculations": {
            "num_contracts": num_contracts,
            "contract_value": contract_value,
            "position_value_usd": round(position_value, 2),
            "stop_loss_price": round(stop_loss_price, 2),
            "take_profit_price": round(take_profit_price, 2),
            "stop_loss_percent": stop_loss_percent,
            "take_profit_percent": take_profit_percent,
            "risk_amount_usd": risk_amount,
            "potential_loss_usd": round(potential_loss, 2),
            "potential_profit_usd": round(potential_profit, 2),
            "risk_reward_ratio": round(risk_reward_ratio, 2),
        },
        "leverage": {
            "leverage": leverage,
            "margin_required_usd": round(margin_required, 2),
            "liquidation_price": round(liquidation_price, 2) if liquidation_price else None,
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

@app.get("/market")
def get_all_market_data():
    return {
        "status": "success",
        "data": latest_ticks,
        "signals": trading_signals,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

@app.get("/market/{symbol}")
def get_symbol_data(symbol: str):
    symbol = symbol.upper()
    if symbol not in SYMBOLS:
        return {"status": "error", "message": f"Symbol {symbol} not supported"}
    
    data = latest_ticks.get(symbol, {})
    signal = trading_signals.get(symbol, {})
    
    trading_info = {
        "min_order_size": TRADING_CONFIG["min_order_size"].get(symbol),
        "tick_size": TRADING_CONFIG["tick_size"].get(symbol),
        "contract_value": TRADING_CONFIG["contract_values"].get(symbol),
        "leverage_options": TRADING_CONFIG["leverage_options"],
    }
    
    return {
        "status": "success",
        "symbol": symbol,
        "market_data": data,
        "trading_info": trading_info,
        "signal": signal,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

@app.websocket("/ws/market")
async def ws_market(ws: WebSocket):
    await ws.accept()
    active_clients.add(ws)
    logger.info(f"✅ Client connected. Total: {len(active_clients)}")
    
    try:
        await broadcast()
        
        while True:
            try:
                data = await asyncio.wait_for(ws.receive_text(), timeout=30.0)
                
                try:
                    command = json.loads(data)
                    cmd_type = command.get("type")
                    
                    if cmd_type == "ping":
                        await ws.send_json({"type": "pong", "timestamp": datetime.now(timezone.utc).isoformat()})
                    
                except json.JSONDecodeError:
                    pass
                    
            except asyncio.TimeoutError:
                try:
                    await ws.send_json({"type": "ping", "timestamp": datetime.now(timezone.utc).isoformat()})
                except Exception:
                    break
                    
    except WebSocketDisconnect:
        logger.info("❌ Client disconnected")
    except Exception as e:
        logger.error(f"❌ WebSocket error: {e}")
    finally:
        active_clients.discard(ws)
        logger.info(f"📊 Active clients: {len(active_clients)}")

# ===============================
# RUN
# ===============================
if __name__ == "__main__":
    import uvicorn
    
    logger.info(f"🚀 Starting on 0.0.0.0:{PORT}")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=PORT,
        log_level="info",
        access_log=True,
        timeout_keep_alive=75,
    )
