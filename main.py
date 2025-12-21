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
# CONFIG - DELTA EXCHANGE INDIA SPECIFIC
# ===============================
DELTA_WS_URL = "wss://socket.delta.exchange"

# ✅ Delta Exchange India के मुख्य trading pairs
SYMBOLS = [
    "BTCUSD",      # Bitcoin
    "ETHUSD",      # Ethereum
    "SOLUSD",      # Solana
    "BNBUSD",      # BNB
    "XRPUSD",      # Ripple
    "ADAUSD",      # Cardano
    "DOGEUSD",     # Dogecoin
    "DOTUSD",      # Polkadot
    "LTCUSD",      # Litecoin
    "MATICUSD",    # Polygon
]

# ✅ Trading के लिए जरूरी configuration
TRADING_CONFIG = {
    "min_order_size": {
        "BTCUSD": 0.001,
        "ETHUSD": 0.01,
        "SOLUSD": 0.1,
        "BNBUSD": 0.01,
        "XRPUSD": 10,
        "ADAUSD": 10,
        "DOGEUSD": 100,
        "DOTUSD": 1,
        "LTCUSD": 0.1,
        "MATICUSD": 10,
    },
    "tick_size": {
        "BTCUSD": 0.5,
        "ETHUSD": 0.05,
        "SOLUSD": 0.01,
        "BNBUSD": 0.01,
        "XRPUSD": 0.0001,
        "ADAUSD": 0.0001,
        "DOGEUSD": 0.000001,
        "DOTUSD": 0.001,
        "LTCUSD": 0.01,
        "MATICUSD": 0.001,
    },
    "leverage_options": [1, 2, 3, 5, 10, 20, 50, 100],
    "supported_order_types": ["limit", "market", "stop", "stop_limit"],
}

# ✅ Railway PORT support
PORT = int(os.environ.get("PORT", 8000))

# ===============================
# GLOBAL STATE (Trading के लिए enhanced)
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
        
        # ORDER BOOK DATA (trading ke liye)
        "order_book": {
            "bids": [],
            "asks": [],
            "spread": None,
            "depth": 0,
        },
        
        # BID/ASK for quick access
        "bid": None,
        "ask": None,
        "spread": None,
        
        # VOLATILITY DATA
        "volatility": {
            "24h_high": None,
            "24h_low": None,
            "atr_14": None,  # Average True Range
            "current_range": None,
        },
        
        # LIQUIDITY DATA
        "liquidity": {
            "bid_volume": 0,
            "ask_volume": 0,
            "total_volume": 0,
        },
        
        "timestamp": None,
        "last_update": None,
    }
    for s in SYMBOLS
}

# ✅ Trading Signals Storage
trading_signals: Dict[str, Dict[str, Any]] = {
    s: {
        "buy_signals": [],
        "sell_signals": [],
        "strength": 0,
        "last_signal_time": None,
    }
    for s in SYMBOLS
}

# ✅ Portfolio State (agar user trading kare)
user_portfolio = {
    "balance": 0.0,
    "available_balance": 0.0,
    "positions": [],
    "open_orders": [],
    "total_pnl": 0.0,
    "day_pnl": 0.0,
}

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
# TRADING HELPER FUNCTIONS
# ===============================
def calculate_position_size(symbol: str, risk_amount: float, stop_loss_percent: float) -> float:
    """
    Position size calculator (Risk Management)
    
    Args:
        symbol: Trading symbol
        risk_amount: Amount willing to risk (in USD)
        stop_loss_percent: Stop loss percentage
    
    Returns:
        float: Position size in coin units
    """
    current_price = latest_ticks[symbol].get("price")
    if not current_price:
        return 0
    
    min_size = TRADING_CONFIG["min_order_size"].get(symbol, 0.001)
    
    # Calculate position size
    risk_per_unit = current_price * (stop_loss_percent / 100)
    if risk_per_unit == 0:
        return min_size
    
    position_size = risk_amount / risk_per_unit
    
    # Apply min order size
    return max(position_size, min_size)

def calculate_support_resistance(candles: list, period: int = 20) -> dict:
    """
    Calculate support and resistance levels
    
    Args:
        candles: List of candle data
        period: Lookback period
    
    Returns:
        dict: Support and resistance levels
    """
    if len(candles) < period:
        return {"support": None, "resistance": None}
    
    highs = [c['high'] for c in candles[-period:]]
    lows = [c['low'] for c in candles[-period:]]
    
    resistance = max(highs)
    support = min(lows)
    
    return {
        "support": round(support, 2),
        "resistance": round(resistance, 2),
        "current_range": round((resistance - support), 2),
        "range_percentage": round(((resistance - support) / support) * 100, 2),
    }

def generate_trading_signal(symbol: str, indicators: dict) -> dict:
    """
    Generate trading signal based on multiple indicators
    
    Returns:
        dict: Trading signal with confidence
    """
    if not indicators:
        return {"signal": "NEUTRAL", "confidence": 0, "reason": "No data"}
    
    ema_9 = indicators.get("ema_9")
    ema_20 = indicators.get("ema_20")
    ema_50 = indicators.get("ema_50")
    current_price = latest_ticks[symbol].get("price")
    
    if not all([ema_9, ema_20, ema_50, current_price]):
        return {"signal": "NEUTRAL", "confidence": 0, "reason": "Incomplete data"}
    
    signal_score = 0
    reasons = []
    
    # EMA Crossover Strategy
    if ema_9 > ema_20 > ema_50:
        signal_score += 30
        reasons.append("EMA Bullish Alignment (9 > 20 > 50)")
    
    if current_price > ema_20:
        signal_score += 20
        reasons.append("Price above EMA20")
    
    if current_price > ema_50:
        signal_score += 20
        reasons.append("Price above EMA50")
    
    # RSI would be added here if available
    # MACD would be added here if available
    
    # Determine final signal
    if signal_score >= 50:
        signal = "BUY"
    elif signal_score <= -50:
        signal = "SELL"
    else:
        signal = "NEUTRAL"
    
    return {
        "signal": signal,
        "confidence": min(abs(signal_score), 100),
        "reasons": reasons,
        "score": signal_score,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

# ===============================
# DELTA WEBSOCKET LISTENER (ENHANCED FOR TRADING)
# ===============================
async def delta_ws_listener():
    """
    Connects to Delta Exchange WebSocket and streams market data.
    Enhanced with trading-specific data.
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

                # ✅ Subscribe to multiple channels for trading
                subscribe_msg = {
                    "type": "subscribe",
                    "payload": {
                        "channels": [
                            {"name": "v2/ticker", "symbols": SYMBOLS},
                            {"name": "v2/trades", "symbols": SYMBOLS[:5]},  # Top 5 for trade history
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
                        channel = data.get("channel")
                        symbol = data.get("symbol")
                        
                        if not symbol or symbol not in SYMBOLS:
                            continue

                        # Check for day rollover
                        now = datetime.now(timezone.utc)
                        if now.day != current_day:
                            # Reset daily data for all symbols
                            for sym in SYMBOLS:
                                if latest_ticks[sym]["today"]["close"]:
                                    latest_ticks[sym]["previous_day"] = latest_ticks[sym]["today"].copy()
                                    latest_ticks[sym]["today"] = {
                                        "open": latest_ticks[sym]["price"],
                                        "high": latest_ticks[sym]["price"],
                                        "low": latest_ticks[sym]["price"],
                                        "close": latest_ticks[sym]["price"],
                                        "volume": 0,
                                        "change_value": 0,
                                        "change_percent": 0,
                                    }
                            current_day = now.day
                            logger.info("📅 New trading day started")

                        # Process based on channel type
                        if channel == "v2/ticker":
                            await _process_ticker_data(symbol, data)
                        elif channel == "v2/trades":
                            await _process_trade_data(symbol, data)
                            
                        # Generate trading signals if analysis worker is active
                        if ANALYSIS_ENABLED and analysis_worker:
                            indicators = analysis_worker.get_indicators(symbol)
                            if indicators:
                                signal = generate_trading_signal(symbol, indicators.get("indicators", {}))
                                if signal["signal"] != "NEUTRAL":
                                    trading_signals[symbol] = {
                                        **signal,
                                        "symbol": symbol,
                                        "current_price": latest_ticks[symbol]["price"],
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

async def _process_ticker_data(symbol: str, data: dict):
    """Process ticker data from Delta"""
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
    bid_size = quotes.get("best_bid_size")
    ask_size = quotes.get("best_ask_size")

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
    latest_ticks[symbol].update({
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
        
        # ORDER BOOK
        "bid": float(bid) if bid else None,
        "ask": float(ask) if ask else None,
        "spread": round(spread, 4) if spread else None,
        
        # LIQUIDITY
        "liquidity": {
            "bid_volume": float(bid_size) if bid_size else 0,
            "ask_volume": float(ask_size) if ask_size else 0,
            "total_volume": float(volume_24h) if volume_24h else 0,
        },
        
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "last_update": datetime.now(timezone.utc).timestamp(),
    })

async def _process_trade_data(symbol: str, data: dict):
    """Process trade data (for volume analysis)"""
    # This would process recent trades for volume profile analysis
    trades = data.get("trades", [])
    if trades:
        # Update volume profile
        buy_volume = sum(t['size'] for t in trades if t.get('side') == 'buy')
        sell_volume = sum(t['size'] for t in trades if t.get('side') == 'sell')
        
        latest_ticks[symbol]["liquidity"].update({
            "recent_buy_volume": buy_volume,
            "recent_sell_volume": sell_volume,
            "buy_sell_ratio": buy_volume / sell_volume if sell_volume > 0 else 0,
        })

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
        "signals": trading_signals,
        "config": TRADING_CONFIG,
        "portfolio": user_portfolio,
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
# FASTAPI SETUP
# ===============================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager with proper cleanup"""
    logger.info("🚀 Starting trading application...")
    
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
    
    logger.info(f"✅ Trading application started on port {PORT}")
    
    yield
    
    # Cleanup on shutdown
    logger.info("🛑 Shutting down application...")
    shutdown_event.set()
    
    if ws_task and not ws_task.done():
        ws_task.cancel()
    
    if analysis_task and not analysis_task.done():
        analysis_task.cancel()
    
    if analysis_worker:
        try:
            analysis_worker.stop()
        except Exception as e:
            logger.error(f"Error stopping analysis worker: {e}")
    
    for ws in list(active_clients):
        try:
            await ws.close()
        except Exception:
            pass
    active_clients.clear()
    
    logger.info("✅ Application stopped gracefully")

app = FastAPI(
    title="Delta India Trading API",
    description="Real-time trading data from Delta Exchange India with technical analysis",
    version="2.0.0",
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
    """Health check endpoint"""
    return {
        "status": "healthy",
        "exchange": "Delta Exchange India",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "websocket_connected": is_delta_connected,
        "analysis_enabled": ANALYSIS_ENABLED,
        "active_clients": len(active_clients),
        "symbols": SYMBOLS,
        "symbols_count": len(SYMBOLS),
    }

# ===============================
# TRADING ENDPOINTS
# ===============================
@app.get("/trading/config")
def get_trading_config():
    """Get trading configuration (min order size, tick size, etc.)"""
    return {
        "status": "success",
        "config": TRADING_CONFIG,
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
    """
    Trading calculator for position sizing
    """
    symbol = symbol.upper()
    if symbol not in SYMBOLS:
        return {
            "status": "error",
            "message": f"Symbol {symbol} not supported"
        }
    
    current_price = latest_ticks[symbol].get("price", 0)
    if not current_price:
        return {
            "status": "error",
            "message": "No price data available"
        }
    
    # Calculate position size
    position_size = calculate_position_size(symbol, risk_amount, stop_loss_percent)
    
    # Calculations
    stop_loss_price = current_price * (1 - stop_loss_percent/100)
    take_profit_price = current_price * (1 + take_profit_percent/100)
    risk_reward_ratio = take_profit_percent / stop_loss_percent
    
    # Leverage calculations
    position_value = position_size * current_price
    margin_required = position_value / leverage
    liquidation_price = current_price * (1 - (1/leverage) + 0.01) if leverage > 1 else None
    
    return {
        "status": "success",
        "symbol": symbol,
        "current_price": current_price,
        "calculations": {
            "position_size": round(position_size, 6),
            "position_value_usd": round(position_value, 2),
            "stop_loss_price": round(stop_loss_price, 2),
            "take_profit_price": round(take_profit_price, 2),
            "stop_loss_percent": stop_loss_percent,
            "take_profit_percent": take_profit_percent,
            "risk_amount_usd": risk_amount,
            "potential_loss_usd": round(position_size * (current_price - stop_loss_price), 2),
            "potential_profit_usd": round(position_size * (take_profit_price - current_price), 2),
            "risk_reward_ratio": round(risk_reward_ratio, 2),
        },
        "leverage": {
            "leverage": leverage,
            "margin_required_usd": round(margin_required, 2),
            "liquidation_price": round(liquidation_price, 2) if liquidation_price else None,
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

@app.get("/trading/signals")
def get_trading_signals(symbol: str = None):
    """Get trading signals for all or specific symbol"""
    if symbol:
        symbol = symbol.upper()
        if symbol not in SYMBOLS:
            return {
                "status": "error",
                "message": f"Symbol {symbol} not supported"
            }
        
        signal = trading_signals.get(symbol)
        if not signal:
            return {
                "status": "no_signal",
                "message": "No trading signals available"
            }
        
        return {
            "status": "success",
            "signal": signal,
            "symbol": symbol,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    
    # Return all signals
    active_signals = {s: sig for s, sig in trading_signals.items() if sig.get("signal") != "NEUTRAL"}
    
    return {
        "status": "success",
        "signals": active_signals,
        "total_signals": len(active_signals),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

# ===============================
# MARKET DATA ENDPOINTS
# ===============================
@app.get("/market")
def get_all_market_data():
    """Get all market data"""
    return {
        "status": "success",
        "data": latest_ticks,
        "signals": trading_signals,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

@app.get("/market/{symbol}")
def get_symbol_data(symbol: str):
    """Get detailed market data for a symbol"""
    symbol = symbol.upper()
    if symbol not in SYMBOLS:
        return {
            "status": "error",
            "message": f"Symbol {symbol} not supported"
        }
    
    data = latest_ticks.get(symbol, {})
    signal = trading_signals.get(symbol, {})
    
    # Add trading info
    trading_info = {
        "min_order_size": TRADING_CONFIG["min_order_size"].get(symbol),
        "tick_size": TRADING_CONFIG["tick_size"].get(symbol),
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

# ===============================
# INDICATORS ENDPOINTS
# ===============================
@app.get("/indicators")
def get_all_indicators():
    """Get all technical indicators"""
    if not ANALYSIS_ENABLED or not analysis_worker:
        return {
            "status": "disabled",
            "message": "Analysis worker not available"
        }
    
    try:
        indicators = analysis_worker.get_all_indicators()
        
        # Add trading signals based on indicators
        signals = {}
        for sym, ind_data in indicators.items():
            if ind_data:
                signal = generate_trading_signal(sym, ind_data.get("indicators", {}))
                signals[sym] = signal
        
        return {
            "status": "success",
            "indicators": indicators,
            "signals": signals,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        logger.error(f"Error in /indicators: {e}")
        return {"status": "error", "message": str(e)}

# ===============================
# WEBSOCKET ENDPOINT
# ===============================
# Add this endpoint to match your client's expectation
@app.websocket("/ws/market")
async def ws_market(ws: WebSocket):
    """WebSocket endpoint for real-time market data (alias for /ws/trading)"""
    await ws.accept()
    active_clients.add(ws)
    logger.info(f"✅ Market client connected. Total: {len(active_clients)}")
    
    try:
        # Send initial data immediately
        await broadcast()
        
        # Keep connection alive and handle messages
        while True:
            try:
                # Wait for client messages with timeout
                data = await asyncio.wait_for(ws.receive_text(), timeout=30.0)
                
                # Process client commands
                try:
                    command = json.loads(data)
                    cmd_type = command.get("type")
                    
                    if cmd_type == "ping":
                        await ws.send_json({"type": "pong"})
                    elif cmd_type == "subscribe":
                        # Handle symbol subscription
                        symbols = command.get("symbols", [])
                        await ws.send_json({
                            "type": "subscribed",
                            "symbols": symbols,
                        })
                    elif cmd_type == "get_portfolio":
                        await ws.send_json({
                            "type": "portfolio",
                            "data": user_portfolio,
                        })
                    
                except json.JSONDecodeError:
                    logger.warning("⚠️ Invalid JSON from client")
                    
            except asyncio.TimeoutError:
                # Send ping to keep connection alive
                try:
                    await ws.send_json({"type": "ping"})
                except Exception:
                    break
                    
    except WebSocketDisconnect:
        logger.info("❌ Market client disconnected")
    except Exception as e:
        logger.error(f"❌ WebSocket error: {e}")
    finally:
        active_clients.discard(ws)
        logger.info(f"📊 Active clients: {len(active_clients)}")

# ===============================
# RUN SERVER
# ===============================
if __name__ == "__main__":
    import uvicorn
    
    logger.info(f"🚀 Starting Delta India Trading API on 0.0.0.0:{PORT}")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=PORT,
        log_level="info",
        access_log=True,
        timeout_keep_alive=75,
    )

