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
        except Exception:
            disconnected.add(ws)

    for ws in disconnected:
        active_clients.discard(ws)

# ===============================
# DELTA WS LISTENER
# ===============================
async def delta_ws_listener():
    global is_delta_connected

    while True:
        try:
            logger.info("Connecting to Delta WebSocket...")
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
                            {"name": "v2/trades", "symbols": SYMBOLS},
                        ]
                    },
                }

                await ws.send(json.dumps(subscribe_msg))
                logger.info("Subscribed to ticker + trades")

                async for message in ws:
                    msg = json.loads(message)

                    # subscription confirm
                    if msg.get("type") == "subscriptions":
                        is_delta_connected = True
                        logger.info("Delta subscription ACTIVE")
                        continue

                    # real data comes as channel_data
                    if msg.get("type") != "channel_data":
                        continue

                    channel = msg.get("channel")
                    symbol = msg.get("symbol")
                    data = msg.get("data", {})

                    if symbol not in SYMBOLS:
                        continue

                    # ===============================
                    # TICKER
                    # ===============================
                    if channel == "v2/ticker":
                        ltp = data.get("close")
                        mark = data.get("mark_price")
                        bid = data.get("best_bid")
                        ask = data.get("best_ask")

                        # smooth price (Delta-style UI)
                        if bid and ask:
                            price = (float(bid) + float(ask)) / 2
                        elif ltp:
                            price = float(ltp)
                        elif mark:
                            price = float(mark)
                        else:
                            price = None

                        latest_ticks[symbol] = {
                            "symbol": symbol,
                            "price": price,
                            "ltp": float(ltp) if ltp else None,
                            "mark_price": float(mark) if mark else None,
                            "bid": float(bid) if bid else None,
                            "ask": float(ask) if ask else None,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }

                        logger.info(
                            f"{symbol} | price={price} bid={bid} ask={ask}"
                        )

                        await broadcast()

                    # ===============================
                    # TRADES
                    # ===============================
                    elif channel == "v2/trades":
                        trade = {
                            "price": float(data.get("price", 0)),
                            "size": data.get("size"),
                            "side": data.get("side"),  # buy / sell
                            "timestamp": data.get("timestamp"),
                        }

                        latest_trades[symbol].insert(0, trade)
                        latest_trades[symbol] = latest_trades[symbol][:MAX_TRADES]

                        await broadcast()

        except Exception as e:
            is_delta_connected = False
            logger.error(f"Delta WS error: {e}")
            await asyncio.sleep(5)

# ===============================
# FASTAPI LIFESPAN
# ===============================
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(delta_ws_listener())
    yield
    task.cancel()

# ===============================
# FASTAPI APP
# ===============================
app = FastAPI(title="Delta Market Pro", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===============================
# REST ENDPOINTS
# ===============================
@app.get("/")
async def health():
    return {
        "status": "ok",
        "delta_connected": is_delta_connected,
        "symbols": SYMBOLS,
    }

@app.get("/api/tickers")
async def get_tickers():
    return {
        "ticks": latest_ticks,
        "status": "connected" if is_delta_connected else "disconnected",
    }

@app.get("/api/trades/{symbol}")
async def get_trades(symbol: str):
    symbol = symbol.upper()
    if symbol not in SYMBOLS:
        return {"error": "Invalid symbol"}
    return {"symbol": symbol, "trades": latest_trades[symbol]}

# ===============================
# FLUTTER WEBSOCKET
# ===============================
@app.websocket("/ws/market")
async def ws_market(websocket: WebSocket):
    await websocket.accept()
    active_clients.add(websocket)
    logger.info(f"Flutter connected | total={len(active_clients)}")

    # send initial snapshot
    await websocket.send_json({
        "ticks": latest_ticks,
        "trades": latest_trades,
        "status": "connected" if is_delta_connected else "reconnecting",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })

    try:
        while True:
            # keep alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_clients.discard(websocket)
        logger.info(f"Flutter disconnected | total={len(active_clients)}")
