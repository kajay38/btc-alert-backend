import asyncio
import json
import os
from datetime import datetime
from typing import Dict, Any, List

import websockets
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# ================= SETUP =================
load_dotenv()

app = FastAPI(title="Delta WS Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DELTA_WS_URL = "wss://socket.delta.exchange"


SYMBOLS = ["BTCUSD"]

latest_ticks: Dict[str, Dict[str, Any]] = {}
is_connected = False


# ================= DELTA WS =================
async def delta_ws_listener():
    global is_connected

    while True:
        try:
            async with websockets.connect(
                "wss://socket.delta.exchange",
                ping_interval=20,
                ping_timeout=20,
            ) as ws:

                subscribe_msg = {
                    "type": "subscribe",
                    "payload": {
                        "channels": [
                            {
                                "name": "ticker",
                                "symbols": ["BTCUSD"]
                            }
                        ]
                    }
                }

                await ws.send(json.dumps(subscribe_msg))
                is_connected = True
                print("✅ Connected to Delta WS")

                async for msg in ws:
                    data = json.loads(msg)

                    # ✅ CORRECT PARSING
                    if data.get("type") == "ticker" and "data" in data:
                        ticker = data["data"]

                        if ticker.get("symbol") == "BTCUSD":
                            latest_ticks["BTCUSD"] = {
                                "symbol": "BTCUSD",
                                "price": float(ticker.get("mark_price", 0)),
                                "timestamp": datetime.utcnow().isoformat()
                            }

        except Exception as e:
            is_connected = False
            print("❌ Delta WS error:", e)
            await asyncio.sleep(5)



@app.on_event("startup")
async def startup():
    for s in SYMBOLS:
        latest_ticks[s] = {
            "symbol": s,
            "price": 0.0,
            "timestamp": datetime.utcnow().isoformat()
        }

    asyncio.create_task(delta_ws_listener())
    print("🚀 Backend started, WS task running")


# ================= FLUTTER WS =================
@app.websocket("/ws/tickers")
async def flutter_ws(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            await ws.send_json({"ticks": latest_ticks})
            await asyncio.sleep(0.2)  # 200ms tick
    except:
        pass


# ================= REST =================
@app.get("/status")
def status():
    return {
        "status": "up",
        "delta_ws": "connected" if is_connected else "disconnected",
        "ticks": latest_ticks,
        "time": datetime.utcnow().isoformat()
    }
