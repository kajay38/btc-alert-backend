import httpx
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

logger = logging.getLogger("DELTA-HISTORY")

class DeltaHistory:
    BASE_URL = "https://api.delta.exchange/v2"

    @staticmethod
    async def fetch_candles(
        symbol: str, 
        resolution: str = "1h", 
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Delta Exchange se historical candle data fetch karega.
        Resolutions: '1m', '5m', '15m', '30m', '1h', '2h', '4h', '1d', '1w'
        """
        # Delta API endpoint for history
        url = f"{DeltaHistory.BASE_URL}/history/candles"
        
        # Start and End timestamps calculate karein
        # Example: pichle kuch dinon ka data
        end_time = int(datetime.now().timestamp())
        # Resolution ke hisaab se piche jayein (approx)
        start_time = int((datetime.now() - timedelta(days=7)).timestamp())

        params = {
            "symbol": symbol,
            "resolution": resolution,
            "start": start_time,
            "end": end_time,
            "limit": limit
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if data.get("success"):
                    # Delta API returns a list of candle objects
                    candles = data.get("result", [])
                    logger.info(f"✅ Fetched {len(candles)} candles for {symbol}")
                    return candles
                else:
                    logger.error(f"❌ API Error: {data.get('error')}")
                    return []
                    
            except Exception as e:
                logger.error(f"❌ Failed to fetch history for {symbol}: {e}")
                return []

# Example Format jo Delta return karta hai:
# {
#   "time": 1672531200,
#   "open": "16500.5",
#   "high": "16600.0",
#   "low": "16450.0",
#   "close": "16580.0",
#   "volume": "120.5"
# }
