import aiohttp
import time

class DeltaHistory:
    BASE_URL = "https://api.india.delta.exchange/v2"
    
    @staticmethod
    async def fetch_candles(symbol, resolution, limit=100):
        """
        Fetch historical candles from Delta Exchange
        
        Args:
            symbol: Trading symbol (e.g., "BTCUSD")
            resolution: Timeframe (e.g., "1h", "5m", "1d")
            limit: Number of candles to fetch (default: 100)
        
        Returns:
            List of candle data or None if error
        """
        try:
            # Calculate time range based on limit
            end_time = int(time.time())
            
            # Resolution to seconds mapping
            resolution_seconds = {
                "1m": 60,
                "3m": 180,
                "5m": 300,
                "15m": 900,
                "30m": 1800,
                "1h": 3600,
                "2h": 7200,
                "4h": 14400,
                "6h": 21600,
                "1d": 86400,
                "1w": 604800
            }
            
            if resolution not in resolution_seconds:
                print(f"❌ Invalid resolution: {resolution}")
                return None
            
            # Calculate start time
            seconds_per_candle = resolution_seconds[resolution]
            start_time = end_time - (limit * seconds_per_candle)
            
            # API endpoint
            url = f"{DeltaHistory.BASE_URL}/history/candles"
            params = {
                "symbol": symbol,
                "resolution": resolution,
                "start": start_time,
                "end": end_time
            }
            
            # Make async request
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("success"):
                            return data.get("result", [])
                        else:
                            print(f"❌ API Error: {data}")
                            return None
                    else:
                        print(f"❌ HTTP Error: {response.status}")
                        return None
                        
        except Exception as e:
            print(f"❌ Exception: {str(e)}")
            return None
