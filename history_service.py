import aiohttp
import time
import logging

logger = logging.getLogger("HISTORY-SERVICE")

class DeltaHistory:
    """Delta Exchange Historical Data Service"""
    
    BASE_URL = "https://api.india.delta.exchange/v2"
    
    # Resolution to seconds mapping
    RESOLUTION_SECONDS = {
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
    
    @staticmethod
    async def fetch_candles(symbol, resolution, limit=100):
        """
        Fetch historical candles from Delta Exchange
        
        Args:
            symbol (str): Trading symbol (e.g., "BTCUSD", "ETHUSD")
            resolution (str): Timeframe (e.g., "1h", "5m", "1d")
            limit (int): Number of candles to fetch (max 2000)
        
        Returns:
            list: List of candle dictionaries or None if error
        """
        try:
            # Validate resolution
            if resolution not in DeltaHistory.RESOLUTION_SECONDS:
                logger.error(f"Invalid resolution: {resolution}")
                return None
            
            # Limit check (API max is 2000)
            if limit > 2000:
                logger.warning(f"Limit reduced from {limit} to 2000 (API maximum)")
                limit = 2000
            
            # Calculate time range
            end_time = int(time.time())
            seconds_per_candle = DeltaHistory.RESOLUTION_SECONDS[resolution]
            start_time = end_time - (limit * seconds_per_candle)
            
            # API endpoint
            url = f"{DeltaHistory.BASE_URL}/history/candles"
            params = {
                "symbol": symbol,
                "resolution": resolution,
                "start": start_time,
                "end": end_time
            }
            
            logger.info(f"Fetching {limit} candles for {symbol} ({resolution})")
            
            # Make async HTTP request
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if data.get("success"):
                            candles = data.get("result", [])
                            logger.info(f"✅ Fetched {len(candles)} candles for {symbol}")
                            return candles
                        else:
                            logger.error(f"API error for {symbol}: {data}")
                            return None
                    else:
                        error_text = await response.text()
                        logger.error(f"HTTP {response.status} for {symbol}: {error_text}")
                        return None
                        
        except aiohttp.ClientError as e:
            logger.error(f"Network error fetching {symbol}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching {symbol}: {str(e)}")
            return None
