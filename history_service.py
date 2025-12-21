import aiohttp
import time
import logging
from typing import Optional, List, Dict

logger = logging.getLogger("DELTA-HISTORY")

class DeltaHistory:
    """Delta Exchange India Historical Data Service"""
    
    BASE_URL = "https://api.india.delta.exchange/v2"
    
    # Delta Exchange supported resolutions
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
        "12h": 43200,
        "1d": 86400,
        "1w": 604800,
        "1M": 2592000,  # 30 days approximation
    }
    
    @staticmethod
    async def fetch_candles(symbol: str, resolution: str, limit: int = 100) -> Optional[List[Dict]]:
        """
        Fetch historical candles from Delta Exchange India
        
        Args:
            symbol: Trading symbol (e.g., "BTCUSD")
            resolution: Timeframe (e.g., "1h", "5m")
            limit: Number of candles to fetch (max 2000)
        
        Returns:
            List of candle dictionaries or None if error
        """
        try:
            if resolution not in DeltaHistory.RESOLUTION_SECONDS:
                logger.error(f"Invalid resolution: {resolution}")
                return None
            
            if limit > 2000:
                logger.warning(f"Limit reduced from {limit} to 2000")
                limit = 2000
            
            # Calculate time range
            end_time = int(time.time())
            seconds_per_candle = DeltaHistory.RESOLUTION_SECONDS[resolution]
            start_time = end_time - (limit * seconds_per_candle)
            
            # API endpoint for candles
            url = f"{DeltaHistory.BASE_URL}/history/candles"
            params = {
                "symbol": symbol,
                "resolution": resolution,
                "start": start_time,
                "end": end_time
            }
            
            logger.debug(f"Fetching {limit} {resolution} candles for {symbol}")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, 
                    params=params, 
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    
                    if response.status == 200:
                        data = await response.json()
                        
                        if data.get("success", False):
                            candles = data.get("result", [])
                            
                            # Format candles for consistency
                            formatted_candles = []
                            for candle in candles:
                                formatted_candles.append({
                                    "timestamp": candle.get("time"),
                                    "open": float(candle.get("open", 0)),
                                    "high": float(candle.get("high", 0)),
                                    "low": float(candle.get("low", 0)),
                                    "close": float(candle.get("close", 0)),
                                    "volume": float(candle.get("volume", 0)),
                                })
                            
                            logger.info(f"✅ Fetched {len(formatted_candles)} candles for {symbol} ({resolution})")
                            return formatted_candles
                        else:
                            logger.error(f"API error for {symbol}: {data.get('message', 'Unknown error')}")
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
    
    @staticmethod
    async def fetch_multiple_timeframes(symbol: str, timeframes: List[str]) -> Dict[str, List[Dict]]:
        """
        Fetch candles for multiple timeframes
        
        Args:
            symbol: Trading symbol
            timeframes: List of resolutions to fetch
        
        Returns:
            Dict with timeframe as key and candles as value
        """
        result = {}
        
        for tf in timeframes:
            candles = await DeltaHistory.fetch_candles(symbol, tf, limit=100)
            if candles:
                result[tf] = candles
        
        return result
    
    @staticmethod
    async def fetch_order_book(symbol: str, depth: int = 10) -> Optional[Dict]:
        """
        Fetch order book data
        
        Args:
            symbol: Trading symbol
            depth: Order book depth
        
        Returns:
            Order book data or None
        """
        try:
            url = f"{DeltaHistory.BASE_URL}/orderbook/L2"
            params = {
                "symbol": symbol,
                "depth": depth
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("success"):
                            return data.get("result", {})
            
            return None
        except Exception as e:
            logger.error(f"Error fetching order book for {symbol}: {e}")
            return None
    
    @staticmethod
    async def fetch_recent_trades(symbol: str, limit: int = 50) -> Optional[List[Dict]]:
        """
        Fetch recent trades
        
        Args:
            symbol: Trading symbol
            limit: Number of trades to fetch
        
        Returns:
            List of recent trades or None
        """
        try:
            url = f"{DeltaHistory.BASE_URL}/trades"
            params = {
                "symbol": symbol,
                "limit": min(limit, 100)
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("success"):
                            return data.get("result", [])
            
            return None
        except Exception as e:
            logger.error(f"Error fetching trades for {symbol}: {e}")
            return None
