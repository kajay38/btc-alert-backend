import aiohttp
import asyncio
import time
import logging
from typing import Optional, List, Dict
from functools import wraps

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
    
    # Class-level session for reuse
    _session: Optional[aiohttp.ClientSession] = None
    _session_lock = asyncio.Lock()
    
    # Rate limiting
    _last_request_time = 0
    _min_request_interval = 0.1  # 100ms between requests (10 requests/second)
    _rate_limit_lock = asyncio.Lock()
    
    @classmethod
    async def get_session(cls) -> aiohttp.ClientSession:
        """Get or create aiohttp session (singleton pattern)"""
        async with cls._session_lock:
            if cls._session is None or cls._session.closed:
                timeout = aiohttp.ClientTimeout(total=30, connect=10)
                cls._session = aiohttp.ClientSession(
                    timeout=timeout,
                    headers={
                        "User-Agent": "DeltaHistory/1.0",
                        "Accept": "application/json"
                    }
                )
                logger.info("Created new aiohttp session")
            return cls._session
    
    @classmethod
    async def close_session(cls):
        """Close the aiohttp session"""
        async with cls._session_lock:
            if cls._session and not cls._session.closed:
                await cls._session.close()
                cls._session = None
                logger.info("Closed aiohttp session")
    
    @classmethod
    async def _rate_limit(cls):
        """Implement rate limiting between requests"""
        async with cls._rate_limit_lock:
            current_time = time.time()
            time_since_last_request = current_time - cls._last_request_time
            
            if time_since_last_request < cls._min_request_interval:
                sleep_time = cls._min_request_interval - time_since_last_request
                await asyncio.sleep(sleep_time)
            
            cls._last_request_time = time.time()
    
    @staticmethod
    def retry_on_failure(max_retries: int = 3, backoff_factor: float = 1.5):
        """Decorator for retry logic with exponential backoff"""
        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                last_exception = None
                
                for attempt in range(max_retries):
                    try:
                        return await func(*args, **kwargs)
                    except aiohttp.ClientError as e:
                        last_exception = e
                        if attempt < max_retries - 1:
                            wait_time = backoff_factor ** attempt
                            logger.warning(
                                f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}. "
                                f"Retrying in {wait_time:.1f}s..."
                            )
                            await asyncio.sleep(wait_time)
                        else:
                            logger.error(f"All {max_retries} attempts failed: {str(e)}")
                    except Exception as e:
                        logger.error(f"Unexpected error in {func.__name__}: {str(e)}")
                        raise
                
                # If all retries failed, return None or raise
                logger.error(f"Failed after {max_retries} attempts: {last_exception}")
                return None
            
            return wrapper
        return decorator
    
    @staticmethod
    def validate_symbol(symbol: str) -> bool:
        """Validate trading symbol format"""
        if not symbol or not isinstance(symbol, str):
            return False
        
        # Basic validation: should be alphanumeric with possible hyphens/underscores
        if not symbol.replace("-", "").replace("_", "").replace("|", "").isalnum():
            return False
        
        return True
    
    @staticmethod
    def validate_resolution(resolution: str) -> bool:
        """Validate resolution format"""
        return resolution in DeltaHistory.RESOLUTION_SECONDS
    
    @classmethod
    @retry_on_failure(max_retries=3, backoff_factor=1.5)
    async def fetch_candles(
        cls, 
        symbol: str, 
        resolution: str, 
        limit: int = 100,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None
    ) -> Optional[List[Dict]]:
        """
        Fetch historical candles from Delta Exchange India
        
        Args:
            symbol: Trading symbol (e.g., "BTCUSD")
            resolution: Timeframe (e.g., "1h", "5m")
            limit: Number of candles to fetch (max 2000)
            start_time: Optional start timestamp (unix seconds)
            end_time: Optional end timestamp (unix seconds)
        
        Returns:
            List of candle dictionaries or None if error
        """
        # Validation
        if not cls.validate_symbol(symbol):
            logger.error(f"Invalid symbol format: {symbol}")
            return None
        
        if not cls.validate_resolution(resolution):
            logger.error(f"Invalid resolution: {resolution}. Must be one of {list(cls.RESOLUTION_SECONDS.keys())}")
            return None
        
        if limit > 2000:
            logger.warning(f"Limit reduced from {limit} to 2000 (API maximum)")
            limit = 2000
        
        if limit < 1:
            logger.error(f"Invalid limit: {limit}. Must be at least 1")
            return None
        
        try:
            # Calculate time range if not provided
            if end_time is None:
                end_time = int(time.time())
            
            if start_time is None:
                seconds_per_candle = cls.RESOLUTION_SECONDS[resolution]
                start_time = end_time - (limit * seconds_per_candle)
            
            # API endpoint for candles
            url = f"{cls.BASE_URL}/history/candles"
            params = {
                "symbol": symbol,
                "resolution": resolution,  # Delta API accepts string format
                "start": start_time,
                "end": end_time
            }
            
            logger.debug(f"Fetching {limit} {resolution} candles for {symbol}")
            
            # Rate limiting
            await cls._rate_limit()
            
            # Get session
            session = await cls.get_session()
            
            async with session.get(url, params=params) as response:
                
                if response.status == 200:
                    data = await response.json()
                    
                    if data.get("success", False):
                        candles = data.get("result", [])
                        
                        if not candles:
                            logger.warning(f"No candle data returned for {symbol} ({resolution})")
                            return []
                        
                        # Format candles for consistency
                        formatted_candles = []
                        for candle in candles:
                            try:
                                formatted_candles.append({
                                    "timestamp": candle.get("time"),
                                    "open": float(candle.get("open", 0)),
                                    "high": float(candle.get("high", 0)),
                                    "low": float(candle.get("low", 0)),
                                    "close": float(candle.get("close", 0)),
                                    "volume": float(candle.get("volume", 0)),
                                })
                            except (ValueError, TypeError) as e:
                                logger.warning(f"Skipping invalid candle data: {e}")
                                continue
                        
                        logger.info(f"✅ Fetched {len(formatted_candles)} candles for {symbol} ({resolution})")
                        return formatted_candles
                    else:
                        error_msg = data.get("error", {}).get("message") if isinstance(data.get("error"), dict) else data.get("message", "Unknown error")
                        logger.error(f"API error for {symbol}: {error_msg}")
                        return None
                
                elif response.status == 429:
                    logger.error(f"Rate limit exceeded for {symbol}. Please slow down requests.")
                    await asyncio.sleep(2)  # Wait before retry
                    return None
                
                elif response.status == 400:
                    error_text = await response.text()
                    logger.error(f"Bad request for {symbol}: {error_text}")
                    return None
                
                else:
                    error_text = await response.text()
                    logger.error(f"HTTP {response.status} for {symbol}: {error_text}")
                    return None
                    
        except aiohttp.ClientError as e:
            logger.error(f"Network error fetching {symbol}: {str(e)}")
            raise  # Let retry decorator handle it
        
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching {symbol}")
            raise  # Let retry decorator handle it
        
        except Exception as e:
            logger.error(f"Unexpected error fetching {symbol}: {str(e)}")
            return None
    
    @classmethod
    async def fetch_multiple_timeframes(
        cls, 
        symbol: str, 
        timeframes: List[str],
        limit: int = 100
    ) -> Dict[str, List[Dict]]:
        """
        Fetch candles for multiple timeframes with rate limiting
        
        Args:
            symbol: Trading symbol
            timeframes: List of resolutions to fetch
            limit: Number of candles per timeframe
        
        Returns:
            Dict with timeframe as key and candles as value
        """
        if not cls.validate_symbol(symbol):
            logger.error(f"Invalid symbol: {symbol}")
            return {}
        
        result = {}
        
        # Validate all timeframes first
        valid_timeframes = [tf for tf in timeframes if cls.validate_resolution(tf)]
        invalid_timeframes = [tf for tf in timeframes if not cls.validate_resolution(tf)]
        
        if invalid_timeframes:
            logger.warning(f"Skipping invalid timeframes: {invalid_timeframes}")
        
        if not valid_timeframes:
            logger.error("No valid timeframes provided")
            return {}
        
        # Fetch sequentially with rate limiting (already handled in fetch_candles)
        for tf in valid_timeframes:
            candles = await cls.fetch_candles(symbol, tf, limit=limit)
            if candles:
                result[tf] = candles
            else:
                logger.warning(f"Failed to fetch {tf} data for {symbol}")
        
        logger.info(f"Fetched {len(result)}/{len(valid_timeframes)} timeframes for {symbol}")
        return result
    
    @classmethod
    @retry_on_failure(max_retries=3, backoff_factor=1.5)
    async def fetch_order_book(cls, symbol: str, depth: int = 10) -> Optional[Dict]:
        """
        Fetch order book data (L2)
        
        Args:
            symbol: Trading symbol
            depth: Order book depth (number of levels on each side)
        
        Returns:
            Order book data or None
        """
        if not cls.validate_symbol(symbol):
            logger.error(f"Invalid symbol: {symbol}")
            return None
        
        if depth < 1 or depth > 100:
            logger.warning(f"Invalid depth {depth}, using default 10")
            depth = 10
        
        try:
            # Fixed: Correct endpoint with symbol in path
            url = f"{cls.BASE_URL}/l2orderbook/{symbol}"
            params = {"depth": depth}
            
            logger.debug(f"Fetching L2 orderbook for {symbol} (depth={depth})")
            
            # Rate limiting
            await cls._rate_limit()
            
            session = await cls.get_session()
            
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("success"):
                        result = data.get("result", {})
                        logger.info(f"✅ Fetched orderbook for {symbol}")
                        return result
                    else:
                        error_msg = data.get("error", {}).get("message") if isinstance(data.get("error"), dict) else data.get("message", "Unknown error")
                        logger.error(f"API error fetching orderbook for {symbol}: {error_msg}")
                        return None
                else:
                    error_text = await response.text()
                    logger.error(f"HTTP {response.status} fetching orderbook for {symbol}: {error_text}")
                    return None
        
        except aiohttp.ClientError as e:
            logger.error(f"Network error fetching orderbook for {symbol}: {str(e)}")
            raise  # Let retry decorator handle it
        
        except Exception as e:
            logger.error(f"Error fetching orderbook for {symbol}: {e}")
            return None
    
    @classmethod
    @retry_on_failure(max_retries=3, backoff_factor=1.5)
    async def fetch_recent_trades(cls, symbol: str) -> Optional[List[Dict]]:
        """
        Fetch recent trades
        
        Args:
            symbol: Trading symbol
        
        Returns:
            List of recent trades or None
        """
        if not cls.validate_symbol(symbol):
            logger.error(f"Invalid symbol: {symbol}")
            return None
        
        try:
            # Fixed: Correct endpoint with symbol in path, no limit parameter
            url = f"{cls.BASE_URL}/trades/{symbol}"
            
            logger.debug(f"Fetching recent trades for {symbol}")
            
            # Rate limiting
            await cls._rate_limit()
            
            session = await cls.get_session()
            
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("success"):
                        result = data.get("result", {})
                        trades = result.get("trades", [])
                        logger.info(f"✅ Fetched {len(trades)} recent trades for {symbol}")
                        return trades
                    else:
                        error_msg = data.get("error", {}).get("message") if isinstance(data.get("error"), dict) else data.get("message", "Unknown error")
                        logger.error(f"API error fetching trades for {symbol}: {error_msg}")
                        return None
                else:
                    error_text = await response.text()
                    logger.error(f"HTTP {response.status} fetching trades for {symbol}: {error_text}")
                    return None
        
        except aiohttp.ClientError as e:
            logger.error(f"Network error fetching trades for {symbol}: {str(e)}")
            raise  # Let retry decorator handle it
        
        except Exception as e:
            logger.error(f"Error fetching trades for {symbol}: {e}")
            return None
    
    @classmethod
    async def fetch_ticker(cls, symbol: str) -> Optional[Dict]:
        """
        Fetch ticker data for a symbol
        
        Args:
            symbol: Trading symbol
        
        Returns:
            Ticker data or None
        """
        if not cls.validate_symbol(symbol):
            logger.error(f"Invalid symbol: {symbol}")
            return None
        
        try:
            url = f"{cls.BASE_URL}/tickers/{symbol}"
            
            logger.debug(f"Fetching ticker for {symbol}")
            
            # Rate limiting
            await cls._rate_limit()
            
            session = await cls.get_session()
            
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("success"):
                        result = data.get("result")
                        logger.info(f"✅ Fetched ticker for {symbol}")
                        return result
                    else:
                        error_msg = data.get("error", {}).get("message") if isinstance(data.get("error"), dict) else data.get("message", "Unknown error")
                        logger.error(f"API error fetching ticker for {symbol}: {error_msg}")
                        return None
                else:
                    error_text = await response.text()
                    logger.error(f"HTTP {response.status} fetching ticker for {symbol}: {error_text}")
                    return None
        
        except Exception as e:
            logger.error(f"Error fetching ticker for {symbol}: {e}")
            return None
    
    @classmethod
    async def get_product_info(cls, symbol: str) -> Optional[Dict]:
        """
        Get product information for a symbol
        
        Args:
            symbol: Trading symbol
        
        Returns:
            Product info or None
        """
        if not cls.validate_symbol(symbol):
            logger.error(f"Invalid symbol: {symbol}")
            return None
        
        try:
            url = f"{cls.BASE_URL}/products"
            
            logger.debug(f"Fetching product info for {symbol}")
            
            # Rate limiting
            await cls._rate_limit()
            
            session = await cls.get_session()
            
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("success"):
                        products = data.get("result", [])
                        # Find the product with matching symbol
                        for product in products:
                            if product.get("symbol") == symbol:
                                logger.info(f"✅ Found product info for {symbol}")
                                return product
                        
                        logger.warning(f"Product {symbol} not found in products list")
                        return None
                    else:
                        error_msg = data.get("error", {}).get("message") if isinstance(data.get("error"), dict) else data.get("message", "Unknown error")
                        logger.error(f"API error fetching products: {error_msg}")
                        return None
                else:
                    error_text = await response.text()
                    logger.error(f"HTTP {response.status} fetching products: {error_text}")
                    return None
        
        except Exception as e:
            logger.error(f"Error fetching product info for {symbol}: {e}")
            return None
    
    @classmethod
    async def health_check(cls) -> bool:
        """
        Check if Delta Exchange API is accessible
        
        Returns:
            True if API is healthy, False otherwise
        """
        try:
            url = f"{cls.BASE_URL}/products"
            
            session = await cls.get_session()
            
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("success"):
                        logger.info("✅ Delta Exchange API health check passed")
                        return True
            
            logger.warning("❌ Delta Exchange API health check failed")
            return False
        
        except Exception as e:
            logger.error(f"❌ Delta Exchange API health check error: {e}")
            return False


# Context manager for proper session cleanup
class DeltaHistoryContext:
    """Context manager for DeltaHistory to ensure proper cleanup"""
    
    async def __aenter__(self):
        return DeltaHistory
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await DeltaHistory.close_session()


# Example usage
async def example_usage():
    """Example of how to use DeltaHistory"""
    
    # Using context manager (recommended)
    async with DeltaHistoryContext() as delta:
        # Health check
        is_healthy = await delta.health_check()
        print(f"API Health: {is_healthy}")
        
        # Fetch candles
        candles = await delta.fetch_candles("BTCUSD", "1h", limit=100)
        if candles:
            print(f"Fetched {len(candles)} candles")
            print(f"Latest candle: {candles[-1]}")
        
        # Fetch multiple timeframes
        multi_tf = await delta.fetch_multiple_timeframes(
            "BTCUSD", 
            ["15m", "1h", "4h"],
            limit=50
        )
        print(f"Fetched {len(multi_tf)} timeframes")
        
        # Fetch orderbook
        orderbook = await delta.fetch_order_book("BTCUSD", depth=10)
        if orderbook:
            print(f"Orderbook: {len(orderbook.get('buy', []))} bids, {len(orderbook.get('sell', []))} asks")
        
        # Fetch recent trades
        trades = await delta.fetch_recent_trades("BTCUSD")
        if trades:
            print(f"Recent trades: {len(trades)}")
        
        # Fetch ticker
        ticker = await delta.fetch_ticker("BTCUSD")
        if ticker:
            print(f"Ticker: {ticker.get('close')}")
        
        # Get product info
        product = await delta.get_product_info("BTCUSD")
        if product:
            print(f"Product: {product.get('description')}")


if __name__ == "__main__":
    # Run example
    asyncio.run(example_usage())
