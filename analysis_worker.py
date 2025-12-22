import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
from history_service import DeltaHistory
from indicators import TechnicalIndicators

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("TRADING-ANALYSIS")


class AnalysisWorker:
    """Background worker for trading analysis with rate limiting and memory management"""
    
    def __init__(
        self, 
        symbols: List[str], 
        resolution: str = "1h", 
        update_interval: int = 300,
        batch_size: int = 5,
        max_data_age_hours: int = 24
    ):
        """
        Initialize analysis worker
        
        Args:
            symbols (list): List of symbols to analyze
            resolution (str): Primary timeframe for analysis (e.g., "1h", "15m", "4h")
            update_interval (int): Update frequency in seconds (default: 300 = 5 minutes)
            batch_size (int): Number of symbols to process concurrently (default: 5)
            max_data_age_hours (int): Maximum age of cached data before cleanup (default: 24)
        """
        self.symbols = symbols
        self.resolution = resolution
        self.update_interval = update_interval
        self.batch_size = batch_size
        self.max_data_age_hours = max_data_age_hours
        
        self.indicators_data = {}
        self.trading_signals = {}
        self.is_running = False
        self.task = None
        
        # Multiple timeframes for better analysis
        self.timeframes = ["15m", "1h", "4h", "1d"]
        
        logger.info(f"✅ AnalysisWorker initialized with {len(symbols)} symbols")
        logger.info(f"⚙️ Primary resolution: {resolution}, Update interval: {update_interval}s")
    
    async def start(self):
        """Start the background analysis worker"""
        if self.is_running:
            logger.warning("⚠️ Worker already running")
            return
        
        self.is_running = True
        self.task = asyncio.create_task(self._run())
        logger.info("🚀 Trading analysis worker started")
    
    async def _run(self):
        """Main worker loop"""
        try:
            # Initial update
            await self.update_all_indicators()
            
            # Periodic updates
            while self.is_running:
                try:
                    await asyncio.sleep(self.update_interval)
                    
                    if not self.is_running:
                        break
                    
                    await self.update_all_indicators()
                    
                    # Periodic cleanup of old data
                    self._cleanup_old_data()
                    
                except asyncio.CancelledError:
                    logger.info("🛑 Analysis worker task cancelled")
                    break
                except Exception as e:
                    logger.error(f"❌ Analysis worker error: {e}", exc_info=True)
                    await asyncio.sleep(60)  # Wait before retry
        
        finally:
            logger.info("🏁 Analysis worker loop ended")
    
    async def update_all_indicators(self):
        """Update indicators for all symbols with rate limiting"""
        logger.info(f"🔄 Updating trading indicators for {len(self.symbols)} symbols...")
        
        start_time = datetime.now(timezone.utc)
        success_count = 0
        error_count = 0
        
        # Process symbols in batches to respect rate limits
        for i in range(0, len(self.symbols), self.batch_size):
            batch = self.symbols[i:i + self.batch_size]
            
            # Process batch concurrently
            results = await asyncio.gather(
                *[self._update_symbol(symbol) for symbol in batch],
                return_exceptions=True
            )
            
            # Count successes and errors
            for result in results:
                if isinstance(result, Exception):
                    error_count += 1
                elif result:
                    success_count += 1
                else:
                    error_count += 1
            
            # Rate limit delay between batches (avoid hitting API limits)
            if i + self.batch_size < len(self.symbols):
                await asyncio.sleep(1)
        
        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info(
            f"✅ Update complete: {success_count} success, {error_count} errors "
            f"in {elapsed:.2f}s"
        )
    
    async def _update_symbol(self, symbol: str) -> bool:
        """
        Update indicators for a single symbol
        
        Args:
            symbol (str): Trading symbol to analyze
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Fetch candles for primary timeframe (use self.resolution)
            candles = await DeltaHistory.fetch_candles(
                symbol, 
                self.resolution, 
                limit=200
            )
            
            if not candles or len(candles) < 50:
                logger.warning(
                    f"⚠️ Insufficient candles for {symbol} "
                    f"({len(candles) if candles else 0} candles)"
                )
                return False
            
            # Calculate all indicators
            indicators = TechnicalIndicators.calculate_all_indicators(candles)
            
            # Generate trading signal
            trading_signal = TechnicalIndicators.get_trading_signal(candles)
            
            # Fetch additional timeframes for multi-timeframe analysis
            multi_timeframe_analysis = await self._get_multi_timeframe_analysis(symbol)
            
            # Store in memory
            self.indicators_data[symbol] = {
                "symbol": symbol,
                "resolution": self.resolution,
                "indicators": indicators,
                "trading_signal": trading_signal,
                "multi_timeframe": multi_timeframe_analysis,
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "candles_count": len(candles),
                "last_price": candles[-1].get("close") if candles else None,
            }
            
            # Store trading signal separately for quick access
            self.trading_signals[symbol] = trading_signal
            
            # Log important signals
            if trading_signal and trading_signal.get("signal") != "NEUTRAL":
                logger.info(
                    f"📊 {symbol} | Signal: {trading_signal['signal']} "
                    f"| Confidence: {trading_signal.get('confidence', 0)}% "
                    f"| RSI: {indicators.get('rsi_14', 'N/A')} "
                    f"| Price: {candles[-1].get('close', 'N/A')}"
                )
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error analyzing {symbol}: {e}", exc_info=True)
            return False
    
    async def _get_multi_timeframe_analysis(self, symbol: str) -> Dict:
        """
        Get multi-timeframe analysis for a symbol
        
        Args:
            symbol (str): Trading symbol
            
        Returns:
            dict: Multi-timeframe analysis data
        """
        multi_timeframe_analysis = {}
        
        # Get additional timeframes (exclude primary resolution)
        additional_timeframes = [tf for tf in self.timeframes if tf != self.resolution]
        
        for tf in additional_timeframes:
            try:
                tf_candles = await DeltaHistory.fetch_candles(symbol, tf, limit=100)
                
                if tf_candles and len(tf_candles) >= 20:
                    tf_indicators = TechnicalIndicators.calculate_all_indicators(tf_candles)
                    
                    multi_timeframe_analysis[tf] = {
                        "trend": TechnicalIndicators.get_trend_signal(tf_candles),
                        "rsi": tf_indicators.get("rsi_14"),
                        "ema_alignment": self._get_ema_alignment(tf_indicators),
                        "last_price": tf_candles[-1].get("close") if tf_candles else None,
                    }
                    
            except Exception as e:
                logger.warning(f"⚠️ Error fetching {tf} data for {symbol}: {e}")
        
        return multi_timeframe_analysis
    
    def _get_ema_alignment(self, indicators: Dict) -> str:
        """
        Get EMA alignment status
        
        Args:
            indicators (dict): Calculated indicators
            
        Returns:
            str: "bullish", "bearish", "mixed", or "unknown"
        """
        ema_9 = indicators.get("ema_9")
        ema_20 = indicators.get("ema_20")
        ema_50 = indicators.get("ema_50")
        
        if not all([ema_9, ema_20, ema_50]):
            return "unknown"
        
        if ema_9 > ema_20 > ema_50:
            return "bullish"
        elif ema_9 < ema_20 < ema_50:
            return "bearish"
        else:
            return "mixed"
    
    def _cleanup_old_data(self):
        """Remove stale data to prevent memory leaks"""
        if not self.indicators_data:
            return
        
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self.max_data_age_hours)
        removed_count = 0
        
        for symbol in list(self.indicators_data.keys()):
            try:
                last_updated_str = self.indicators_data[symbol].get("last_updated")
                if last_updated_str:
                    last_updated = datetime.fromisoformat(last_updated_str)
                    
                    if last_updated < cutoff:
                        del self.indicators_data[symbol]
                        if symbol in self.trading_signals:
                            del self.trading_signals[symbol]
                        removed_count += 1
                        
            except Exception as e:
                logger.warning(f"⚠️ Error cleaning up data for {symbol}: {e}")
        
        if removed_count > 0:
            logger.info(f"🧹 Cleaned up {removed_count} stale data entries")
    
    def get_indicators(self, symbol: str) -> Optional[Dict]:
        """
        Get indicators for a specific symbol
        
        Args:
            symbol (str): Trading symbol
            
        Returns:
            dict: Indicator data or None if not found
        """
        return self.indicators_data.get(symbol)
    
    def get_all_indicators(self) -> Dict:
        """
        Get indicators for all symbols
        
        Returns:
            dict: All indicator data
        """
        return self.indicators_data.copy()
    
    def get_trading_signals(self, symbol: Optional[str] = None) -> Dict:
        """
        Get trading signals
        
        Args:
            symbol (str, optional): Specific symbol or None for all
            
        Returns:
            dict: Trading signal(s)
        """
        if symbol:
            return self.trading_signals.get(symbol)
        return self.trading_signals.copy()
    
    def get_multi_timeframe_analysis(self, symbol: str) -> Dict:
        """
        Get multi-timeframe analysis for a symbol
        
        Args:
            symbol (str): Trading symbol
            
        Returns:
            dict: Multi-timeframe analysis data
        """
        data = self.indicators_data.get(symbol, {})
        return data.get("multi_timeframe", {})
    
    def get_top_signals(self, min_confidence: int = 60, limit: Optional[int] = None) -> List[Dict]:
        """
        Get top trading signals by confidence
        
        Args:
            min_confidence (int): Minimum confidence threshold (default: 60)
            limit (int, optional): Maximum number of signals to return
            
        Returns:
            list: Top trading signals sorted by confidence
        """
        top_signals = []
        
        for symbol, signal in self.trading_signals.items():
            if signal and signal.get("confidence", 0) >= min_confidence:
                top_signals.append({
                    "symbol": symbol,
                    "signal": signal.get("signal"),
                    "confidence": signal.get("confidence"),
                    "reasons": signal.get("reasons", []),
                    "score": signal.get("score", 0),
                    "last_price": self.indicators_data.get(symbol, {}).get("last_price"),
                })
        
        # Sort by confidence descending
        top_signals.sort(key=lambda x: x["confidence"], reverse=True)
        
        # Apply limit if specified
        if limit:
            top_signals = top_signals[:limit]
        
        return top_signals
    
    def get_status(self) -> Dict:
        """
        Get worker status information
        
        Returns:
            dict: Status information
        """
        return {
            "is_running": self.is_running,
            "symbols_count": len(self.symbols),
            "tracked_symbols": len(self.indicators_data),
            "resolution": self.resolution,
            "update_interval": self.update_interval,
            "batch_size": self.batch_size,
            "last_update": max(
                [data.get("last_updated") for data in self.indicators_data.values()],
                default=None
            ),
        }
    
    async def stop(self):
        """Stop the worker gracefully"""
        if not self.is_running:
            logger.warning("⚠️ Worker is not running")
            return
        
        logger.info("🛑 Stopping trading analysis worker...")
        self.is_running = False
        
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        
        logger.info("⏹️ Trading analysis stopped")
    
    def __repr__(self):
        return (
            f"AnalysisWorker(symbols={len(self.symbols)}, "
            f"resolution={self.resolution}, "
            f"running={self.is_running})"
        )


# Example usage
async def main():
    """Example usage of AnalysisWorker"""
    
    # Initialize worker
    symbols = ["BTCUSD", "ETHUSD", "SOLUSD", "BNBUSD"]
    worker = AnalysisWorker(
        symbols=symbols,
        resolution="1h",
        update_interval=300,  # 5 minutes
        batch_size=5,
        max_data_age_hours=24
    )
    
    # Start worker
    await worker.start()
    
    # Let it run for some time
    await asyncio.sleep(10)
    
    # Get status
    status = worker.get_status()
    print(f"Worker Status: {status}")
    
    # Get top signals
    top_signals = worker.get_top_signals(min_confidence=60, limit=5)
    print(f"\nTop Signals: {top_signals}")
    
    # Get specific symbol data
    btc_data = worker.get_indicators("BTCUSD")
    if btc_data:
        print(f"\nBTC Indicators: {btc_data['indicators']}")
    
    # Stop worker
    await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
