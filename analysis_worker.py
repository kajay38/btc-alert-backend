import asyncio
import logging
from datetime import datetime, timezone
from history_service import DeltaHistory
from indicators import TechnicalIndicators

logger = logging.getLogger("ANALYSIS-WORKER")

class AnalysisWorker:
    """Background worker for calculating EMAs and MAs"""
    
    def __init__(self, symbols, resolution="1h", update_interval=300):
        """
        Initialize analysis worker
        
        Args:
            symbols (list): List of symbols to analyze
            resolution (str): Timeframe for analysis (default: 1h)
            update_interval (int): How often to update (seconds, default: 300 = 5 min)
        """
        self.symbols = symbols
        self.resolution = resolution
        self.update_interval = update_interval
        self.indicators_data = {}
        self.is_running = False
    
    async def start(self):
        """Start the background analysis worker"""
        self.is_running = True
        logger.info(f"🚀 Analysis worker started for {self.symbols}")
        
        while self.is_running:
            try:
                await self.update_all_indicators()
                await asyncio.sleep(self.update_interval)
            except Exception as e:
                logger.error(f"❌ Analysis worker error: {e}")
                await asyncio.sleep(60)  # Wait 1 min on error
    
    async def update_all_indicators(self):
        """Update indicators for all symbols"""
        logger.info("🔄 Updating indicators for all symbols...")
        
        for symbol in self.symbols:
            try:
                # Fetch candles (200 for better EMA calculation)
                candles = await DeltaHistory.fetch_candles(symbol, self.resolution, limit=200)
                
                if not candles:
                    logger.warning(f"⚠️ No candles received for {symbol}")
                    continue
                
                # Calculate all indicators
                indicators = TechnicalIndicators.calculate_all_indicators(candles)
                trend = TechnicalIndicators.get_trend_signal(candles)
                
                # Store in memory
                self.indicators_data[symbol] = {
                    "symbol": symbol,
                    "resolution": self.resolution,
                    "indicators": indicators,
                    "trend": trend,
                    "last_updated": datetime.now(timezone.utc).isoformat(),
                    "candles_count": len(candles),
                }
                
                logger.info(
                    f"✅ {symbol} | EMA(20): {indicators['ema_20']} | "
                    f"SMA(20): {indicators['sma_20']} | Trend: {trend}"
                )
                
            except Exception as e:
                logger.error(f"❌ Error updating {symbol}: {e}")
    
    def get_indicators(self, symbol):
        """Get indicators for a specific symbol"""
        return self.indicators_data.get(symbol)
    
    def get_all_indicators(self):
        """Get indicators for all symbols"""
        return self.indicators_data
    
    def stop(self):
        """Stop the worker"""
        self.is_running = False
        logger.info("⏹️ Analysis worker stopped")
