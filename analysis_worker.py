[file name]: analysis_worker.py
[file content begin]
import asyncio
import logging
from datetime import datetime, timezone
from history_service import DeltaHistory
from indicators import TechnicalIndicators

logger = logging.getLogger("TRADING-ANALYSIS")

class AnalysisWorker:
    """Background worker for trading analysis"""
    
    def __init__(self, symbols, resolution="1h", update_interval=300):
        """
        Initialize analysis worker
        
        Args:
            symbols (list): List of symbols to analyze
            resolution (str): Timeframe for analysis
            update_interval (int): Update frequency in seconds
        """
        self.symbols = symbols
        self.resolution = resolution
        self.update_interval = update_interval
        self.indicators_data = {}
        self.trading_signals = {}
        self.is_running = False
        
        # Multiple timeframes for better analysis
        self.timeframes = ["15m", "1h", "4h", "1d"]
    
    async def start(self):
        """Start the background analysis worker"""
        self.is_running = True
        logger.info(f"🚀 Trading analysis started for {len(self.symbols)} symbols")
        
        # Initial update
        await self.update_all_indicators()
        
        # Periodic updates
        while self.is_running:
            try:
                await asyncio.sleep(self.update_interval)
                await self.update_all_indicators()
            except Exception as e:
                logger.error(f"❌ Analysis worker error: {e}")
                await asyncio.sleep(60)
    
    async def update_all_indicators(self):
        """Update indicators for all symbols"""
        logger.info("🔄 Updating trading indicators...")
        
        for symbol in self.symbols:
            try:
                # Fetch candles for primary timeframe
                candles_1h = await DeltaHistory.fetch_candles(symbol, "1h", limit=200)
                
                if not candles_1h:
                    logger.warning(f"⚠️ No 1h candles for {symbol}")
                    continue
                
                # Calculate all indicators
                indicators = TechnicalIndicators.calculate_all_indicators(candles_1h)
                
                # Generate trading signal
                trading_signal = TechnicalIndicators.get_trading_signal(candles_1h)
                
                # Fetch additional timeframes for multi-timeframe analysis
                multi_timeframe_analysis = {}
                for tf in ["15m", "4h", "1d"]:
                    try:
                        tf_candles = await DeltaHistory.fetch_candles(symbol, tf, limit=100)
                        if tf_candles:
                            tf_indicators = TechnicalIndicators.calculate_all_indicators(tf_candles)
                            multi_timeframe_analysis[tf] = {
                                "trend": TechnicalIndicators.get_trend_signal(tf_candles),
                                "rsi": tf_indicators.get("rsi_14"),
                                "ema_alignment": self._get_ema_alignment(tf_indicators),
                            }
                    except Exception as e:
                        logger.warning(f"⚠️ Error fetching {tf} data for {symbol}: {e}")
                
                # Store in memory
                self.indicators_data[symbol] = {
                    "symbol": symbol,
                    "resolution": self.resolution,
                    "indicators": indicators,
                    "trading_signal": trading_signal,
                    "multi_timeframe": multi_timeframe_analysis,
                    "last_updated": datetime.now(timezone.utc).isoformat(),
                    "candles_count": len(candles_1h),
                }
                
                # Store trading signal separately
                self.trading_signals[symbol] = trading_signal
                
                # Log important signals
                if trading_signal["signal"] != "NEUTRAL":
                    logger.info(
                        f"📊 {symbol} | Signal: {trading_signal['signal']} "
                        f"| Confidence: {trading_signal['confidence']}% "
                        f"| RSI: {indicators.get('rsi_14')} "
                        f"| Price: {candles_1h[-1]['close'] if candles_1h else 'N/A'}"
                    )
                
            except Exception as e:
                logger.error(f"❌ Error analyzing {symbol}: {e}")
    
    def _get_ema_alignment(self, indicators: dict) -> str:
        """Get EMA alignment status"""
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
    
    def get_indicators(self, symbol: str) -> dict:
        """Get indicators for a specific symbol"""
        return self.indicators_data.get(symbol)
    
    def get_all_indicators(self) -> dict:
        """Get indicators for all symbols"""
        return self.indicators_data
    
    def get_trading_signals(self, symbol: str = None) -> dict:
        """Get trading signals"""
        if symbol:
            return self.trading_signals.get(symbol)
        return self.trading_signals
    
    def get_multi_timeframe_analysis(self, symbol: str) -> dict:
        """Get multi-timeframe analysis for a symbol"""
        data = self.indicators_data.get(symbol, {})
        return data.get("multi_timeframe", {})
    
    def get_top_signals(self, min_confidence: int = 60) -> list:
        """Get top trading signals by confidence"""
        top_signals = []
        
        for symbol, signal in self.trading_signals.items():
            if signal and signal.get("confidence", 0) >= min_confidence:
                top_signals.append({
                    "symbol": symbol,
                    "signal": signal.get("signal"),
                    "confidence": signal.get("confidence"),
                    "reasons": signal.get("reasons", []),
                    "score": signal.get("score", 0),
                })
        
        # Sort by confidence descending
        top_signals.sort(key=lambda x: x["confidence"], reverse=True)
        return top_signals
    
    def stop(self):
        """Stop the worker"""
        self.is_running = False
        logger.info("⏹️ Trading analysis stopped")
[file content end]
