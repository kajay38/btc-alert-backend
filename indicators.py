import logging
import math
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger("TRADING-INDICATORS")

class TechnicalIndicators:
    """Technical Indicators Calculator for Trading"""
    
    @staticmethod
    def validate_candles(candles: List[Dict], min_length: int = 1) -> bool:
        """Validate candle data quality"""
        if not candles or len(candles) < min_length:
            return False
        
        try:
            for candle in candles:
                if not all(key in candle for key in ['open', 'high', 'low', 'close', 'volume']):
                    return False
                if not all(isinstance(candle[key], (int, float)) for key in ['open', 'high', 'low', 'close', 'volume']):
                    return False
                # Validate price logic
                if candle['high'] < candle['low']:
                    return False
                if candle['high'] < candle['close'] or candle['high'] < candle['open']:
                    return False
                if candle['low'] > candle['close'] or candle['low'] > candle['open']:
                    return False
        except (TypeError, KeyError):
            return False
        
        return True
    
    @staticmethod
    def calculate_sma(candles: List[Dict], period: int = 20) -> Optional[float]:
        """Calculate Simple Moving Average"""
        if not TechnicalIndicators.validate_candles(candles, period):
            return None
        
        if len(candles) < period:
            return None
        
        closes = [candles[i]['close'] for i in range(-period, 0)]
        sma = sum(closes) / period
        return round(sma, 6)
    
    @staticmethod
    def calculate_ema(candles: List[Dict], period: int = 20) -> Optional[float]:
        """Calculate Exponential Moving Average"""
        if not TechnicalIndicators.validate_candles(candles, period):
            return None
        
        if len(candles) < period:
            return None
        
        multiplier = 2 / (period + 1)
        
        # Calculate initial SMA
        sma = sum([candles[i]['close'] for i in range(period)]) / period
        ema = sma
        
        # Calculate EMA for remaining candles
        for i in range(period, len(candles)):
            close = candles[i]['close']
            ema = (close - ema) * multiplier + ema
        
        return round(ema, 6)
    
    @staticmethod
    def calculate_rsi(candles: List[Dict], period: int = 14) -> Optional[float]:
        """
        Calculate Relative Strength Index (RSI)
        Fixed: Now calculates from oldest to newest in chronological order
        """
        if not TechnicalIndicators.validate_candles(candles, period + 1):
            return None
        
        if len(candles) < period + 1:
            return None
        
        gains = []
        losses = []
        
        # Calculate price changes from oldest to newest (forward iteration)
        start_idx = len(candles) - period - 1
        for i in range(start_idx, len(candles) - 1):
            change = candles[i + 1]['close'] - candles[i]['close']
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return round(rsi, 2)
    
    @staticmethod
    def calculate_macd(candles: List[Dict], fast: int = 12, slow: int = 26, signal: int = 9) -> Optional[Dict]:
        """
        Calculate MACD (Moving Average Convergence Divergence)
        Fixed: Proper signal line calculation using EMA of MACD line
        """
        if not TechnicalIndicators.validate_candles(candles, slow + signal):
            return None
        
        if len(candles) < slow + signal:
            return None
        
        # Calculate MACD line for multiple periods to get signal line
        macd_values = []
        
        # Calculate MACD for last 'signal + 10' periods to have enough data for signal EMA
        lookback = signal + 10
        for i in range(max(0, len(candles) - lookback), len(candles)):
            subset = candles[:i+1]
            if len(subset) >= slow:
                ema_fast = TechnicalIndicators.calculate_ema(subset, fast)
                ema_slow = TechnicalIndicators.calculate_ema(subset, slow)
                
                if ema_fast is not None and ema_slow is not None:
                    macd_values.append(ema_fast - ema_slow)
        
        if len(macd_values) < signal:
            return None
        
        # Current MACD line value
        macd_line = macd_values[-1]
        
        # Calculate signal line as EMA of MACD values
        multiplier = 2 / (signal + 1)
        signal_line = sum(macd_values[:signal]) / signal  # Initial SMA
        
        for i in range(signal, len(macd_values)):
            signal_line = (macd_values[i] - signal_line) * multiplier + signal_line
        
        histogram = macd_line - signal_line
        
        return {
            "macd": round(macd_line, 6),
            "signal": round(signal_line, 6),
            "histogram": round(histogram, 6),
            "crossover": "bullish" if histogram > 0 else "bearish",
        }
    
    @staticmethod
    def calculate_bollinger_bands(candles: List[Dict], period: int = 20, std_dev: float = 2.0) -> Optional[Dict]:
        """
        Calculate Bollinger Bands
        Fixed: Better squeeze detection using bandwidth percentile
        """
        if not TechnicalIndicators.validate_candles(candles, period):
            return None
        
        if len(candles) < period:
            return None
        
        closes = [candles[i]['close'] for i in range(-period, 0)]
        sma = sum(closes) / period
        
        # Calculate standard deviation
        variance = sum([(price - sma) ** 2 for price in closes]) / period
        std = math.sqrt(variance)
        
        upper_band = sma + (std_dev * std)
        lower_band = sma - (std_dev * std)
        
        current_price = candles[-1]['close']
        
        # BB Percentage (where price is within the bands)
        band_width = upper_band - lower_band
        bb_percentage = ((current_price - lower_band) / band_width * 100) if band_width > 0 else 50
        
        # Bandwidth as percentage of middle band
        bandwidth_percent = (band_width / sma * 100) if sma > 0 else 0
        
        # Squeeze detection: bandwidth less than 2 standard deviations of historical bandwidth
        # Simplified: if bandwidth is less than 5% of price, consider it a squeeze
        squeeze = "yes" if bandwidth_percent < 5 else "no"
        
        return {
            "upper": round(upper_band, 6),
            "middle": round(sma, 6),
            "lower": round(lower_band, 6),
            "width": round(band_width, 6),
            "bandwidth_percent": round(bandwidth_percent, 2),
            "percentage": round(bb_percentage, 2),
            "squeeze": squeeze,
        }
    
    @staticmethod
    def calculate_atr(candles: List[Dict], period: int = 14) -> Optional[float]:
        """
        Calculate Average True Range (volatility indicator)
        Fixed: Forward iteration for consistency
        """
        if not TechnicalIndicators.validate_candles(candles, period + 1):
            return None
        
        if len(candles) < period + 1:
            return None
        
        true_ranges = []
        
        # Calculate from oldest to newest (forward iteration)
        start_idx = len(candles) - period - 1
        for i in range(start_idx, len(candles) - 1):
            high = candles[i + 1]['high']
            low = candles[i + 1]['low']
            prev_close = candles[i]['close']
            
            tr1 = high - low
            tr2 = abs(high - prev_close)
            tr3 = abs(low - prev_close)
            
            true_range = max(tr1, tr2, tr3)
            true_ranges.append(true_range)
        
        atr = sum(true_ranges) / len(true_ranges)
        return round(atr, 6)
    
    @staticmethod
    def calculate_support_resistance(candles: List[Dict], lookback: int = 50) -> Dict:
        """
        Calculate support and resistance levels using swing highs/lows
        Fixed: Better algorithm using local extrema
        """
        if not TechnicalIndicators.validate_candles(candles, min(lookback, 10)):
            return {"support": None, "resistance": None}
        
        if len(candles) < 10:
            return {"support": None, "resistance": None}
        
        actual_lookback = min(lookback, len(candles))
        recent_candles = candles[-actual_lookback:]
        
        # Find swing highs and lows (local extrema)
        swing_highs = []
        swing_lows = []
        
        for i in range(2, len(recent_candles) - 2):
            # Swing high: higher than 2 candles before and after
            if (recent_candles[i]['high'] > recent_candles[i-1]['high'] and 
                recent_candles[i]['high'] > recent_candles[i-2]['high'] and
                recent_candles[i]['high'] > recent_candles[i+1]['high'] and 
                recent_candles[i]['high'] > recent_candles[i+2]['high']):
                swing_highs.append(recent_candles[i]['high'])
            
            # Swing low: lower than 2 candles before and after
            if (recent_candles[i]['low'] < recent_candles[i-1]['low'] and 
                recent_candles[i]['low'] < recent_candles[i-2]['low'] and
                recent_candles[i]['low'] < recent_candles[i+1]['low'] and 
                recent_candles[i]['low'] < recent_candles[i+2]['low']):
                swing_lows.append(recent_candles[i]['low'])
        
        # If no swing points found, use simple min/max
        if not swing_highs:
            swing_highs = [c['high'] for c in recent_candles]
        if not swing_lows:
            swing_lows = [c['low'] for c in recent_candles]
        
        # Primary levels
        resistance = max(swing_highs) if swing_highs else recent_candles[-1]['high']
        support = min(swing_lows) if swing_lows else recent_candles[-1]['low']
        
        # Secondary levels (median of swing points)
        swing_highs_sorted = sorted(swing_highs)
        swing_lows_sorted = sorted(swing_lows)
        
        secondary_resistance = swing_highs_sorted[len(swing_highs_sorted) * 3 // 4] if len(swing_highs_sorted) > 3 else resistance
        secondary_support = swing_lows_sorted[len(swing_lows_sorted) // 4] if len(swing_lows_sorted) > 3 else support
        
        price_range = resistance - support
        range_percent = (price_range / support * 100) if support > 0 else 0
        
        return {
            "primary_support": round(support, 6),
            "primary_resistance": round(resistance, 6),
            "secondary_support": round(secondary_support, 6),
            "secondary_resistance": round(secondary_resistance, 6),
            "range": round(price_range, 6),
            "range_percent": round(range_percent, 2),
        }
    
    @staticmethod
    def calculate_volume_average(candles: List[Dict], period: int = 20) -> Optional[float]:
        """Calculate average volume"""
        if not TechnicalIndicators.validate_candles(candles, period):
            return None
        
        if len(candles) < period:
            return None
        
        volumes = [c['volume'] for c in candles[-period:]]
        avg_volume = sum(volumes) / period
        return round(avg_volume, 2)
    
    @staticmethod
    def calculate_volume_trend(candles: List[Dict]) -> str:
        """
        Determine volume trend
        Fixed: More reasonable thresholds
        """
        if not TechnicalIndicators.validate_candles(candles, 10):
            return "neutral"
        
        if len(candles) < 10:
            return "neutral"
        
        recent_volumes = [c['volume'] for c in candles[-5:]]
        older_volumes = [c['volume'] for c in candles[-10:-5]]
        
        avg_recent = sum(recent_volumes) / len(recent_volumes)
        avg_older = sum(older_volumes) / len(older_volumes)
        
        if avg_older == 0:
            return "neutral"
        
        # More reasonable thresholds: 20% increase/decrease
        if avg_recent > avg_older * 1.2:
            return "increasing"
        elif avg_recent < avg_older * 0.8:
            return "decreasing"
        else:
            return "stable"
    
    @staticmethod
    def get_trend_signal(candles: List[Dict]) -> str:
        """
        Determine overall trend direction
        NEW METHOD: Added to fix missing method error
        """
        if not TechnicalIndicators.validate_candles(candles, 50):
            return "neutral"
        
        if len(candles) < 50:
            return "neutral"
        
        ema_20 = TechnicalIndicators.calculate_ema(candles, 20)
        ema_50 = TechnicalIndicators.calculate_ema(candles, 50)
        current_price = candles[-1]['close']
        
        if ema_20 is None or ema_50 is None:
            return "neutral"
        
        # Strong trend conditions
        if current_price > ema_20 > ema_50:
            # Check if trend is strong (price significantly above EMAs)
            if current_price > ema_20 * 1.02:  # 2% above EMA20
                return "strong_bullish"
            return "bullish"
        elif current_price < ema_20 < ema_50:
            # Check if trend is strong (price significantly below EMAs)
            if current_price < ema_20 * 0.98:  # 2% below EMA20
                return "strong_bearish"
            return "bearish"
        else:
            return "neutral"
    
    @staticmethod
    def calculate_all_indicators(candles: List[Dict]) -> Dict:
        """Calculate all technical indicators at once"""
        if not TechnicalIndicators.validate_candles(candles):
            logger.warning("Invalid candle data provided to calculate_all_indicators")
            return {}
        
        return {
            # Moving Averages
            "sma_9": TechnicalIndicators.calculate_sma(candles, 9),
            "sma_20": TechnicalIndicators.calculate_sma(candles, 20),
            "sma_50": TechnicalIndicators.calculate_sma(candles, 50),
            "sma_200": TechnicalIndicators.calculate_sma(candles, 200),
            
            "ema_9": TechnicalIndicators.calculate_ema(candles, 9),
            "ema_20": TechnicalIndicators.calculate_ema(candles, 20),
            "ema_50": TechnicalIndicators.calculate_ema(candles, 50),
            "ema_200": TechnicalIndicators.calculate_ema(candles, 200),
            
            # Oscillators
            "rsi_14": TechnicalIndicators.calculate_rsi(candles, 14),
            "macd": TechnicalIndicators.calculate_macd(candles),
            
            # Volatility
            "bollinger_bands": TechnicalIndicators.calculate_bollinger_bands(candles),
            "atr_14": TechnicalIndicators.calculate_atr(candles, 14),
            
            # Support & Resistance
            "support_resistance": TechnicalIndicators.calculate_support_resistance(candles),
            
            # Volume Analysis
            "volume_avg_20": TechnicalIndicators.calculate_volume_average(candles, 20),
            "volume_trend": TechnicalIndicators.calculate_volume_trend(candles),
            
            # Trend
            "trend": TechnicalIndicators.get_trend_signal(candles),
        }
    
    @staticmethod
    def get_trading_signal(candles: List[Dict]) -> Dict:
        """
        Generate comprehensive trading signal
        Fixed: Better confidence calculation and distance formulas
        
        Returns:
            Dict: Signal with confidence and reasons
        """
        if not TechnicalIndicators.validate_candles(candles):
            return {
                "signal": "NEUTRAL",
                "score": 0,
                "confidence": 0,
                "reasons": ["Insufficient or invalid candle data"],
                "indicators_summary": {}
            }
        
        indicators = TechnicalIndicators.calculate_all_indicators(candles)
        
        signal_score = 0
        reasons = []
        current_price = candles[-1]['close'] if candles else 0
        
        # RSI Signal (Weight: 25 points)
        rsi = indicators.get("rsi_14")
        if rsi is not None:
            if rsi < 30:
                signal_score += 25
                reasons.append(f"RSI ({rsi}) indicates oversold condition")
            elif rsi > 70:
                signal_score -= 25
                reasons.append(f"RSI ({rsi}) indicates overbought condition")
            elif rsi < 40:
                signal_score += 10
                reasons.append(f"RSI ({rsi}) slightly oversold")
            elif rsi > 60:
                signal_score -= 10
                reasons.append(f"RSI ({rsi}) slightly overbought")
        
        # MACD Signal (Weight: 20 points)
        macd_data = indicators.get("macd")
        if macd_data:
            histogram = macd_data.get("histogram", 0)
            if macd_data.get("crossover") == "bullish" and histogram > 0:
                signal_score += 20
                reasons.append("MACD bullish crossover")
            elif macd_data.get("crossover") == "bearish" and histogram < 0:
                signal_score -= 20
                reasons.append("MACD bearish crossover")
        
        # EMA Alignment (Weight: 30 points)
        ema_9 = indicators.get("ema_9")
        ema_20 = indicators.get("ema_20")
        ema_50 = indicators.get("ema_50")
        
        if all([ema_9 is not None, ema_20 is not None, ema_50 is not None]):
            if ema_9 > ema_20 > ema_50 and current_price > ema_9:
                signal_score += 30
                reasons.append("Strong bullish EMA alignment (9 > 20 > 50)")
            elif ema_9 < ema_20 < ema_50 and current_price < ema_9:
                signal_score -= 30
                reasons.append("Strong bearish EMA alignment (9 < 20 < 50)")
            elif ema_9 > ema_20 and current_price > ema_9:
                signal_score += 15
                reasons.append("Moderate bullish EMA alignment")
            elif ema_9 < ema_20 and current_price < ema_9:
                signal_score -= 15
                reasons.append("Moderate bearish EMA alignment")
        
        # Bollinger Bands (Weight: 15 points)
        bb = indicators.get("bollinger_bands")
        if bb:
            bb_percent = bb.get("percentage", 50)
            if bb_percent < 20:
                signal_score += 15
                reasons.append(f"Price near lower Bollinger Band ({bb_percent}%)")
            elif bb_percent > 80:
                signal_score -= 15
                reasons.append(f"Price near upper Bollinger Band ({bb_percent}%)")
        
        # Support/Resistance (Weight: 15 points)
        sr = indicators.get("support_resistance", {})
        support = sr.get("primary_support")
        resistance = sr.get("primary_resistance")
        
        if support is not None and current_price and support > 0:
            # Fixed: Calculate distance relative to support level
            distance_to_support = ((current_price - support) / support) * 100
            if 0 < distance_to_support < 2:  # Within 2% above support
                signal_score += 15
                reasons.append(f"Price near support level ({support:.2f})")
        
        if resistance is not None and current_price and resistance > 0:
            # Fixed: Calculate distance relative to resistance level
            distance_to_resistance = ((resistance - current_price) / resistance) * 100
            if 0 < distance_to_resistance < 2:  # Within 2% below resistance
                signal_score -= 15
                reasons.append(f"Price near resistance level ({resistance:.2f})")
        
        # Volume Trend (Weight: 10 points)
        volume_trend = indicators.get("volume_trend")
        if volume_trend == "increasing" and signal_score > 0:
            signal_score += 10
            reasons.append("Increasing volume supports bullish move")
        elif volume_trend == "increasing" and signal_score < 0:
            signal_score -= 10
            reasons.append("Increasing volume supports bearish move")
        
        # Determine final signal
        if signal_score >= 50:
            signal = "STRONG_BUY"
        elif signal_score >= 25:
            signal = "BUY"
        elif signal_score <= -50:
            signal = "STRONG_SELL"
        elif signal_score <= -25:
            signal = "SELL"
        else:
            signal = "NEUTRAL"
        
        # Fixed: Better confidence calculation
        # Map score to confidence: 0-25 = low, 25-50 = medium, 50+ = high
        abs_score = abs(signal_score)
        if abs_score >= 50:
            confidence = min(80 + (abs_score - 50) / 2, 100)  # 80-100%
        elif abs_score >= 25:
            confidence = 50 + (abs_score - 25)  # 50-80%
        else:
            confidence = abs_score * 2  # 0-50%
        
        return {
            "signal": signal,
            "score": signal_score,
            "confidence": round(confidence, 1),
            "reasons": reasons,
            "indicators_summary": {
                "rsi": rsi,
                "ema_alignment": "bullish" if all([ema_9, ema_20, ema_50, ema_9 > ema_20 > ema_50]) else 
                                "bearish" if all([ema_9, ema_20, ema_50, ema_9 < ema_20 < ema_50]) else 
                                "neutral",
                "macd_signal": macd_data.get("crossover") if macd_data else None,
                "trend": indicators.get("trend", "neutral"),
                "volume_trend": volume_trend,
                "current_price": round(current_price, 6) if current_price else None,
                "support": support,
                "resistance": resistance,
            }
        }
