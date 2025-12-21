import logging
import math
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger("TRADING-INDICATORS")

class TechnicalIndicators:
    """Technical Indicators Calculator for Trading"""
    
    @staticmethod
    def calculate_sma(candles: List[Dict], period: int = 20) -> Optional[float]:
        """Calculate Simple Moving Average"""
        if len(candles) < period:
            return None
        
        closes = [candles[i]['close'] for i in range(-period, 0)]
        sma = sum(closes) / period
        return round(sma, 2)
    
    @staticmethod
    def calculate_ema(candles: List[Dict], period: int = 20) -> Optional[float]:
        """Calculate Exponential Moving Average"""
        if len(candles) < period:
            return None
        
        multiplier = 2 / (period + 1)
        sma = sum([candles[i]['close'] for i in range(period)]) / period
        ema = sma
        
        for i in range(period, len(candles)):
            close = candles[i]['close']
            ema = (close - ema) * multiplier + ema
        
        return round(ema, 2)
    
    @staticmethod
    def calculate_rsi(candles: List[Dict], period: int = 14) -> Optional[float]:
        """Calculate Relative Strength Index"""
        if len(candles) < period + 1:
            return None
        
        gains = []
        losses = []
        
        for i in range(1, period + 1):
            change = candles[-i]['close'] - candles[-i-1]['close']
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
        """Calculate MACD (Moving Average Convergence Divergence)"""
        if len(candles) < slow:
            return None
        
        # Calculate EMAs
        ema_fast = TechnicalIndicators.calculate_ema(candles, fast)
        ema_slow = TechnicalIndicators.calculate_ema(candles, slow)
        
        if not ema_fast or not ema_slow:
            return None
        
        macd_line = ema_fast - ema_slow
        
        # For signal line, we need MACD history
        # Simplified version using last few values
        macd_history = []
        for i in range(signal):
            if len(candles) >= slow + i:
                fast_ema = TechnicalIndicators.calculate_ema(candles[:-(i+1)], fast)
                slow_ema = TechnicalIndicators.calculate_ema(candles[:-(i+1)], slow)
                if fast_ema and slow_ema:
                    macd_history.append(fast_ema - slow_ema)
        
        if len(macd_history) < signal:
            signal_line = macd_line
        else:
            signal_line = sum(macd_history) / len(macd_history)
        
        histogram = macd_line - signal_line
        
        return {
            "macd": round(macd_line, 4),
            "signal": round(signal_line, 4),
            "histogram": round(histogram, 4),
            "crossover": "bullish" if histogram > 0 else "bearish",
        }
    
    @staticmethod
    def calculate_bollinger_bands(candles: List[Dict], period: int = 20, std_dev: float = 2.0) -> Optional[Dict]:
        """Calculate Bollinger Bands"""
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
        bb_percentage = (current_price - lower_band) / (upper_band - lower_band) * 100 if (upper_band - lower_band) > 0 else 50
        
        return {
            "upper": round(upper_band, 2),
            "middle": round(sma, 2),
            "lower": round(lower_band, 2),
            "width": round(upper_band - lower_band, 2),
            "percentage": round(bb_percentage, 2),
            "squeeze": "yes" if (upper_band - lower_band) < sma * 0.1 else "no",  # 10% of price
        }
    
    @staticmethod
    def calculate_atr(candles: List[Dict], period: int = 14) -> Optional[float]:
        """Calculate Average True Range (volatility indicator)"""
        if len(candles) < period + 1:
            return None
        
        true_ranges = []
        
        for i in range(1, period + 1):
            high = candles[-i]['high']
            low = candles[-i]['low']
            prev_close = candles[-i-1]['close']
            
            tr1 = high - low
            tr2 = abs(high - prev_close)
            tr3 = abs(low - prev_close)
            
            true_range = max(tr1, tr2, tr3)
            true_ranges.append(true_range)
        
        atr = sum(true_ranges) / period
        return round(atr, 4)
    
    @staticmethod
    def calculate_support_resistance(candles: List[Dict], lookback: int = 50) -> Dict:
        """Calculate support and resistance levels"""
        if len(candles) < lookback:
            return {"support": None, "resistance": None}
        
        prices = [c['close'] for c in candles[-lookback:]]
        highs = [c['high'] for c in candles[-lookback:]]
        lows = [c['low'] for c in candles[-lookback:]]
        
        resistance = max(highs)
        support = min(lows)
        
        # Find secondary levels
        prices_sorted = sorted(prices)
        q1 = prices_sorted[len(prices_sorted)//4]  # 25th percentile
        q3 = prices_sorted[3*len(prices_sorted)//4]  # 75th percentile
        
        return {
            "primary_support": round(support, 2),
            "primary_resistance": round(resistance, 2),
            "secondary_support": round(q1, 2),
            "secondary_resistance": round(q3, 2),
            "range": round(resistance - support, 2),
            "range_percent": round(((resistance - support) / support) * 100, 2),
        }
    
    @staticmethod
    def calculate_all_indicators(candles: List[Dict]) -> Dict:
        """Calculate all technical indicators at once"""
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
        }
    
    @staticmethod
    def calculate_volume_average(candles: List[Dict], period: int = 20) -> Optional[float]:
        """Calculate average volume"""
        if len(candles) < period:
            return None
        
        volumes = [c['volume'] for c in candles[-period:]]
        avg_volume = sum(volumes) / period
        return round(avg_volume, 2)
    
    @staticmethod
    def calculate_volume_trend(candles: List[Dict]) -> str:
        """Determine volume trend"""
        if len(candles) < 10:
            return "neutral"
        
        recent_volumes = [c['volume'] for c in candles[-5:]]
        older_volumes = [c['volume'] for c in candles[-10:-5]]
        
        avg_recent = sum(recent_volumes) / len(recent_volumes)
        avg_older = sum(older_volumes) / len(older_volumes)
        
        if avg_recent > avg_older * 1.5:
            return "increasing"
        elif avg_recent < avg_older * 0.5:
            return "decreasing"
        else:
            return "stable"
    
    @staticmethod
    def get_trading_signal(candles: List[Dict]) -> Dict:
        """
        Generate comprehensive trading signal
        
        Returns:
            Dict: Signal with confidence and reasons
        """
        indicators = TechnicalIndicators.calculate_all_indicators(candles)
        
        signal_score = 0
        reasons = []
        current_price = candles[-1]['close'] if candles else 0
        
        # RSI Signal
        rsi = indicators.get("rsi_14")
        if rsi:
            if rsi < 30:
                signal_score += 25
                reasons.append(f"RSI ({rsi}) indicates oversold")
            elif rsi > 70:
                signal_score -= 25
                reasons.append(f"RSI ({rsi}) indicates overbought")
        
        # MACD Signal
        macd_data = indicators.get("macd")
        if macd_data and macd_data.get("crossover") == "bullish":
            signal_score += 20
            reasons.append("MACD bullish crossover")
        elif macd_data and macd_data.get("crossover") == "bearish":
            signal_score -= 20
            reasons.append("MACD bearish crossover")
        
        # EMA Alignment
        ema_9 = indicators.get("ema_9")
        ema_20 = indicators.get("ema_20")
        ema_50 = indicators.get("ema_50")
        
        if all([ema_9, ema_20, ema_50]):
            if ema_9 > ema_20 > ema_50 and current_price > ema_9:
                signal_score += 30
                reasons.append("EMA alignment bullish (9 > 20 > 50)")
            elif ema_9 < ema_20 < ema_50 and current_price < ema_9:
                signal_score -= 30
                reasons.append("EMA alignment bearish (9 < 20 < 50)")
        
        # Support/Resistance
        sr = indicators.get("support_resistance", {})
        support = sr.get("primary_support")
        resistance = sr.get("primary_resistance")
        
        if support and current_price:
            distance_to_support = ((current_price - support) / current_price) * 100
            if distance_to_support < 2:  # Within 2% of support
                signal_score += 15
                reasons.append(f"Near support level (${support})")
        
        if resistance and current_price:
            distance_to_resistance = ((resistance - current_price) / current_price) * 100
            if distance_to_resistance < 2:  # Within 2% of resistance
                signal_score -= 15
                reasons.append(f"Near resistance level (${resistance})")
        
        # Determine final signal
        if signal_score >= 40:
            signal = "STRONG_BUY"
        elif signal_score >= 20:
            signal = "BUY"
        elif signal_score <= -40:
            signal = "STRONG_SELL"
        elif signal_score <= -20:
            signal = "SELL"
        else:
            signal = "NEUTRAL"
        
        return {
            "signal": signal,
            "score": signal_score,
            "confidence": min(abs(signal_score), 100),
            "reasons": reasons,
            "indicators_summary": {
                "rsi": rsi,
                "ema_alignment": "bullish" if all([ema_9, ema_20, ema_50, ema_9 > ema_20 > ema_50]) else 
                                "bearish" if all([ema_9, ema_20, ema_50, ema_9 < ema_20 < ema_50]) else 
                                "neutral",
                "macd_signal": macd_data.get("crossover") if macd_data else None,
                "current_price": current_price,
            }
        }
