import logging

logger = logging.getLogger("INDICATORS")

class TechnicalIndicators:
    """Technical Indicators Calculator"""
    
    @staticmethod
    def calculate_sma(candles, period=20):
        """
        Calculate Simple Moving Average (SMA/MA)
        
        Args:
            candles (list): List of candle dictionaries
            period (int): MA period (default: 20)
        
        Returns:
            float: Latest SMA value or None if insufficient data
        """
        if len(candles) < period:
            logger.warning(f"Not enough data for SMA({period}). Need {period}, got {len(candles)}")
            return None
        
        # Get last 'period' closing prices
        closes = [candles[i]['close'] for i in range(-period, 0)]
        sma = sum(closes) / period
        
        return round(sma, 2)
    
    @staticmethod
    def calculate_ema(candles, period=20):
        """
        Calculate Exponential Moving Average (EMA)
        
        Args:
            candles (list): List of candle dictionaries
            period (int): EMA period (default: 20)
        
        Returns:
            float: Latest EMA value or None if insufficient data
        """
        if len(candles) < period:
            logger.warning(f"Not enough data for EMA({period}). Need {period}, got {len(candles)}")
            return None
        
        multiplier = 2 / (period + 1)
        
        # First EMA is SMA
        sma = sum([candles[i]['close'] for i in range(period)]) / period
        ema = sma
        
        # Calculate EMA for remaining candles
        for i in range(period, len(candles)):
            close = candles[i]['close']
            ema = (close - ema) * multiplier + ema
        
        return round(ema, 2)
    
    @staticmethod
    def calculate_all_indicators(candles):
        """
        Calculate all indicators at once
        
        Args:
            candles (list): List of candle dictionaries
        
        Returns:
            dict: Dictionary with all indicator values
        """
        return {
            "ema_9": TechnicalIndicators.calculate_ema(candles, 9),
            "ema_20": TechnicalIndicators.calculate_ema(candles, 20),
            "ema_50": TechnicalIndicators.calculate_ema(candles, 50),
            "ema_200": TechnicalIndicators.calculate_ema(candles, 200),
            "sma_20": TechnicalIndicators.calculate_sma(candles, 20),
            "sma_50": TechnicalIndicators.calculate_sma(candles, 50),
            "sma_100": TechnicalIndicators.calculate_sma(candles, 100),
            "sma_200": TechnicalIndicators.calculate_sma(candles, 200),
        }
    
    @staticmethod
    def get_trend_signal(candles):
        """
        Get simple trend signal based on EMAs
        
        Args:
            candles (list): List of candle dictionaries
        
        Returns:
            str: "bullish", "bearish", or "neutral"
        """
        ema_9 = TechnicalIndicators.calculate_ema(candles, 9)
        ema_20 = TechnicalIndicators.calculate_ema(candles, 20)
        ema_50 = TechnicalIndicators.calculate_ema(candles, 50)
        
        if not all([ema_9, ema_20, ema_50]):
            return "neutral"
        
        if ema_9 > ema_20 > ema_50:
            return "bullish"
        elif ema_9 < ema_20 < ema_50:
            return "bearish"
        else:
            return "neutral"
