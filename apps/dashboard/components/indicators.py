#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 15 18:01:17 2026

@author: twi-dev
"""

#!/usr/bin/env python3
"""
Technische Indikatoren für Stock-Daten
"""
import pandas as pd
import numpy as np

class TechnicalIndicators:
    """Berechnet technische Indikatoren"""
    
    @staticmethod
    def calculate_sma(df, period=20, column='close'):
        """Simple Moving Average"""
        return df[column].rolling(window=period).mean()
    
    @staticmethod
    def calculate_ema(df, period=20, column='close'):
        """Exponential Moving Average"""
        return df[column].ewm(span=period, adjust=False).mean()
    
    @staticmethod
    def calculate_rsi(df, period=14, column='close'):
        """Relative Strength Index"""
        delta = df[column].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    @staticmethod
    def calculate_macd(df, fast=12, slow=26, signal=9, column='close'):
        """MACD (Moving Average Convergence Divergence)"""
        ema_fast = df[column].ewm(span=fast, adjust=False).mean()
        ema_slow = df[column].ewm(span=slow, adjust=False).mean()
        
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        
        return pd.DataFrame({
            'macd': macd_line,
            'signal': signal_line,
            'histogram': histogram
        })
    
    @staticmethod
    def calculate_bollinger_bands(df, period=20, std_dev=2, column='close'):
        """Bollinger Bands"""
        sma = df[column].rolling(window=period).mean()
        std = df[column].rolling(window=period).std()
        
        upper_band = sma + (std * std_dev)
        lower_band = sma - (std * std_dev)
        
        return pd.DataFrame({
            'middle': sma,
            'upper': upper_band,
            'lower': lower_band
        })
    
    @staticmethod
    def calculate_atr(df, period=14):
        """Average True Range (Volatilität)"""
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = ranges.max(axis=1)
        
        atr = true_range.rolling(window=period).mean()
        return atr
    
    @staticmethod
    def add_all_indicators(df, 
                          sma_periods=[20, 50, 200],
                          ema_periods=[12, 26],
                          rsi_period=14,
                          macd_params=(12, 26, 9),
                          bb_params=(20, 2)):
        """Fügt alle Indikatoren zum DataFrame hinzu"""
        
        df = df.copy()
        
        # Moving Averages
        for period in sma_periods:
            df[f'sma_{period}'] = TechnicalIndicators.calculate_sma(df, period)
        
        for period in ema_periods:
            df[f'ema_{period}'] = TechnicalIndicators.calculate_ema(df, period)
        
        # RSI
        df['rsi'] = TechnicalIndicators.calculate_rsi(df, rsi_period)
        
        # MACD
        macd_df = TechnicalIndicators.calculate_macd(df, *macd_params)
        df['macd'] = macd_df['macd']
        df['macd_signal'] = macd_df['signal']
        df['macd_histogram'] = macd_df['histogram']
        
        # Bollinger Bands
        bb_df = TechnicalIndicators.calculate_bollinger_bands(df, *bb_params)
        df['bb_middle'] = bb_df['middle']
        df['bb_upper'] = bb_df['upper']
        df['bb_lower'] = bb_df['lower']
        
        # ATR
        df['atr'] = TechnicalIndicators.calculate_atr(df)
        
        return df
