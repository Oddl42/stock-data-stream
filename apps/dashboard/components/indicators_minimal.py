#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 15 18:17:46 2026

@author: twi-dev
"""

#!/usr/bin/env python3
"""
Technische Indikatoren für Stock-Daten
VEREINFACHTE VERSION - kompatibel mit NumPy 2.0
"""
import pandas as pd
import numpy as np

class TechnicalIndicators:
    """Berechnet technische Indikatoren"""
    
    @staticmethod
    def calculate_sma(df, period=20, column='close'):
        """Simple Moving Average - NumPy 2.0 kompatibel"""
        try:
            values = df[column].values
            result = np.full(len(values), np.nan)
            
            for i in range(period - 1, len(values)):
                result[i] = np.mean(values[i - period + 1:i + 1])
            
            return pd.Series(result, index=df.index)
        except Exception as e:
            print(f"⚠️  SMA Fehler: {e}")
            return pd.Series([np.nan] * len(df), index=df.index)
    
    @staticmethod
    def calculate_ema(df, period=20, column='close'):
        """Exponential Moving Average"""
        try:
            return df[column].ewm(span=period, adjust=False, ignore_na=True).mean()
        except Exception as e:
            print(f"⚠️  EMA Fehler: {e}")
            return pd.Series([np.nan] * len(df), index=df.index)
    
    @staticmethod
    def calculate_rsi(df, period=14, column='close'):
        """Relative Strength Index"""
        try:
            delta = df[column].diff()
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)
            
            # Manuelle Rolling-Berechnung
            avg_gain = pd.Series([np.nan] * len(df), index=df.index)
            avg_loss = pd.Series([np.nan] * len(df), index=df.index)
            
            for i in range(period, len(df)):
                avg_gain.iloc[i] = gain.iloc[i - period + 1:i + 1].mean()
                avg_loss.iloc[i] = loss.iloc[i - period + 1:i + 1].mean()
            
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            return rsi
        except Exception as e:
            print(f"⚠️  RSI Fehler: {e}")
            return pd.Series([np.nan] * len(df), index=df.index)
    
    @staticmethod
    def calculate_macd(df, fast=12, slow=26, signal=9, column='close'):
        """MACD"""
        try:
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
        except Exception as e:
            print(f"⚠️  MACD Fehler: {e}")
            return pd.DataFrame({
                'macd': [np.nan] * len(df),
                'signal': [np.nan] * len(df),
                'histogram': [np.nan] * len(df)
            })
    
    @staticmethod
    def calculate_bollinger_bands(df, period=20, std_dev=2, column='close'):
        """Bollinger Bands - Manuelle Berechnung"""
        try:
            values = df[column].values
            sma = np.full(len(values), np.nan)
            upper = np.full(len(values), np.nan)
            lower = np.full(len(values), np.nan)
            
            for i in range(period - 1, len(values)):
                window = values[i - period + 1:i + 1]
                mean = np.mean(window)
                std = np.std(window)
                
                sma[i] = mean
                upper[i] = mean + (std * std_dev)
                lower[i] = mean - (std * std_dev)
            
            return pd.DataFrame({
                'middle': pd.Series(sma, index=df.index),
                'upper': pd.Series(upper, index=df.index),
                'lower': pd.Series(lower, index=df.index)
            })
        except Exception as e:
            print(f"⚠️  Bollinger Bands Fehler: {e}")
            return pd.DataFrame({
                'middle': [np.nan] * len(df),
                'upper': [np.nan] * len(df),
                'lower': [np.nan] * len(df)
            })
    
    @staticmethod
    def calculate_atr(df, period=14):
        """Average True Range"""
        try:
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            
            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            
            # Manuelle ATR-Berechnung
            atr = pd.Series([np.nan] * len(df), index=df.index)
            for i in range(period, len(df)):
                atr.iloc[i] = true_range.iloc[i - period + 1:i + 1].mean()
            
            return atr
        except Exception as e:
            print(f"⚠️  ATR Fehler: {e}")
            return pd.Series([np.nan] * len(df), index=df.index)
    
    @staticmethod
    def add_all_indicators(df, 
                          sma_periods=[20, 50, 200],
                          ema_periods=[12, 26],
                          rsi_period=14,
                          macd_params=(12, 26, 9),
                          bb_params=(20, 2)):
        """Fügt alle Indikatoren zum DataFrame hinzu"""
        
        if df.empty or len(df) < 20:
            print("⚠️  Zu wenig Daten für Indikatoren")
            return df
        
        df = df.copy()
        
        try:
            # Moving Averages
            for period in sma_periods:
                if len(df) >= period:
                    df[f'sma_{period}'] = TechnicalIndicators.calculate_sma(df, period)
            
            for period in ema_periods:
                if len(df) >= period:
                    df[f'ema_{period}'] = TechnicalIndicators.calculate_ema(df, period)
            
            # RSI
            if len(df) >= rsi_period + 1:
                df['rsi'] = TechnicalIndicators.calculate_rsi(df, rsi_period)
            
            # MACD
            if len(df) >= macd_params[1] + macd_params[2]:
                macd_df = TechnicalIndicators.calculate_macd(df, *macd_params)
                df['macd'] = macd_df['macd']
                df['macd_signal'] = macd_df['signal']
                df['macd_histogram'] = macd_df['histogram']
            
            # Bollinger Bands
            if len(df) >= bb_params[0]:
                bb_df = TechnicalIndicators.calculate_bollinger_bands(df, *bb_params)
                df['bb_middle'] = bb_df['middle']
                df['bb_upper'] = bb_df['upper']
                df['bb_lower'] = bb_df['lower']
            
            # ATR
            if len(df) >= 14:
                df['atr'] = TechnicalIndicators.calculate_atr(df)
            
            print(f"✅ Indikatoren erfolgreich berechnet")
            
        except Exception as e:
            print(f"⚠️  Fehler bei Indikator-Berechnung: {e}")
            import traceback
            traceback.print_exc()
        
        return df
