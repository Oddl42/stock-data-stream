#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 14 22:02:25 2026

@author: twi-dev
"""

#from database import get_db_session, engine
#from massive_client import MassiveClient
from apps.data_ingestion.src.database import get_db_session, engine
from apps.data_ingestion.src.massive_client import MassiveClient
from sqlalchemy import text
from datetime import datetime
import pandas as pd
from typing import List, Dict
from dotenv import load_dotenv
import os

load_dotenv()

class StockDataIngestion:
    """Service zum Laden und Speichern von Stock-Daten über Massive.com API"""
    
    def __init__(self, api_key: str = None):
        self.client = MassiveClient(api_key=api_key)
    
    def _transform_massive_response(self, raw_data: List[Dict], symbol: str) -> pd.DataFrame:
        """
        Transformiert Massive.com API Response in DataFrame
        
        Massive Response Format:
        {
            "c": close,
            "h": high,
            "l": low,
            "o": open,
            "t": timestamp (milliseconds),
            "v": volume,
            "vw": volume weighted average,
            "n": number of trades
        }
        """
        if not raw_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(raw_data)
        
        # Spalten umbenennen
        column_mapping = {
            't': 'time',
            'o': 'open',
            'h': 'high',
            'l': 'low',
            'c': 'close',
            'v': 'volume',
            'vw': 'vwap',  # Volume Weighted Average Price
            'n': 'transactions'
        }
        
        df.rename(columns=column_mapping, inplace=True)
        
        # Timestamp von Millisekunden zu datetime konvertieren
        df['time'] = pd.to_datetime(df['time'], unit='ms')
        
        # Symbol hinzufügen
        df['symbol'] = symbol
        
        # Nur benötigte Spalten behalten
        columns_to_keep = ['time', 'symbol', 'open', 'high', 'low', 'close', 'volume']
        existing_columns = [col for col in columns_to_keep if col in df.columns]
        df = df[existing_columns]
        
        return df
    
    def save_ohlcv_data(self, df: pd.DataFrame, interval: str = '1day'):
        """
        Speichert OHLCV-Daten in der stock_ohlcv Tabelle
        
        Args:
            df: DataFrame mit Spalten [time, symbol, open, high, low, close, volume]
            interval: Zeitintervall (z.B. '1min', '5min', '1day')
        """
        if df.empty:
            print("⚠️  Keine Daten zum Speichern")
            return
        
        # Interval-Spalte hinzufügen
        df['interval'] = interval
        
        # Datenbank-kompatible Spaltennamen sicherstellen
        df = df[['time', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'interval']]
        
        try:
            # Bulk insert mit pandas + ON CONFLICT handling
            with engine.begin() as connection:
                # Temporäre Tabelle erstellen
                df.to_sql(
                    'temp_ohlcv',
                    connection,
                    if_exists='replace',
                    index=False,
                    method='multi'
                )
                
                # UPSERT: INSERT mit ON CONFLICT DO UPDATE
                upsert_query = text("""
                    INSERT INTO stock_ohlcv (time, symbol, open, high, low, close, volume, interval)
                    SELECT time, symbol, open, high, low, close, volume, interval
                    FROM temp_ohlcv
                    ON CONFLICT (symbol, time, interval) 
                    DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume
                """)
                
                connection.execute(upsert_query)
                
                # Temp-Tabelle droppen
                connection.execute(text("DROP TABLE IF EXISTS temp_ohlcv"))
            
            print(f"✅ {len(df)} OHLCV-Datensätze für {df['symbol'].iloc[0]} gespeichert")
            
        except Exception as e:
            print(f"❌ Fehler beim Speichern: {e}")
            raise
    
    def save_quotes_data(self, df: pd.DataFrame):
        """
        Speichert Tick-Daten in der stock_quotes Tabelle
        Hinweis: Für minutengenaue und höhere Auflösungen
        """
        if df.empty:
            print("⚠️  Keine Quote-Daten zum Speichern")
            return
        
        # Für stock_quotes brauchen wir: time, symbol, price, volume, bid, ask
        # Wir nehmen 'close' als 'price'
        df_quotes = df.copy()
        df_quotes['price'] = df_quotes['close']
        
        # Optionale Spalten (falls nicht vorhanden, NULL)
        for col in ['bid', 'ask', 'bid_size', 'ask_size', 'exchange']:
            if col not in df_quotes.columns:
                df_quotes[col] = None
        
        df_quotes = df_quotes[['time', 'symbol', 'exchange', 'price', 'volume', 
                                'bid', 'ask', 'bid_size', 'ask_size']]
        
        try:
            with engine.begin() as connection:
                # Temporäre Tabelle
                df_quotes.to_sql(
                    'temp_quotes',
                    connection,
                    if_exists='replace',
                    index=False,
                    method='multi'
                )
                
                # UPSERT
                upsert_query = text("""
                    INSERT INTO stock_quotes (time, symbol, exchange, price, volume, bid, ask, bid_size, ask_size)
                    SELECT time, symbol, exchange, price, volume, bid, ask, bid_size, ask_size
                    FROM temp_quotes
                    ON CONFLICT (symbol, time) 
                    DO UPDATE SET
                        price = EXCLUDED.price,
                        volume = EXCLUDED.volume
                """)
                
                connection.execute(upsert_query)
                connection.execute(text("DROP TABLE IF EXISTS temp_quotes"))
            
            print(f"✅ {len(df_quotes)} Quote-Datensätze gespeichert")
            
        except Exception as e:
            print(f"❌ Fehler beim Speichern der Quotes: {e}")
            raise
    
    def ingest_symbol(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = '1day',
        save_as_quotes: bool = False
    ):
        """
        Lädt und speichert historische Daten für ein Symbol
        
        Args:
            symbol: Ticker-Symbol (z.B. 'AAPL')
            start_date: Start-Datum
            end_date: End-Datum
            interval: Zeitintervall ('1min', '5min', '1hour', '1day', etc.)
            save_as_quotes: Wenn True, zusätzlich in stock_quotes speichern
        """
        print(f"\n{'='*60}")
        print(f"📥 Lade Daten für {symbol}")
        print(f"   Zeitraum: {start_date.date()} bis {end_date.date()}")
        print(f"   Interval: {interval}")
        print(f"{'='*60}")
        
        # Daten von Massive.com laden
        raw_data = self.client.get_historical_data(
            ticker=symbol,
            start_date=start_date,
            end_date=end_date,
            interval=interval
        )
        
        if not raw_data:
            print(f"⚠️  Keine Daten für {symbol} gefunden")
            return
        
        # In DataFrame transformieren
        df = self._transform_massive_response(raw_data, symbol)
        
        if df.empty:
            print(f"⚠️  Transformation ergab leeren DataFrame")
            return
        
        # Statistiken anzeigen
        print(f"\n📊 Daten-Statistiken:")
        print(f"   Anzahl Datensätze: {len(df)}")
        print(f"   Zeitraum: {df['time'].min()} bis {df['time'].max()}")
        print(f"   Close-Preis: {df['close'].min():.2f} - {df['close'].max():.2f}")
        print(f"   Durchschn. Volumen: {df['volume'].mean():.0f}")
        
        # In Datenbank speichern
        self.save_ohlcv_data(df, interval=interval)
        
        # Optional: Auch als Quotes speichern (für minutengenaue Daten)
        if save_as_quotes and interval in ['1min', '5min', '15min']:
            self.save_quotes_data(df)
    
    def ingest_multiple_symbols(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        interval: str = '1day'
    ):
        """Lädt Daten für mehrere Symbole"""
        print(f"\n🔄 Lade Daten für {len(symbols)} Symbole...")
        
        for i, symbol in enumerate(symbols, 1):
            print(f"\n[{i}/{len(symbols)}]")
            try:
                self.ingest_symbol(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    interval=interval
                )
            except Exception as e:
                print(f"❌ Fehler bei {symbol}: {e}")
                continue
        
        print(f"\n✅ Batch-Ingestion abgeschlossen!")
