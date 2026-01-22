#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 22 21:24:34 2026

@author: twi-dev
"""

"""
data_loader.py – Lädt Daten von Massive API und speichert in TimescaleDB
"""

import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
import logging

from apps.data_ingestion.src.database import engine
from apps.data_ingestion.src.massive_client import MassiveClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DataLoader")


class DataLoader:
    """
    Lädt Stock-Daten von der Massive API und speichert sie in TimescaleDB.
    """

    def __init__(self):
        """Initialisiert DataLoader mit MassiveClient."""
        self.client = MassiveClient()
        logger.info("✅ DataLoader initialisiert")

    def load_ticker_data(self, ticker: str, days: int = 90, interval: str = "1day"):
        """
        Lädt Daten für einen Ticker von der API und speichert in DB.
        
        Args:
            ticker: Ticker-Symbol (z.B. 'AAPL')
            days: Anzahl Tage zurück (Standard: 90)
            interval: Intervall (z.B. '1day', '1hour', '1min')
            
        Returns:
            bool: True bei Erfolg, False bei Fehler
        """
        try:
            logger.info(f"📥 Lade {ticker} - {days} Tage - Intervall: {interval}")
            
            # Zeitraum berechnen
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Von API laden (verwendet get_ohlcv)
            data = self.client.get_ohlcv(
                symbol=ticker,
                interval=interval,
                start=start_date,
                end=end_date
            )
            
            if not data:
                logger.warning(f"⚠️ {ticker}: Keine Daten von API erhalten")
                return False
            
            # DataFrame erstellen
            df = pd.DataFrame(data)
            
            # Spalten sind bereits korrekt: time, open, high, low, close, volume
            
            # Symbol und Intervall hinzufügen
            df['symbol'] = ticker
            df['interval'] = interval
            
            # Zeit-Spalte konvertieren (Unix-Timestamp in Millisekunden)
            if df['time'].dtype == 'int64':
                df['time'] = pd.to_datetime(df['time'], unit='ms')
            else:
                df['time'] = pd.to_datetime(df['time'])
            
            # In DB speichern
            self._save_to_db(df)
            
            logger.info(f"✅ {ticker}: {len(df)} Datenpunkte gespeichert")
            return True
        
        except Exception as e:
            logger.error(f"❌ {ticker}: Fehler beim Laden - {e}")
            import traceback
            traceback.print_exc()
            return False

    def _save_to_db(self, df: pd.DataFrame):
        """
        Speichert DataFrame in TimescaleDB (mit Duplikat-Handling).
        
        Args:
            df: DataFrame mit OHLCV-Daten
        """
        try:
            # Nur relevante Spalten
            columns_to_save = ['time', 'symbol', 'interval', 'open', 'high', 'low', 'close', 'volume']
            df_to_save = df[columns_to_save].copy()
            
            # Duplikate entfernen
            df_to_save = df_to_save.drop_duplicates(subset=['time', 'symbol', 'interval'], keep='last')
            
            # Zeile-für-Zeile-Insert mit ON CONFLICT (sicher gegen Duplikate)
            with engine.connect() as conn:
                for _, row in df_to_save.iterrows():
                    try:
                        conn.execute(text("""
                            INSERT INTO stock_ohlcv (time, symbol, interval, open, high, low, close, volume)
                            VALUES (:time, :symbol, :interval, :open, :high, :low, :close, :volume)
                            ON CONFLICT (time, symbol, interval) DO UPDATE SET
                                open = EXCLUDED.open,
                                high = EXCLUDED.high,
                                low = EXCLUDED.low,
                                close = EXCLUDED.close,
                                volume = EXCLUDED.volume
                        """), {
                            'time': row['time'],
                            'symbol': row['symbol'],
                            'interval': row['interval'],
                            'open': float(row['open']),
                            'high': float(row['high']),
                            'low': float(row['low']),
                            'close': float(row['close']),
                            'volume': int(row['volume'])
                        })
                    except Exception as insert_error:
                        logger.error(f"❌ Fehler beim Insert einer Zeile: {insert_error}")
                        continue
                
                conn.commit()
                logger.info(f"💾 {len(df_to_save)} Zeilen in DB gespeichert")
        
        except Exception as e:
            logger.error(f"❌ Fehler beim Speichern in DB: {e}")
            import traceback
            traceback.print_exc()

    def update_ticker_data(self, ticker: str, interval: str = "1day"):
        """
        Aktualisiert Daten für einen Ticker (lädt nur fehlende Tage).
        
        Args:
            ticker: Ticker-Symbol
            interval: Intervall
            
        Returns:
            bool: True bei Erfolg, False bei Fehler
        """
        try:
            # Prüfe letzten Datenpunkt in DB
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT MAX(time) as last_date
                    FROM stock_ohlcv
                    WHERE symbol = :ticker AND interval = :interval
                """), {"ticker": ticker, "interval": interval})
                
                row = result.fetchone()
                last_date = row[0] if row and row[0] else None
            
            if last_date:
                # Nur Daten seit letztem Datum laden
                days = (datetime.now() - last_date).days + 1
                logger.info(f"🔄 {ticker}: Update der letzten {days} Tage")
            else:
                # Kompletter Download
                days = 90
                logger.info(f"📥 {ticker}: Erstmaliger Download (90 Tage)")
            
            return self.load_ticker_data(ticker, days=days, interval=interval)
        
        except Exception as e:
            logger.error(f"❌ {ticker}: Fehler beim Update - {e}")
            return False

    def load_multiple_tickers(self, tickers: list, days: int = 90, 
                               interval: str = "1day", callback=None):
        """
        Lädt Daten für mehrere Ticker mit Progress-Callback.
        
        Args:
            tickers: Liste von Ticker-Symbolen
            days: Anzahl Tage
            interval: Intervall
            callback: Callback-Funktion(ticker, success, progress)
            
        Returns:
            dict: Statistik (total, success, failed, failed_tickers)
        """
        total = len(tickers)
        success = 0
        failed = 0
        failed_tickers = []
        
        logger.info(f"📦 Starte Bulk-Download für {total} Ticker...")
        
        for idx, ticker in enumerate(tickers):
            try:
                result = self.load_ticker_data(ticker, days, interval)
                
                if result:
                    success += 1
                else:
                    failed += 1
                    failed_tickers.append(ticker)
                
                # Progress-Callback
                if callback:
                    progress = (idx + 1) / total
                    callback(ticker, result, progress)
            
            except Exception as e:
                logger.error(f"❌ {ticker}: {e}")
                failed += 1
                failed_tickers.append(ticker)
                
                if callback:
                    progress = (idx + 1) / total
                    callback(ticker, False, progress)
        
        logger.info(f"✅ Bulk-Download abgeschlossen: {success}/{total} erfolgreich")
        
        return {
            'total': total,
            'success': success,
            'failed': failed,
            'failed_tickers': failed_tickers
        }

    def check_data_availability(self, ticker: str, interval: str = "1day"):
        """
        Prüft, ob Daten für einen Ticker vorhanden sind.
        
        Args:
            ticker: Ticker-Symbol
            interval: Intervall
            
        Returns:
            dict: Info (has_data, count, first_date, last_date, days_old, needs_update)
        """
        try:
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        COUNT(*) as count,
                        MIN(time) as first_date,
                        MAX(time) as last_date
                    FROM stock_ohlcv
                    WHERE symbol = :ticker AND interval = :interval
                """), {"ticker": ticker, "interval": interval})
                
                row = result.fetchone()
                
                if row and row[0] > 0:
                    days_old = (datetime.now() - row[2]).days if row[2] else 999
                    
                    return {
                        'has_data': True,
                        'count': row[0],
                        'first_date': row[1],
                        'last_date': row[2],
                        'days_old': days_old,
                        'needs_update': days_old > 1
                    }
                else:
                    return {
                        'has_data': False,
                        'needs_update': True
                    }
        
        except Exception as e:
            logger.error(f"❌ Fehler bei Verfügbarkeits-Check: {e}")
            return {'has_data': False, 'needs_update': True}
