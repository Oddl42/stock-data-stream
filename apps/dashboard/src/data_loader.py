#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
data_loader.py - Optimierter Data Loader mit Bulk-Inserts
"""

import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
import logging
from typing import List, Dict, Optional, Callable

from apps.data_ingestion.src.database import engine
from apps.data_ingestion.src.massive_client import MassiveClient
from config import settings

logging.basicConfig(
    level=settings.LOG_LEVEL,
    format=settings.LOG_FORMAT,
    handlers=[
        logging.FileHandler(settings.LOG_FILE) if settings.LOG_FILE else logging.NullHandler(),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataLoader:
    """
    Optimierter Data Loader mit Bulk-Inserts und besserem Error Handling
    """

    def __init__(self):
        """Initialisiert DataLoader mit MassiveClient."""
        self.client = MassiveClient()
        logger.info("✅ DataLoader initialisiert")

    def load_ticker_data(
        self, 
        ticker: str, 
        days: int = settings.DEFAULT_DAYS, 
        interval: str = settings.DEFAULT_INTERVAL
    ) -> bool:
        """
        Lädt Daten für einen Ticker von der API und speichert in DB.
        
        Args:
            ticker: Ticker-Symbol (z.B. 'AAPL')
            days: Anzahl Tage zurück
            interval: Intervall (z.B. '1day', '1hour', '1min')
            
        Returns:
            bool: True bei Erfolg, False bei Fehler
        """
        try:
            logger.info(f"🔄 Lade {ticker} - {days} Tage - Intervall: {interval}")
            
            # Zeitraum berechnen
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Von API laden
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
            df['symbol'] = ticker
            df['interval'] = interval
            
            # Zeit-Spalte konvertieren
            if df['time'].dtype == 'int64':
                df['time'] = pd.to_datetime(df['time'], unit='ms')
            else:
                df['time'] = pd.to_datetime(df['time'])
            
            # ✅ BULK-INSERT (100x schneller)
            self._bulk_save_to_db(df)
            
            logger.info(f"✅ {ticker}: {len(df)} Datenpunkte gespeichert")
            return True
        
        except Exception as e:
            logger.error(f"❌ {ticker}: Fehler beim Laden - {e}", exc_info=True)
            return False

    def _bulk_save_to_db(self, df: pd.DataFrame) -> None:
        """
        ✅ OPTIMIERT: Bulk-Insert mit pandas + ON CONFLICT
        
        Bis zu 100x schneller als row-by-row Inserts!
        
        Args:
            df: DataFrame mit OHLCV-Daten
        """
        try:
            columns_to_save = ['time', 'symbol', 'interval', 'open', 'high', 'low', 'close', 'volume']
            df_to_save = df[columns_to_save].copy()
            
            # Duplikate entfernen
            df_to_save = df_to_save.drop_duplicates(
                subset=['time', 'symbol', 'interval'], 
                keep='last'
            )
            
            # Datentypen sicherstellen
            df_to_save['open'] = df_to_save['open'].astype(float)
            df_to_save['high'] = df_to_save['high'].astype(float)
            df_to_save['low'] = df_to_save['low'].astype(float)
            df_to_save['close'] = df_to_save['close'].astype(float)
            df_to_save['volume'] = df_to_save['volume'].astype(int)
            
            with engine.begin() as conn:
                # ✅ Temporäre Tabelle für Bulk-Insert
                df_to_save.to_sql(
                    'temp_stock_ohlcv',
                    conn,
                    if_exists='replace',
                    index=False,
                    method='multi',
                    chunksize=settings.BULK_INSERT_CHUNK_SIZE
                )
                
                # ✅ UPSERT mit ON CONFLICT
                conn.execute(text("""
                    INSERT INTO stock_ohlcv (time, symbol, interval, open, high, low, close, volume)
                    SELECT time, symbol, interval, open, high, low, close, volume
                    FROM temp_stock_ohlcv
                    ON CONFLICT (time, symbol, interval) 
                    DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume
                """))
                
                # Temp-Tabelle löschen
                conn.execute(text("DROP TABLE IF EXISTS temp_stock_ohlcv"))
            
            logger.info(f"💾 {len(df_to_save)} Zeilen per Bulk-Insert gespeichert")
        
        except Exception as e:
            logger.error(f"❌ Fehler beim Bulk-Insert: {e}", exc_info=True)
            raise

    def update_ticker_data(
        self, 
        ticker: str, 
        interval: str = settings.DEFAULT_INTERVAL
    ) -> bool:
        """
        Aktualisiert Daten für einen Ticker (lädt nur fehlende Tage).
        
        Args:
            ticker: Ticker-Symbol
            interval: Intervall
            
        Returns:
            bool: True bei Erfolg
        """
        try:
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT MAX(time) as last_date
                    FROM stock_ohlcv
                    WHERE symbol = :ticker AND interval = :interval
                """), {"ticker": ticker, "interval": interval})
                
                row = result.fetchone()
                last_date = row[0] if row and row[0] else None
            
            if last_date:
                days = (datetime.now() - last_date).days + 1
                logger.info(f"🔄 {ticker}: Update der letzten {days} Tage")
            else:
                days = settings.DEFAULT_DAYS
                logger.info(f"📥 {ticker}: Erstmaliger Download ({days} Tage)")
            
            return self.load_ticker_data(ticker, days=days, interval=interval)
        
        except Exception as e:
            logger.error(f"❌ {ticker}: Fehler beim Update - {e}", exc_info=True)
            return False

    def load_multiple_tickers(
        self, 
        tickers: List[str], 
        days: int = settings.DEFAULT_DAYS,
        interval: str = settings.DEFAULT_INTERVAL,
        callback: Optional[Callable[[str, bool, float], None]] = None
    ) -> Dict[str, any]:
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
        
        logger.info(f"🚀 Starte Bulk-Download für {total} Ticker...")
        
        for idx, ticker in enumerate(tickers):
            try:
                result = self.load_ticker_data(ticker, days, interval)
                
                if result:
                    success += 1
                else:
                    failed += 1
                    failed_tickers.append(ticker)
                
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

    def check_data_availability(
        self, 
        ticker: str, 
        interval: str = settings.DEFAULT_INTERVAL
    ) -> Dict[str, any]:
        """
        Prüft, ob Daten für einen Ticker vorhanden sind.
        
        Args:
            ticker: Ticker-Symbol
            interval: Intervall
            
        Returns:
            dict: Info über Datenverfügbarkeit
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
