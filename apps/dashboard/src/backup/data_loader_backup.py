#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 18 20:35:38 2026

@author: twi-dev
"""

#!/usr/bin/env python3
"""
Automatisches Laden von Ticker-Daten
"""
from datetime import datetime, timedelta
from apps.data_ingestion.src.massive_client import MassiveClient
from apps.data_ingestion.src.ingestion import StockDataIngestion

class DataLoader:
    """Lädt Daten für ausgewählte Ticker"""
    
    def __init__(self):
        self.ingestion = StockDataIngestion()
        self.client = MassiveClient()
    
    def load_ticker_data(self, ticker, days=365, interval='1day'):
        """
        Lädt historische Daten für einen Ticker
        
        Args:
            ticker: Stock Symbol
            days: Anzahl Tage zurück
            interval: '1day', '1hour', '5min'
        
        Returns:
            bool: Erfolg
        """
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            print(f"📥 Lade {ticker}: {start_date.date()} - {end_date.date()}")
            
            # Lade Daten über Ingestion-Service
            self.ingestion.ingest_symbol(
                symbol=ticker,
                start_date=start_date,
                end_date=end_date,
                interval=interval
            )
            
            return True
            
        except Exception as e:
            print(f"❌ Fehler beim Laden von {ticker}: {e}")
            return False
    
    def load_multiple_tickers(self, tickers, days=365, interval='1day', 
                            callback=None):
        """
        Lädt Daten für mehrere Ticker
        
        Args:
            tickers: Liste von Ticker-Symbolen
            days: Anzahl Tage zurück
            interval: Zeitintervall
            callback: Funktion(ticker, success, progress) für Updates
        
        Returns:
            Dict: Statistiken
        """
        total = len(tickers)
        success_count = 0
        failed = []
        
        print(f"\n📊 Lade Daten für {total} Ticker...")
        
        for i, ticker in enumerate(tickers, 1):
            progress = (i / total) * 100
            
            # Callback für UI-Update
            if callback:
                callback(ticker, None, progress)
            
            success = self.load_ticker_data(ticker, days, interval)
            
            if success:
                success_count += 1
                if callback:
                    callback(ticker, True, progress)
            else:
                failed.append(ticker)
                if callback:
                    callback(ticker, False, progress)
        
        stats = {
            'total': total,
            'success': success_count,
            'failed': len(failed),
            'failed_tickers': failed
        }
        
        print(f"\n✅ Abgeschlossen: {success_count}/{total} erfolgreich")
        if failed:
            print(f"❌ Fehlgeschlagen: {', '.join(failed)}")
        
        return stats
    
    def check_data_availability(self, ticker, min_days=30):
        """
        Prüft ob ausreichend Daten für einen Ticker vorhanden sind
        
        Args:
            ticker: Stock Symbol
            min_days: Minimale Anzahl benötigter Tage
        
        Returns:
            Dict: {'has_data': bool, 'days': int, 'last_date': date}
        """
        from apps.data_ingestion.src.database import engine
        from sqlalchemy import text
        
        try:
            with engine.connect() as conn:
                query = text("""
                    SELECT 
                        COUNT(*) as count,
                        MAX(time) as last_date,
                        MIN(time) as first_date
                    FROM stock_ohlcv
                    WHERE symbol = :ticker
                        AND "interval" = '1day'
                """)
                
                result = conn.execute(query, {'ticker': ticker})
                row = result.fetchone()
                
                if row and row[0] > 0:
                    count = row[0]
                    last_date = row[1]
                    first_date = row[2]
                    
                    # Berechne Alter in Tagen
                    if last_date:
                        age_days = (datetime.now() - last_date.replace(tzinfo=None)).days
                    else:
                        age_days = 999
                    
                    return {
                        'has_data': count >= min_days,
                        'count': count,
                        'last_date': last_date,
                        'first_date': first_date,
                        'age_days': age_days,
                        'needs_update': age_days > 1  # Älter als 1 Tag
                    }
                else:
                    return {
                        'has_data': False,
                        'count': 0,
                        'last_date': None,
                        'first_date': None,
                        'age_days': 999,
                        'needs_update': True
                    }
                    
        except Exception as e:
            print(f"❌ Fehler beim Prüfen von {ticker}: {e}")
            return {'has_data': False, 'count': 0, 'needs_update': True}
