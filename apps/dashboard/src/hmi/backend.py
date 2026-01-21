#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 20 15:07:14 2026

@author: twi-dev
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backend.py – Backend-Logik für das Stock Data Dashboard
"""

import pandas as pd
from datetime import datetime, timedelta
import logging

from sqlalchemy import text
from apps.data_ingestion.src.database import engine
from apps.data_ingestion.src.massive_client import MassiveClient
from apps.dashboard.components.indicators import TechnicalIndicators
from apps.dashboard.src.ticker_db import TickerDatabase
from apps.dashboard.src.data_loader import DataLoader

# Logging-Konfiguration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("StockDashboardBackend")


class StockBackend:
    """
    Kapselt alle Backend-Funktionen: Datenbankzugriffe, Ticker-Management,
    Datenverarbeitung und Indikator-Berechnung.
    """

    def __init__(self):
        self.indicators = TechnicalIndicators()
        self.massive_client = MassiveClient()
        self.ticker_db = TickerDatabase()
        self.data_loader = DataLoader()
        self.available_symbols = []
        self.setup_data()

    # === Datenbankzugriffe & Symbolverwaltung ===

    def setup_data(self):
        """Lädt verfügbare Symbole (aus DB + ausgewählte Ticker)."""
        try:
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT DISTINCT symbol FROM stock_ohlcv ORDER BY symbol
                """))
                db_symbols = [row[0] for row in result]
            selected = self.ticker_db.get_selected_tickers()
            selected_symbols = [t['ticker'] for t in selected]
            all_symbols = sorted(list(set(db_symbols + selected_symbols)))
            self.available_symbols = all_symbols if all_symbols else ['Keine Daten verfügbar']
            logger.info(f"Verfügbare Symbole geladen: {self.available_symbols}")
        except Exception as e:
            logger.error(f"Fehler beim Laden der Symbole: {e}")
            self.available_symbols = ['Keine Daten verfügbar']

    def get_available_symbols(self):
        """Gibt die aktuell verfügbaren Symbole zurück."""
        return self.available_symbols

    # === Ticker-Management ===

    def load_all_tickers(self, asset_class="stocks"):
        """
        Lädt alle Ticker von der Massive API für die angegebene Asset-Klasse.
        Gibt ein DataFrame mit Ticker-Infos zurück.
        """
        try:
            tickers = self.massive_client.get_all_tickers(asset_class=asset_class)
            if tickers:
                df = pd.DataFrame(tickers)
                columns_to_show = ['ticker', 'name', 'primary_exchange', 'market']
                available_columns = [col for col in columns_to_show if col in df.columns]
                df = df[available_columns]
                df['selected'] = df['ticker'].apply(lambda x: '✓' if self.ticker_db.is_selected(x) else '')
                logger.info(f"{len(df)} Ticker für {asset_class} geladen.")
                return df
            else:
                logger.warning("Keine Ticker gefunden.")
                return pd.DataFrame(columns=['ticker', 'name', 'primary_exchange', 'market', 'selected'])
        except Exception as e:
            logger.error(f"Fehler beim Laden der Ticker: {e}")
            return pd.DataFrame(columns=['ticker', 'name', 'primary_exchange', 'market', 'selected'])

    def add_selected_ticker(self, ticker):
        """Fügt einen Ticker zur Auswahl hinzu."""
        try:
            self.ticker_db.add_selected_ticker(ticker)
            logger.info(f"Ticker {ticker} hinzugefügt.")
            self.setup_data()
            return True
        except Exception as e:
            logger.error(f"Fehler beim Hinzufügen von {ticker}: {e}")
            return False

    def remove_selected_ticker(self, ticker):
        """Entfernt einen Ticker aus der Auswahl."""
        try:
            self.ticker_db.remove_selected_ticker(ticker)
            logger.info(f"Ticker {ticker} entfernt.")
            self.setup_data()
            return True
        except Exception as e:
            logger.error(f"Fehler beim Entfernen von {ticker}: {e}")
            return False

    def clear_all_tickers(self):
        """Löscht alle ausgewählten Ticker."""
        try:
            self.ticker_db.clear_selected_tickers()
            logger.info("Alle Ticker entfernt.")
            self.setup_data()
            return True
        except Exception as e:
            logger.error(f"Fehler beim Löschen aller Ticker: {e}")
            return False

    def get_selected_tickers(self):
        """Gibt die aktuell ausgewählten Ticker als Liste zurück."""
        try:
            return self.ticker_db.get_selected_tickers()
        except Exception as e:
            logger.error(f"Fehler beim Abrufen der ausgewählten Ticker: {e}")
            return []

    # === Daten-Laden & Verarbeitung ===

    def load_data(self, symbol, interval, start_date, end_date):
        """
        Lädt Kursdaten für ein Symbol aus der Datenbank.
        Gibt ein DataFrame mit OHLCV-Daten zurück.
        """
        try:
            if not start_date:
                start_date = (datetime.now() - timedelta(days=90)).date()
            if not end_date:
                end_date = datetime.now().date()
            if start_date > end_date:
                start_date, end_date = end_date, start_date

            start_datetime = datetime.combine(start_date, datetime.min.time())
            end_datetime = datetime.combine(end_date, datetime.max.time())

            with engine.connect() as conn:
                query = text("""
                    SELECT 
                        time, symbol,
                        CAST("open" AS DOUBLE PRECISION) as open,
                        CAST(high AS DOUBLE PRECISION) as high,
                        CAST(low AS DOUBLE PRECISION) as low,
                        CAST("close" AS DOUBLE PRECISION) as close,
                        CAST(volume AS BIGINT) as volume
                    FROM stock_ohlcv
                    WHERE symbol = :symbol AND "interval" = :interval
                        AND time BETWEEN :start_date AND :end_date
                    ORDER BY time ASC
                """)
                result = conn.execute(query, {
                    "symbol": symbol,
                    "interval": interval,
                    "start_date": start_datetime,
                    "end_date": end_datetime
                })
                rows = result.fetchall()
                if not rows:
                    logger.warning(f"Keine Daten für {symbol} im Zeitraum {start_date} - {end_date}.")
                    return pd.DataFrame()
                df = pd.DataFrame(rows, columns=result.keys())
                df.rename(columns={'time': 'date'}, inplace=True)
                df['date'] = pd.to_datetime(df['date'])
                logger.info(f"{len(df)} Zeilen für {symbol} geladen.")
                return df
        except Exception as e:
            logger.error(f"Fehler beim Laden der Daten für {symbol}: {e}")
            return pd.DataFrame()

    # === Indikator-Berechnung ===
    
    def calculate_indicators(self, df):
        """
        Berechnet ALLE technischen Indikatoren und fügt sie dem DataFrame hinzu.
        """
        if df.empty:
            return df
        try:
            df = df.copy()
            # Nutze die add_all_indicators Methode (wie in app_backup_1.py)
            if len(df) >= 20:
                df = self.indicators.add_all_indicators(df)
                logger.info("Alle Indikatoren berechnet.")
            return df
        except Exception as e:
            logger.error(f"Fehler bei der Indikator-Berechnung: {e}")
        return df

    # === Statistiken ===

    def calculate_statistics(self, df):
        """
        Berechnet einfache Statistiken für das Symbol.
        Gibt ein Dictionary mit Metriken zurück.
        """
        if df.empty:
            return {}
        try:
            stats = {
                "Anzahl Datenpunkte": len(df),
                "Zeitraum": f"{df['date'].min().date()} – {df['date'].max().date()}",
                "Höchster Kurs": df['high'].max(),
                "Tiefster Kurs": df['low'].min(),
                "Durchschnittskurs": df['close'].mean(),
                "Gesamtvolumen": df['volume'].sum()
            }
            logger.info("Statistiken berechnet.")
            return stats
        except Exception as e:
            logger.error(f"Fehler bei der Statistik-Berechnung: {e}")
            return {}

    # === Bulk- und Batch-Operationen ===

    def bulk_update_all_tickers(self, progress_callback=None):
        """
        Aktualisiert alle Ticker-Daten (z.B. von API in DB).
        Optional: Fortschritts-Callback für UI.
        """
        tickers = self.get_selected_tickers()
        total = len(tickers)
        for idx, ticker in enumerate(tickers):
            try:
                self.data_loader.update_ticker_data(ticker['ticker'])
                if progress_callback:
                    progress_callback(ticker['ticker'], True, (idx + 1) / total)
                logger.info(f"{ticker['ticker']} erfolgreich aktualisiert.")
            except Exception as e:
                if progress_callback:
                    progress_callback(ticker['ticker'], False, (idx + 1) / total)
                logger.error(f"Fehler beim Aktualisieren von {ticker['ticker']}: {e}")

    def batch_update_selected_tickers(self, tickers, progress_callback=None):
        """
        Aktualisiert eine Liste ausgewählter Ticker.
        """
        total = len(tickers)
        for idx, ticker in enumerate(tickers):
            try:
                self.data_loader.update_ticker_data(ticker)
                if progress_callback:
                    progress_callback(ticker, True, (idx + 1) / total)
                logger.info(f"{ticker} erfolgreich aktualisiert.")
            except Exception as e:
                if progress_callback:
                    progress_callback(ticker, False, (idx + 1) / total)
                logger.error(f"Fehler beim Aktualisieren von {ticker}: {e}")

