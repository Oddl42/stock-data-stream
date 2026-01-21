#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 21 23:11:40 2026

@author: twi-dev
"""
"""
ticker_db.py – Verwaltung der ausgewählten Ticker in der Datenbank
"""

import logging
from sqlalchemy import text
from apps.data_ingestion.src.database import engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TickerDatabase")


class TickerDatabase:
    """
    Verwaltet die Ticker-Auswahl in der selected_tickers Tabelle.
    """

    def __init__(self):
        """Initialisiert die TickerDatabase und erstellt Tabelle falls nötig."""
        self._create_table()

    def _create_table(self):
        """Erstellt die selected_tickers Tabelle falls sie nicht existiert."""
        try:
            with engine.connect() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS selected_tickers (
                        id SERIAL PRIMARY KEY,
                        ticker VARCHAR(20) UNIQUE NOT NULL,
                        name VARCHAR(255),
                        primary_exchange VARCHAR(100),
                        market VARCHAR(50),
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                conn.commit()
                logger.info("✅ Tabelle 'selected_tickers' bereit")
        except Exception as e:
            logger.error(f"❌ Fehler beim Erstellen der Tabelle: {e}")

    def add_ticker(self, ticker, name='', primary_exchange='', market=''):
        """
        Fügt einen Ticker zur Auswahl hinzu.
        
        Args:
            ticker: Ticker-Symbol (z.B. 'AAPL')
            name: Firmenname
            primary_exchange: Primärbörse
            market: Markt-Typ
            
        Returns:
            bool: True bei Erfolg, False wenn Ticker bereits existiert
        """
        try:
            with engine.connect() as conn:
                # Prüfe ob Ticker bereits existiert
                result = conn.execute(text("""
                    SELECT ticker FROM selected_tickers WHERE ticker = :ticker
                """), {"ticker": ticker})
                
                if result.fetchone():
                    logger.warning(f"⚠️ Ticker {ticker} bereits vorhanden")
                    return False
                
                # Füge Ticker hinzu
                conn.execute(text("""
                    INSERT INTO selected_tickers (ticker, name, primary_exchange, market)
                    VALUES (:ticker, :name, :primary_exchange, :market)
                """), {
                    "ticker": ticker,
                    "name": name,
                    "primary_exchange": primary_exchange,
                    "market": market
                })
                conn.commit()
                logger.info(f"✅ Ticker {ticker} hinzugefügt")
                return True
        
        except Exception as e:
            logger.error(f"❌ Fehler beim Hinzufügen von {ticker}: {e}")
            return False

    def remove_selected_ticker(self, ticker):
        """
        Entfernt einen Ticker aus der Auswahl.
        
        Args:
            ticker: Ticker-Symbol (z.B. 'AAPL')
            
        Returns:
            bool: True bei Erfolg, False bei Fehler
        """
        try:
            with engine.connect() as conn:
                result = conn.execute(text("""
                    DELETE FROM selected_tickers WHERE ticker = :ticker
                """), {"ticker": ticker})
                conn.commit()
                
                if result.rowcount > 0:
                    logger.info(f"✅ Ticker {ticker} entfernt")
                    return True
                else:
                    logger.warning(f"⚠️ Ticker {ticker} nicht gefunden")
                    return False
        
        except Exception as e:
            logger.error(f"❌ Fehler beim Entfernen von {ticker}: {e}")
            return False

    def get_selected_tickers(self):
        """
        Gibt alle ausgewählten Ticker zurück.
        
        Returns:
            list: Liste von Dictionaries mit Ticker-Infos
        """
        try:
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT ticker, name, primary_exchange, market, added_at
                    FROM selected_tickers
                    ORDER BY ticker ASC
                """))
                
                tickers = []
                for row in result:
                    tickers.append({
                        'ticker': row[0],
                        'name': row[1],
                        'primary_exchange': row[2],
                        'market': row[3],
                        'status': '✓ Ausgewählt'
                    })
                
                logger.info(f"📊 {len(tickers)} ausgewählte Ticker geladen")
                return tickers
        
        except Exception as e:
            logger.error(f"❌ Fehler beim Abrufen der Ticker: {e}")
            return []

    def is_selected(self, ticker):
        """
        Prüft, ob ein Ticker ausgewählt ist.
        
        Args:
            ticker: Ticker-Symbol (z.B. 'AAPL')
            
        Returns:
            bool: True wenn ausgewählt, sonst False
        """
        try:
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT ticker FROM selected_tickers WHERE ticker = :ticker
                """), {"ticker": ticker})
                
                return result.fetchone() is not None
        
        except Exception as e:
            logger.error(f"❌ Fehler beim Prüfen von {ticker}: {e}")
            return False

    def clear_selected_tickers(self):
        """
        Löscht ALLE ausgewählten Ticker aus der Datenbank.
        
        Returns:
            bool: True bei Erfolg, False bei Fehler
        """
        try:
            with engine.connect() as conn:
                # Zähle Ticker vor dem Löschen
                count_result = conn.execute(text("""
                    SELECT COUNT(*) FROM selected_tickers
                """))
                count = count_result.fetchone()[0]
                
                if count == 0:
                    logger.info("ℹ️ Keine Ticker zum Löschen vorhanden")
                    return True
                
                # Alle Ticker löschen
                conn.execute(text("""
                    DELETE FROM selected_tickers
                """))
                conn.commit()
                
                logger.info(f"✅ Alle {count} Ticker aus selected_tickers gelöscht")
                return True
        
        except Exception as e:
            logger.error(f"❌ Fehler beim Löschen aller Ticker: {e}")
            return False

    def get_ticker_count(self):
        """
        Gibt die Anzahl der ausgewählten Ticker zurück.
        
        Returns:
            int: Anzahl der Ticker
        """
        try:
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM selected_tickers
                """))
                count = result.fetchone()[0]
                return count
        
        except Exception as e:
            logger.error(f"❌ Fehler beim Zählen der Ticker: {e}")
            return 0
