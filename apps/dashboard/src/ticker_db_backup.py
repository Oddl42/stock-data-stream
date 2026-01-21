#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 18 00:06:04 2026

@author: twi-dev
"""

#!/usr/bin/env python3
"""
SQLite-Datenbank für ausgewählte Ticker
"""
import sqlite3
import os
from pathlib import Path

class TickerDatabase:
    """Verwaltet ausgewählte Ticker in SQLite"""
    
    def __init__(self, db_path=None):
        if db_path is None:
            # Speichere DB im Projektverzeichnis
            project_root = Path(__file__).parent.parent.parent.parent
            db_path = project_root / 'database' / 'selected_tickers.db'
        
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Erstellt die Datenbank-Tabelle"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS selected_tickers (
                    ticker TEXT PRIMARY KEY,
                    name TEXT,
                    primary_exchange TEXT,
                    market TEXT,
                    added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
    
    def add_ticker(self, ticker, name, primary_exchange, market):
        """Fügt einen Ticker zur Auswahl hinzu"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO selected_tickers 
                    (ticker, name, primary_exchange, market)
                    VALUES (?, ?, ?, ?)
                """, (ticker, name, primary_exchange, market))
                conn.commit()
            return True
        except Exception as e:
            print(f"❌ Fehler beim Hinzufügen: {e}")
            return False
    
    def remove_ticker(self, ticker):
        """Entfernt einen Ticker aus der Auswahl"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    DELETE FROM selected_tickers WHERE ticker = ?
                """, (ticker,))
                conn.commit()
            return True
        except Exception as e:
            print(f"❌ Fehler beim Entfernen: {e}")
            return False
    
    def get_selected_tickers(self):
        """Gibt alle ausgewählten Ticker zurück"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT ticker, name, primary_exchange, market 
                    FROM selected_tickers 
                    ORDER BY ticker
                """)
                rows = cursor.fetchall()
                return [
                    {
                        'ticker': row[0],
                        'name': row[1],
                        'primary_exchange': row[2],
                        'market': row[3]
                    }
                    for row in rows
                ]
        except Exception as e:
            print(f"❌ Fehler beim Laden: {e}")
            return []
    
    def is_selected(self, ticker):
        """Prüft ob ein Ticker ausgewählt ist"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) FROM selected_tickers WHERE ticker = ?
                """, (ticker,))
                count = cursor.fetchone()[0]
                return count > 0
        except:
            return False
    
    def clear_all(self):
        """Löscht alle ausgewählten Ticker"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM selected_tickers")
                conn.commit()
            return True
        except Exception as e:
            print(f"❌ Fehler beim Löschen: {e}")
            return False
