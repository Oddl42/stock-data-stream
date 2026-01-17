2#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 14 22:03:21 2026

@author: twi-dev
"""

"""
Test-Script für Stock Data Ingestion mit Massive.com API
"""
#from database import test_connection
#from massive_client import MassiveClient
#from ingestion import StockDataIngestion
from apps.data_ingestion.src.database import test_connection, get_db_session, engine
from apps.data_ingestion.src.massive_client import MassiveClient
from apps.data_ingestion.src.ingestion import StockDataIngestion

from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()


def test_api_connection():
    """Testet die Massive.com API-Verbindung"""
    print("\n" + "="*60)
    print("🔌 Teste Massive.com API-Verbindung")
    print("="*60)
    
    client = MassiveClient()
    return client.test_connection()

def test_single_symbol():
    """Testet das Laden eines einzelnen Symbols"""
    print("\n" + "="*60)
    print("📊 Test: Einzelnes Symbol (AAPL)")
    print("="*60)
    
    ingestion = StockDataIngestion()
    
    # Letzten 30 Tage laden
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    ingestion.ingest_symbol(
        symbol='AAPL',
        start_date=start_date,
        end_date=end_date,
        interval='1day'
    )

def test_multiple_symbols():
    """Testet das Laden mehrerer Symbole"""
    print("\n" + "="*60)
    print("📊 Test: Mehrere Symbole")
    print("="*60)
    
    ingestion = StockDataIngestion()
    
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX', 'FLNC', 'TTD', 'HIMS']
    end_date = datetime.now()
    start_date = end_date - timedelta(days=720)  # 3 Monate
    
    ingestion.ingest_multiple_symbols(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        interval='1day'
    )

def test_intraday_data():
    """Testet das Laden von Intraday-Daten (Minutengenaue Daten)"""
    print("\n" + "="*60)
    print("📊 Test: Intraday-Daten (5-Minuten)")
    print("="*60)
    
    ingestion = StockDataIngestion()
    
    # Letzten 5 Handelstage
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    ingestion.ingest_symbol(
        symbol='AAPL',
        start_date=start_date,
        end_date=end_date,
        interval='5min',
        save_as_quotes=True  # Auch in stock_quotes speichern
    )

def verify_data():
    """Verifiziert die gespeicherten Daten"""
    print("\n" + "="*60)
    print("🔍 Verifiziere gespeicherte Daten")
    print("="*60)
    
    from sqlalchemy import text
    from database import engine
    
    with engine.connect() as conn:
        # Anzahl Datensätze in stock_ohlcv
        result = conn.execute(text("""
            SELECT 
                symbol,
                interval,
                COUNT(*) as count,
                MIN(time) as first_date,
                MAX(time) as last_date
            FROM stock_ohlcv
            GROUP BY symbol, interval
            ORDER BY symbol, interval
        """))
        
        print("\n📊 Stock OHLCV Daten:")
        for row in result:
            print(f"   {row.symbol:8s} | {row.interval:6s} | "
                  f"{row.count:5d} Datensätze | "
                  f"{row.first_date.date()} bis {row.last_date.date()}")
        
        # Anzahl Datensätze in stock_quotes
        result = conn.execute(text("""
            SELECT 
                symbol,
                COUNT(*) as count,
                MIN(time) as first_date,
                MAX(time) as last_date
            FROM stock_quotes
            GROUP BY symbol
            ORDER BY symbol
        """))
        
        print("\n📊 Stock Quotes Daten:")
        rows = list(result)
        if rows:
            for row in rows:
                print(f"   {row.symbol:8s} | "
                      f"{row.count:6d} Datensätze | "
                      f"{row.first_date.date()} bis {row.last_date.date()}")
        else:
            print("   Keine Daten vorhanden")

def main():
    """Hauptfunktion"""
    print("\n" + "="*60)
    print("🚀 Stock Data Ingestion - Test Suite")
    print("="*60)
    
    # API Key prüfen
    api_key = os.getenv('MASSIVE_API_KEY')
    if not api_key:
        print("\n⚠️  WARNUNG: MASSIVE_API_KEY nicht gesetzt!")
        print("   Setze in .env: MASSIVE_API_KEY=your_key_here")
        print("   Die Tests werden trotzdem ausgeführt, könnten aber fehlschlagen.\n")
    
    # 1. Datenbankverbindung testen
    print("\n" + "="*60)
    print("1️⃣  Teste Datenbankverbindung")
    print("="*60)
    if not test_connection():
        print("\n❌ Abbruch: Keine Datenbankverbindung")
        return
    
    # 2. API-Verbindung testen
    print("\n" + "="*60)
    print("2️⃣  Teste API-Verbindung")
    print("="*60)
    if not test_api_connection():
        print("\n❌ Abbruch: Keine API-Verbindung")
        return
    
    # 3. Tests ausführen (Auswahl)
    while True:
        print("\n" + "="*60)
        print("Welchen Test möchtest du ausführen?")
        print("="*60)
        print("1 - Einzelnes Symbol (AAPL, 30 Tage, täglich)")
        print("2 - Mehrere Symbole ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META'...  1 Jahr)")
        print("3 - Intraday-Daten (AAPL, 7 Tage, 5-Minuten)")
        print("4 - Alle Tests ausführen")
        print("5 - Daten verifizieren")
        print("0 - Beenden")
        
        choice = input("\nDeine Wahl (0-5): ").strip()
        
        if choice == '1':
            test_single_symbol()
        elif choice == '2':
            test_multiple_symbols()
        elif choice == '3':
            test_intraday_data()
        elif choice == '4':
            test_single_symbol()
            test_multiple_symbols()
            test_intraday_data()
        elif choice == '5':
            verify_data()
        elif choice == '0':
            break
        else:
            print("❌ Ungültige Eingabe")
    
    print("\n" + "="*60)
    print("✅ Tests abgeschlossen!")
    print("="*60)
    print("\n💡 Nächste Schritte:")
    print("   - Daten in pgAdmin prüfen: http://localhost:5050")
    print("   - SQL-Queries in database/schemas/ anschauen")
    print("   - Weiter zu Phase 2: GUI-Entwicklung")

if __name__ == "__main__":
    main()
