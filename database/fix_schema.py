#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 15 10:00:59 2026

@author: twi-dev
"""
"""
Behebt das Schema-Problem mit reservierten Wörtern
"""
import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from apps.data_ingestion.src.database import engine
from sqlalchemy import text

def fix_stock_ohlcv():
    """Erstellt stock_ohlcv mit korrekten Spaltennamen neu"""
    
    print("="*60)
    print("🔧 Behebe stock_ohlcv Tabelle")
    print("="*60)
    
    with engine.begin() as conn:
        # 1. Alte Tabelle löschen
        print("\n1. Lösche alte Tabelle...")
        try:
            conn.execute(text("DROP TABLE IF EXISTS stock_ohlcv CASCADE;"))
            print("   ✅ Gelöscht")
        except Exception as e:
            print(f"   ⚠️ {e}")
        
        # 2. Neue Tabelle mit quoted identifiers erstellen
        print("\n2. Erstelle neue Tabelle...")
        conn.execute(text("""
            CREATE TABLE stock_ohlcv (
                time TIMESTAMPTZ NOT NULL,
                symbol VARCHAR(10) NOT NULL,
                "open" DECIMAL(12, 4),
                high DECIMAL(12, 4),
                low DECIMAL(12, 4),
                "close" DECIMAL(12, 4),
                volume BIGINT,
                "interval" VARCHAR(10),
                PRIMARY KEY (symbol, time, "interval")
            );
        """))
        print("   ✅ Tabelle erstellt")
        
        # 3. Als Hypertable konvertieren
        print("\n3. Konvertiere zu Hypertable...")
        conn.execute(text("""
            SELECT create_hypertable('stock_ohlcv', 'time', 
                if_not_exists => TRUE
            );
        """))
        print("   ✅ Hypertable erstellt")
        
        # 4. Index erstellen
        print("\n4. Erstelle Indizes...")
        conn.execute(text("""
            CREATE INDEX idx_ohlcv_symbol_interval 
            ON stock_ohlcv (symbol, "interval", time DESC);
        """))
        print("   ✅ Index erstellt")
        
        # 5. Verifizieren
        print("\n5. Verifiziere Spalten...")
        result = conn.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'stock_ohlcv'
            ORDER BY ordinal_position;
        """))
        
        columns = [row[0] for row in result]
        print(f"   Spalten: {', '.join(columns)}")
        
        # Prüfen ob 'open' und 'close' vorhanden sind
        if 'open' in columns and 'close' in columns:
            print("\n✅ Tabelle korrekt erstellt!")
            return True
        else:
            print("\n❌ Fehler: 'open' oder 'close' fehlt")
            return False

def main():
    print("\n" + "="*60)
    print("🚀 Schema Fix")
    print("="*60)
    
    success = fix_stock_ohlcv()
    
    if success:
        print("\n" + "="*60)
        print("✅ Fix erfolgreich!")
        print("="*60)
        print("\n💡 Nächste Schritte:")
        print("   1. Führe test_ingestion.py erneut aus")
        print("   2. Daten sollten jetzt gespeichert werden")
    else:
        print("\n❌ Fix fehlgeschlagen")

if __name__ == "__main__":
    main()
