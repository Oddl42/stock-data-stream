#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 15 09:52:06 2026

@author: twi-dev
"""

#!/usr/bin/env python3
"""
Erstellt das Datenbank-Schema automatisch
"""
import sys
import os

# Projekt-Root zum Path hinzufügen
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from apps.data_ingestion.src.database import engine
from sqlalchemy import text

def create_schema():
    """Liest SQL-Datei und führt sie aus"""
    
    sql_file = os.path.join(project_root, 'database/schemas/01_create_tables.sql')
    
    print("="*60)
    print("🔧 Erstelle Datenbank-Schema")
    print("="*60)
    print(f"SQL-Datei: {sql_file}")
    
    if not os.path.exists(sql_file):
        print(f"❌ SQL-Datei nicht gefunden: {sql_file}")
        return False
    
    # SQL-Datei lesen
    with open(sql_file, 'r') as f:
        sql_content = f.read()
    
    # SQL ausführen (aufteilen bei Semikolon für einzelne Statements)
    statements = [s.strip() for s in sql_content.split(';') if s.strip()]
    
    try:
        with engine.begin() as connection:
            for i, statement in enumerate(statements, 1):
                if not statement:
                    continue
                    
                # Kommentare entfernen
                if statement.startswith('--'):
                    continue
                
                print(f"\n[{i}/{len(statements)}] Führe Statement aus...")
                
                # Erste 60 Zeichen des Statements anzeigen
                preview = statement[:60].replace('\n', ' ')
                print(f"   {preview}...")
                
                try:
                    connection.execute(text(statement))
                    print(f"   ✅ Erfolgreich")
                except Exception as e:
                    # Manche Fehler sind OK (z.B. "already exists")
                    if "already exists" in str(e):
                        print(f"   ⚠️  Bereits vorhanden (OK)")
                    else:
                        print(f"   ❌ Fehler: {e}")
                        # Weiter mit nächstem Statement
        
        print("\n" + "="*60)
        print("✅ Schema-Erstellung abgeschlossen!")
        print("="*60)
        return True
        
    except Exception as e:
        print(f"\n❌ Fehler beim Erstellen des Schemas: {e}")
        return False

def verify_schema():
    """Prüft ob alle Tabellen existieren"""
    
    print("\n" + "="*60)
    print("🔍 Verifiziere Tabellen")
    print("="*60)
    
    expected_tables = [
        'stock_quotes',
        'stock_ohlcv',
        'stock_metadata'
    ]
    
    with engine.connect() as connection:
        for table in expected_tables:
            result = connection.execute(text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = '{table}'
                );
            """))
            exists = result.scalar()
            
            status = "✅" if exists else "❌"
            print(f"{status} {table}")
            
            if exists:
                # Spalten anzeigen
                result = connection.execute(text(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = '{table}'
                    ORDER BY ordinal_position;
                """))
                columns = result.fetchall()
                print(f"   Spalten: {', '.join([c[0] for c in columns])}")

def main():
    """Hauptfunktion"""
    
    print("\n" + "="*60)
    print("🚀 Datenbank-Schema Setup")
    print("="*60)
    
    # Schema erstellen
    success = create_schema()
    
    if success:
        # Verifizieren
        verify_schema()
        
        print("\n" + "="*60)
        print("✅ Setup abgeschlossen!")
        print("="*60)
        print("\n💡 Nächste Schritte:")
        print("   1. Führe test_ingestion.py erneut aus")
        print("   2. Daten sollten jetzt gespeichert werden können")
    else:
        print("\n" + "="*60)
        print("❌ Setup fehlgeschlagen")
        print("="*60)
        print("\n💡 Versuche:")
        print("   1. docker-compose down -v")
        print("   2. docker-compose up -d")
        print("   3. Dieses Script erneut ausführen")

if __name__ == "__main__":
    main()
