#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 15 18:22:24 2026

@author: twi-dev
"""

#!/usr/bin/env python3
"""
Test-Script für das Bokeh Dashboard mit automatischer Port-Auswahl
"""
import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import panel as pn
import socket

def find_free_port(start_port=5006, max_attempts=10):
    """Findet einen freien Port"""
    for port in range(start_port, start_port + max_attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(('localhost', port))
                return port
            except OSError:
                continue
    raise RuntimeError(f"Kein freier Port zwischen {start_port} und {start_port + max_attempts} gefunden")

"""
def test_imports():
    """Testet ob alle benötigten Pakete verfügbar sind"""
    print("\n🔍 Teste Imports...")
    
    try:
        import bokeh
        print(f"   ✅ Bokeh {bokeh.__version__}")
    except ImportError as e:
        print(f"   ❌ Bokeh: {e}")
        return False
    
    try:
        import panel
        print(f"   ✅ Panel {panel.__version__}")
    except ImportError as e:
        print(f"   ❌ Panel: {e}")
        return False
    
    try:
        import pandas
        print(f"   ✅ Pandas {pandas.__version__}")
    except ImportError as e:
        print(f"   ❌ Pandas: {e}")
        return False
    
    try:
        import numpy
        print(f"   ✅ NumPy {numpy.__version__}")
        if numpy.__version__.startswith('2.'):
            print(f"   ⚠️  WARNUNG: NumPy 2.x erkannt - kann Probleme verursachen!")
    except ImportError as e:
        print(f"   ❌ NumPy: {e}")
        return False
    
    return True
"""

def test_database_connection():
    """Testet die Datenbankverbindung"""
    print("\n🔍 Teste Datenbankverbindung...")
    
    try:
        from apps.data_ingestion.src.database import test_connection
        if test_connection():
            print("   ✅ Datenbank erreichbar")
            return True
        else:
            print("   ❌ Datenbank nicht erreichbar")
            return False
    except Exception as e:
        print(f"   ❌ Fehler: {e}")
        return False

def test_data_availability():
    """Prüft ob Daten in der Datenbank vorhanden sind"""
    print("\n🔍 Teste Datenverfügbarkeit...")
    
    try:
        from apps.data_ingestion.src.database import engine
        from sqlalchemy import text
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM stock_ohlcv"))
            count = result.scalar()
            
            if count > 0:
                print(f"   ✅ {count} Datensätze gefunden")
                
                # Verfügbare Symbole anzeigen
                result = conn.execute(text("SELECT DISTINCT symbol FROM stock_ohlcv ORDER BY symbol"))
                symbols = [row[0] for row in result]
                print(f"   📊 Verfügbare Symbole: {', '.join(symbols[:5])}")
                if len(symbols) > 5:
                    print(f"      ... und {len(symbols) - 5} weitere")
                
                return True
            else:
                print("   ⚠️  Keine Daten in der Datenbank")
                print("   💡 Führe zuerst test_ingestion.py aus, um Daten zu laden")
                return False
                
    except Exception as e:
        print(f"   ❌ Fehler: {e}")
        return False

# Panel-Extension laden
pn.extension('bokeh')

print("="*60)
print("🎨 Bokeh Dashboard Test")
print("="*60)

# 1. Import-Tests
"""
if not test_imports():
    print("\n❌ Import-Tests fehlgeschlagen")
    print("\n💡 Installiere fehlende Pakete:")
    print("   conda install -c conda-forge bokeh panel pandas numpy<2.0")
    sys.exit(1)
"""

# 2. Dashboard importieren
print("\n🔍 Importiere Dashboard...")
try:
    from apps.dashboard.src.app_bokeh import dashboard
    print("   ✅ Dashboard erfolgreich importiert")
except Exception as e:
    print(f"   ❌ Import-Fehler: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# 3. Datenbank-Tests
db_ok = test_database_connection()
data_ok = test_data_availability()

if not db_ok:
    print("\n⚠️  Datenbank nicht erreichbar - Dashboard könnte leer sein")
elif not data_ok:
    print("\n⚠️  Keine Daten vorhanden - Dashboard wird leer sein")

# 4. Server starten
try:
    port = find_free_port()
    
    print("\n" + "="*60)
    print("🚀 Starte Bokeh Dashboard")
    print("="*60)
    print(f"   Port: {port}")
    print(f"   URL: http://localhost:{port}")
    print("\n   Features:")
    print("   ✅ Bokeh Candlestick Charts")
    print("   ✅ Volume Charts")
    print("   ✅ Technische Indikatoren (SMA, EMA)")
    print("   ✅ Interaktive Controls")
    print("\n   Drücke Ctrl+C zum Beenden\n")
    print("="*60)
    
    # Server starten
    dashboard.show().show(port=port, threaded=False)
    
except KeyboardInterrupt:
    print("\n\n👋 Dashboard wurde beendet")
    sys.exit(0)
    
except Exception as e:
    print(f"\n❌ Fehler beim Starten: {e}")
    print("\n💡 Versuche:")
    print("   1. Alle Python-Prozesse beenden: pkill python")
    print("   2. Docker-Container prüfen: docker-compose ps")
    print("   3. Script erneut ausführen")
    import traceback
    traceback.print_exc()
    sys.exit(1)
