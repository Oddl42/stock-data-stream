#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 22 22:07:17 2026

@author: twi-dev
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script für das Dashboard mit automatischer Port-Auswahl
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

# Panel-Extension laden
pn.extension('plotly')

print("="*60)
print("🎨 Dashboard Test")
print("="*60)

# Dashboard importieren
try:
    from apps.dashboard.src.app import dashboard
    print("✅ Dashboard erfolgreich importiert")
except Exception as e:
    print(f"❌ Import-Fehler: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)  # ✅ KORRIGIERT: sys.exit statt exit

# Freien Port finden
try:
    port = find_free_port()
    print(f"\n🚀 Starte Dashboard auf Port {port}...")
    print(f"   URL: http://localhost:{port}")
    print("   Drücke Ctrl+C zum Beenden\n")
    
    # Server starten
    dashboard.show().show(port=port, threaded=False)
except KeyboardInterrupt:
    print("\n\n👋 Dashboard beendet")
    sys.exit(0)
except Exception as e:
    print(f"\n❌ Fehler beim Starten: {e}")
    import traceback
    traceback.print_exc()
    print("\n💡 Versuche:")
    print("   1. Alle Python-Prozesse beenden: pkill python")
    print("   2. Script erneut ausführen")
    sys.exit(1)
