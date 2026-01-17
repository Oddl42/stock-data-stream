#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 15 18:57:40 2026

@author: twi-dev
"""

#!/usr/bin/env python3
"""
Vergleicht Plotly und Bokeh Dashboard-Performance
"""
import subprocess
import sys

print("="*60)
print("📊 Dashboard Vergleich")
print("="*60)
print("\n1 - Plotly Dashboard (Port 5006)")
print("2 - Bokeh Dashboard (Port 5007)")
print("3 - Beide parallel starten")
print("0 - Beenden")

choice = input("\nDeine Wahl (0-3): ").strip()

if choice == '1':
    print("\n🚀 Starte Plotly Dashboard...")
    subprocess.run([sys.executable, "test/test_dashboard.py"])
elif choice == '2':
    print("\n🚀 Starte Bokeh Dashboard...")
    subprocess.run([sys.executable, "test/test_dashboard_bokeh.py"])
elif choice == '3':
    print("\n🚀 Starte beide Dashboards...")
    print("   Plotly: http://localhost:5006")
    print("   Bokeh:  http://localhost:5007")
    # Beide parallel starten (nur Linux/Mac)
    import threading
    
    def start_plotly():
        subprocess.run([sys.executable, "test_dashboard.py"])
    
    def start_bokeh():
        subprocess.run([sys.executable, "test_dashboard_bokeh.py"])
    
    t1 = threading.Thread(target=start_plotly)
    t2 = threading.Thread(target=start_bokeh)
    
    t1.start()
    t2.start()
    
    t1.join()
    t2.join()
else:
    print("👋 Tschüss!")
