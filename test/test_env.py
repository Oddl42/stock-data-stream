#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 15 08:05:29 2026

@author: twi-dev
"""

"""Test ob .env Datei korrekt geladen wird"""

import os
from dotenv import load_dotenv

# .env laden
load_dotenv()

print("="*60)
print("🔍 .env Datei Test")
print("="*60)

# Alle relevanten Variablen prüfen
variables = {
    'DATABASE_URL': os.getenv('DATABASE_URL'),
    'MASSIVE_API_KEY': os.getenv('MASSIVE_API_KEY'),
    'LOG_LEVEL': os.getenv('LOG_LEVEL'),
}

print("\n📋 Geladene Umgebungsvariablen:\n")

for key, value in variables.items():
    if value:
        # API-Key nur teilweise anzeigen (Sicherheit)
        if 'KEY' in key and len(value) > 10:
            display_value = value[:8] + "..." + value[-4:]
        else:
            display_value = value
        print(f"✅ {key:20s} = {display_value}")
    else:
        print(f"❌ {key:20s} = NICHT GESETZT")

# .env Datei-Pfad anzeigen
env_path = os.path.join(os.getcwd(), '.env')
if os.path.exists(env_path):
    print(f"\n📁 .env Datei gefunden: {env_path}")
else:
    print(f"\n⚠️  .env Datei NICHT gefunden: {env_path}")
    print("   Erstelle die Datei im Projektverzeichnis!")

print("\n" + "="*60)
