#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 18 15:54:08 2026

@author: twi-dev
"""

# Test ob get_all_tickers funktioniert
from apps.data_ingestion.src.massive_client import MassiveClient

client = MassiveClient()

# Test Verbindung
#client.test_connection()

# Test Ticker laden
tickers = client.get_all_tickers(asset_class='stocks', active=True)
print(f"\nErste 5 Ticker:")
for ticker in tickers[:5]:
    print(f"  {ticker.get('ticker', 'N/A')}: {ticker.get('name', 'N/A')}")
