#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 18 15:51:20 2026

@author: twi-dev
"""

"""
Massive.com API Client für Stock-Daten
"""
import os
import requests
from datetime import datetime
from dotenv import load_dotenv

class MassiveClient:
    """Client für Massive.com Stock Market API"""
    
    def __init__(self):
        """Initialisiert den Massive API Client"""
        load_dotenv()
        
        self.api_key = os.getenv('MASSIVE_API_KEY')
        self.base_url = "https://api.massive.com"
        
        # Headers definieren
        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        
        if not self.api_key:
            raise ValueError(
                "MASSIVE_API_KEY nicht gefunden!\n"
                "Bitte in .env Datei setzen: MASSIVE_API_KEY=your_key_here"
            )
    
    def test_connection(self):
        """Testet die API-Verbindung"""
        print("\n" + "="*60)
        print("🔌 Teste Massive.com API-Verbindung")
        print("="*60)
        
        try:
            # Test mit einfachem Ticker-Abruf
            url = f"{self.base_url}/v1/stocks/tickers/AAPL"
            response = requests.get(url, headers=self.headers, timeout=10)
            
            if response.status_code == 200:
                print("✅ Massive.com API-Verbindung erfolgreich!")
                return True
            else:
                print(f"❌ API-Fehler: Status {response.status_code}")
                print(f"   Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Verbindungsfehler: {e}")
            return False
    
    def get_historical_data(self, ticker, start_date, end_date, interval='1day'):
        """
        Lädt historische OHLCV-Daten
        
        Args:
            ticker: Stock Symbol (z.B. 'AAPL')
            start_date: Start-Datum (datetime)
            end_date: End-Datum (datetime)
            interval: '1min', '5min', '15min', '1hour', '1day'
        
        Returns:
            List[Dict]: OHLCV-Daten
        """
        url = f"{self.base_url}/v1/stocks/{ticker}/historical"
        
        params = {
            'start': start_date.strftime('%Y-%m-%d'),
            'end': end_date.strftime('%Y-%m-%d'),
            'interval': interval
        }
        
        try:
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('results', [])
            else:
                print(f"❌ API Fehler {response.status_code}: {response.text}")
                return []
                
        except Exception as e:
            print(f"❌ Fehler beim Abrufen der Daten: {e}")
            return []
    
    def get_all_tickers(self, asset_class='stocks', active=True):
        """
        Lädt alle verfügbaren Ticker von Massive.com
        
        Args:
            asset_class: 'stocks', 'crypto', 'forex', 'indices'
            active: Nur aktive Ticker (True/False)
        
        Returns:
            List[Dict]: Liste aller Ticker mit Details
        """
        url = f"{self.base_url}/v3/reference/tickers?market=stocks&active=true&order=asc&limit=1000&sort=ticker&apiKey={self.api_key}"
        
        params = {}
        if active:
            params['active'] = 'true'
        
        try:
            print(f"📡 Lade alle {asset_class} Ticker von Massive API...")
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                print(f"✅ {len(results)} Ticker geladen")
                return results
            elif response.status_code == 401:
                print(f"❌ Authentifizierungsfehler: API-Key ungültig oder abgelaufen")
                return []
            elif response.status_code == 429:
                print(f"❌ Rate-Limit erreicht: Zu viele Anfragen")
                return []
            else:
                print(f"❌ API Fehler {response.status_code}")
                print(f"   URL: {url}")
                print(f"   Response: {response.text[:200]}")
                return []
                
        except requests.exceptions.Timeout:
            print(f"❌ Timeout: API antwortet nicht innerhalb von 30 Sekunden")
            return []
        except requests.exceptions.ConnectionError:
            print(f"❌ Verbindungsfehler: Kann API nicht erreichen")
            return []
        except Exception as e:
            print(f"❌ Unerwarteter Fehler beim Laden der Ticker: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_ticker_details(self, ticker):
        """
        Lädt Details zu einem spezifischen Ticker
        
        Args:
            ticker: Stock Symbol (z.B. 'AAPL')
        
        Returns:
            Dict: Ticker-Details
        """
        url = f"{self.base_url}/v1/stocks/tickers/{ticker}"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"❌ Fehler {response.status_code} für {ticker}")
                return {}
                
        except Exception as e:
            print(f"❌ Fehler beim Abrufen von {ticker}: {e}")
            return {}
