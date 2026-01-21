#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Massive.com API Client für Stock-Daten (Polygon.io kompatibel)
"""
import os
import requests
from datetime import datetime
from dotenv import load_dotenv

class MassiveClient:
    """Client für Massive.com Stock Market API (verwendet Polygon.io API)"""
    
    def __init__(self):
        """Initialisiert den Massive API Client"""
        load_dotenv()
        
        self.api_key = os.getenv('MASSIVE_API_KEY')
        self.base_url = "https://api.polygon.io"  # ✅ Korrigiert
        
        if not self.api_key:
            raise ValueError(
                "MASSIVE_API_KEY nicht gefunden!\n"
                "Bitte in .env Datei setzen: MASSIVE_API_KEY=your_key_here"
            )
        
        print(f"✅ MassiveClient initialisiert (Base: {self.base_url})")
    
    def test_connection(self):
        """Testet die API-Verbindung"""
        print("\n" + "="*60)
        print("🔌 Teste Massive.com API-Verbindung")
        print("="*60)
        
        try:
            url = f"{self.base_url}/v3/reference/tickers/AAPL"
            params = {'apiKey': self.api_key}
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                print("✅ Massive.com API-Verbindung erfolgreich!")
                return True
            else:
                print(f"❌ API-Fehler: Status {response.status_code}")
                print(f"   Response: {response.text[:200]}")
                return False
                
        except Exception as e:
            print(f"❌ Verbindungsfehler: {e}")
            return False
    
    def get_ohlcv(self, symbol, interval='1day', start=None, end=None):
        """
        Lädt historische OHLCV-Daten (Polygon.io API)
        
        Args:
            symbol: Stock Symbol (z.B. 'AAPL')
            interval: '1min', '5min', '15min', '30min', '1hour', '4hour', '1day', '1week', '1month'
            start: Start-Datum (datetime)
            end: End-Datum (datetime)
        
        Returns:
            List[Dict]: OHLCV-Daten
        """
        # Intervall-Mapping für Polygon.io
        interval_map = {
            '1min': ('1', 'minute'),
            '5min': ('5', 'minute'),
            '15min': ('15', 'minute'),
            '30min': ('30', 'minute'),
            '1hour': ('1', 'hour'),
            '4hour': ('4', 'hour'),
            '1day': ('1', 'day'),
            '1week': ('1', 'week'),
            '1month': ('1', 'month')
        }
        
        if interval not in interval_map:
            print(f"⚠️ Unbekanntes Intervall {interval}, verwende 1day")
            interval = '1day'
        
        multiplier, timespan = interval_map[interval]
        
        # Datum formatieren
        from_date = start.strftime('%Y-%m-%d') if start else (datetime.now().replace(day=1)).strftime('%Y-%m-%d')
        to_date = end.strftime('%Y-%m-%d') if end else datetime.now().strftime('%Y-%m-%d')
        
        # ✅ Korrekte Polygon.io API URL
        url = f"{self.base_url}/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_date}/{to_date}"
        
        params = {
            'adjusted': 'true',
            'sort': 'asc',
            'limit': 50000,
            'apiKey': self.api_key
        }
        
        try:
            print(f"📡 API Request: {symbol} ({interval}) {from_date} → {to_date}")
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                
                if results:
                    # Polygon.io Format → Standard-Format konvertieren
                    ohlcv_data = []
                    for bar in results:
                        ohlcv_data.append({
                            'time': bar.get('t'),  # Unix timestamp in milliseconds
                            'open': bar.get('o'),
                            'high': bar.get('h'),
                            'low': bar.get('l'),
                            'close': bar.get('c'),
                            'volume': bar.get('v')
                        })
                    
                    print(f"✅ {len(ohlcv_data)} Datenpunkte geladen")
                    return ohlcv_data
                else:
                    print(f"⚠️ Keine Daten für {symbol} im Zeitraum {from_date} - {to_date}")
                    return []
            
            elif response.status_code == 401:
                print(f"❌ Authentifizierungsfehler: API-Key ungültig")
                return []
            elif response.status_code == 429:
                print(f"❌ Rate-Limit erreicht")
                return []
            else:
                print(f"❌ API Fehler {response.status_code}: {response.text[:200]}")
                return []
                
        except Exception as e:
            print(f"❌ Fehler beim Abrufen der Daten: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_historical_data(self, ticker, start_date, end_date, interval='1day'):
        """
        Alias für get_ohlcv() (für Kompatibilität)
        """
        return self.get_ohlcv(ticker, interval, start_date, end_date)
    
    def get_all_tickers(self, asset_class='stocks', active=True):
        """
        Lädt alle verfügbaren Ticker von Massive.com
        
        Args:
            asset_class: 'stocks', 'crypto', 'forex', 'indices'
            active: Nur aktive Ticker (True/False)
        
        Returns:
            List[Dict]: Liste aller Ticker mit Details
        """
        url = f"{self.base_url}/v3/reference/tickers"
        
        params = {
            'type': 'CS',  # Common Stock
            'market': 'stocks',
            'exchange': 'XNAS',  # NASDAQ
            'active': 'true' if active else 'false',
            'order': 'asc',
            'limit': 1000,
            'sort': 'ticker',
            'apiKey': self.api_key
        }
        
        try:
            print(f"📡 Lade alle {asset_class} Ticker von Massive API...")
            response = requests.get(url, params=params, timeout=30)
            
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
                print(f"   Response: {response.text[:200]}")
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
        url = f"{self.base_url}/v3/reference/tickers/{ticker}"
        params = {'apiKey': self.api_key}
        
        try:
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return data.get('results', {})
            else:
                print(f"❌ Fehler {response.status_code} für {ticker}")
                return {}
                
        except Exception as e:
            print(f"❌ Fehler beim Abrufen von {ticker}: {e}")
            return {}
