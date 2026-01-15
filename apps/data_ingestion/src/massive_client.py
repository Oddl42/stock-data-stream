#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 14 22:01:55 2026

@author: twi-dev
"""

import requests
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv
import os

load_dotenv()

class MassiveClient:
    """Client für Massive.com REST API v2"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('MASSIVE_API_KEY')
        self.base_url = "https://api.massive.com"
        self.session = requests.Session()
        
        if self.api_key:
            self.session.headers.update({
                'Authorization': f'Bearer {self.api_key}'
            })
    
    def get_aggregate_bars(
        self,
        ticker: str,
        multiplier: int = 1,
        timespan: str = 'day',
        from_date: str = None,
        to_date: str = None,
        adjusted: bool = True,
        sort: str = 'asc',
        limit: int = 5000
    ) -> Dict:
        """
        Holt OHLC Aggregate Bars für einen Stock Ticker
        
        Args:
            ticker: Stock ticker symbol (z.B. 'AAPL')
            multiplier: Größe des Timespan-Multipliers (z.B. 5 für 5-Minuten)
            timespan: Zeiteinheit - 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year'
            from_date: Start-Datum (YYYY-MM-DD oder Millisekunden-Timestamp)
            to_date: End-Datum (YYYY-MM-DD oder Millisekunden-Timestamp)
            adjusted: Ob Ergebnisse für Splits angepasst werden
            sort: 'asc' (aufsteigend) oder 'desc' (absteigend)
            limit: Max. Anzahl Ergebnisse (max 50000, default 5000)
        
        Returns:
            Dict mit results, status, und metadata
        """
        endpoint = (
            f"{self.base_url}/v2/aggs/ticker/{ticker}/range/"
            f"{multiplier}/{timespan}/{from_date}/{to_date}"
        )
        
        params = {
            'adjusted': str(adjusted).lower(),
            'sort': sort,
            'limit': limit
        }
        
        try:
            response = self.session.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            
            # API-Status prüfen
            #if data.get('status') != 'OK':
            if data.get('count') == 0:
                print(f"⚠️  API Status: {data.get('status')}")
                print(f"   Message: {data.get('message', 'Keine Details')}")
            
            return data
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print("⚠️  Rate Limit erreicht, warte 60 Sekunden...")
                time.sleep(60)
                return self.get_aggregate_bars(
                    ticker, multiplier, timespan, from_date, to_date,
                    adjusted, sort, limit
                )
            else:
                print(f"❌ HTTP Fehler {e.response.status_code}: {e}")
                return {'status': 'ERROR', 'results': []}
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Request Fehler für {ticker}: {e}")
            return {'status': 'ERROR', 'results': []}
    
    def get_historical_data(
        self,
        ticker: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = '1min',
        adjusted: bool = True
    ) -> List[Dict]:
        """
        Vereinfachte Methode zum Abrufen historischer Daten
        
        Args:
            ticker: Stock ticker symbol
            start_date: Start-Datum als datetime
            end_date: End-Datum als datetime
            interval: Interval-String wie '1min', '5min', '1hour', '1day'
            adjusted: Ob für Splits angepasst werden soll
        
        Returns:
            Liste von OHLC Dictionaries
        """
        # Interval parsen
        multiplier, timespan = self._parse_interval(interval)
        
        # Datums-Format: YYYY-MM-DD
        from_str = start_date.strftime('%Y-%m-%d')
        to_str = end_date.strftime('%Y-%m-%d')
        
        print(f"📊 Lade {ticker} Daten: {from_str} bis {to_str} ({interval})")
        
        response = self.get_aggregate_bars(
            ticker=ticker,
            multiplier=multiplier,
            timespan=timespan,
            from_date=from_str,
            to_date=to_str,
            adjusted=adjusted,
            sort='asc',
            limit=50000  # Maximum für mehr Datenpunkte
        )
        
        results = response.get('results', [])
        
        # Pagination behandeln (falls next_url vorhanden)
        all_results = results.copy()
        next_url = response.get('next_url')
        
        while next_url and len(all_results) < 50000:  # Sicherheitslimit
            print(f"   📄 Lade nächste Seite... (bisher {len(all_results)} Datensätze)")
            try:
                paginated_response = self.session.get(next_url)
                paginated_response.raise_for_status()
                paginated_data = paginated_response.json()
                
                page_results = paginated_data.get('results', [])
                all_results.extend(page_results)
                next_url = paginated_data.get('next_url')
                
                time.sleep(0.2)  # Rate limiting berücksichtigen
                
            except Exception as e:
                print(f"   ⚠️  Pagination Fehler: {e}")
                break
        
        print(f"   ✅ {len(all_results)} Datensätze geladen")
        return all_results
    
    def _parse_interval(self, interval: str) -> tuple:
        """
        Parst Interval-Strings in multiplier und timespan
        
        Examples:
            '1min' -> (1, 'minute')
            '5min' -> (5, 'minute')
            '1hour' -> (1, 'hour')
            '1day' -> (1, 'day')
        """
        interval = interval.lower()
        
        # Mapping für häufige Formate
        mappings = {
            'min': 'minute',
            'minute': 'minute',
            'hour': 'hour',
            'day': 'day',
            'week': 'week',
            'month': 'month',
        }
        
        # Multiplier extrahieren
        multiplier = 1
        timespan = 'day'
        
        for key, value in mappings.items():
            if key in interval:
                # Zahl vor dem Schlüsselwort extrahieren
                number_part = interval.split(key)[0]
                try:
                    multiplier = int(number_part) if number_part else 1
                except ValueError:
                    multiplier = 1
                timespan = value
                break
        
        return multiplier, timespan
    
    def get_multiple_tickers_data(
        self,
        tickers: List[str],
        start_date: datetime,
        end_date: datetime,
        interval: str = '1day'
    ) -> Dict[str, List[Dict]]:
        """
        Lädt Daten für mehrere Ticker
        
        Returns:
            Dictionary: {ticker: [data]}
        """
        results = {}
        
        for i, ticker in enumerate(tickers, 1):
            print(f"\n[{i}/{len(tickers)}] Verarbeite {ticker}...")
            
            data = self.get_historical_data(
                ticker=ticker,
                start_date=start_date,
                end_date=end_date,
                interval=interval
            )
            
            results[ticker] = data
            
            # Rate limiting: Pause zwischen Requests
            if i < len(tickers):
                time.sleep(0.3)
        
        return results
    
    def test_connection(self) -> bool:
        """Testet die API-Verbindung mit einem einfachen Request"""
        try:
            # Teste mit Apple für einen Tag
            test_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            today = datetime.now().strftime('%Y-%m-%d')
            
            response = self.get_aggregate_bars(
                ticker='AAPL',
                multiplier=1,
                timespan='day',
                from_date=test_date,
                to_date=today,
                limit=5
            )
            
            if response.get('count') != 0:
                print("✅ Massive.com API-Verbindung erfolgreich!")
                return True
            else:
                print(f"⚠️  API-Test fehlgeschlagen: {response.get('status')}")
                return False
                
        except Exception as e:
            print(f"❌ API-Test fehlgeschlagen: {e}")
            return False
