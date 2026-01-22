#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 22 21:32:20 2026

@author: twi-dev
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
massive_client.py - Optimierter API Client mit Retry-Logik
"""

import os
import requests
from datetime import datetime
from dotenv import load_dotenv
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)
from requests.exceptions import HTTPError, Timeout, RequestException
import logging
from typing import List, Dict, Optional

from config import settings

logging.basicConfig(
    level=settings.LOG_LEVEL,
    format=settings.LOG_FORMAT
)
logger = logging.getLogger(__name__)


class MassiveClient:
    """
    Optimierter API Client mit Retry-Logik und besserem Error Handling
    """
    
    def __init__(self):
        """Initialisiert den Massive API Client"""
        load_dotenv()
        
        self.api_key = settings.MASSIVE_API_KEY
        self.base_url = settings.MASSIVE_BASE_URL
        self.timeout = settings.API_TIMEOUT
        
        if not self.api_key:
            raise ValueError(
                "MASSIVE_API_KEY nicht gefunden!\n"
                "Bitte in .env Datei setzen: MASSIVE_API_KEY=your_key_here"
            )
        
        logger.info(f"✅ MassiveClient initialisiert (Base: {self.base_url})")
    
    def test_connection(self) -> bool:
        """
        Testet die API-Verbindung
        
        Returns:
            bool: True bei erfolgreicher Verbindung
        """
        logger.info("=" * 60)
        logger.info("🌐 Teste Massive.com API-Verbindung")
        logger.info("=" * 60)
        
        try:
            response = self._make_request(
                f"{self.base_url}/v3/reference/tickers/AAPL"
            )
            
            if response:
                logger.info("✅ Massive.com API-Verbindung erfolgreich!")
                return True
            return False
                
        except Exception as e:
            logger.error(f"❌ Verbindungsfehler: {e}")
            return False
    
    @retry(
        stop=stop_after_attempt(settings.API_MAX_RETRIES),
        wait=wait_exponential(
            multiplier=settings.API_RETRY_DELAY,
            min=1,
            max=10
        ),
        retry=retry_if_exception_type((Timeout, ConnectionError)),
        reraise=True
    )
    def _make_request(
        self, 
        url: str, 
        params: Optional[Dict] = None
    ) -> Optional[Dict]:
        """
        ✅ Macht API-Request mit automatischem Retry bei Timeout/Connection-Errors
        
        Args:
            url: API-Endpunkt
            params: Query-Parameter
            
        Returns:
            dict: API-Response oder None bei Fehler
        """
        if params is None:
            params = {}
        
        params['apiKey'] = self.api_key
        
        try:
            response = requests.get(
                url, 
                params=params, 
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        
        except HTTPError as e:
            if e.response.status_code == 429:
                logger.warning(
                    f"⚠️ Rate Limit erreicht - warte {settings.API_RATE_LIMIT_DELAY}s"
                )
                import time
                time.sleep(settings.API_RATE_LIMIT_DELAY)
                # Retry automatisch durch @retry decorator
                raise
            
            elif e.response.status_code == 401:
                logger.error("❌ Ungültiger API-Key!")
                raise ValueError("Ungültiger API-Key")
            
            elif e.response.status_code == 404:
                logger.warning(f"⚠️ Ressource nicht gefunden: {url}")
                return None
            
            else:
                logger.error(f"❌ HTTP Error {e.response.status_code}: {e.response.text[:200]}")
                return None
        
        except Timeout:
            logger.error(f"❌ API Timeout nach {self.timeout}s")
            raise  # Retry durch @retry decorator
        
        except ConnectionError as e:
            logger.error(f"❌ Verbindungsfehler: {e}")
            raise  # Retry durch @retry decorator
        
        except RequestException as e:
            logger.error(f"❌ Netzwerkfehler: {e}")
            return None
        
        except Exception as e:
            logger.error(f"❌ Unerwarteter Fehler: {e}", exc_info=True)
            return None
    
    def get_ohlcv(
        self,
        symbol: str,
        interval: str = settings.DEFAULT_INTERVAL,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None
    ) -> List[Dict]:
        """
        Lädt historische OHLCV-Daten (Polygon.io API)
        
        Args:
            symbol: Stock Symbol (z.B. 'AAPL')
            interval: '1min', '5min', '15min', '30min', '1hour', '4hour', '1day', '1week', '1month'
            start: Start-Datum
            end: End-Datum
        
        Returns:
            List[Dict]: OHLCV-Daten
        """
        # Intervall-Mapping
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
            logger.warning(f"⚠️ Unbekanntes Intervall {interval}, verwende 1day")
            interval = '1day'
        
        multiplier, timespan = interval_map[interval]
        
        # Datum formatieren
        if not start:
            start = datetime.now().replace(day=1)
        if not end:
            end = datetime.now()
        
        from_date = start.strftime('%Y-%m-%d')
        to_date = end.strftime('%Y-%m-%d')
        
        url = f"{self.base_url}/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_date}/{to_date}"
        
        params = {
            'adjusted': 'true',
            'sort': 'asc',
            'limit': settings.MAX_DATA_POINTS
        }
        
        logger.info(f"📊 API Request: {symbol} ({interval}) {from_date} → {to_date}")
        
        data = self._make_request(url, params)
        
        if not data:
            return []
        
        results = data.get('results', [])
        
        if results:
            ohlcv_data = []
            for bar in results:
                ohlcv_data.append({
                    'time': bar.get('t'),
                    'open': bar.get('o'),
                    'high': bar.get('h'),
                    'low': bar.get('l'),
                    'close': bar.get('c'),
                    'volume': bar.get('v')
                })
            
            logger.info(f"✅ {len(ohlcv_data)} Datenpunkte geladen")
            return ohlcv_data
        else:
            logger.warning(f"⚠️ Keine Daten für {symbol} im Zeitraum {from_date} - {to_date}")
            return []
    
    def get_historical_data(
        self,
        ticker: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = settings.DEFAULT_INTERVAL
    ) -> List[Dict]:
        """Alias für get_ohlcv() (für Kompatibilität)"""
        return self.get_ohlcv(ticker, interval, start_date, end_date)
    
    def get_all_tickers(
        self,
        asset_class: str = 'stocks',
        active: bool = True
    ) -> List[Dict]:
        """
        Lädt alle verfügbaren Ticker
        
        Args:
            asset_class: 'stocks', 'crypto', 'forex', 'indices'
            active: Nur aktive Ticker
        
        Returns:
            List[Dict]: Liste aller Ticker
        """
        url = f"{self.base_url}/v3/reference/tickers"
        
        params = {
            'type': 'CS',
            'market': asset_class,
            'active': 'true' if active else 'false',
            'order': 'asc',
            'limit': 1000,
            'sort': 'ticker'
        }
        
        logger.info(f"📥 Lade alle {asset_class} Ticker...")
        
        data = self._make_request(url, params)
        
        if data:
            results = data.get('results', [])
            logger.info(f"✅ {len(results)} Ticker geladen")
            return results
        
        return []
    
    def get_ticker_details(self, ticker: str) -> Dict:
        """
        Lädt Details zu einem Ticker
        
        Args:
            ticker: Stock Symbol
        
        Returns:
            Dict: Ticker-Details
        """
        url = f"{self.base_url}/v3/reference/tickers/{ticker}"
        
        data = self._make_request(url)
        
        if data:
            return data.get('results', {})
        
        return {}
    
    def get_custom_bars(
        self,
        symbol: str,
        limit: int = 5000,
        timespan: str = 'minute',
        multiplier: int = 1
    ) -> List[Dict]:
        """
        Lädt Custom Aggregate Bars (für initiales Laden von Streaming-Daten)
        
        Args:
            symbol: Ticker-Symbol
            limit: Anzahl Bars (max 50000)
            timespan: 'minute', 'hour', 'day', 'week', 'month'
            multiplier: Multiplikator (z.B. 5 für 5-Minuten-Bars)
        
        Returns:
            List[Dict]: OHLCV-Daten
        """
        from datetime import datetime, timedelta
        
        # Zeitraum berechnen (genug für limit Bars)
        end_date = datetime.now()
        
        # Geschätzter Zeitraum basierend auf timespan
        if timespan == 'minute':
            days_back = (limit * multiplier) // (60 * 24) + 1  # Minuten -> Tage
        elif timespan == 'hour':
            days_back = (limit * multiplier) // 24 + 1
        else:
            days_back = limit + 1
        
        start_date = end_date - timedelta(days=min(days_back, 730))  # Max 2 Jahre
        
        from_date = start_date.strftime('%Y-%m-%d')
        to_date = end_date.strftime('%Y-%m-%d')
        
        url = f"{self.base_url}/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_date}/{to_date}"
        
        params = {
            'adjusted': 'true',
            'sort': 'desc',  # Neueste zuerst
            'limit': limit
        }
        
        logger.info(f"📊 Custom Bars: {symbol} - {limit} x {multiplier}{timespan}")
        
        data = self._make_request(url, params)
        
        if not data:
            return []
        
        results = data.get('results', [])
        
        if results:
            ohlcv_data = []
            for bar in results:
                ohlcv_data.append({
                    'time': bar.get('t'),
                    'open': bar.get('o'),
                    'high': bar.get('h'),
                    'low': bar.get('l'),
                    'close': bar.get('c'),
                    'volume': bar.get('v')
                })
            
            logger.info(f"✅ {len(ohlcv_data)} Custom Bars geladen")
            return ohlcv_data
        
        return []
