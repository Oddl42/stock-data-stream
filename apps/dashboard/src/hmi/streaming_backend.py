#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 22 22:48:05 2026

@author: twi-dev
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
streaming_backend.py – WebSocket Streaming Backend für Echtzeit-Daten
"""

import websocket
import json
import threading
import logging
from datetime import datetime
from typing import Callable, List, Dict
import pandas as pd
from sqlalchemy import text

from apps.data_ingestion.src.database import engine
from config import settings

logging.basicConfig(
    level=settings.LOG_LEVEL,
    format=settings.LOG_FORMAT
)
logger = logging.getLogger("StreamingBackend")


class StreamingBackend:
    """
    WebSocket-basiertes Streaming für Echtzeit-Kursdaten
    """
    
    def __init__(self):
        self.ws = None
        self.is_streaming = False
        self.subscribed_tickers = []
        self.data_callback = None
        self.status_callback = None
        self.ws_thread = None
        
        # WebSocket URL für Polygon.io
        self.ws_url = f"wss://socket.polygon.io/stocks"
        
        logger.info("✅ StreamingBackend initialisiert")
    
    def set_callbacks(self, data_callback: Callable = None, status_callback: Callable = None):
        """
        Setzt Callback-Funktionen für Daten und Status-Updates
        
        Args:
            data_callback: Funktion(ticker, data_dict) für neue Daten
            status_callback: Funktion(message) für Status-Updates
        """
        self.data_callback = data_callback
        self.status_callback = status_callback
    
    def _send_status(self, message: str):
        """Sendet Status-Update an UI"""
        logger.info(message)
        if self.status_callback:
            self.status_callback(message)
    
    def start_streaming(self, tickers: List[str]):
        """
        Startet WebSocket-Streaming für die angegebenen Ticker
        
        Args:
            tickers: Liste von Ticker-Symbolen (z.B. ['AAPL', 'MSFT'])
        """
        if self.is_streaming:
            self._send_status("⚠️ Streaming läuft bereits!")
            return False
        
        if not tickers:
            self._send_status("❌ Keine Ticker ausgewählt!")
            return False
        
        self.subscribed_tickers = tickers
        self.is_streaming = True
        
        # WebSocket in separatem Thread starten
        self.ws_thread = threading.Thread(target=self._run_websocket, daemon=True)
        self.ws_thread.start()
        
        self._send_status(f"🚀 Streaming gestartet für: {', '.join(tickers)}")
        return True
    
    def stop_streaming(self):
        """Stoppt das WebSocket-Streaming"""
        if not self.is_streaming:
            self._send_status("⚠️ Streaming läuft nicht!")
            return False
        
        self.is_streaming = False
        
        if self.ws:
            try:
                # Unsubscribe von allen Tickern
                for ticker in self.subscribed_tickers:
                    self.ws.send(json.dumps({
                        "action": "unsubscribe",
                        "params": f"AM.{ticker}"
                    }))
                
                self.ws.close()
            except Exception as e:
                logger.error(f"Fehler beim Schließen: {e}")
        
        self.subscribed_tickers = []
        self._send_status("🛑 Streaming gestoppt")
        return True
    
    def _run_websocket(self):
        """Hauptschleife für WebSocket-Verbindung"""
        try:
            self.ws = websocket.WebSocketApp(
                self.ws_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close
            )
            
            # Blocking call (läuft in eigenem Thread)
            self.ws.run_forever()
        
        except Exception as e:
            logger.error(f"❌ WebSocket Fehler: {e}")
            self.is_streaming = False
            self._send_status(f"❌ WebSocket-Fehler: {e}")
    
    def _on_open(self, ws):
        """Callback: WebSocket-Verbindung hergestellt"""
        logger.info("🔗 WebSocket verbunden")
        
        try:
            # 1. Authentifizierung
            auth_message = {
                "action": "auth",
                "params": settings.MASSIVE_API_KEY
            }
            ws.send(json.dumps(auth_message))
            logger.info("🔐 Authentifizierung gesendet")
            
            # 2. Subscribe zu Aggregates (1-Minuten-Bars)
            for ticker in self.subscribed_tickers:
                subscribe_message = {
                    "action": "subscribe",
                    "params": f"AM.{ticker}"  # AM = Aggregate per Minute
                }
                ws.send(json.dumps(subscribe_message))
                logger.info(f"📊 Subscribed zu {ticker}")
            
            self._send_status(f"✅ Verbunden und subscribed zu {len(self.subscribed_tickers)} Tickern")
        
        except Exception as e:
            logger.error(f"❌ Fehler bei on_open: {e}")
            self._send_status(f"❌ Verbindungsfehler: {e}")
    
    def _on_message(self, ws, message):
        """
        Callback: Neue Nachricht empfangen
        
        Polygon.io Format für Aggregates:
        [{
            "ev": "AM",          # Event Type (Aggregate Minute)
            "sym": "AAPL",       # Symbol
            "v": 1000,           # Volume
            "av": 50000,         # Accumulated Volume
            "op": 150.0,         # Open
            "vw": 150.5,         # Volume Weighted Average
            "o": 150.0,          # Open (current bar)
            "c": 151.0,          # Close (current bar)
            "h": 151.5,          # High
            "l": 149.5,          # Low
            "a": 151.2,          # Today's official open
            "z": 100,            # Average trade size
            "s": 1640000000000,  # Start time (ms)
            "e": 1640000060000   # End time (ms)
        }]
        """
        try:
            data = json.loads(message)
            
            # Polygon.io sendet Liste von Events
            if isinstance(data, list):
                for event in data:
                    event_type = event.get('ev')
                    
                    # Status-Nachrichten
                    if event_type == 'status':
                        status = event.get('status')
                        msg = event.get('message', '')
                        logger.info(f"Status: {status} - {msg}")
                        
                        if status == 'auth_success':
                            self._send_status("✅ Authentifizierung erfolgreich")
                        elif status == 'success':
                            self._send_status(f"✅ {msg}")
                    
                    # Aggregate Minute Data
                    elif event_type == 'AM':
                        self._process_aggregate(event)
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON Decode Fehler: {e}")
        except Exception as e:
            logger.error(f"❌ Fehler bei Nachrichtenverarbeitung: {e}")
    
    def _process_aggregate(self, event: Dict):
        """
        Verarbeitet einen Aggregate-Event und speichert in DB
        
        Args:
            event: Aggregate-Event von Polygon.io
        """
        try:
            ticker = event.get('sym')
            
            # Daten extrahieren
            data = {
                'time': datetime.fromtimestamp(event.get('s') / 1000),  # Start time
                'symbol': ticker,
                'interval': '1min',
                'open': float(event.get('o', 0)),
                'high': float(event.get('h', 0)),
                'low': float(event.get('l', 0)),
                'close': float(event.get('c', 0)),
                'volume': int(event.get('v', 0))
            }
            
            # In Datenbank speichern
            self._save_to_db(data)
            
            # UI-Callback aufrufen
            if self.data_callback:
                self.data_callback(ticker, data)
            
            logger.debug(f"💾 {ticker}: {data['close']} @ {data['time']}")
        
        except Exception as e:
            logger.error(f"❌ Fehler bei _process_aggregate: {e}")
    
    def _save_to_db(self, data: Dict):
        """
        Speichert einen einzelnen Datenpunkt in der Datenbank
        
        Args:
            data: Dict mit OHLCV-Daten
        """
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO stock_ohlcv (time, symbol, interval, open, high, low, close, volume)
                    VALUES (:time, :symbol, :interval, :open, :high, :low, :close, :volume)
                    ON CONFLICT (time, symbol, interval) 
                    DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume
                """), data)
        
        except Exception as e:
            logger.error(f"❌ DB-Fehler: {e}")
    
    def _on_error(self, ws, error):
        """Callback: WebSocket-Fehler"""
        logger.error(f"❌ WebSocket Error: {error}")
        self._send_status(f"❌ Fehler: {error}")
    
    def _on_close(self, ws, close_status_code, close_msg):
        """Callback: WebSocket geschlossen"""
        logger.info(f"🔌 WebSocket geschlossen: {close_status_code} - {close_msg}")
        self.is_streaming = False
        self._send_status("🔌 Verbindung geschlossen")
    
    def load_initial_data(self, ticker: str, limit: int = 5000) -> pd.DataFrame:
        """
        Lädt die letzten N Datenpunkte für einen Ticker (1-Minuten-Bars)
        
        Args:
            ticker: Ticker-Symbol
            limit: Anzahl Datenpunkte (max 50000)
        
        Returns:
            DataFrame mit historischen Daten
        """
        try:
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        time, symbol, open, high, low, close, volume
                    FROM stock_ohlcv
                    WHERE symbol = :ticker AND interval = '1min'
                    ORDER BY time DESC
                    LIMIT :limit
                """), {"ticker": ticker, "limit": limit})
                
                rows = result.fetchall()
                
                if not rows:
                    logger.warning(f"⚠️ Keine Daten für {ticker} in DB")
                    return pd.DataFrame()
                
                df = pd.DataFrame(rows, columns=result.keys())
                df = df.sort_values('time')  # Chronologisch sortieren
                
                logger.info(f"✅ {len(df)} Datenpunkte für {ticker} geladen")
                return df
        
        except Exception as e:
            logger.error(f"❌ Fehler beim Laden: {e}")
            return pd.DataFrame()
    
    def get_streaming_status(self) -> Dict:
        """
        Gibt aktuellen Streaming-Status zurück
        
        Returns:
            dict: Status-Informationen
        """
        return {
            'is_streaming': self.is_streaming,
            'subscribed_tickers': self.subscribed_tickers,
            'ticker_count': len(self.subscribed_tickers)
        }
