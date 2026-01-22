#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
controller.py – Controller für das Stock Data Dashboard (MVC-Architektur)
VOLLSTÄNDIG mit Streaming-Funktionalität
"""

from ..hmi.ui import DashboardUI
from ..hmi.backend import StockBackend
import panel as pn
import pandas as pd
from datetime import datetime, timedelta
import logging

# Streaming Backend importieren
from ..hmi.streaming_backend import StreamingBackend

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DashboardController")


class StockDashboardController:
    """
    Controller-Klasse mit ALLEN Funktionen inkl. WebSocket-Streaming
    """

    def __init__(self, ui: DashboardUI = None, backend: StockBackend = None):
        self.ui = ui if ui else DashboardUI()
        self.backend = backend if backend else StockBackend()
        
        # ✅ Streaming Backend initialisieren
        self.streaming = StreamingBackend()
        self.streaming.set_callbacks(
            data_callback=self._on_streaming_data,
            status_callback=self._on_streaming_status
        )
        
        # Cache für Streaming-Daten (für Performance)
        self.streaming_cache = {}

        # Initialisierung
        self._init_symbol_options()
        self._create_streaming_widgets()  # ✅ Streaming-Widgets erstellen
        self._register_callbacks()
        self._update_selected_tickers_table()

    def _init_symbol_options(self):
        """Setzt die verfügbaren Symbole im UI-Widget."""
        symbols = self.backend.get_available_symbols()
        self.ui.widgets['symbol_select'].options = symbols
        if symbols and symbols[0] != 'Keine Daten verfügbar':
            self.ui.widgets['symbol_select'].value = symbols[0]

    # ========================================
    # STREAMING-WIDGETS ERSTELLEN
    # ========================================
    
    def _create_streaming_widgets(self):
        """Erstellt alle Widgets für das Streaming-Tab"""
        
        self.streaming_widgets = {}
        
        # === Ticker-Eingabe ===
        self.streaming_widgets['ticker_input'] = pn.widgets.TextInput(
            name='📊 Ticker-Symbol',
            placeholder='z.B. AAPL, MSFT, GOOGL',
            width=300
        )
        
        self.streaming_widgets['add_ticker_btn'] = pn.widgets.Button(
            name='➕ Hinzufügen',
            button_type='success',
            width=150
        )
        
        # === Ticker-Liste (Tabelle) ===
        self.streaming_widgets['ticker_list'] = pn.widgets.Tabulator(
            pd.DataFrame(columns=['ticker', 'status', 'last_price', 'last_update']),
            theme='default',
            layout='fit_columns',
            height=200,
            selectable='checkbox',
            show_index=False
        )
        
        self.streaming_widgets['remove_ticker_btn'] = pn.widgets.Button(
            name='❌ Entfernen',
            button_type='danger',
            width=150
        )
        
        # === Start/Stop Controls ===
        self.streaming_widgets['start_btn'] = pn.widgets.Button(
            name='▶️ Start Streaming',
            button_type='success',
            width=200,
            height=50
        )
        
        self.streaming_widgets['stop_btn'] = pn.widgets.Button(
            name='⏹️ Stop Streaming',
            button_type='danger',
            width=200,
            height=50,
            disabled=True
        )
        
        # === Status-Anzeige ===
        self.streaming_widgets['status'] = pn.pane.Markdown(
            "### 📡 Streaming Status\n\n⚪ Bereit zum Starten",
            styles={'padding': '15px', 'background': '#f5f5f5', 'border-radius': '5px'}
        )
        
        # === Streaming-Statistiken ===
        self.streaming_widgets['stats'] = pn.pane.Markdown(
            "### 📊 Statistiken\n\n*Keine Daten*",
            styles={'padding': '15px'}
        )
        
        # === Live-Chart Container ===
        self.streaming_widgets['chart'] = pn.Column(
            pn.pane.Markdown("### 📈 Live-Chart\n\nWähle Ticker und starte Streaming"),
            sizing_mode='stretch_width',
            height=600
        )
        
        # === Chart-Ticker-Auswahl ===
        self.streaming_widgets['chart_ticker_select'] = pn.widgets.Select(
            name='📊 Chart-Ticker',
            options=['Keine Ticker'],
            width=200
        )
        
        # === Chart-Fenster (Datenpunkte) ===
        self.streaming_widgets['chart_window'] = pn.widgets.IntSlider(
            name='Chart-Fenster (Datenpunkte)',
            start=50,
            end=5000,
            value=500,
            step=50,
            width=300
        )
        
        # === Loading Indicator ===
        self.streaming_widgets['loading'] = pn.indicators.LoadingSpinner(
            value=False,
            width=30,
            height=30
        )
        
        # Streaming-Tab zum UI hinzufügen
        self._add_streaming_tab_to_ui()

    def _add_streaming_tab_to_ui(self):
        """Fügt das Streaming-Tab zum UI-Layout hinzu"""
        
        streaming_tab = pn.Column(
            pn.pane.Markdown("# 📡 Echtzeit-Streaming (WebSocket)"),
            
            # === Ticker-Verwaltung ===
            pn.Card(
                pn.Row(
                    self.streaming_widgets['ticker_input'],
                    self.streaming_widgets['add_ticker_btn'],
                    self.streaming_widgets['loading']
                ),
                self.streaming_widgets['ticker_list'],
                self.streaming_widgets['remove_ticker_btn'],
                title="📊 Streaming-Ticker verwalten",
                collapsed=False
            ),
            
            # === Streaming Controls ===
            pn.Card(
                pn.Row(
                    self.streaming_widgets['start_btn'],
                    self.streaming_widgets['stop_btn']
                ),
                self.streaming_widgets['status'],
                title="🎛️ Streaming-Kontrolle",
                collapsed=False
            ),
            
            # === Live-Chart ===
            pn.Card(
                pn.Row(
                    self.streaming_widgets['chart_ticker_select'],
                    self.streaming_widgets['chart_window']
                ),
                self.streaming_widgets['chart'],
                self.streaming_widgets['stats'],
                title="📈 Live-Chart",
                collapsed=False
            ),
            
            sizing_mode='stretch_width'
        )
        
        # Tabs im UI erweitern
        if hasattr(self.ui, 'layout') and hasattr(self.ui.layout, 'main'):
            # Existierendes Tabs-Objekt erweitern
            existing_tabs = self.ui.layout.main[0]
            if isinstance(existing_tabs, pn.Tabs):
                existing_tabs.append(('📡 Streaming', streaming_tab))

    # ========================================
    # CALLBACK-REGISTRIERUNG
    # ========================================

    def _register_callbacks(self):
        """Registriert ALLE Event-Handler für UI-Widgets."""
        
        # === CHART-TAB CALLBACKS ===
        self.ui.widgets['symbol_select'].param.watch(self._on_symbol_change, 'value')
        self.ui.widgets['start_date'].param.watch(self._on_date_change, 'value')
        self.ui.widgets['end_date'].param.watch(self._on_date_change, 'value')
        
        # Intervall-Callbacks (für alle 3 Zeilen)
        self.ui.widgets['interval_row1'].param.watch(self._on_interval_change, 'value')
        self.ui.widgets['interval_row2'].param.watch(self._on_interval_change, 'value')
        self.ui.widgets['interval_row3'].param.watch(self._on_interval_change, 'value')
        
        self.ui.widgets['chart_type'].param.watch(self._on_chart_type_change, 'value')
        self.ui.widgets['indicators'].param.watch(self._on_indicators_change, 'value')
        self.ui.widgets['refresh_btn'].on_click(self._on_refresh_click)
        
        # Schnellauswahl für Zeitraum
        def quick_range_callback(event):
            if event.new:
                days = event.new
                self.ui.widgets['end_date'].value = datetime.now().date()
                self.ui.widgets['start_date'].value = (datetime.now() - timedelta(days=days)).date()
        
        self.ui.widgets['quick_range'].param.watch(quick_range_callback, 'value')
        
        # === TICKER-MANAGEMENT CALLBACKS ===
        self.ui.ticker_widgets['load_tickers_btn'].on_click(self._on_load_tickers)
        self.ui.ticker_widgets['add_selected_btn'].on_click(self._on_add_selected_tickers)
        self.ui.ticker_widgets['remove_selected_btn'].on_click(self._on_remove_selected_tickers)
        self.ui.ticker_widgets['clear_all_btn'].on_click(self._on_clear_all_tickers)
        self.ui.ticker_widgets['load_data_btn'].on_click(self._on_load_data_for_selected)
        self.ui.ticker_widgets['bulk_update_btn'].on_click(self._on_bulk_update_all_tickers)
        
        # === ✅ STREAMING CALLBACKS ===
        self.streaming_widgets['add_ticker_btn'].on_click(self._on_add_streaming_ticker)
        self.streaming_widgets['remove_ticker_btn'].on_click(self._on_remove_streaming_ticker)
        self.streaming_widgets['start_btn'].on_click(self._on_start_streaming)
        self.streaming_widgets['stop_btn'].on_click(self._on_stop_streaming)
        self.streaming_widgets['chart_ticker_select'].param.watch(self._on_streaming_chart_ticker_change, 'value')
        self.streaming_widgets['chart_window'].param.watch(self._update_streaming_chart, 'value')

    # ========================================
    # CHART EVENT-HANDLER
    # ========================================

    def _on_symbol_change(self, event):
        """Wird aufgerufen, wenn das Symbol gewechselt wird."""
        logger.info(f"🔄 Update getriggert durch: {event.name} = {event.new}")
        self._update_chart()

    def _on_date_change(self, event):
        """Wird aufgerufen, wenn Start- oder End-Datum geändert wird."""
        logger.info(f"🔄 Update getriggert durch: {event.name} = {event.new}")
        self._update_chart()

    def _on_interval_change(self, event):
        """Wird aufgerufen, wenn das Intervall geändert wird."""
        logger.info(f"🔄 Update getriggert durch: {event.name} = {event.new}")
        self._update_chart()

    def _on_chart_type_change(self, event):
        """Wird aufgerufen, wenn der Chart-Typ geändert wird."""
        logger.info(f"🔄 Update getriggert durch: {event.name} = {event.new}")
        self._update_chart()

    def _on_indicators_change(self, event):
        """Wird aufgerufen, wenn Indikatoren geändert werden."""
        logger.info(f"🔄 Update getriggert durch: {event.name} = {event.new}")
        self._update_chart()

    def _on_refresh_click(self, event=None):
        """Wird aufgerufen, wenn der Refresh-Button geklickt wird."""
        logger.info("🔄 Manuelles Update")
        self._update_chart()

    # ========================================
    # TICKER-MANAGEMENT EVENT-HANDLER
    # ========================================

    def _on_load_tickers(self, event=None):
        """Lädt alle Ticker von der API."""
        asset_class = self.ui.ticker_widgets['asset_class'].value
        self.ui.ticker_widgets['ticker_status'].value = True
        self.ui.ticker_widgets['ticker_info'].object = f"### 📥 Lade {asset_class} Ticker..."
        
        df = self.backend.load_all_tickers(asset_class)
        
        if not df.empty:
            self.ui.ticker_widgets['all_tickers_table'].value = df
            self.ui.ticker_widgets['ticker_info'].object = f"### ✅ {len(df)} Ticker geladen"
        else:
            self.ui.ticker_widgets['ticker_info'].object = "### ⚠️ Keine Ticker gefunden"
        
        self.ui.ticker_widgets['ticker_status'].value = False

    def _on_add_selected_tickers(self, event=None):
        """Fügt ausgewählte Ticker zur Datenbank hinzu UND lädt Daten."""
        selected_indices = self.ui.ticker_widgets['all_tickers_table'].selection
        
        if not selected_indices:
            self.ui.ticker_widgets['ticker_info'].object = "### ⚠️ Keine Ticker ausgewählt"
            return
        
        df = self.ui.ticker_widgets['all_tickers_table'].value
        added_tickers = []
        
        # Ticker zur DB hinzufügen
        for idx in selected_indices:
            row = df.iloc[idx]
            ticker = row.get('ticker', '')
            success = self.backend.ticker_db.add_ticker(
                ticker=ticker,
                name=row.get('name', ''),
                primary_exchange=row.get('primary_exchange', ''),
                market=row.get('market', '')
            )
            if success:
                added_tickers.append(ticker)
        
        if added_tickers:
            self.ui.ticker_widgets['ticker_info'].object = f"### ✅ {len(added_tickers)} Ticker hinzugefügt\n\n**Lade jetzt automatisch Daten...**"
            
            # Automatisch Daten laden
            self.ui.ticker_widgets['progress'].visible = True
            self.ui.ticker_widgets['progress'].value = 0
            
            days = self.ui.ticker_widgets['data_load_days'].value
            interval = self.ui.ticker_widgets['data_load_interval'].value
            
            def progress_callback(ticker, success, progress):
                self.ui.ticker_widgets['progress'].value = int(progress * 100)
                if success is not None:
                    status = "✅" if success else "❌"
                    self.ui.ticker_widgets['status'].object = f"{status} {ticker}"
            
            stats = self.backend.data_loader.load_multiple_tickers(
                added_tickers, 
                days=days,
                interval=interval,
                callback=progress_callback
            )
            
            self.ui.ticker_widgets['progress'].visible = False
            
            result_text = f"""
### ✅ Ticker hinzugefügt und Daten geladen

**Statistik:**
- Hinzugefügt: {len(added_tickers)}
- Erfolgreich geladen: {stats['success']}
- Fehlgeschlagen: {stats['failed']}
"""
            
            if stats['failed_tickers']:
                result_text += f"\n**Fehlgeschlagen:** {', '.join(stats['failed_tickers'])}"
            
            self.ui.ticker_widgets['ticker_info'].object = result_text
        
        self._update_selected_tickers_table()
        self.backend.setup_data()
        self._init_symbol_options()

    def _on_remove_selected_tickers(self, event=None):
        """Entfernt ausgewählte Ticker aus der Datenbank."""
        selected_indices = self.ui.ticker_widgets['selected_tickers_table'].selection
        
        if not selected_indices:
            self.ui.ticker_widgets['ticker_info'].object = "### ⚠️ Keine Ticker ausgewählt\n\nWähle Ticker in der Tabelle aus."
            return
        
        df = self.ui.ticker_widgets['selected_tickers_table'].value
        tickers = [df.iloc[idx]['ticker'] for idx in selected_indices]
        
        for ticker in tickers:
            self.backend.remove_selected_ticker(ticker)
        
        self.ui.ticker_widgets['ticker_info'].object = f"### ✅ {len(tickers)} Ticker entfernt"
        self._update_selected_tickers_table()
        self._init_symbol_options()

    def _on_clear_all_tickers(self, event=None):
        """Löscht alle ausgewählten Ticker."""
        if self.backend.clear_all_tickers():
            self.ui.ticker_widgets['ticker_info'].object = "### ✅ Alle Ticker gelöscht"
            self._update_selected_tickers_table()
            self._init_symbol_options()

    def _on_load_data_for_selected(self, event=None):
        """Lädt Daten für ausgewählte Ticker aus der Tabelle."""
        selected_indices = self.ui.ticker_widgets['selected_tickers_table'].selection
        
        if not selected_indices:
            self.ui.ticker_widgets['ticker_info'].object = "### ⚠️ Keine Ticker ausgewählt\n\nWähle Ticker in der Tabelle aus."
            return
        
        df = self.ui.ticker_widgets['selected_tickers_table'].value
        tickers = [df.iloc[idx]['ticker'] for idx in selected_indices]
        
        self.ui.ticker_widgets['ticker_info'].object = f"### 📥 Lade Daten für {len(tickers)} Ticker..."
        self.ui.ticker_widgets['progress'].visible = True
        self.ui.ticker_widgets['progress'].value = 0
        
        days = self.ui.ticker_widgets['data_load_days'].value
        interval = self.ui.ticker_widgets['data_load_interval'].value
        
        def progress_callback(ticker, success, progress):
            self.ui.ticker_widgets['progress'].value = int(progress * 100)
            if success is not None:
                status = "✅" if success else "❌"
                self.ui.ticker_widgets['status'].object = f"{status} {ticker}"
        
        stats = self.backend.data_loader.load_multiple_tickers(
            tickers,
            days=days,
            interval=interval,
            callback=progress_callback
        )
        
        self.ui.ticker_widgets['progress'].visible = False
        
        self.ui.ticker_widgets['ticker_info'].object = f"""
### ✅ Daten geladen

**Erfolgreich:** {stats['success']}/{stats['total']}
**Fehlgeschlagen:** {stats['failed']}

{f"**Fehler bei:** {', '.join(stats['failed_tickers'])}" if stats['failed_tickers'] else ""}
"""

    def _on_bulk_update_all_tickers(self, event=None):
        """Aktualisiert Daten für alle ausgewählten Ticker."""
        selected = self.backend.get_selected_tickers()
        
        if not selected:
            self.ui.ticker_widgets['ticker_info'].object = "### ⚠️ Keine Ticker ausgewählt\n\nFüge zuerst Ticker hinzu."
            return
        
        tickers = [t['ticker'] for t in selected]
        
        self.ui.ticker_widgets['ticker_info'].object = f"### 🔄 Prüfe {len(tickers)} Ticker auf Updates..."
        self.ui.ticker_widgets['progress'].visible = True
        self.ui.ticker_widgets['progress'].value = 0
        
        # Prüfe welche Ticker Updates brauchen
        tickers_to_update = []
        for ticker in tickers:
            info = self.backend.data_loader.check_data_availability(ticker)
            if info['needs_update'] or not info['has_data']:
                tickers_to_update.append(ticker)
        
        if not tickers_to_update:
            self.ui.ticker_widgets['ticker_info'].object = "### ✅ Alle Ticker sind aktuell\n\nKeine Updates notwendig."
            self.ui.ticker_widgets['progress'].visible = False
            return
        
        self.ui.ticker_widgets['ticker_info'].object = f"### 🔄 Aktualisiere {len(tickers_to_update)} von {len(tickers)} Tickern..."
        
        days = 30  # Nur letzte 30 Tage updaten
        interval = self.ui.ticker_widgets['data_load_interval'].value
        
        def progress_callback(ticker, success, progress):
            self.ui.ticker_widgets['progress'].value = int(progress * 100)
            if success is not None:
                status = "✅" if success else "❌"
                self.ui.ticker_widgets['status'].object = f"{status} {ticker}"
        
        stats = self.backend.data_loader.load_multiple_tickers(
            tickers_to_update,
            days=days,
            interval=interval,
            callback=progress_callback
        )
        
        self.ui.ticker_widgets['progress'].visible = False
        
        self.ui.ticker_widgets['ticker_info'].object = f"""
### ✅ Update abgeschlossen

**Geprüft:** {len(tickers)} Ticker
**Aktualisiert:** {len(tickers_to_update)} Ticker
**Erfolgreich:** {stats['success']}
**Fehlgeschlagen:** {stats['failed']}
**Übersprungen:** {len(tickers) - len(tickers_to_update)} (bereits aktuell)

{f"**Fehler bei:** {', '.join(stats['failed_tickers'])}" if stats['failed_tickers'] else ""}
"""

    # ========================================
    # ✅ STREAMING EVENT-HANDLER
    # ========================================

    def _on_add_streaming_ticker(self, event=None):
        """Fügt Ticker zur Streaming-Liste hinzu und lädt initiale Daten"""
        ticker = self.streaming_widgets['ticker_input'].value.strip().upper()
        
        if not ticker:
            return
        
        # Prüfe ob bereits vorhanden
        df = self.streaming_widgets['ticker_list'].value
        if ticker in df['ticker'].values:
            self.streaming_widgets['status'].object = f"### ⚠️ {ticker} bereits vorhanden"
            return
        
        # Loading anzeigen
        self.streaming_widgets['loading'].value = True
        self.streaming_widgets['status'].object = f"### 📥 Lade initiale Daten für {ticker}..."
        
        try:
            # Von API laden (5000 x 1min Bars)
            data = self.backend.massive_client.get_custom_bars(
                symbol=ticker,
                limit=5000,
                timespan='minute',
                multiplier=1
            )
            
            if data:
                # In DB speichern
                df_save = pd.DataFrame(data)
                df_save['symbol'] = ticker
                df_save['interval'] = '1min'
                df_save['time'] = pd.to_datetime(df_save['time'], unit='ms')
                
                self.backend.data_loader._bulk_save_to_db(df_save)
                
                # Zur Ticker-Liste hinzufügen
                new_row = pd.DataFrame([{
                    'ticker': ticker,
                    'status': '✅ Bereit',
                    'last_price': df_save['close'].iloc[-1] if len(df_save) > 0 else 0,
                    'last_update': datetime.now().strftime('%H:%M:%S')
                }])
                df = pd.concat([df, new_row], ignore_index=True)
                self.streaming_widgets['ticker_list'].value = df
                
                # Chart-Select aktualisieren
                options = df['ticker'].tolist()
                self.streaming_widgets['chart_ticker_select'].options = options
                if len(options) == 1:
                    self.streaming_widgets['chart_ticker_select'].value = options[0]
                    self._update_streaming_chart()
                
                self.streaming_widgets['status'].object = f"### ✅ {ticker} hinzugefügt ({len(data)} Datenpunkte geladen)"
                self.streaming_widgets['ticker_input'].value = ""
            else:
                self.streaming_widgets['status'].object = f"### ❌ Keine Daten für {ticker}"
        
        except Exception as e:
            logger.error(f"Fehler beim Hinzufügen von {ticker}: {e}")
            self.streaming_widgets['status'].object = f"### ❌ Fehler: {e}"
        
        finally:
            self.streaming_widgets['loading'].value = False

    def _on_remove_streaming_ticker(self, event=None):
        """Entfernt ausgewählte Ticker aus Streaming-Liste"""
        selected = self.streaming_widgets['ticker_list'].selection
        if not selected:
            return
        
        df = self.streaming_widgets['ticker_list'].value
        df = df.drop(df.index[selected])
        self.streaming_widgets['ticker_list'].value = df
        
        # Chart-Select aktualisieren
        if df.empty:
            self.streaming_widgets['chart_ticker_select'].options = ['Keine Ticker']
            self.streaming_widgets['chart'].objects = [
                pn.pane.Markdown("### 📈 Live-Chart\n\nWähle Ticker und starte Streaming")
            ]
        else:
            self.streaming_widgets['chart_ticker_select'].options = df['ticker'].tolist()
        
        self.streaming_widgets['status'].object = f"### ✅ {len(selected)} Ticker entfernt"

    def _on_start_streaming(self, event=None):
        """Startet WebSocket-Streaming für alle Ticker in der Liste"""
        df = self.streaming_widgets['ticker_list'].value
        
        if df.empty:
            self.streaming_widgets['status'].object = "### ⚠️ Keine Ticker ausgewählt"
            return
        
        tickers = df['ticker'].tolist()
        
        # Streaming starten
        success = self.streaming.start_streaming(tickers)
        
        if success:
            self.streaming_widgets['start_btn'].disabled = True
            self.streaming_widgets['stop_btn'].disabled = False
            
            # Status in Tabelle aktualisieren
            df['status'] = '🔴 Live'
            self.streaming_widgets['ticker_list'].value = df

    def _on_stop_streaming(self, event=None):
        """Stoppt WebSocket-Streaming"""
        self.streaming.stop_streaming()
        
        self.streaming_widgets['start_btn'].disabled = False
        self.streaming_widgets['stop_btn'].disabled = True
        
        # Status in Tabelle aktualisieren
        df = self.streaming_widgets['ticker_list'].value
        if not df.empty:
            df['status'] = '⚪ Gestoppt'
            self.streaming_widgets['ticker_list'].value = df

    def _on_streaming_data(self, ticker: str, data: dict):
        """
        Callback für neue Streaming-Daten
        
        Args:
            ticker: Ticker-Symbol
            data: Dict mit OHLCV-Daten
        """
        # Ticker-Liste aktualisieren
        df = self.streaming_widgets['ticker_list'].value
        if ticker in df['ticker'].values:
            df.loc[df['ticker'] == ticker, 'last_price'] = data['close']
            df.loc[df['ticker'] == ticker, 'last_update'] = datetime.now().strftime('%H:%M:%S')
            self.streaming_widgets['ticker_list'].value = df
        
        # Chart aktualisieren wenn dieser Ticker angezeigt wird
        selected_ticker = self.streaming_widgets['chart_ticker_select'].value
        if ticker == selected_ticker:
            self._update_streaming_chart()

    def _on_streaming_status(self, message: str):
        """Callback für Status-Updates vom Streaming Backend"""
        self.streaming_widgets['status'].object = f"### {message}"

    def _on_streaming_chart_ticker_change(self, event):
        """Ticker für Chart gewechselt"""
        self._update_streaming_chart()

    def _update_streaming_chart(self, event=None):
        """Aktualisiert den Live-Streaming-Chart"""
        ticker = self.streaming_widgets['chart_ticker_select'].value
        window = self.streaming_widgets['chart_window'].value
        
        if not ticker or ticker == 'Keine Ticker':
            return
        
        # Daten aus DB laden
        df = self.streaming.load_initial_data(ticker, limit=window)
        
        if df.empty:
            self.streaming_widgets['chart'].objects = [
                pn.pane.Markdown("### ⚠️ Keine Daten verfügbar")
            ]
            return
        
        df.rename(columns={'time': 'date'}, inplace=True)
        
        # Candlestick-Chart erstellen
        import plotly.graph_objs as go
        
        fig = go.Figure(data=[go.Candlestick(
            x=df['date'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            name='OHLC',
            increasing_line_color='#26a69a',
            decreasing_line_color='#ef5350'
        )])
        
        fig.update_layout(
            title=f"{ticker} - Live Stream (1min)",
            yaxis_title="Preis (USD)",
            template="plotly_white",
            hovermode='x unified',
            xaxis_rangeslider_visible=False,
            margin=dict(l=50, r=50, t=80, b=50)
        )
        
        self.streaming_widgets['chart'].objects = [
            pn.pane.Plotly(fig, sizing_mode='stretch_width', height=600)
        ]
        
        # Statistiken berechnen
        current_price = df['close'].iloc[-1]
        high_24h = df['high'].max()
        low_24h = df['low'].min()
        volume_24h = df['volume'].sum()
        change = ((current_price - df['close'].iloc[0]) / df['close'].iloc[0]) * 100
        
        stats = f"""### 📊 Statistiken

**Ticker:** {ticker}  
**Datenpunkte:** {len(df)}  
**Zeitraum:** {df['date'].min()} - {df['date'].max()}  

**Aktueller Kurs:** ${current_price:.2f}  
**24h Hoch:** ${high_24h:.2f}  
**24h Tief:** ${low_24h:.2f}  
**24h Volumen:** {volume_24h:,.0f}  
**24h Change:** {change:+.2f}%
"""
        
        self.streaming_widgets['stats'].object = stats

    # ========================================
    # CHART-UPDATE
    # ========================================

    def _update_selected_tickers_table(self):
        """Aktualisiert die Tabelle der ausgewählten Ticker."""
        tickers = self.backend.get_selected_tickers()
        if not tickers:
            empty_df = pd.DataFrame(columns=["ticker", "name", "status"])
            self.ui.ticker_widgets['selected_tickers_table'].value = empty_df
            return
        
        df = pd.DataFrame(tickers)
        self.ui.ticker_widgets['selected_tickers_table'].value = df

    def _update_chart(self, event=None):
        """Lädt Daten, berechnet Indikatoren und aktualisiert die Charts."""
        symbol = self.ui.widgets['symbol_select'].value
        interval = self.ui.widgets['get_interval_value']()
        start_date = self.ui.widgets['start_date'].value
        end_date = self.ui.widgets['end_date'].value
        chart_type = self.ui.widgets['chart_type'].value
        indicators = self.ui.widgets['indicators'].value
    
        if not symbol or symbol == 'Keine Daten verfügbar':
            self.ui.set_status("Bitte wählen Sie einen gültigen Ticker.", "warning")
            return
    
        self.ui.widgets['status_indicator'].value = True
    
        try:
            if start_date and end_date and start_date > end_date:
                logger.info(f"Start-Datum ({start_date}) nach End-Datum ({end_date}) - korrigiere")
                start_date, end_date = end_date, start_date
    
            # Daten laden
            df = self.backend.load_data(symbol, interval, start_date, end_date)
            if df.empty:
                self.ui.set_status("Keine Daten für diesen Zeitraum verfügbar.", "warning")
                self.ui.chart_containers['main'].objects = [
                    pn.pane.Markdown("### ⚠️ Keine Daten verfügbar")
                ]
                return
    
            # Indikatoren berechnen
            df = self.backend.calculate_indicators(df)
            stats = self.backend.calculate_statistics(df)
    
            # Charts aktualisieren
            self.ui.chart_containers['main'].objects = [
                self.ui.create_candlestick_chart(df, chart_type, indicators)
            ]
            self.ui.chart_containers['volume'].objects = [
                self.ui.create_volume_chart(df)
            ]
            
            # RSI Chart
            if 'rsi' in indicators:
                rsi_chart = self.ui.create_rsi_chart(df)
                if rsi_chart:
                    self.ui.chart_containers['rsi'].objects = [rsi_chart]
                    self.ui.chart_containers['rsi'].visible = True
            else:
                self.ui.chart_containers['rsi'].visible = False
            
            # MACD Chart
            if 'macd' in indicators:
                macd_chart = self.ui.create_macd_chart(df)
                if macd_chart:
                    self.ui.chart_containers['macd'].objects = [macd_chart]
                    self.ui.chart_containers['macd'].visible = True
            else:
                self.ui.chart_containers['macd'].visible = False
            
            # Statistiken
            self.ui.chart_containers['stats'].objects = [
                self.ui.create_statistics_table(stats)
            ]
            
            if not df.empty:
                active_indicators = len(indicators)
                logger.info(f"✅ Dashboard aktualisiert: {len(df)} Datenpunkte, "
                           f"{active_indicators} Indikatoren für {symbol}")
            
            self.ui.set_status(f"Chart für {symbol} aktualisiert.", "success")
        
        except Exception as e:
            self.ui.set_status(f"Fehler beim Laden der Daten: {str(e)}", "danger")
            logger.error(f"❌ Dashboard-Fehler: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            self.ui.widgets['status_indicator'].value = False

    def show(self):
        """Gibt das Panel-Layout zur Anzeige zurück."""
        return self.ui.show()
