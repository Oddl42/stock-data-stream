#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 20 15:07:29 2026

@author: twi-dev
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
controller.py – Controller für das Stock Data Dashboard (MVC-Architektur)
VOLLSTÄNDIG mit allen Funktionen aus app_backup_1.py
"""

from ..hmi.ui import DashboardUI
from ..hmi.backend import StockBackend
import panel as pn
import pandas as pd
from datetime import datetime, timedelta

class StockDashboardController:
    """
    Controller-Klasse mit ALLEN Funktionen aus app_backup_1.py
    """

    def __init__(self, ui: DashboardUI = None, backend: StockBackend = None):
        self.ui = ui if ui else DashboardUI()
        self.backend = backend if backend else StockBackend()

        # Initialisierung
        self._init_symbol_options()
        self._register_callbacks()
        self._update_selected_tickers_table()

    def _init_symbol_options(self):
        """Setzt die verfügbaren Symbole im UI-Widget."""
        symbols = self.backend.get_available_symbols()
        self.ui.widgets['symbol_select'].options = symbols
        if symbols and symbols[0] != 'Keine Daten verfügbar':
            self.ui.widgets['symbol_select'].value = symbols[0]

    def _register_callbacks(self):
        """Registriert ALLE Event-Handler für UI-Widgets."""
        
        # === CHART-TAB CALLBACKS ===
        self.ui.widgets['symbol_select'].param.watch(self._on_symbol_change, 'value')
        self.ui.widgets['start_date'].param.watch(self._on_date_change, 'value')
        self.ui.widgets['end_date'].param.watch(self._on_date_change, 'value')
        # ✅ Callbacks für alle 3 Intervall-Zeilen
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

    # === CHART EVENT-HANDLER ===

    def _on_symbol_change(self, event):
        """Wird aufgerufen, wenn das Symbol gewechselt wird."""
        print(f"🔄 Update getriggert durch: {event.name} = {event.new}")
        self._update_chart()

    def _on_date_change(self, event):
        """Wird aufgerufen, wenn Start- oder End-Datum geändert wird."""
        print(f"🔄 Update getriggert durch: {event.name} = {event.new}")
        self._update_chart()

    def _on_interval_change(self, event):
        """Wird aufgerufen, wenn das Intervall geändert wird."""
        print(f"🔄 Update getriggert durch: {event.name} = {event.new}")
        self._update_chart()

    def _on_chart_type_change(self, event):
        """Wird aufgerufen, wenn der Chart-Typ geändert wird."""
        print(f"🔄 Update getriggert durch: {event.name} = {event.new}")
        self._update_chart()

    def _on_indicators_change(self, event):
        """Wird aufgerufen, wenn Indikatoren geändert werden."""
        print(f"🔄 Update getriggert durch: {event.name} = {event.new}")
        self._update_chart()

    def _on_refresh_click(self, event=None):
        """Wird aufgerufen, wenn der Refresh-Button geklickt wird."""
        print("🔄 Manuelles Update")
        self._update_chart()

    # === TICKER-MANAGEMENT EVENT-HANDLER ===

    def _on_load_tickers(self, event=None):
        """Lädt alle Ticker von der API."""
        asset_class = self.ui.ticker_widgets['asset_class'].value
        self.ui.ticker_widgets['ticker_status'].value = True
        self.ui.ticker_widgets['ticker_info'].object = f"### 📡 Lade {asset_class} Ticker..."
        
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

    # === UI-UPDATES ===

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
        
        # ✅ KORRIGIERT: Verwende Helper-Methode
        interval = self.ui.widgets['get_interval_value']()
        
        start_date = self.ui.widgets['start_date'].value
        end_date = self.ui.widgets['end_date'].value
        chart_type = self.ui.widgets['chart_type'].value
        indicators = self.ui.widgets['indicators'].value
    
        if not symbol or symbol == 'Keine Daten verfügbar':
            self.ui.set_status("Bitte wählen Sie einen gültigen Ticker.", "warning")
            return
    
        # Status anzeigen
        self.ui.widgets['status_indicator'].value = True
    
        try:
            # Validierung: Start muss vor Ende liegen
            if start_date and end_date and start_date > end_date:
                print(f"⚠️  Start-Datum ({start_date}) ist nach End-Datum ({end_date})")
                start_date, end_date = end_date, start_date
                print(f"   → Automatisch korrigiert: {start_date} - {end_date}")
    
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
            
            # RSI Chart (nur wenn Indikator aktiviert)
            if 'rsi' in indicators:
                rsi_chart = self.ui.create_rsi_chart(df)
                if rsi_chart:
                    self.ui.chart_containers['rsi'].objects = [rsi_chart]
                    self.ui.chart_containers['rsi'].visible = True
            else:
                self.ui.chart_containers['rsi'].visible = False
            
            # MACD Chart (nur wenn Indikator aktiviert)
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
            
            # Console-Output
            if not df.empty:
                active_indicators = len(indicators)
                print(f"✅ Dashboard aktualisiert: {len(df)} Datenpunkte, "
                      f"{active_indicators} Indikatoren für {symbol}")
            
            self.ui.set_status(f"Chart für {symbol} aktualisiert.", "success")
        
        except Exception as e:
            self.ui.set_status(f"Fehler beim Laden der Daten: {str(e)}", "danger")
            print(f"❌ Dashboard-Fehler: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            self.ui.widgets['status_indicator'].value = False
    

    def show(self):
        """Gibt das Panel-Layout zur Anzeige zurück."""
        return self.ui.show()
