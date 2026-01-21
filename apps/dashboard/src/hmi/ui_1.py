#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ui.py – UI-Komponenten für das Stock Data Dashboard
Mit FastListTemplate, Sidebar und set_status() (Original-Design)
"""

import panel as pn
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objs as go

pn.extension('plotly', 'tabulator')
pn.config.sizing_mode = 'stretch_width'


class DashboardUI:
    """
    DashboardUI mit FastListTemplate und Sidebar (Original-Design)
    """

    def __init__(self):
        self.title = "📈 Stock Data Platform"
        self.widgets = {}
        self.ticker_widgets = {}
        self.chart_containers = {}
        
        # Chart-Container initialisieren
        self._init_chart_containers()
        
        # Widgets erstellen
        self.create_widgets()
        self.create_ticker_widgets()
        self.create_layout()

    def _init_chart_containers(self):
        """Initialisiert die dynamischen Chart-Container"""
        self.chart_containers['main'] = pn.Column(
            pn.pane.Markdown("### 🔄 Lade Daten...\n\nBitte warten..."),
            sizing_mode='stretch_width',
            height=600
        )
        self.chart_containers['volume'] = pn.Column(sizing_mode='stretch_width', height=200)
        self.chart_containers['rsi'] = pn.Column(sizing_mode='stretch_width', height=200, visible=False)
        self.chart_containers['macd'] = pn.Column(sizing_mode='stretch_width', height=200, visible=False)
        self.chart_containers['stats'] = pn.Column(
            pn.pane.Markdown("### Statistiken werden geladen..."),
            sizing_mode='stretch_width'
        )

    def create_widgets(self):
        """Erstellt die Haupt-Widgets (wie app_backup_1.py)."""
        
        # Symbol-Auswahl
        self.widgets['symbol_select'] = pn.widgets.Select(
            name='Stock Symbol',
            options=['Keine Daten verfügbar'],
            value='Keine Daten verfügbar',
            width=200
        )
        
        # Schnellauswahl für Zeiträume
        self.widgets['quick_range'] = pn.widgets.RadioButtonGroup(
            name='Schnellauswahl',
            options={
                '1 Woche': 7,
                '1 Monat': 30,
                '3 Monate': 90,
                '6 Monate': 180,
                '1 Jahr': 365
            },
            button_type='default'
        )
        
        # Start-Datum (DatePicker statt DateRangeSlider)
        self.widgets['start_date'] = pn.widgets.DatePicker(
            name='📅 Start-Datum',
            value=(datetime.now() - timedelta(days=90)).date(),
            start=(datetime.now() - timedelta(days=365)).date(),
            end=datetime.now().date(),
            width=200
        )
        
        # End-Datum (DatePicker)
        self.widgets['end_date'] = pn.widgets.DatePicker(
            name='📅 End-Datum',
            value=datetime.now().date(),
            start=(datetime.now() - timedelta(days=365)).date(),
            end=datetime.now().date(),
            width=200
        )
        
        # Intervall
        self.widgets['interval'] = pn.widgets.Select(
            name='Interval',
            options=['1day', '1hour', '5min', '1min'],
            value='1day',
            width=150
        )
        
        # Chart-Typ Auswahl
        self.widgets['chart_type'] = pn.widgets.Select(
            name='Chart-Typ',
            options={
                '🕯️ Candlestick': 'candlestick',
                '📈 Linie (Close)': 'line_close',
                '📉 Linie mit High/Low': 'line_range',
                '🏔️ Area Chart': 'area',
                '📊 OHLC Bars': 'ohlc',
                '📈 Beides': 'both'
            },
            value='candlestick',
            width=220
        )
        
        # Indikator-Auswahl
        self.widgets['indicators'] = pn.widgets.MultiChoice(
            name='Technische Indikatoren',
            options={
                'SMA 20': 'sma_20',
                'SMA 50': 'sma_50',
                'SMA 200': 'sma_200',
                'EMA 12': 'ema_12',
                'EMA 26': 'ema_26',
                'Bollinger Bands': 'bollinger',
                'RSI': 'rsi',
                'MACD': 'macd'
            },
            value=['sma_20'],
            width=250
        )
        
        # Status & Update
        self.widgets['status_indicator'] = pn.indicators.LoadingSpinner(
            value=False,
            width=30,
            height=30
        )
        
        self.widgets['refresh_btn'] = pn.widgets.Button(
            name='🔄 Manuell aktualisieren',
            button_type='primary',
            width=200
        )

    def create_ticker_widgets(self):
        """Erstellt Widgets für Ticker-Verwaltung (wie app_backup_1.py)."""
        
        # Asset Class Auswahl
        self.ticker_widgets['asset_class'] = pn.widgets.Select(
            name='Asset Class',
            options=['stocks', 'crypto', 'forex', 'indices'],
            value='stocks',
            width=150
        )
        
        # Laden-Button
        self.ticker_widgets['load_tickers_btn'] = pn.widgets.Button(
            name='📊 Ticker von API laden',
            button_type='primary',
            width=200
        )
        
        # Status Spinner
        self.ticker_widgets['ticker_status'] = pn.indicators.LoadingSpinner(
            value=False, 
            width=30, 
            height=30
        )
        
        # Info Text
        self.ticker_widgets['ticker_info'] = pn.pane.Markdown(
            "### 📊 Ticker-Verwaltung\n\nLade Ticker von der API."
        )
        
        # Tabelle für alle verfügbaren Ticker
        self.ticker_widgets['all_tickers_table'] = pn.widgets.Tabulator(
            pd.DataFrame(),
            theme='midnight',
            layout='fit_columns',
            pagination='remote',
            page_size=20,
            height=500,
            selectable='checkbox',
            show_index=False
        )
        
        # Tabelle für ausgewählte Ticker
        self.ticker_widgets['selected_tickers_table'] = pn.widgets.Tabulator(
            pd.DataFrame(columns=["ticker", "name", "status"]),
            theme='midnight',
            layout='fit_columns',
            height=300,
            selectable='checkbox',
            show_index=False
        )
        
        # Buttons
        self.ticker_widgets['add_selected_btn'] = pn.widgets.Button(
            name='✅ Ausgewählte hinzufügen',
            button_type='success',
            width=200
        )
        
        self.ticker_widgets['remove_selected_btn'] = pn.widgets.Button(
            name='❌ Ausgewählte entfernen',
            button_type='danger',
            width=200
        )
        
        self.ticker_widgets['clear_all_btn'] = pn.widgets.Button(
            name='🗑️ Alle löschen',
            button_type='warning',
            width=200
        )
        
        # Daten-Lade-Widgets
        self.ticker_widgets['data_load_days'] = pn.widgets.IntSlider(
            name='Tage laden',
            start=7,
            end=365,
            value=90,
            step=1,
            width=200
        )
        
        self.ticker_widgets['data_load_interval'] = pn.widgets.Select(
            name='Intervall',
            options=['1day', '1hour', '5min'],
            value='1day',
            width=150
        )
        
        self.ticker_widgets['load_data_btn'] = pn.widgets.Button(
            name='📥 Daten für ausgewählte Ticker laden',
            button_type='success',
            width=250
        )
        
        self.ticker_widgets['bulk_update_btn'] = pn.widgets.Button(
            name='🔄 Alle Ticker aktualisieren',
            button_type='primary',
            width=250
        )
        
        # Progress-Anzeige
        self.ticker_widgets['progress'] = pn.indicators.Progress(
            name='Lade Daten...',
            value=0,
            max=100,
            width=300,
            visible=False
        )
        
        self.ticker_widgets['status'] = pn.pane.Markdown("", width=400)

    def create_candlestick_chart(self, data: pd.DataFrame, chart_type: str = 'candlestick', 
                                  indicators: list = None) -> pn.pane.Plotly:
        """Erstellt ein Chart (wie app_backup_1.py)."""
        if data.empty:
            return pn.pane.Markdown(
                "### ⚠️ Keine Daten verfügbar\n\n"
                "Bitte wähle ein anderes Symbol oder ändere den Zeitraum."
            )
        
        fig = go.Figure()
        
        # Chart basierend auf Typ erstellen
        if chart_type == 'candlestick':
            fig.add_trace(go.Candlestick(
                x=data['date'],
                open=data['open'],
                high=data['high'],
                low=data['low'],
                close=data['close'],
                name='OHLC',
                increasing_line_color='#26a69a',
                decreasing_line_color='#ef5350'
            ))
            title_text = "Candlestick Chart"
        
        elif chart_type == 'line_close':
            fig.add_trace(go.Scatter(
                x=data['date'], 
                y=data['close'],
                mode='lines', 
                name='Close',
                line=dict(color='#2196f3', width=2)
            ))
            title_text = "Close Price"
        
        elif chart_type == 'line_range':
            fig.add_trace(go.Scatter(
                x=data['date'], 
                y=data['high'],
                mode='lines', 
                name='High',
                line=dict(width=0), 
                showlegend=False
            ))
            fig.add_trace(go.Scatter(
                x=data['date'], 
                y=data['low'],
                mode='lines', 
                name='Low',
                fill='tonexty', 
                fillcolor='rgba(68, 138, 255, 0.2)',
                line=dict(width=0), 
                showlegend=False
            ))
            fig.add_trace(go.Scatter(
                x=data['date'], 
                y=data['close'],
                mode='lines', 
                name='Close',
                line=dict(color='#2196f3', width=2)
            ))
            title_text = "Close Price mit High/Low Range"
        
        elif chart_type == 'area':
            fig.add_trace(go.Scatter(
                x=data['date'], 
                y=data['close'],
                mode='lines', 
                name='Close',
                fill='tozeroy',
                fillcolor='rgba(33, 150, 243, 0.3)',
                line=dict(color='#2196f3', width=2)
            ))
            title_text = "Area Chart"
        
        elif chart_type == 'ohlc':
            fig.add_trace(go.Ohlc(
                x=data['date'],
                open=data['open'], 
                high=data['high'],
                low=data['low'], 
                close=data['close'],
                name='OHLC',
                increasing_line_color='#26a69a',
                decreasing_line_color='#ef5350'
            ))
            title_text = "OHLC Bars"
        
        else:  # 'both'
            fig.add_trace(go.Candlestick(
                x=data['date'],
                open=data['open'],
                high=data['high'],
                low=data['low'],
                close=data['close'],
                name='OHLC',
                increasing_line_color='#26a69a',
                decreasing_line_color='#ef5350'
            ))
            fig.add_trace(go.Scatter(
                x=data['date'],
                y=data['close'],
                mode='lines',
                name='Close Line',
                line=dict(color='#2196f3', width=2),
                visible='legendonly'
            ))
            title_text = "Candlestick & Line"
        
        # Indikatoren hinzufügen
        if indicators:
            if 'sma_20' in indicators and 'sma_20' in data.columns:
                fig.add_trace(go.Scatter(
                    x=data['date'], y=data['sma_20'],
                    name='SMA 20', 
                    line=dict(color='orange', width=1.5),
                    mode='lines'
                ))
            
            if 'sma_50' in indicators and 'sma_50' in data.columns:
                fig.add_trace(go.Scatter(
                    x=data['date'], y=data['sma_50'],
                    name='SMA 50', 
                    line=dict(color='blue', width=1.5),
                    mode='lines'
                ))
            
            if 'sma_200' in indicators and 'sma_200' in data.columns:
                fig.add_trace(go.Scatter(
                    x=data['date'], y=data['sma_200'],
                    name='SMA 200', 
                    line=dict(color='purple', width=2),
                    mode='lines'
                ))
            
            if 'ema_12' in indicators and 'ema_12' in data.columns:
                fig.add_trace(go.Scatter(
                    x=data['date'], y=data['ema_12'],
                    name='EMA 12', 
                    line=dict(color='cyan', width=1.5, dash='dash'),
                    mode='lines'
                ))
            
            if 'ema_26' in indicators and 'ema_26' in data.columns:
                fig.add_trace(go.Scatter(
                    x=data['date'], y=data['ema_26'],
                    name='EMA 26', 
                    line=dict(color='magenta', width=1.5, dash='dash'),
                    mode='lines'
                ))
            
            # Bollinger Bands
            if 'bollinger' in indicators:
                if 'bb_upper' in data.columns:
                    fig.add_trace(go.Scatter(
                        x=data['date'], y=data['bb_upper'],
                        name='BB Upper',
                        line=dict(color='gray', width=1, dash='dot'),
                        showlegend=True
                    ))
                if 'bb_middle' in data.columns:
                    fig.add_trace(go.Scatter(
                        x=data['date'], y=data['bb_middle'],
                        name='BB Middle',
                        line=dict(color='gray', width=1),
                        showlegend=True
                    ))
                if 'bb_lower' in data.columns:
                    fig.add_trace(go.Scatter(
                        x=data['date'], y=data['bb_lower'],
                        name='BB Lower',
                        line=dict(color='gray', width=1, dash='dot'),
                        fill='tonexty',
                        fillcolor='rgba(128,128,128,0.1)',
                        showlegend=True
                    ))
        
        # Symbol aus dem ersten Datenpunkt oder als Fallback
        symbol = data['symbol'].iloc[0] if 'symbol' in data.columns else "Stock"
        
        fig.update_layout(
            title=dict(
                text=f"{symbol} - {title_text} mit Indikatoren",
                font=dict(size=20)
            ),
            yaxis_title="Preis (USD)",
            xaxis_title="Datum",
            template="plotly_dark",
            hovermode='x unified',
            xaxis_rangeslider_visible=False,
            margin=dict(l=50, r=50, t=80, b=50),
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor="rgba(0,0,0,0.5)"
            )
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width', height=600)

    def create_volume_chart(self, data: pd.DataFrame) -> pn.pane.Plotly:
        """Erstellt ein Volumen-Chart."""
        if data.empty:
            return pn.pane.Markdown("")
        
        colors = ['#ef5350' if row['close'] < row['open'] else '#26a69a' 
                  for idx, row in data.iterrows()]
        
        fig = go.Figure(data=[go.Bar(
            x=data['date'],
            y=data['volume'],
            marker_color=colors,
            name='Volume',
            showlegend=False
        )])
        
        fig.update_layout(
            title="Handelsvolumen",
            yaxis_title="Volumen",
            xaxis_title="Datum",
            template="plotly_dark",
            showlegend=False,
            margin=dict(l=50, r=50, t=50, b=50)
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width', height=200)

    def create_rsi_chart(self, data: pd.DataFrame) -> pn.pane.Plotly:
        """Erstellt ein RSI-Chart."""
        if 'rsi' not in data.columns or data['rsi'].isnull().all():
            return None
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=data['date'],
            y=data['rsi'],
            name='RSI',
            line=dict(color='#ffeb3b', width=2)
        ))
        
        fig.add_hline(y=70, line_dash="dash", line_color="red", 
                      annotation_text="Überkauft")
        fig.add_hline(y=30, line_dash="dash", line_color="green", 
                      annotation_text="Überverkauft")
        
        fig.update_layout(
            title="RSI (Relative Strength Index)",
            yaxis_title="RSI",
            xaxis_title="Datum",
            template="plotly_dark",
            yaxis=dict(range=[0, 100]),
            margin=dict(l=50, r=50, t=50, b=50)
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width', height=200)

    def create_macd_chart(self, data: pd.DataFrame) -> pn.pane.Plotly:
        """Erstellt ein MACD-Chart."""
        if 'macd' not in data.columns or data['macd'].isnull().all():
            return None
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=data['date'],
            y=data['macd'],
            name='MACD',
            line=dict(color='#2196f3', width=2)
        ))
        
        if 'macd_signal' in data.columns:
            fig.add_trace(go.Scatter(
                x=data['date'],
                y=data['macd_signal'],
                name='Signal',
                line=dict(color='#ff9800', width=2)
            ))
        
        if 'macd_histogram' in data.columns:
            colors = ['#26a69a' if val >= 0 else '#ef5350' 
                      for val in data['macd_histogram']]
            fig.add_trace(go.Bar(
                x=data['date'],
                y=data['macd_histogram'],
                name='Histogram',
                marker_color=colors
            ))
        
        fig.update_layout(
            title="MACD (Moving Average Convergence Divergence)",
            yaxis_title="MACD",
            xaxis_title="Datum",
            template="plotly_dark",
            margin=dict(l=50, r=50, t=50, b=50)
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width', height=200)

    def create_statistics_table(self, stats: dict) -> pn.widgets.Tabulator:
        """Erstellt eine Statistik-Tabelle."""
        if not stats:
            stats_data = {
                'Metrik': ['Status'],
                'Wert': ['Keine Daten verfügbar']
            }
            stats_df = pd.DataFrame(stats_data)
        else:
            stats_df = pd.DataFrame(list(stats.items()), columns=["Metrik", "Wert"])
        
        return pn.widgets.Tabulator(
            stats_df,
            theme='midnight',
            layout='fit_columns',
            height=360,
            disabled=True,
            show_index=False
        )

    def create_layout(self):
        """Erstellt das Layout mit FastListTemplate und Sidebar (wie app_backup_1.py)."""
        
        # ========================================
        # TAB 1: Charts & Analyse
        # ========================================
        
        charts_tab = pn.Column(
            pn.pane.Markdown(f"# {self.title}"),
            self.chart_containers['main'],
            self.chart_containers['volume'],
            self.chart_containers['rsi'],
            self.chart_containers['macd'],
            sizing_mode='stretch_width'
        )
        
        # ========================================
        # TAB 2: Ticker-Verwaltung
        # ========================================
        
        ticker_management_tab = pn.Column(
            pn.pane.Markdown("# 📊 Ticker-Verwaltung"),
            
            # Sektion: API Ticker laden
            pn.pane.Markdown("## 📡 Ticker von API laden"),
            pn.Row(
                self.ticker_widgets['asset_class'],
                self.ticker_widgets['load_tickers_btn'],
                self.ticker_widgets['ticker_status']
            ),
            self.ticker_widgets['ticker_info'],
            
            pn.layout.Divider(),
            
            # Sektion: Alle verfügbaren Ticker
            pn.pane.Markdown("## 📊 Alle verfügbaren Ticker"),
            pn.pane.Markdown("*Wähle Ticker aus und klicke 'Hinzufügen' (Daten werden automatisch geladen)*"),
            self.ticker_widgets['all_tickers_table'],
            pn.Row(
                self.ticker_widgets['add_selected_btn'],
                self.ticker_widgets['remove_selected_btn']
            ),
            
            pn.layout.Divider(),
            
            # Sektion: Meine ausgewählten Ticker
            pn.pane.Markdown("## ✅ Meine ausgewählten Ticker"),
            self.ticker_widgets['selected_tickers_table'],
            
            pn.layout.Divider(),
            
            # Sektion: Daten laden/aktualisieren
            pn.pane.Markdown("## 📥 Daten verwalten"),
            pn.pane.Markdown("*Lade historische Daten für ausgewählte Ticker*"),
            pn.Row(
                pn.Column(
                    self.ticker_widgets['data_load_days'],
                    self.ticker_widgets['data_load_interval'],
                    width=300
                ),
                pn.Column(
                    self.ticker_widgets['load_data_btn'],
                    self.ticker_widgets['bulk_update_btn'],
                    width=300
                )
            ),
            self.ticker_widgets['progress'],
            self.ticker_widgets['status'],
            
            pn.layout.Divider(),
            
            # Sektion: Verwaltung
            pn.pane.Markdown("## 🗑️ Verwaltung"),
            pn.Row(
                self.ticker_widgets['clear_all_btn']
            ),
            
            sizing_mode='stretch_width'
        )
        
        # ========================================
        # Tabs zusammenfügen
        # ========================================
        
        tabs = pn.Tabs(
            ('📈 Charts', charts_tab),
            ('📊 Ticker-Verwaltung', ticker_management_tab),
            dynamic=True
        )
        
        # ========================================
        # Sidebar (Original-Design)
        # ========================================
        
        info_text = pn.pane.Markdown("""
        ### 📊 Anleitung
        
        **Charts Tab:**
        - Symbol auswählen
        - Chart-Typ wählen
        - Zeitraum mit Kalender
        - Indikatoren aktivieren
        - *"Manuell aktualisieren"* lädt fehlende Daten automatisch nach
        
        **Ticker-Verwaltung Tab:**
        1. Asset Class wählen (stocks/crypto/...)
        2. "Ticker von API laden" klicken
        3. Ticker in Tabelle auswählen
        4. "Ausgewählte hinzufügen" → **Daten werden automatisch geladen**
        5. Optional: "Alle aktualisieren" für tägliches Update
        
        **Daten-Intervalle:**
        - 1day: Tageskerzen (Standard)
        - 1hour: Stundenkerzen
        - 5min: 5-Minuten-Kerzen
        
        ---
        """)
        
        sidebar = pn.Column(
            pn.pane.Markdown("## ⚙️ Einstellungen"),
            info_text,
            
            # Chart-Einstellungen
            pn.pane.Markdown("### 📈 Chart-Einstellungen"),
            self.widgets['symbol_select'],
            self.widgets['chart_type'],
            self.widgets['interval'],
            
            pn.layout.Divider(),
            
            # Zeitraum
            pn.pane.Markdown("### 📅 Zeitraum"),
            self.widgets['quick_range'],
            self.widgets['start_date'],
            self.widgets['end_date'],
            
            pn.layout.Divider(),
            
            # Indikatoren
            pn.pane.Markdown("### 📈 Technische Indikatoren"),
            self.widgets['indicators'],
            pn.Row(
                self.widgets['refresh_btn'],
                self.widgets['status_indicator']
            ),
            
            pn.layout.Divider(),
            
            # Statistiken
            pn.pane.Markdown("### 📊 Statistiken"),
            self.chart_containers['stats'],
            
            width=350,
            scroll=True
        )
        
        # ========================================
        # FastListTemplate (Original-Design)
        # ========================================
        
        self.layout = pn.template.FastListTemplate(
            title=self.title,
            sidebar=[sidebar],
            main=[tabs],
            theme='dark',
            theme_toggle=True,
            header_background='#1f77b4',
            header_color='white'
        )

    def show(self):
        """Gibt das Panel-Layout zur Anzeige zurück."""
        return self.layout

    # ✅ NEU: set_status() Methode hinzugefügt
    def set_status(self, message: str, alert_type: str = "info"):
        """
        Setzt eine Statusmeldung (wird im Moment nur in console ausgegeben).
        In Zukunft kann hier ein Alert-Widget hinzugefügt werden.
        """
        print(f"[{alert_type.upper()}] {message}")
