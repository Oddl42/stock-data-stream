#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ui.py – Material Design UI mit optimiertem Layout (KEINE Überlappungen)
"""

import panel as pn
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objs as go

pn.extension('plotly', 'tabulator')
pn.config.sizing_mode = 'stretch_width'


class DashboardUI:
    """
    Material Design Dashboard mit optimiertem Grid Layout
    """

    def __init__(self):
        self.title = "📈 Stock Data Platform"
        self.widgets = {}
        self.ticker_widgets = {}
        self.chart_containers = {}
        
        self._init_chart_containers()
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
        """Erstellt die Haupt-Widgets mit Material Design."""
        
        # Symbol-Auswahl (kompakt)
        self.widgets['symbol_select'] = pn.widgets.Select(
            name='📊 Stock Symbol',
            options=['Keine Daten verfügbar'],
            value='Keine Daten verfügbar',
            width=200,
            styles={'font-size': '13px'}
        )
        
        # ✅ Chart-Typ als RadioBoxGroup (VERTIKAL angeordnet)
        self.widgets['chart_type'] = pn.widgets.RadioBoxGroup(
            name='Chart-Typ',
            options={
                '🕯️ Candles': 'candlestick',
                '📈 Line': 'line_close',
                '📊 OHLC': 'ohlc',
                '🏔️ Area': 'area'
            },
            value='candlestick',
            inline=False,
            width=200
        )
        
        # ✅ ERWEITERTE Intervalle als 3x3 Grid (manuell mit 3 Zeilen)
        # Zeile 1: 1m, 5m, 15m
        interval_row1 = pn.widgets.RadioButtonGroup(
            name='',
            options={'1m': '1min', '5m': '5min', '15m': '15min'},
            button_type='default',
            width=200
        )
        
        # Zeile 2: 30m, 1h, 4h
        interval_row2 = pn.widgets.RadioButtonGroup(
            name='',
            options={'30m': '30min', '1h': '1hour', '4h': '4hour'},
            button_type='default',
            width=200
        )
        
        # Zeile 3: 1d, 1w, 1M (Standard: 1d)
        interval_row3 = pn.widgets.RadioButtonGroup(
            name='',
            options={'1d': '1day', '1w': '1week', '1M': '1month'},
            value='1day',
            button_type='default',
            width=200
        )
        
        # Wrapper-Column
        interval_column = pn.Column(
            pn.pane.Markdown("**Intervall:**", styles={'margin': '0 0 5px 0', 'font-size': '13px'}),
            interval_row1,
            interval_row2,
            interval_row3,
            width=250,
            styles={'gap': '3px'}
        )
        
        # Speichere Komponenten
        self.widgets['interval_row1'] = interval_row1
        self.widgets['interval_row2'] = interval_row2
        self.widgets['interval_row3'] = interval_row3
        self.widgets['interval'] = interval_column
        
        # Helper-Methode für Wert-Zugriff
        def get_interval_value():
            """Gibt den aktuell ausgewählten Intervall-Wert zurück."""
            for row in [interval_row1, interval_row2, interval_row3]:
                if row.value:
                    return row.value
            return '1day'
        
        self.widgets['get_interval_value'] = get_interval_value
        
        # Synchronisation zwischen Zeilen
        def sync_intervals(event):
            if event.new:
                if event.obj == interval_row1:
                    interval_row2.value = None
                    interval_row3.value = None
                elif event.obj == interval_row2:
                    interval_row1.value = None
                    interval_row3.value = None
                elif event.obj == interval_row3:
                    interval_row1.value = None
                    interval_row2.value = None
        
        interval_row1.param.watch(sync_intervals, 'value')
        interval_row2.param.watch(sync_intervals, 'value')
        interval_row3.param.watch(sync_intervals, 'value')
        
        # Schnellauswahl für Zeiträume (kompakt)
        self.widgets['quick_range'] = pn.widgets.RadioButtonGroup(
            name='Schnellauswahl',
            options={
                '1W': 7,
                '1M': 30,
                '3M': 90,
                '6M': 180,
                '1J': 365
            },
            button_type='default'
        )
        
        # ✅ KOMPAKTE Datum-Picker
        self.widgets['start_date'] = pn.widgets.DatePicker(
            name='Von',
            value=(datetime.now() - timedelta(days=90)).date(),
            start=(datetime.now() - timedelta(days=365)).date(),
            end=datetime.now().date(),
            width=120
        )
        
        self.widgets['end_date'] = pn.widgets.DatePicker(
            name='Bis',
            value=datetime.now().date(),
            start=(datetime.now() - timedelta(days=365)).date(),
            end=datetime.now().date(),
            width=120
        )
        
        # Indikatoren als CheckBoxGroup
        self.widgets['indicators'] = pn.widgets.CheckBoxGroup(
            name='Technische Indikatoren',
            options={
                'SMA 20': 'sma_20',
                'SMA 50': 'sma_50',
                'SMA 200': 'sma_200',
                'EMA 12': 'ema_12',
                'EMA 26': 'ema_26',
                'Bollinger': 'bollinger',
                'RSI': 'rsi',
                'MACD': 'macd'
            },
            value=['sma_20'],
            inline=False
        )
        
        # Status & Update
        self.widgets['status_indicator'] = pn.indicators.LoadingSpinner(
            value=False,
            width=25,
            height=25
        )
        
        self.widgets['refresh_btn'] = pn.widgets.Button(
            name='🔄 Update',
            button_type='primary',
            width=120
        )


    def create_ticker_widgets(self):
        """Erstellt Widgets für Ticker-Verwaltung."""
        
        self.ticker_widgets['asset_class'] = pn.widgets.Select(
            name='Asset Class',
            options=['stocks', 'crypto', 'forex', 'indices'],
            value='stocks',
            width=150
        )
        
        self.ticker_widgets['load_tickers_btn'] = pn.widgets.Button(
            name='📊 Ticker laden',
            button_type='primary',
            width=200
        )
        
        self.ticker_widgets['ticker_status'] = pn.indicators.LoadingSpinner(
            value=False, width=30, height=30
        )
        
        self.ticker_widgets['ticker_info'] = pn.pane.Markdown(
            "### 📊 Ticker-Verwaltung\n\nLade Ticker von der API."
        )
        
        self.ticker_widgets['all_tickers_table'] = pn.widgets.Tabulator(
            pd.DataFrame(),
            theme='default',
            layout='fit_columns',
            pagination='remote',
            page_size=20,
            height=500,
            selectable='checkbox',
            show_index=False
        )
        
        self.ticker_widgets['selected_tickers_table'] = pn.widgets.Tabulator(
            pd.DataFrame(columns=["ticker", "name", "status"]),
            theme='default',
            layout='fit_columns',
            height=300,
            selectable='checkbox',
            show_index=False
        )
        
        self.ticker_widgets['add_selected_btn'] = pn.widgets.Button(
            name='✅ Hinzufügen',
            button_type='success',
            width=150
        )
        
        self.ticker_widgets['remove_selected_btn'] = pn.widgets.Button(
            name='❌ Entfernen',
            button_type='danger',
            width=150
        )
        
        self.ticker_widgets['clear_all_btn'] = pn.widgets.Button(
            name='🗑️ Alle löschen',
            button_type='warning',
            width=150
        )
        
        self.ticker_widgets['data_load_days'] = pn.widgets.IntSlider(
            name='Tage laden',
            start=7,
            end=365,
            value=90,
            step=1,
            width=200
        )
        
        # ✅ Intervalle erweitert
        self.ticker_widgets['data_load_interval'] = pn.widgets.Select(
            name='Intervall',
            options=['1min', '5min', '15min', '30min', '1hour', '4hour', '1day', '1week', '1month'],  # ✅ Erweitert
            value='1day',
            width=150
        )
        
        self.ticker_widgets['load_data_btn'] = pn.widgets.Button(
            name='📥 Daten laden',
            button_type='success',
            width=200
        )
        
        self.ticker_widgets['bulk_update_btn'] = pn.widgets.Button(
            name='🔄 Alle aktualisieren',
            button_type='primary',
            width=200
        )
        
        self.ticker_widgets['progress'] = pn.indicators.Progress(
            name='Lade Daten...',
            value=0,
            max=100,
            width=300,
            visible=False
        )
        
        self.ticker_widgets['status'] = pn.pane.Markdown("", width=400)

    # === CHART-METHODEN (unverändert aus vorheriger Version) ===
    
    def create_candlestick_chart(self, data: pd.DataFrame, chart_type: str = 'candlestick', 
                                  indicators: list = None) -> pn.pane.Plotly:
        """Erstellt ein Chart."""
        if data.empty:
            return pn.pane.Markdown(
                "### ⚠️ Keine Daten verfügbar\n\n"
                "Bitte wähle ein anderes Symbol oder ändere den Zeitraum."
            )
        
        fig = go.Figure()
        
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
                x=data['date'], y=data['close'],
                mode='lines', name='Close',
                line=dict(color='#2196f3', width=2)
            ))
            title_text = "Close Price"
        
        elif chart_type == 'area':
            fig.add_trace(go.Scatter(
                x=data['date'], y=data['close'],
                mode='lines', name='Close',
                fill='tozeroy',
                fillcolor='rgba(33, 150, 243, 0.3)',
                line=dict(color='#2196f3', width=2)
            ))
            title_text = "Area Chart"
        
        elif chart_type == 'ohlc':
            fig.add_trace(go.Ohlc(
                x=data['date'],
                open=data['open'], high=data['high'],
                low=data['low'], close=data['close'],
                name='OHLC',
                increasing_line_color='#26a69a',
                decreasing_line_color='#ef5350'
            ))
            title_text = "OHLC Bars"
        
        # Indikatoren hinzufügen
        if indicators:
            if 'sma_20' in indicators and 'sma_20' in data.columns:
                fig.add_trace(go.Scatter(
                    x=data['date'], y=data['sma_20'],
                    name='SMA 20', line=dict(color='orange', width=1.5),
                    mode='lines'
                ))
            
            if 'sma_50' in indicators and 'sma_50' in data.columns:
                fig.add_trace(go.Scatter(
                    x=data['date'], y=data['sma_50'],
                    name='SMA 50', line=dict(color='blue', width=1.5),
                    mode='lines'
                ))
            
            if 'sma_200' in indicators and 'sma_200' in data.columns:
                fig.add_trace(go.Scatter(
                    x=data['date'], y=data['sma_200'],
                    name='SMA 200', line=dict(color='purple', width=2),
                    mode='lines'
                ))
            
            if 'ema_12' in indicators and 'ema_12' in data.columns:
                fig.add_trace(go.Scatter(
                    x=data['date'], y=data['ema_12'],
                    name='EMA 12', line=dict(color='cyan', width=1.5, dash='dash'),
                    mode='lines'
                ))
            
            if 'ema_26' in indicators and 'ema_26' in data.columns:
                fig.add_trace(go.Scatter(
                    x=data['date'], y=data['ema_26'],
                    name='EMA 26', line=dict(color='magenta', width=1.5, dash='dash'),
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
        
        symbol = data['symbol'].iloc[0] if 'symbol' in data.columns else "Stock"
        
        fig.update_layout(
            title=dict(text=f"{symbol} - {title_text}", font=dict(size=20)),
            yaxis_title="Preis (USD)",
            xaxis_title="Datum",
            template="plotly_white",
            hovermode='x unified',
            xaxis_rangeslider_visible=False,
            margin=dict(l=50, r=50, t=80, b=50)
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width', height=600)

    def create_volume_chart(self, data: pd.DataFrame) -> pn.pane.Plotly:
        """Erstellt ein Volumen-Chart."""
        if data.empty:
            return pn.pane.Markdown("")
        
        colors = ['#ef5350' if row['close'] < row['open'] else '#26a69a' 
                  for idx, row in data.iterrows()]
        
        fig = go.Figure(data=[go.Bar(
            x=data['date'], y=data['volume'],
            marker_color=colors, name='Volume',
            showlegend=False
        )])
        
        fig.update_layout(
            title="Handelsvolumen",
            yaxis_title="Volumen",
            template="plotly_white",
            margin=dict(l=50, r=50, t=50, b=50)
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width', height=200)

    def create_rsi_chart(self, data: pd.DataFrame) -> pn.pane.Plotly:
        """Erstellt ein RSI-Chart."""
        if 'rsi' not in data.columns or data['rsi'].isnull().all():
            return None
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=data['date'], y=data['rsi'],
            name='RSI', line=dict(color='#ff9800', width=2)
        ))
        
        fig.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="Überkauft")
        fig.add_hline(y=30, line_dash="dash", line_color="green", annotation_text="Überverkauft")
        
        fig.update_layout(
            title="RSI (Relative Strength Index)",
            template="plotly_white",
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
            x=data['date'], y=data['macd'],
            name='MACD', line=dict(color='#2196f3', width=2)
        ))
        
        if 'macd_signal' in data.columns:
            fig.add_trace(go.Scatter(
                x=data['date'], y=data['macd_signal'],
                name='Signal', line=dict(color='#ff9800', width=2)
            ))
        
        if 'macd_histogram' in data.columns:
            colors = ['#26a69a' if val >= 0 else '#ef5350' 
                      for val in data['macd_histogram']]
            fig.add_trace(go.Bar(
                x=data['date'], y=data['macd_histogram'],
                name='Histogram', marker_color=colors
            ))
        
        fig.update_layout(
            title="MACD",
            template="plotly_white",
            margin=dict(l=50, r=50, t=50, b=50)
        )
        
        return pn.pane.Plotly(fig, sizing_mode='stretch_width', height=200)

    def create_statistics_table(self, stats: dict) -> pn.widgets.Tabulator:
        """Erstellt eine Statistik-Tabelle."""
        if not stats:
            stats_data = {'Metrik': ['Status'], 'Wert': ['Keine Daten verfügbar']}
            stats_df = pd.DataFrame(stats_data)
        else:
            stats_df = pd.DataFrame(list(stats.items()), columns=["Metrik", "Wert"])
        
        return pn.widgets.Tabulator(
            stats_df,
            theme='default',
            layout='fit_columns',
            height=360,
            disabled=True,
            show_index=False
        )

    # === ✅ OPTIMIERTES MATERIAL DESIGN LAYOUT (KEINE ÜBERLAPPUNGEN) ===
    
    def create_layout(self):
        """
        Material Design Layout mit optimiertem Control Panel
        """
        
        # ✅ Custom CSS für 3x3 Grid Intervalle
        pn.config.raw_css.append("""
        .interval-grid .bk-btn-group {
            display: grid !important;
            grid-template-columns: repeat(3, 1fr) !important;
            gap: 5px !important;
            width: 100% !important;
        }
        .interval-grid .bk-btn {
            min-width: 70px !important;
            padding: 5px 10px !important;
            font-size: 12px !important;
        }
        """)
        # ========================================
        # TAB 1: Charts & Analyse
        # ========================================
        
        # ✅ OPTIMIERTES Control Panel (4 kompakte Spalten)
        control_panel = pn.Card(
            pn.Row(
                # Spalte 1: Symbol & Chart-Typ (250px)
                pn.Column(
                    pn.pane.Markdown("#### 📊 Symbol & Chart", styles={'margin': '0'}),
                    self.widgets['symbol_select'],
                    self.widgets['chart_type'],
                    width=250,
                    styles={'padding': '10px'}
                ),
                
                # Spalte 2: Zeitraum (280px) ✅ KOMPAKT
                pn.Column(
                    pn.pane.Markdown("#### 📅 Zeitraum", styles={'margin': '0'}),
                    self.widgets['quick_range'],
                    pn.Row(
                        self.widgets['start_date'],
                        self.widgets['end_date'],
                        sizing_mode='fixed'
                    ),
                    width=280,  # ✅ Kleiner gemacht
                    styles={'padding': '10px'}
                ),
                
                # Spalte 3: Intervall & Update (220px) ✅ KOMPAKT
                pn.Column(
                    pn.pane.Markdown("#### ⚙️ Einstellungen", styles={'margin': '0'}),
                    self.widgets['interval'],
                    pn.Row(
                        self.widgets['refresh_btn'],
                        self.widgets['status_indicator']
                    ),
                    width=220,  # ✅ Kleiner gemacht
                    styles={'padding': '10px'}
                ),
                
                # Spalte 4: Indikatoren (220px) ✅ KOMPAKT
                pn.Column(
                    pn.pane.Markdown("#### 📈 Indikatoren", styles={'margin': '0'}),
                    self.widgets['indicators'],
                    width=220,  # ✅ Feste Breite
                    styles={'padding': '10px', 'overflow-y': 'auto', 'max-height': '300px'}
                ),
                
                sizing_mode='stretch_width',
                styles={'padding': '10px'}
            ),
            title="⚙️ Einstellungen & Indikatoren",
            collapsed=False,
            collapsible=True,
            header_background='#2196f3',
            header_color='white',
            styles={'margin-bottom': '15px'}
        )
        
        # Charts Grid
        charts_grid = pn.GridSpec(sizing_mode='stretch_both', min_height=1400)
        
        # Row 1: Haupt-Chart (75%) + Statistiken (25%)
        charts_grid[0:2, 0:9] = pn.Card(
            self.chart_containers['main'],
            title="📈 Kursdiagramm",
            collapsed=False,
            styles={'margin-bottom': '10px'}
        )
        
        charts_grid[0:2, 9:12] = pn.Card(
            self.chart_containers['stats'],
            title="📊 Statistiken",
            collapsed=False,
            styles={'margin-bottom': '10px'}
        )
        
        # Row 2: Volume Chart (Full Width)
        charts_grid[2, 0:12] = pn.Card(
            self.chart_containers['volume'],
            title="📊 Handelsvolumen",
            collapsed=False,
            styles={'margin-bottom': '10px'}
        )
        
        # Row 3: RSI & MACD nebeneinander
        charts_grid[3, 0:6] = pn.Card(
            self.chart_containers['rsi'],
            title="📈 RSI",
            collapsed=False,
            styles={'margin-right': '5px'}
        )
        
        charts_grid[3, 6:12] = pn.Card(
            self.chart_containers['macd'],
            title="📈 MACD",
            collapsed=False,
            styles={'margin-left': '5px'}
        )
        
        charts_tab = pn.Column(
            control_panel,
            charts_grid,
            sizing_mode='stretch_width'
        )
        
        # ========================================
        # TAB 2: Ticker-Verwaltung
        # ========================================
        
        ticker_management_tab = pn.Column(
            pn.pane.Markdown("# 📊 Ticker-Verwaltung"),
            
            pn.Card(
                pn.Row(
                    self.ticker_widgets['asset_class'],
                    self.ticker_widgets['load_tickers_btn'],
                    self.ticker_widgets['ticker_status']
                ),
                self.ticker_widgets['ticker_info'],
                title="📡 Ticker von API laden",
                collapsed=False
            ),
            
            pn.Card(
                self.ticker_widgets['all_tickers_table'],
                pn.Row(
                    self.ticker_widgets['add_selected_btn'],
                    self.ticker_widgets['remove_selected_btn']
                ),
                title="📊 Alle verfügbaren Ticker",
                collapsed=False
            ),
            
            pn.Card(
                self.ticker_widgets['selected_tickers_table'],
                title="✅ Meine ausgewählten Ticker",
                collapsed=False
            ),
            
            pn.Card(
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
                title="📥 Daten verwalten",
                collapsed=False
            ),
            
            pn.Card(
                pn.Row(self.ticker_widgets['clear_all_btn']),
                title="🗑️ Verwaltung",
                collapsed=True
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
        # MaterialTemplate
        # ========================================
        
        self.layout = pn.template.MaterialTemplate(
            title=self.title,
            header_background='#1976d2',
            main=[tabs],
            theme='default',
            main_max_width='1600px'
        )

    def show(self):
        """Gibt das Panel-Layout zur Anzeige zurück."""
        return self.layout

    def set_status(self, message: str, alert_type: str = "info"):
        """Setzt eine Statusmeldung."""
        print(f"[{alert_type.upper()}] {message}")
