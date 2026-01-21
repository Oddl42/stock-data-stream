#!/usr/bin/env python3
"""
Stock Data Dashboard - Hauptanwendung mit technischen Indikatoren
"""
"""
Stock Data Dashboard - Mit Ticker-Verwaltung
"""
import panel as pn
import pandas as pd
from datetime import datetime, timedelta
import os

os.environ['NO_AT_BRIDGE'] = '1'

# Panel extensions laden
pn.extension('plotly', 'tabulator')

# Imports
from apps.data_ingestion.src.database import engine
from apps.data_ingestion.src.massive_client import MassiveClient
from apps.dashboard.components.indicators import TechnicalIndicators
from apps.dashboard.src.ticker_db import TickerDatabase
from sqlalchemy import text

pn.config.sizing_mode = 'stretch_width'

class StockDashboard:
    """Haupt-Dashboard-Klasse mit technischen Indikatoren"""
    
    def __init__(self):
        self.title = "📈 Stock Data Platform"
        self.indicators = TechnicalIndicators()
        self.setup_data()
        self.create_widgets()
        self.create_layout()
    
    def setup_data(self):
        """Lädt verfügbare Symbole aus der Datenbank"""
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT DISTINCT symbol 
                FROM stock_ohlcv 
                ORDER BY symbol
            """))
            self.available_symbols = [row[0] for row in result]
        
        if not self.available_symbols:
            self.available_symbols = ['Keine Daten verfügbar']
    
    def create_widgets(self):
        """Erstellt interaktive Widgets mit Auto-Update"""
        
        # Symbol-Auswahl
        self.symbol_select = pn.widgets.Select(
            name='Stock Symbol',
            options=self.available_symbols,
            value=self.available_symbols[0] if self.available_symbols else None,
            width=200
        )
        self.symbol_select.param.watch(self.update_chart, 'value')
        
        # Schnellauswahl-Buttons für Zeiträume
        self.quick_range_buttons = pn.widgets.RadioButtonGroup(
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
        
        # Callback für Schnellauswahl - WICHTIG: .date() verwenden!
        def quick_range_callback(event):
            if event.new:
                days = event.new
                self.end_date_picker.value = datetime.now().date()  # ← .date()!
                self.start_date_picker.value = (datetime.now() - timedelta(days=days)).date()  # ← .date()!
        
        self.quick_range_buttons.param.watch(quick_range_callback, 'value')
        
        # NEU: Start-Datum (Popup-Kalender) - WICHTIG: .date() verwenden!
        self.start_date_picker = pn.widgets.DatePicker(
            name='📅 Start-Datum',
            value=(datetime.now() - timedelta(days=90)).date(),  # ← .date() hinzugefügt!
            start=(datetime.now() - timedelta(days=365)).date(),  # ← .date() hinzugefügt!
            end=datetime.now().date(),                             # ← .date() hinzugefügt!
            width=200
        )
        self.start_date_picker.param.watch(self.update_chart, 'value')
        
        # NEU: End-Datum (Popup-Kalender) - WICHTIG: .date() verwenden!
        self.end_date_picker = pn.widgets.DatePicker(
            name='📅 End-Datum',
            value=datetime.now().date(),                           # ← .date() hinzugefügt!
            start=(datetime.now() - timedelta(days=365)).date(),  # ← .date() hinzugefügt!
            end=datetime.now().date(),                             # ← .date() hinzugefügt!
            width=200
        )
        self.end_date_picker.param.watch(self.update_chart, 'value')
        
        # Interval-Auswahl
        self.interval_select = pn.widgets.Select(
            name='Interval',
            options=['1day', '1hour', '5min', '1min'],
            value='1day',
            width=150
        )
        self.interval_select.param.watch(self.update_chart, 'value')
        
        # Chart-Typ Auswahl
        self.chart_type_select = pn.widgets.Select(
            name='Chart-Typ',
            options={
                '🕯️ Candlestick': 'candlestick',
                '📈 Linie (Close)': 'line_close',
                '📊 Linie mit High/Low': 'line_range',
                '🎨 Area Chart': 'area',
                '📉 OHLC Bars': 'ohlc',
                '🔀 Beides': 'both'
            },
            value='candlestick',
            width=220
        )
        self.chart_type_select.param.watch(self.update_chart, 'value')
        
        # Indikator-Auswahl
        self.indicator_select = pn.widgets.MultiChoice(
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
        self.indicator_select.param.watch(self.update_chart, 'value')
        
        # Aktualisieren-Button
        self.refresh_button = pn.widgets.Button(
            name='🔄 Manuell aktualisieren',
            button_type='primary',
            width=200
        )
        self.refresh_button.on_click(self.update_chart)
        
        # Status-Indikator
        self.status_indicator = pn.indicators.LoadingSpinner(
            value=False,
            width=30,
            height=30
        )

    def load_data(self):
        """Lädt Daten aus der Datenbank"""
        try:
            symbol = self.symbol_select.value
            interval = self.interval_select.value
            
            # Daten von DatePicker holen (geben date-Objekte zurück)
            start_date = self.start_date_picker.value
            end_date = self.end_date_picker.value
            
            # Fallback falls None
            if not start_date:
                start_date = (datetime.now() - timedelta(days=90)).date()
            if not end_date:
                end_date = datetime.now().date()
            
            # Validierung: Start muss vor Ende liegen
            if start_date > end_date:
                print(f"⚠️  Start-Datum ({start_date}) ist nach End-Datum ({end_date})")
                # Tausche die Daten
                start_date, end_date = end_date, start_date
                print(f"   → Automatisch korrigiert: {start_date} - {end_date}")
            
            # Konvertiere date zu datetime für SQL-Query
            start_datetime = datetime.combine(start_date, datetime.min.time())
            end_datetime = datetime.combine(end_date, datetime.max.time())
            
            print(f"📊 Lade Daten: {symbol}, {interval}, {start_date} - {end_date}")
            
            # SQL-Query
            with engine.connect() as conn:
                query = text("""
                    SELECT 
                        time,
                        symbol,
                        CAST("open" AS DOUBLE PRECISION) as open,
                        CAST(high AS DOUBLE PRECISION) as high,
                        CAST(low AS DOUBLE PRECISION) as low,
                        CAST("close" AS DOUBLE PRECISION) as close,
                        CAST(volume AS BIGINT) as volume
                    FROM stock_ohlcv
                    WHERE symbol = :symbol
                        AND "interval" = :interval
                        AND time BETWEEN :start_date AND :end_date
                    ORDER BY time ASC
                """)
                
                result = conn.execute(query, {
                    'symbol': symbol,
                    'interval': interval,
                    'start_date': start_datetime,
                    'end_date': end_datetime
                })
                
                rows = result.fetchall()
                df = pd.DataFrame(rows, columns=['time', 'symbol', 'open', 'high', 'low', 'close', 'volume']) if rows else pd.DataFrame()
            
            if not df.empty:
                print(f"✅ {len(df)} Datensätze geladen")
                
                # Indikatoren hinzufügen
                if len(df) >= 20:
                    try:
                        df = self.indicators.add_all_indicators(df)
                        print(f"✅ Indikatoren berechnet")
                    except Exception as e:
                        print(f"⚠️  Indikator-Fehler: {e}")
            else:
                print(f"⚠️  Keine Daten gefunden für {symbol} im Zeitraum {start_date} - {end_date}")
            
            return df
            
        except Exception as e:
            print(f"❌ Fehler: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def create_candlestick_chart(self, df):
        """Erstellt einen Chart mit wählbarem Typ"""
        import plotly.graph_objects as go
        
        if df.empty:
            return pn.pane.Markdown(
                "### ⚠️ Keine Daten verfügbar\n\n"
                "Bitte wähle ein anderes Symbol oder ändere den Zeitraum."
            )
        
        fig = go.Figure()
        chart_type = self.chart_type_select.value
        
        # Chart basierend auf Typ erstellen
        if chart_type == 'candlestick':
            # Candlestick Chart
            fig.add_trace(go.Candlestick(
                x=df['time'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                name='OHLC',
                increasing_line_color='#26a69a',
                decreasing_line_color='#ef5350'
            ))
            title_text = "Candlestick Chart"
        
        elif chart_type == 'line_close':
            # Nur Close-Linie
            fig.add_trace(go.Scatter(
                x=df['time'], 
                y=df['close'],
                mode='lines', 
                name='Close',
                line=dict(color='#2196f3', width=2)
            ))
            title_text = "Close Price"
        
        elif chart_type == 'line_range':
            # Close-Linie mit High/Low Bereich
            fig.add_trace(go.Scatter(
                x=df['time'], 
                y=df['high'],
                mode='lines', 
                name='High',
                line=dict(width=0), 
                showlegend=False
            ))
            fig.add_trace(go.Scatter(
                x=df['time'], 
                y=df['low'],
                mode='lines', 
                name='Low',
                fill='tonexty', 
                fillcolor='rgba(68, 138, 255, 0.2)',
                line=dict(width=0), 
                showlegend=False
            ))
            fig.add_trace(go.Scatter(
                x=df['time'], 
                y=df['close'],
                mode='lines', 
                name='Close',
                line=dict(color='#2196f3', width=2)
            ))
            title_text = "Close Price mit High/Low Range"
        
        elif chart_type == 'area':
            # Area Chart (gefüllte Linie)
            fig.add_trace(go.Scatter(
                x=df['time'], 
                y=df['close'],
                mode='lines', 
                name='Close',
                fill='tozeroy',
                fillcolor='rgba(33, 150, 243, 0.3)',
                line=dict(color='#2196f3', width=2)
            ))
            title_text = "Area Chart"
        
        elif chart_type == 'ohlc':
            # OHLC Bars
            fig.add_trace(go.Ohlc(
                x=df['time'],
                open=df['open'], 
                high=df['high'],
                low=df['low'], 
                close=df['close'],
                name='OHLC',
                increasing_line_color='#26a69a',
                decreasing_line_color='#ef5350'
            ))
            title_text = "OHLC Bars"
        
        elif chart_type == 'both':
            # Candlestick + Close-Linie überlagert
            fig.add_trace(go.Candlestick(
                x=df['time'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                name='OHLC',
                increasing_line_color='#26a69a',
                decreasing_line_color='#ef5350'
            ))
            fig.add_trace(go.Scatter(
                x=df['time'],
                y=df['close'],
                mode='lines',
                name='Close Line',
                line=dict(color='#2196f3', width=2),
                visible='legendonly'  # Initial ausgeblendet, kann in Legende aktiviert werden
            ))
            title_text = "Candlestick & Line"
        
        # Ausgewählte Indikatoren hinzufügen
        selected_indicators = self.indicator_select.value
        
        # Moving Averages
        if 'sma_20' in selected_indicators and 'sma_20' in df.columns:
            fig.add_trace(go.Scatter(
                x=df['time'], y=df['sma_20'],
                name='SMA 20', 
                line=dict(color='orange', width=1.5),
                mode='lines'
            ))
        
        if 'sma_50' in selected_indicators and 'sma_50' in df.columns:
            fig.add_trace(go.Scatter(
                x=df['time'], y=df['sma_50'],
                name='SMA 50', 
                line=dict(color='blue', width=1.5),
                mode='lines'
            ))
        
        if 'sma_200' in selected_indicators and 'sma_200' in df.columns:
            fig.add_trace(go.Scatter(
                x=df['time'], y=df['sma_200'],
                name='SMA 200', 
                line=dict(color='purple', width=2),
                mode='lines'
            ))
        
        if 'ema_12' in selected_indicators and 'ema_12' in df.columns:
            fig.add_trace(go.Scatter(
                x=df['time'], y=df['ema_12'],
                name='EMA 12', 
                line=dict(color='cyan', width=1.5, dash='dash'),
                mode='lines'
            ))
        
        if 'ema_26' in selected_indicators and 'ema_26' in df.columns:
            fig.add_trace(go.Scatter(
                x=df['time'], y=df['ema_26'],
                name='EMA 26', 
                line=dict(color='magenta', width=1.5, dash='dash'),
                mode='lines'
            ))
        
        # Bollinger Bands
        if 'bollinger' in selected_indicators:
            if 'bb_upper' in df.columns:
                fig.add_trace(go.Scatter(
                    x=df['time'], y=df['bb_upper'],
                    name='BB Upper',
                    line=dict(color='gray', width=1, dash='dot'),
                    showlegend=True
                ))
            if 'bb_middle' in df.columns:
                fig.add_trace(go.Scatter(
                    x=df['time'], y=df['bb_middle'],
                    name='BB Middle',
                    line=dict(color='gray', width=1),
                    showlegend=True
                ))
            if 'bb_lower' in df.columns:
                fig.add_trace(go.Scatter(
                    x=df['time'], y=df['bb_lower'],
                    name='BB Lower',
                    line=dict(color='gray', width=1, dash='dot'),
                    fill='tonexty',
                    fillcolor='rgba(128,128,128,0.1)',
                    showlegend=True
                ))
        
        # Layout
        fig.update_layout(
            title=dict(
                text=f"{self.symbol_select.value} - {title_text} mit Indikatoren",
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

    def create_volume_chart(self, df):
        """Erstellt einen Volume-Chart"""
        import plotly.graph_objects as go
        
        if df.empty:
            return pn.pane.Markdown("")
        
        colors = ['#ef5350' if row['close'] < row['open'] else '#26a69a' 
                  for idx, row in df.iterrows()]
        
        fig = go.Figure(data=[go.Bar(
            x=df['time'],
            y=df['volume'],
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
    
    def create_rsi_chart(self, df):
        """Erstellt einen RSI-Chart"""
        import plotly.graph_objects as go
        
        if df.empty or 'rsi' not in self.indicator_select.value:
            return None
        
        fig = go.Figure()
        
        # RSI Linie
        fig.add_trace(go.Scatter(
            x=df['time'],
            y=df['rsi'],
            name='RSI',
            line=dict(color='#ffeb3b', width=2)
        ))
        
        # Überkauft/Überverkauft Linien
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
    
    def create_macd_chart(self, df):
        """Erstellt einen MACD-Chart"""
        import plotly.graph_objects as go
        
        if df.empty or 'macd' not in self.indicator_select.value:
            return None
        
        fig = go.Figure()
        
        # MACD Linie
        fig.add_trace(go.Scatter(
            x=df['time'],
            y=df['macd'],
            name='MACD',
            line=dict(color='#2196f3', width=2)
        ))
        
        # Signal Linie
        fig.add_trace(go.Scatter(
            x=df['time'],
            y=df['macd_signal'],
            name='Signal',
            line=dict(color='#ff9800', width=2)
        ))
        
        # Histogram
        colors = ['#26a69a' if val >= 0 else '#ef5350' 
                  for val in df['macd_histogram']]
        fig.add_trace(go.Bar(
            x=df['time'],
            y=df['macd_histogram'],
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
    
    def create_statistics_table(self, df):
        """Erstellt eine Statistik-Tabelle"""
        if df.empty:
            stats_data = {
                'Metrik': ['Status'],
                'Wert': ['Keine Daten verfügbar']
            }
            stats_df = pd.DataFrame(stats_data)
        else:
            price_change = ((df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0] * 100)
            price_change_color = "🟢" if price_change >= 0 else "🔴"
            
            stats = {
                'Metrik': [
                    'Anzahl Datenpunkte',
                    'Zeitraum',
                    'Aktueller Preis',
                    'Höchstpreis',
                    'Tiefstpreis',
                    'Durchschnittspreis',
                    'Preisänderung',
                    'RSI (aktuell)',
                    'Volatilität (ATR)'
                ],
                'Wert': [
                    f"{len(df):,}",
                    f"{df['time'].min().date()} bis {df['time'].max().date()}",
                    f"${df['close'].iloc[-1]:.2f}",
                    f"${df['high'].max():.2f}",
                    f"${df['low'].min():.2f}",
                    f"${df['close'].mean():.2f}",
                    f"{price_change_color} {price_change:+.2f}%",
                    f"{df['rsi'].iloc[-1]:.1f}" if 'rsi' in df.columns else "N/A",
                    f"${df['atr'].iloc[-1]:.2f}" if 'atr' in df.columns else "N/A"
                ]
            }
            stats_df = pd.DataFrame(stats)
        
        return pn.widgets.Tabulator(
            stats_df,
            theme='midnight',
            layout='fit_columns',
            height=360,
            disabled=True,
            show_index=False
        )
    
    def update_chart(self, event=None):
        """Aktualisiert alle Charts"""
    
        # DEBUG: Was hat das Update getriggert?
        if event:
            print(f"🔔 Update getriggert durch: {event.name} = {event.new}")
        
        self.status_indicator.value = True
           
        try:
            # Daten laden (mit Indikatoren)
            df = self.load_data()
            
            # Haupt-Charts aktualisieren
            self.main_chart.objects = [self.create_candlestick_chart(df)]
            self.volume_chart.objects = [self.create_volume_chart(df)]
            self.stats_table.objects = [self.create_statistics_table(df)]
            
            # RSI Chart
            rsi_chart = self.create_rsi_chart(df)
            if rsi_chart:
                self.rsi_chart.objects = [rsi_chart]
                self.rsi_chart.visible = True
            else:
                self.rsi_chart.visible = False
            
            # MACD Chart
            macd_chart = self.create_macd_chart(df)
            if macd_chart:
                self.macd_chart.objects = [macd_chart]
                self.macd_chart.visible = True
            else:
                self.macd_chart.visible = False
            
            # Console-Output
            if not df.empty:
                active_indicators = len(self.indicator_select.value)
                print(f"✅ Dashboard aktualisiert: {len(df)} Datenpunkte, "
                      f"{active_indicators} Indikatoren für {self.symbol_select.value}")
            
        except Exception as e:
            error_msg = pn.pane.Alert(
                f"❌ Fehler beim Laden der Daten: {str(e)}",
                alert_type='danger'
            )
            self.main_chart.objects = [error_msg]
            print(f"❌ Dashboard-Fehler: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            self.status_indicator.value = False
    
    def create_layout(self):
        """Erstellt das Layout der Anwendung"""
        
        # Chart-Container OHNE initiale Daten (werden beim ersten Update geladen)
        self.main_chart = pn.Column(
            pn.pane.Markdown("### 🔄 Lade Daten...\n\nBitte warten..."),
            sizing_mode='stretch_width',
            height=600
        )
        
        self.volume_chart = pn.Column(
            pn.pane.Markdown(""),
            sizing_mode='stretch_width',
            height=200
        )
        
        # Indikator-Charts (initial unsichtbar)
        self.rsi_chart = pn.Column(
            sizing_mode='stretch_width',
            height=200,
            visible=False
        )
        
        self.macd_chart = pn.Column(
            sizing_mode='stretch_width',
            height=200,
            visible=False
        )
        
        self.stats_table = pn.Column(
            pn.pane.Markdown("### Statistiken\n\nWerden geladen..."),
            sizing_mode='stretch_width'
        )
        
        # Info-Text aktualisieren
        info_text = pn.pane.Markdown("""
        ### 📊 Anleitung
        
        1. **Symbol** auswählen
        2. **Chart-Typ** wählen
        3. **Zeitraum** anpassen
        4. **Interval** wählen
        5. **Indikatoren** aktivieren
        
        **Chart-Typen:**
        - 🕯️ Candlestick: OHLC-Kerzen
        - 📈 Linie: Close-Preis-Linie
        - 📊 Beides: Beide überlagert
        
        **Verfügbare Indikatoren:**
        - SMA: Simple Moving Average
        - EMA: Exponential Moving Average
        - Bollinger Bands: Volatilitätsbänder
        - RSI: Relative Strength Index
        - MACD: Trend-Momentum
        
        ---
        """)
        
        # Sidebar mit Controls
        sidebar = pn.Column(
            pn.pane.Markdown("## ⚙️ Einstellungen"),
            info_text,
            self.symbol_select,
            self.chart_type_select,  # ← NEU HINZUGEFÜGT
            self.interval_select,
            pn.layout.Divider(),
            pn.pane.Markdown("## 📅 Zeitraum"),
            self.quick_range_buttons, 
            self.start_date_picker,  # ← NEU
            self.end_date_picker,    # ← NEU
            pn.layout.Divider(),
            pn.pane.Markdown("## 📈 Indikatoren"),
            self.indicator_select,
            pn.Row(self.refresh_button, self.status_indicator),
            pn.layout.Divider(),
            pn.pane.Markdown("## 📊 Statistiken"),
            self.stats_table,
            width=350,
            scroll=True
        )
        
        # Hauptbereich
        main_area = pn.Column(
            pn.pane.Markdown(f"# {self.title}"),
            self.main_chart,
            self.volume_chart,
            self.rsi_chart,
            self.macd_chart,
            sizing_mode='stretch_width'
        )
        
        # Gesamtlayout
        self.layout = pn.template.FastListTemplate(
            title=self.title,
            sidebar=[sidebar],
            main=[main_area],
            theme='dark',
            theme_toggle=True,
            header_background='#1f77b4'
        )
        
        # WICHTIG: Initiales Update nach Layout-Erstellung
        # Verwende pn.state.onload für verzögertes Laden
        pn.state.onload(self.update_chart)

    
    def show(self):
        """Zeigt die Anwendung an"""
        return self.layout

# Dashboard-Instanz erstellen
dashboard = StockDashboard()

# Servable machen
dashboard.show().servable()
