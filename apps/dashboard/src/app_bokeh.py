#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 15 18:20:38 2026

@author: twi-dev
"""

#!/usr/bin/env python3
"""
Stock Data Dashboard - Bokeh Version (ohne Plotly)
"""
import panel as pn
import pandas as pd
from datetime import datetime, timedelta
import os

os.environ['NO_AT_BRIDGE'] = '1'

pn.extension('bokeh', 'tabulator')

from apps.data_ingestion.src.database import engine
from apps.dashboard.components.indicators import TechnicalIndicators
from sqlalchemy import text
from bokeh.plotting import figure
from bokeh.models import HoverTool

pn.config.sizing_mode = 'stretch_width'

class StockDashboard:
    """Dashboard mit Bokeh Charts"""
    
    def __init__(self):
        self.title = "📈 Stock Data Platform"
        self.indicators = TechnicalIndicators()
        self.setup_data()
        self.create_widgets()
        self.create_layout()
        pn.state.onload(lambda: self.update_chart())
    
    def setup_data(self):
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT DISTINCT symbol FROM stock_ohlcv ORDER BY symbol
            """))
            self.available_symbols = [row[0] for row in result]
        
        if not self.available_symbols:
            self.available_symbols = ['Keine Daten verfügbar']
    
    def create_widgets(self):
        self.symbol_select = pn.widgets.Select(
            name='Stock Symbol',
            options=self.available_symbols,
            value=self.available_symbols[0],
            width=200
        )
        self.symbol_select.param.watch(self.update_chart, 'value')
        
        self.date_range_slider = pn.widgets.DateRangeSlider(
            name='Zeitraum',
            start=datetime.now() - timedelta(days=365),
            end=datetime.now(),
            value=(datetime.now() - timedelta(days=90), datetime.now()),
            step=86400000
        )
        self.date_range_slider.param.watch(self.update_chart, 'value_throttled')
        
        self.interval_select = pn.widgets.Select(
            name='Interval',
            options=['1day', '1hour', '5min'],
            value='1day',
            width=150
        )
        self.interval_select.param.watch(self.update_chart, 'value')
        
        self.indicator_select = pn.widgets.MultiChoice(
            name='Technische Indikatoren',
            options={
                'SMA 20': 'sma_20',
                'SMA 50': 'sma_50',
                'EMA 12': 'ema_12'
            },
            value=['sma_20'],
            width=250
        )
        self.indicator_select.param.watch(self.update_chart, 'value')
        
        self.refresh_button = pn.widgets.Button(
            name='🔄 Aktualisieren',
            button_type='primary',
            width=200
        )
        self.refresh_button.on_click(self.update_chart)
        
        self.status_indicator = pn.indicators.LoadingSpinner(value=False, width=30, height=30)
    
    def load_data(self):
        try:
            symbol = self.symbol_select.value
            date_range = self.date_range_slider.value
            
            if date_range is None:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=90)
            else:
                start_date, end_date = date_range
            
            interval = self.interval_select.value
            
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
                    'start_date': start_date,
                    'end_date': end_date
                })
                
                rows = result.fetchall()
                if rows:
                    df = pd.DataFrame(rows, columns=['time', 'symbol', 'open', 'high', 'low', 'close', 'volume'])
                else:
                    df = pd.DataFrame()
            
            if not df.empty and len(df) >= 20:
                try:
                    df = self.indicators.add_all_indicators(df)
                except:
                    pass
            
            return df
        except:
            return pd.DataFrame()
    
    def create_candlestick_chart(self, df):
        """Bokeh Candlestick Chart"""
        if df.empty:
            return pn.pane.Markdown("### ⚠️ Keine Daten verfügbar")
        
        # Berechne ob Kerze grün oder rot
        inc = df.close > df.open
        dec = df.open > df.close
        
        w = 12*60*60*1000  # Breite der Kerzen (12 Stunden in ms)
        
        p = figure(
            x_axis_type="datetime",
            width=900,
            height=600,
            title=f"{self.symbol_select.value} - OHLC Chart",
            toolbar_location="above"
        )
        
        # Grüne Kerzen (Steigend)
        p.segment(df.time[inc], df.high[inc], df.time[inc], df.low[inc], color="green")
        p.vbar(df.time[inc], w, df.open[inc], df.close[inc], fill_color="green", line_color="green")
        
        # Rote Kerzen (Fallend)
        p.segment(df.time[dec], df.high[dec], df.time[dec], df.low[dec], color="red")
        p.vbar(df.time[dec], w, df.open[dec], df.close[dec], fill_color="red", line_color="red")
        
        # Indikatoren hinzufügen
        if 'sma_20' in self.indicator_select.value and 'sma_20' in df.columns:
            p.line(df.time, df.sma_20, legend_label="SMA 20", line_width=2, color='orange')
        
        if 'sma_50' in self.indicator_select.value and 'sma_50' in df.columns:
            p.line(df.time, df.sma_50, legend_label="SMA 50", line_width=2, color='blue')
        
        if 'ema_12' in self.indicator_select.value and 'ema_12' in df.columns:
            p.line(df.time, df.ema_12, legend_label="EMA 12", line_width=2, color='cyan', line_dash='dashed')
        
        p.legend.location = "top_left"
        p.legend.background_fill_alpha = 0.5
        
        return pn.pane.Bokeh(p, sizing_mode='stretch_width', height=600)
    
    def create_volume_chart(self, df):
        """Bokeh Volume Chart"""
        if df.empty:
            return pn.pane.Markdown("")
        
        colors = ['red' if row['close'] < row['open'] else 'green' for idx, row in df.iterrows()]
        
        p = figure(
            x_axis_type="datetime",
            width=900,
            height=200,
            title="Handelsvolumen"
        )
        
        p.vbar(x=df.time, top=df.volume, width=12*60*60*1000, color=colors)
        
        return pn.pane.Bokeh(p, sizing_mode='stretch_width', height=200)
    
    def create_statistics_table(self, df):
        if df.empty:
            stats_df = pd.DataFrame({'Metrik': ['Status'], 'Wert': ['Keine Daten']})
        else:
            price_change = ((df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0] * 100)
            stats_df = pd.DataFrame({
                'Metrik': ['Datenpunkte', 'Zeitraum', 'Höchstpreis', 'Tiefstpreis', 'Preisänderung'],
                'Wert': [
                    f"{len(df):,}",
                    f"{df['time'].min().date()} - {df['time'].max().date()}",
                    f"${df['high'].max():.2f}",
                    f"${df['low'].min():.2f}",
                    f"{price_change:+.2f}%"
                ]
            })
        
        return pn.widgets.Tabulator(stats_df, theme='midnight', layout='fit_columns', height=240, disabled=True, show_index=False)
    
    def update_chart(self, event=None):
        self.status_indicator.value = True
        
        try:
            df = self.load_data()
            self.main_chart.objects = [self.create_candlestick_chart(df)]
            self.volume_chart.objects = [self.create_volume_chart(df)]
            self.stats_table.objects = [self.create_statistics_table(df)]
            
            if not df.empty:
                print(f"✅ Dashboard aktualisiert: {len(df)} Datenpunkte")
        except Exception as e:
            print(f"❌ Fehler: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.status_indicator.value = False
    
    def create_layout(self):
        self.main_chart = pn.Column(pn.pane.Markdown("### 🔄 Lade..."), sizing_mode='stretch_width', height=600)
        self.volume_chart = pn.Column(sizing_mode='stretch_width', height=200)
        self.stats_table = pn.Column(pn.pane.Markdown("### Statistiken..."), sizing_mode='stretch_width')
        
        sidebar = pn.Column(
            pn.pane.Markdown("## ⚙️ Einstellungen"),
            self.symbol_select,
            self.interval_select,
            self.date_range_slider,
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
        
        main_area = pn.Column(
            pn.pane.Markdown(f"# {self.title}"),
            self.main_chart,
            self.volume_chart,
            sizing_mode='stretch_width'
        )
        
        self.layout = pn.template.FastListTemplate(
            title=self.title,
            sidebar=[sidebar],
            main=[main_area],
            theme='dark',
            theme_toggle=True,
            header_background='#1f77b4'
        )
    
    def show(self):
        return self.layout

dashboard = StockDashboard()
dashboard.show().servable()
