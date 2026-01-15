-- TimescaleDB Extension aktivieren
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Tabelle für Stock Quotes (z.B. minutengenaue Daten)
CREATE TABLE stock_quotes (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    exchange VARCHAR(20),
    price DECIMAL(12, 4),
    volume BIGINT,
    bid DECIMAL(12, 4),
    ask DECIMAL(12, 4),
    bid_size INTEGER,
    ask_size INTEGER,
    PRIMARY KEY (symbol, time)
);

-- Als Hypertable konvertieren (wichtig für TimescaleDB!)
SELECT create_hypertable('stock_quotes', 'time');

-- Tabelle für aggregierte Daten (OHLCV)
CREATE TABLE stock_ohlcv (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    "open" DECIMAL(12, 4),
    high DECIMAL(12, 4),
    low DECIMAL(12, 4),
    "closed" DECIMAL(12, 4),
    volume BIGINT,
    interval VARCHAR(10), -- '1min', '5min', '15min', '1hour', '4hour', '1day', '1week'
    PRIMARY KEY (symbol, time, interval)
);

SELECT create_hypertable('stock_ohlcv', 'time');

-- Metadaten-Tabelle für Symbole
CREATE TABLE stock_metadata (
    symbol VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(255),
    exchange VARCHAR(20),
    sector VARCHAR(100),
    industry VARCHAR(100),
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

-- Indizes für bessere Performance
CREATE INDEX idx_quotes_symbol_time ON stock_quotes (symbol, time DESC);
CREATE INDEX idx_ohlcv_symbol_interval ON stock_ohlcv (symbol, interval, time DESC);

-- Continuous Aggregate für 5-Minuten-Kerzen
CREATE MATERIALIZED VIEW stock_5min_candles
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', time) AS bucket,
    symbol,
    FIRST(price, time) AS "open",
    MAX(price) AS high,
    MIN(price) AS low,
    LAST(price, time) AS "closed",
    SUM(volume) AS volume
FROM stock_quotes
GROUP BY bucket, symbol;

-- Refresh Policy (automatisch aktualisieren)
SELECT add_continuous_aggregate_policy('stock_5min_candles',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '5 minutes');

-- Compression Policy (Daten älter als 7 Tage komprimieren)
SELECT add_compression_policy('stock_quotes', INTERVAL '7 days');

-- Retention Policy (Rohdaten älter als 90 Tage löschen)
SELECT add_retention_policy('stock_quotes', INTERVAL '90 days');
