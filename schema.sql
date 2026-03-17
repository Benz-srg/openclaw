-- openclaw/schema.sql
-- Run these in your Supabase SQL editor for each project

-- ============================================================
-- DB 1: market_data table (Read-Only source of OHLCV data)
-- Run this in your SUPABASE_DATA_URL project
-- ============================================================

CREATE TABLE IF NOT EXISTS market_data (
    id          BIGSERIAL PRIMARY KEY,
    symbol      TEXT        NOT NULL,
    timeframe   TEXT        NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,
    open        NUMERIC     NOT NULL,
    high        NUMERIC     NOT NULL,
    low         NUMERIC     NOT NULL,
    close       NUMERIC     NOT NULL,
    volume      NUMERIC     NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE (symbol, timeframe, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_market_data_lookup
    ON market_data (symbol, timeframe, timestamp DESC);

-- ============================================================
-- DB 2: indicators table (Write-Only destination)
-- Run this in your SUPABASE_INDICATOR_URL project
-- ============================================================

CREATE TABLE IF NOT EXISTS indicators (
    id              BIGSERIAL PRIMARY KEY,
    symbol          TEXT        NOT NULL,
    timeframe       TEXT        NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    rsi_14          NUMERIC,
    ema_21          NUMERIC,
    ema_50          NUMERIC,
    macd_line       NUMERIC,
    macd_signal     NUMERIC,
    macd_histogram  NUMERIC,
    updated_at      TIMESTAMPTZ,
    UNIQUE (symbol, timeframe, timestamp)  -- required for ON CONFLICT upsert
);

CREATE INDEX IF NOT EXISTS idx_indicators_lookup
    ON indicators (symbol, timeframe, timestamp DESC);
