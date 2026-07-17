-- Our bind mount replaces the image's /docker-entrypoint-initdb.d (which is what
-- normally creates the extension), so create it here explicitly.
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Serving table: 1-minute OHLCV candles, upserted by the processing layer.
CREATE TABLE IF NOT EXISTS ohlcv_1m (
    symbol       TEXT             NOT NULL,
    bucket_start TIMESTAMPTZ      NOT NULL,
    open         DOUBLE PRECISION NOT NULL,
    high         DOUBLE PRECISION NOT NULL,
    low          DOUBLE PRECISION NOT NULL,
    close        DOUBLE PRECISION NOT NULL,
    volume       DOUBLE PRECISION NOT NULL DEFAULT 0,
    trade_count  BIGINT           NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, bucket_start)
);
