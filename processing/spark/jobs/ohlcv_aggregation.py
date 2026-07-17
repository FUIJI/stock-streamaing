"""Kafka trades -> 1-minute OHLCV candles -> upsert into TimescaleDB.

Event-time windowing with a 30s watermark; update output mode so the
currently-forming candle is upserted every trigger instead of appearing
only after the watermark closes the window.
"""

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from jobs.common import build_session, read_trades_stream

UPSERT_SQL = """
    INSERT INTO ohlcv_1m
        (symbol, bucket_start, open, high, low, close, volume, trade_count, updated_at)
    VALUES %s
    ON CONFLICT (symbol, bucket_start) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        trade_count = EXCLUDED.trade_count,
        updated_at = now()
"""
UPSERT_TEMPLATE = "(%s, %s, %s, %s, %s, %s, %s, %s, now())"


def aggregate_ohlcv(trades: DataFrame) -> DataFrame:
    # first/last are non-deterministic under aggregation, so order open/close
    # by trade timestamp via struct min/max instead.
    return (
        trades.withWatermark("event_time", "30 seconds")
        .groupBy(F.window("event_time", "1 minute").alias("w"), "symbol")
        .agg(
            F.min(F.struct("ts", "price")).alias("first_trade"),
            F.max(F.struct("ts", "price")).alias("last_trade"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.sum("volume").alias("volume"),
            F.count(F.lit(1)).alias("trade_count"),
        )
        .select(
            "symbol",
            F.col("w.start").alias("bucket_start"),
            F.col("first_trade.price").alias("open"),
            "high",
            "low",
            F.col("last_trade.price").alias("close"),
            "volume",
            "trade_count",
        )
    )


def upsert_to_timescale(batch_df: DataFrame, batch_id: int) -> None:
    rows = [
        (r.symbol, r.bucket_start, r.open, r.high, r.low, r.close, r.volume, r.trade_count)
        for r in batch_df.collect()
    ]
    if not rows:
        return

    import psycopg2
    from psycopg2.extras import execute_values

    conn = psycopg2.connect(
        host=os.environ.get("PG_HOST", "timescaledb"),
        port=int(os.environ.get("PG_PORT", "5432")),
        dbname=os.environ.get("PG_DB", "stocks"),
        user=os.environ.get("PG_USER", "stock"),
        password=os.environ.get("PG_PASSWORD", "stock_dev_password"),
    )
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, rows, template=UPSERT_TEMPLATE)
    finally:
        conn.close()


def main() -> None:
    spark = build_session("ohlcv-aggregation")
    spark.sparkContext.setLogLevel("WARN")

    candles = aggregate_ohlcv(read_trades_stream(spark))
    query = (
        candles.writeStream.outputMode("update")
        .foreachBatch(upsert_to_timescale)
        .option(
            "checkpointLocation",
            os.environ.get("CHECKPOINT_LOCATION", "s3a://checkpoints/ohlcv_aggregation"),
        )
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
