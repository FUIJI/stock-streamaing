import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from jobs.common import TRADE_SCHEMA, parse_trades
from jobs.ohlcv_aggregation import aggregate_ohlcv

BASE_MS = 1_784_284_800_000  # 2026-07-17 10:40:00 UTC


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("processing-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def make_trades(spark, rows):
    return spark.createDataFrame(rows, schema=TRADE_SCHEMA).withColumn(
        "event_time", F.timestamp_millis(F.col("ts"))
    )


def test_ohlcv_single_window(spark):
    trades = make_trades(
        spark,
        [
            ("AAPL", 100.0, 10.0, BASE_MS + 1_000),
            ("AAPL", 105.0, 5.0, BASE_MS + 20_000),
            ("AAPL", 95.0, 2.0, BASE_MS + 40_000),
            ("AAPL", 102.0, 3.0, BASE_MS + 59_000),
        ],
    )
    [row] = aggregate_ohlcv(trades).collect()

    assert row.symbol == "AAPL"
    assert row.open == 100.0
    assert row.high == 105.0
    assert row.low == 95.0
    assert row.close == 102.0
    assert row.volume == 20.0
    assert row.trade_count == 4


def test_ohlcv_splits_windows_and_symbols(spark):
    trades = make_trades(
        spark,
        [
            ("AAPL", 100.0, 1.0, BASE_MS + 1_000),
            ("AAPL", 110.0, 1.0, BASE_MS + 61_000),  # next minute
            ("MSFT", 400.0, 1.0, BASE_MS + 2_000),
        ],
    )
    out = {(r.symbol, r.bucket_start.minute): r for r in aggregate_ohlcv(trades).collect()}

    assert len(out) == 3
    assert out[("AAPL", 40)].close == 100.0
    assert out[("AAPL", 41)].open == 110.0
    assert out[("MSFT", 40)].trade_count == 1


def test_parse_trades_drops_malformed(spark):
    raw = spark.createDataFrame(
        [
            ('{"symbol":"AAPL","price":10.5,"volume":1.0,"ts":%d}' % BASE_MS,),
            ("not json at all",),
        ],
        ["value"],
    )
    rows = parse_trades(raw).collect()
    assert len(rows) == 1
    assert rows[0].symbol == "AAPL"
    assert rows[0].event_time is not None
