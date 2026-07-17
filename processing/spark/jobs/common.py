import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

TRADE_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("volume", DoubleType(), False),
        StructField("ts", LongType(), False),
    ]
)


def build_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # S3A -> MinIO
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("S3_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("S3_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def parse_trades(raw: DataFrame) -> DataFrame:
    """Kafka value bytes -> typed trade rows with an event_time column."""
    return (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(F.from_json("json", TRADE_SCHEMA).alias("t"))
        .select("t.*")
        .where(F.col("symbol").isNotNull())
        .withColumn("event_time", F.timestamp_millis(F.col("ts")))
    )


def read_trades_stream(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BROKER", "redpanda:9092"))
        .option("subscribe", os.environ.get("TRADES_TOPIC", "trades.raw"))
        .option("startingOffsets", "latest")
        .load()
    )
    return parse_trades(raw)
