"""Kafka trades -> raw Parquet archive on MinIO (bronze layer).

Append-only, partitioned by trade date and symbol.
"""

import os

from pyspark.sql import functions as F

from jobs.common import build_session, read_trades_stream


def main() -> None:
    spark = build_session("trades-to-bronze")
    spark.sparkContext.setLogLevel("WARN")

    trades = read_trades_stream(spark).withColumn("trade_date", F.to_date("event_time"))
    query = (
        trades.writeStream.format("parquet")
        .outputMode("append")
        .option("path", os.environ.get("BRONZE_PATH", "s3a://bronze/trades"))
        .option(
            "checkpointLocation",
            os.environ.get("CHECKPOINT_LOCATION", "s3a://checkpoints/trades_to_bronze"),
        )
        .partitionBy("trade_date", "symbol")
        .trigger(processingTime="60 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
