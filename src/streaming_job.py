"""Real-time layer: Spark Structured Streaming over live events.

Reads from Kafka (or a file source for local runs), applies a watermark so
late-arriving events are still counted, aggregates per room into 1-minute
tumbling windows, and writes Parquet that the batch warehouse loads.

    python -m src.streaming_job --source file --seconds 90
    python -m src.streaming_job --source kafka --bootstrap localhost:9092
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType,
)

TOPIC = "live_events"

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("ingest_ts", TimestampType()),
    StructField("room_id", StringType()),
    StructField("user_id", StringType()),
    StructField("country", StringType()),
    StructField("gift_name", StringType()),
    StructField("coins", IntegerType()),
    StructField("comment_len", IntegerType()),
])


def build_spark(app="live-streaming-warehouse"):
    return (
        SparkSession.builder.appName(app)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )


def read_stream(spark, source, bootstrap, indir):
    """Kafka and file sources both land on the same parsed schema."""
    if source == "kafka":
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "latest")
            .load()
            .select(F.col("value").cast("string").alias("json"))
        )
    else:
        raw = (
            spark.readStream.format("text")
            .option("maxFilesPerTrigger", 4)
            .load(indir)
            .select(F.col("value").alias("json"))
        )
    return raw.select(F.from_json("json", EVENT_SCHEMA).alias("e")).select("e.*")


def aggregate(events, window="30 seconds", watermark="20 seconds"):
    """Per-room, per-window engagement metrics.

    The watermark bounds state size and still admits events that arrive late,
    which the producer deliberately generates.
    """
    return (
        events.withWatermark("event_ts", watermark)
        .groupBy(F.window("event_ts", window).alias("w"), "room_id")
        .agg(
            F.approx_count_distinct("user_id").alias("unique_viewers"),
            F.count("*").alias("events"),
            F.sum(F.when(F.col("event_type") == "gift", 1).otherwise(0)).alias("gifts"),
            F.sum("coins").alias("coins"),
            F.sum(F.when(F.col("event_type") == "comment", 1).otherwise(0)).alias("comments"),
            F.max("ingest_ts").alias("last_ingest_ts"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "room_id", "unique_viewers", "events", "gifts", "coins", "comments",
            "last_ingest_ts",
        )
    )


def main(a):
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    events = read_stream(spark, a.source, a.bootstrap, a.indir)

    # Sink 1: raw events, appended for the batch/offline warehouse to model.
    raw_q = (
        events.writeStream.format("parquet")
        .option("path", a.raw_out)
        .option("checkpointLocation", a.raw_out + "/_ckpt")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # Sink 2: windowed metrics, the real-time serving table.
    agg_q = (
        aggregate(events, a.window, a.watermark).writeStream.format("parquet")
        .option("path", a.agg_out)
        .option("checkpointLocation", a.agg_out + "/_ckpt")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print(f"streaming started (source={a.source}); running {a.seconds}s")
    raw_q.awaitTermination(a.seconds)
    agg_q.awaitTermination(5)
    for q in (raw_q, agg_q):
        q.stop()
    print("streaming stopped")
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--source", choices=["kafka", "file"], default="file")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--indir", default="data/raw")
    p.add_argument("--raw-out", default="data/lake/events")
    p.add_argument("--agg-out", default="data/lake/room_metrics")
    p.add_argument("--window", default="30 seconds")
    p.add_argument("--watermark", default="20 seconds")
    p.add_argument("--seconds", type=int, default=90)
    main(p.parse_args())
