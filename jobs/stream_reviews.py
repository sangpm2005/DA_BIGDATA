"""Bounded replay demo, using durable HDFS checkpoint and stateful dedup."""
import argparse
import json
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T

from review_analytics.spark_ops import analytical_columns, enrich, validation_reasons
from review_analytics.io import atomic_write_text

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()), T.StructField("appid", T.StringType()),
    T.StructField("content", T.StringType()), T.StructField("score", T.IntegerType()),
    T.StructField("event_time", T.StringType()), T.StructField("ingested_at", T.StringType()),
    T.StructField("source_mode", T.StringType()),
])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="kafka:29092")
    parser.add_argument("--topic", default="ecommerce-reviews")
    parser.add_argument("--root", default="hdfs://namenode:9000/ecommerce")
    parser.add_argument("--continuous", action="store_true")
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("ecommerce-streaming").config("spark.cores.max", 4)
        .config("spark.executor.cores", 2).config("spark.sql.shuffle.partitions", 8)
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    kafka = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic).option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 500).option("failOnDataLoss", "true").load())
    events = (kafka.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"),
        "topic", "partition", "offset").select("event.*", "topic", "partition", "offset")
        .withColumn("dedup_key", F.coalesce("event_id", F.concat_ws(":", "topic", "partition", "offset")))
        .dropDuplicates(["dedup_key"]))

    def write_batch(frame, batch_id):
        staged = (frame.withColumn("reviewid", F.col("event_id"))
            .withColumn("at", F.col("event_time")).withColumn("_corrupt_record", F.lit(None).cast("string")))
        enriched = validation_reasons(enrich(staged)).withColumn("review_key", F.col("event_id")) \
            .persist(StorageLevel.MEMORY_AND_DISK)
        valid = enriched.filter(F.size("validation_reasons") == 0)
        invalid = enriched.filter(F.size("validation_reasons") > 0)
        total_n, valid_n, invalid_n = enriched.count(), valid.count(), invalid.count()
        (analytical_columns(valid).write.mode("overwrite")
            .parquet(f"{args.root}/streaming/reviews/batch_id={batch_id}"))
        invalid.select("event_id", "topic", "partition", "offset", "validation_reasons").write.mode("overwrite") \
            .parquet(f"{args.root}/quarantine/streaming/batch_id={batch_id}")
        meta = {"batch_id": batch_id, "new_unique_events": total_n, "valid": valid_n, "invalid": invalid_n,
            "source_mode": "historical_replay", "checkpoint": f"{args.root}/checkpoints/reviews-v1"}
        spark.read.json(spark.sparkContext.parallelize([json.dumps(meta)])) \
            .coalesce(1).write.mode("overwrite").json(f"{args.root}/streaming/metrics/batch_id={batch_id}")
        atomic_write_text(Path(f"/exports/stream_batch_{batch_id}.json"), json.dumps(meta, indent=2))
        enriched.unpersist()
        print(json.dumps(meta), flush=True)

    writer = events.writeStream.foreachBatch(write_batch).option(
        "checkpointLocation", f"{args.root}/checkpoints/reviews-v1")
    writer = writer.trigger(processingTime="10 seconds") if args.continuous else writer.trigger(availableNow=True)
    try:
        query = writer.start()
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

