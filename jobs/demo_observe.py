"""Read existing HDFS outputs and keep their Spark application UI available for a talk.

This observes stored pipeline results. It does not retrain models, rewrite HDFS,
or pretend to be a fresh execution of all nine stages.
"""
import argparse
from contextlib import redirect_stdout
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import time

from pyspark.sql import SparkSession, functions as F

from review_analytics.io import atomic_write_text
from review_analytics.spark_ops import read_raw


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="hdfs://namenode:9000/ecommerce")
    parser.add_argument("--exports", type=Path, required=True)
    parser.add_argument("--hold-seconds", type=int, default=7200)
    args = parser.parse_args()
    if args.hold_seconds < 0:
        parser.error("hold-seconds must be nonnegative")
    args.exports.mkdir(parents=True, exist_ok=True)
    spark = (SparkSession.builder.appName("Bigdata-5min-observation")
        .config("spark.cores.max", 4).config("spark.executor.cores", 2)
        .config("spark.sql.shuffle.partitions", 8)
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    def save(name, value):
        atomic_write_text(args.exports / name, json.dumps(value, ensure_ascii=False, indent=2))

    try:
        spark.sparkContext.setJobGroup("03-read-source", "Read original multiline CSV from HDFS")
        raw = read_raw(spark, args.root + "/raw/reviews.csv")
        raw_n = raw.count()
        save("02_read.json", {"raw_rows": raw_n, "reader_partitions": raw.rdd.getNumPartitions(),
            "schema": raw.select("appid", "content", "score", "at").schema.jsonValue(),
            "source": args.root + "/raw/reviews.csv"})

        spark.sparkContext.setJobGroup("04-read-clean-output", "Verify persisted clean keys and quarantine")
        reviews = spark.read.parquet(args.root + "/processed/reviews")
        clean = reviews.agg(F.count("*").alias("rows"),
            F.countDistinct("review_key").alias("unique_keys")).first().asDict()
        invalid_n = spark.read.parquet(args.root + "/quarantine/reviews").count()
        quality = json.loads(Path("/exports/data_quality.json").read_text(encoding="utf-8"))
        if clean["rows"] != clean["unique_keys"] or clean["rows"] != quality["clean_rows"]:
            raise ValueError("Persisted clean rows disagree with their unique keys or quality metadata")
        if raw_n != clean["rows"] + invalid_n + quality["duplicate_valid_id_rows_removed"]:
            raise ValueError("Raw/clean/quarantine/duplicate accounting does not balance")
        save("03_quality.json", {"raw_rows": raw_n, "clean_rows": clean["rows"],
            "clean_unique_keys": clean["unique_keys"], "quarantined_rows": invalid_n,
            "duplicate_valid_id_rows_removed": quality["duplicate_valid_id_rows_removed"],
            "row_balance_ok": True, "source": "Fresh reads of stored HDFS outputs"})

        spark.sparkContext.setJobGroup("05-spark-sql", "Aggregate all stored reviews and inspect partition pruning")
        reviews.createOrReplaceTempView("reviews")
        summary = spark.sql("""SELECT platform, COUNT(*) AS reviews,
            SUM(CASE WHEN low_rating_proxy THEN 1 ELSE 0 END) AS low_rating_reviews,
            CAST(ROUND(100.0 * AVG(CASE WHEN low_rating_proxy THEN 1.0 ELSE 0.0 END), 2) AS DOUBLE) AS low_rating_pct
            FROM reviews GROUP BY platform ORDER BY platform""")
        summary_rows = [row.asDict() for row in summary.collect()]
        save("04_sql.json", summary_rows)
        filtered = spark.sql("""SELECT platform, month, COUNT(*) AS reviews FROM reviews
            WHERE platform = 'Shopee' AND year = 2022
            GROUP BY platform, month ORDER BY month""")
        filtered.show(12, truncate=False)
        plan_text = io.StringIO()
        with redirect_stdout(plan_text):
            filtered.explain("formatted")
        atomic_write_text(args.exports / "04_sql_plan.txt", plan_text.getvalue())

        spark.sparkContext.setJobGroup("06-read-model-output", "Read existing cluster assignment counts")
        assignments = spark.read.parquet(args.root + "/output/cluster_assignments_sample")
        groups = assignments.groupBy("prediction").count().orderBy("prediction").collect()
        metadata = json.loads(Path("/exports/cluster_metadata.json").read_text(encoding="utf-8"))
        save("05_model.json", {key: metadata[key] for key in
            ["k", "training_rows", "sample_percent", "silhouette_cosine"]} |
            {"assignment_counts": [row.asDict() for row in groups],
             "mode": "Read existing model metadata and assignments; no model fitting"})

        spark.sparkContext.setJobGroup("08-read-stream-output", "Verify existing streaming unique keys")
        stream = spark.read.parquet(args.root + "/streaming/reviews")
        stream_counts = stream.agg(F.count("*").alias("rows"),
            F.countDistinct("review_key").alias("unique_keys")).first().asDict()
        if stream_counts["rows"] != stream_counts["unique_keys"]:
            raise ValueError("Streaming output contains duplicate or missing keys")
        metrics = spark.read.option("recursiveFileLookup", True).json(args.root + "/streaming/metrics")
        latest = metrics.orderBy(F.col("batch_id").desc()).first()
        save("06_stream.json", {"streaming_rows": stream_counts["rows"],
            "streaming_unique_keys": stream_counts["unique_keys"],
            "latest_batch": latest.asDict() if latest else None,
            "source": "Fresh reads of stored HDFS streaming output and metrics"})

        now = datetime.now(timezone.utc).isoformat()
        save("READY.json", {"state": "ready", "prepared_utc": now,
            "application_id": spark.sparkContext.applicationId, "spark_ui": "http://localhost:4040",
            "mode": "Read-only observation of stored pipeline outputs",
            "hold_seconds": args.hold_seconds, "stop_file": str(args.exports / "STOP")})
        print("DEMO READY: existing HDFS results have been read and verified.", flush=True)
        print("Spark UI remains available. Create the session STOP file to release resources.", flush=True)
        deadline = time.monotonic() + args.hold_seconds
        while time.monotonic() < deadline and not (args.exports / "STOP").exists():
            time.sleep(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
