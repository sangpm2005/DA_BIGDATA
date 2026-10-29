"""Check the published evidence bundle without Docker or the raw dataset.

This validates stored artifacts and their consistency. It does not execute or
benchmark HDFS, Spark, Kafka, Airflow or the dashboard.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


def audit(root: Path) -> dict:
    bundle = root / "evidence/verified_20261028"
    results = []

    def check(name, condition, detail):
        results.append({"check": name, "passed": bool(condition), "detail": detail})

    def read(name):
        return json.loads((bundle / name).read_text(encoding="utf-8-sig"))

    def csv_rows(name):
        with (bundle / name).open(encoding="utf-8-sig", newline="") as handle:
            return list(csv.DictReader(handle))

    manifest = read("MANIFEST_SHA256.json")
    bad = []
    for entry in manifest["files"]:
        path = (bundle / entry["path"]).resolve()
        if not path.is_relative_to(bundle.resolve()) or not path.is_file():
            bad.append(entry["path"])
            continue
        data = path.read_bytes()
        if len(data) != entry["bytes"] or hashlib.sha256(data).hexdigest() != entry["sha256"]:
            bad.append(entry["path"])
    check("manifest_sha256", not bad,
          {"files": len(manifest["files"]), "mismatched_or_missing": bad})

    raw = read("raw_inspection.json")
    quality = read("data_quality.json")
    check("spark_hdfs_input", raw["input"].startswith("hdfs://")
          and raw["raw_rows"] == quality["raw_rows"] == 1300086,
          {"input": raw["input"], "rows": raw["raw_rows"],
           "reader_partitions": raw["reader_partitions"]})
    check("clean_row_balance", quality["raw_rows"] == quality["quarantined_rows"]
          + quality["duplicate_valid_id_rows_removed"] + quality["clean_rows"], quality)
    platform_total = sum(int(r["reviews"]) for r in csv_rows("platform_summary.csv"))
    low_total = sum(int(r["low_rating_reviews"]) for r in csv_rows("platform_summary.csv"))
    check("sql_platform_totals", platform_total == quality["clean_rows"] == 1299873
          and low_total == 282772, {"reviews": platform_total, "low_rated_reviews": low_total})
    hdfs = (bundle / "logs/hdfs_blocks.txt").read_text(encoding="utf-8")
    check("hdfs_fsck_log", all(s in hdfs for s in
          ["642967981 bytes", "5 block(s)", "Status: HEALTHY"]),
          "Stored fsck log; execution date is inside the log")
    plan = (bundle / "spark_query_plan.txt").read_text(encoding="utf-8")
    check("spark_sql_plan", all(s in plan for s in
          ["Exchange hashpartitioning", "PartitionFilters:", "ReadSchema: struct<month:string>"]),
          "Stored plan shows shuffle, partition filters and column projection")

    historic = read("demo_evidence.json")
    tasks = historic["airflow"]["tasks"]
    check("airflow_recorded_run", historic["airflow"]["state"] == "success"
          and len(tasks) == 9 and all(t["state"] == "success" for t in tasks),
          {"run_id": historic["airflow"]["run_id"], "successful_tasks": len(tasks)})
    task_log = (bundle / "logs/dashboard_airflow_task09.log").read_text(encoding="utf-8")
    check("airflow_extended_task_log", "Command exited with return code 0" in task_log
          and "Marking task as SUCCESS" in task_log,
          "Extended task 09 has its own log, separate from the original run record")
    first_six = [read(f"stream_batch_{i}.json")["new_unique_events"] for i in range(6)]
    check("kafka_original_replay", first_six == [499, 498, 3, 0, 0, 0]
          and sum(historic["kafka_topic_end_offsets"].values()) == 2000,
          {"new_unique_events": first_six, "partition_end_offsets": historic["kafka_topic_end_offsets"]})
    persisted = read("persisted_validation.json")
    check("persisted_hdfs_counts", persisted["verification"] == "passed"
          and persisted["processed_rows"] == persisted["processed_unique_keys"] == platform_total
          and persisted["streaming_rows"] == persisted["streaming_unique_keys"] == 1000
          and persisted["streaming_batches"] == 12,
          persisted)
    snapshot = read("dashboard_validation.json")
    check("dashboard_snapshot_audit", snapshot["all_spark_cube_counts_match_full_index"]
          and snapshot["indexed_reviews"] == platform_total
          and snapshot["cube_cells_verified"] == 1910 and snapshot["metrics_per_cell"] == 9,
          snapshot)
    check("dashboard_images", all((bundle / "screenshots" / name).is_file() for name in
          ["01-overview.jpg", "02-aspects.jpg", "03-pipeline.jpg"]),
          "Historical product images, not a live availability check")
    return {"checked_utc": datetime.now(timezone.utc).isoformat(),
            "scope": "stored evidence integrity and cross-artifact consistency only",
            "full_pipeline_rerun": False, "passed": all(r["passed"] for r in results),
            "checks": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = audit(args.root.resolve())
    for item in result["checks"]:
        print(f"{'PASS' if item['passed'] else 'FAIL'} {item['check']}")
    print("Scope: stored evidence checks; the full pipeline was not rerun.")
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    raise SystemExit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
