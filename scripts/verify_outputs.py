"""Fail a demo/DAG when outputs or accounting checks are missing."""
import argparse
import csv
import json
from pathlib import Path


def verify(root, require_stream=False):
    quality = json.loads((root / "data_quality.json").read_text(encoding="utf-8"))
    assert quality["row_balance_ok"], "Raw/clean/quarantine/duplicate counts do not balance"
    assert quality["raw_rows"] == (quality["clean_rows"] + quality["quarantined_rows"]
        + quality["duplicate_valid_id_rows_removed"])
    assert quality["clean_rows"] > 0
    inspection = json.loads((root / "raw_inspection.json").read_text(encoding="utf-8"))
    assert inspection.get("audit_rows_match", True), "Spark disagrees with the independent CSV audit"
    success = json.loads((root / "batch_success.json").read_text(encoding="utf-8"))
    for name in success["exported_tables"]:
        with (root / f"{name}.csv").open(encoding="utf-8-sig", newline="") as handle:
            records = list(csv.DictReader(handle))
        assert records, f"Empty table: {name}"
    clusters = json.loads((root / "cluster_metadata.json").read_text(encoding="utf-8"))
    assert clusters["training_rows"] > clusters["k"]
    assert -1 <= clusters["silhouette_cosine"] <= 1
    if require_stream:
        metrics = [json.loads(path.read_text(encoding="utf-8")) for path in root.glob("stream_batch_*.json")]
        assert metrics and sum(metric["valid"] for metric in metrics) > 0, "No valid streaming events"
        assert all(m["new_unique_events"] == m["valid"] + m["invalid"] for m in metrics)
    print(json.dumps({"verification": "passed", "clean_rows": quality["clean_rows"],
        "stream_checked": require_stream}, ensure_ascii=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/exports"))
    parser.add_argument("--require-stream", action="store_true")
    args = parser.parse_args()
    verify(args.root, args.require_stream)

