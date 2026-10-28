from __future__ import annotations

import argparse
from contextlib import closing
import csv
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile
import threading
import unicodedata
from urllib.parse import parse_qs, urlsplit

PLATFORMS = ["Shopee", "Lazada", "Sendo", "Tiki"]
WEB = Path(__file__).resolve().parent / "web"


def read_json(path, fallback=None):
    return json.loads(path.read_text(encoding="utf-8-sig")) if path.exists() else fallback


def query_text(value):
    value = unicodedata.normalize("NFKD", value.lower().replace("đ", "d"))
    value = "".join(c for c in value if unicodedata.category(c) != "Mn")
    terms = re.findall(r"[^\W_]+", value, re.UNICODE)[:16]
    return " AND ".join('"' + term + '"' for term in terms)


def csv_safe(value):
    value = str(value)
    return "'" + value if value.lstrip().startswith(("=", "+", "-", "@")) else value


class Store:
    def __init__(self, root, cache=None):
        self.root = Path(root)
        self.cache = Path(cache) if cache else None
        self.index_path = self.root / "review_search.sqlite"
        self.lock = threading.Lock()
        self.key, self.cube, self.metadata = None, None, None

    def connect(self, path=None):
        connection = sqlite3.connect((path or self.index_path).resolve().as_uri() + "?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        return connection

    def index_metadata(self, path):
        with closing(self.connect(path)) as connection:
            return json.loads(connection.execute("SELECT value FROM metadata WHERE key='snapshot'").fetchone()[0])

    def cache_index(self, generation):
        # Docker Desktop bind mounts make random SQLite reads expensive. Keep an
        # immutable serving copy on the Linux volume; HDFS-derived exports remain
        # the source of truth. Replace only after the copied generation is checked.
        if not self.cache:
            return
        self.cache.mkdir(parents=True, exist_ok=True)
        target = self.cache / "review_search.sqlite"
        if target.exists():
            try:
                if self.index_metadata(target)["generation"] == generation:
                    self.index_path = target
                    return
            except (sqlite3.Error, KeyError, ValueError):
                pass
        descriptor, name = tempfile.mkstemp(prefix="review-index-", suffix=".sqlite", dir=self.cache)
        os.close(descriptor)
        temporary = Path(name)
        try:
            shutil.copyfile(self.root / "review_search.sqlite", temporary)
            if self.index_metadata(temporary)["generation"] != generation:
                raise RuntimeError("Chỉ mục đang đổi phiên dữ liệu. Vui lòng thử lại sau.")
            os.replace(temporary, target)
            self.index_path = target
        finally:
            temporary.unlink(missing_ok=True)

    def snapshot(self):
        with self.lock:
            metadata = read_json(self.root / "dashboard_preparation.json")
            if not metadata or not (self.root / "review_search.sqlite").exists():
                raise RuntimeError("Chưa có snapshot dashboard. Cần hoàn tất bước chuẩn bị dữ liệu và chỉ mục.")
            key = metadata["generation"]
            if self.key != key:
                index = self.index_metadata(self.root / "review_search.sqlite")
                if index["generation"] != key:
                    raise RuntimeError("Snapshot đang được cập nhật. Vui lòng thử lại sau khi job hoàn tất.")
                with (self.root / "dashboard_cube.csv").open(encoding="utf-8-sig", newline="") as handle:
                    rows = list(csv.DictReader(handle))
                if any(row["generation"] != key for row in rows):
                    raise RuntimeError("Các tệp tổng hợp chưa cùng phiên dữ liệu.")
                for row in rows:
                    for field in ["reviews", "score", "unmatched_reviews"] + ["aspect_" + a["code"] for a in metadata["aspects"]]:
                        row[field] = int(row[field])
                    row["spam_candidate"] = row["spam_candidate"].lower() == "true"
                if sum(row["reviews"] for row in rows) != index["indexed_rows"]:
                    raise RuntimeError("Số dòng trong biểu đồ và chỉ mục không khớp.")
                self.cache_index(key)
                self.key, self.cube, self.metadata = key, rows, {**metadata, "indexed_rows": index["indexed_rows"]}
            return self.metadata, self.cube

    def filters(self, params, metadata):
        platforms = params.get("platforms", [",".join(PLATFORMS)])[0].split(",")
        if not platforms or any(platform not in PLATFORMS for platform in platforms):
            raise ValueError("Chọn ít nhất một sàn hợp lệ.")
        start = params.get("from", [metadata["month_min"]])[0]
        end = params.get("to", [metadata["month_max"]])[0]
        if (not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", start)
                or not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", end)
                or not metadata["month_min"] <= start <= end <= metadata["month_max"]):
            raise ValueError("Khoảng tháng phải nằm trong phạm vi dữ liệu và từ tháng không lớn hơn đến tháng.")
        return {"platforms": list(dict.fromkeys(platforms)), "from": start, "to": end,
            "exclude_spam": params.get("exclude_spam", ["0"])[0] == "1"}

    def overview(self, params):
        metadata, cube = self.snapshot()
        filters = self.filters(params, metadata)
        selected = [row for row in cube if row["platform"] in filters["platforms"]
            and filters["from"] <= row["month"] <= filters["to"]
            and not (filters["exclude_spam"] and row["spam_candidate"])]
        by_platform = {p: {"platform": p, "reviews": 0, "low": 0, "rating_sum": 0,
            "unmatched": 0, "ratings": [0] * 5, "aspects": {a["code"]: 0 for a in metadata["aspects"]}}
            for p in filters["platforms"]}
        series = {}
        for row in selected:
            platform = by_platform[row["platform"]]
            platform["reviews"] += row["reviews"]
            platform["rating_sum"] += row["reviews"] * row["score"]
            platform["ratings"][row["score"] - 1] += row["reviews"]
            point = series.setdefault((row["platform"], row["month"]),
                {"platform": row["platform"], "month": row["month"], "reviews": 0, "low": 0})
            point["reviews"] += row["reviews"]
            if row["score"] <= 2:
                platform["low"] += row["reviews"]
                platform["unmatched"] += row["unmatched_reviews"]
                point["low"] += row["reviews"]
                for aspect in metadata["aspects"]:
                    platform["aspects"][aspect["code"]] += row["aspect_" + aspect["code"]]
        platforms = list(by_platform.values())
        for platform in platforms:
            n = platform["reviews"]
            platform["rate"] = platform["low"] / n if n else None
            platform["mean"] = platform["rating_sum"] / n if n else None
        total = sum(p["reviews"] for p in platforms)
        low = sum(p["low"] for p in platforms)
        unmatched = sum(p["unmatched"] for p in platforms)
        return {"filters": filters, "stats": {"reviews": total, "low": low,
            "rate": low / total if total else None,
            "mean": sum(p["rating_sum"] for p in platforms) / total if total else None,
            "unmatched": unmatched, "coverage": (low - unmatched) / low if low else None},
            "platforms": platforms, "series": sorted(series.values(), key=lambda r: (r["month"], r["platform"])),
            "aspects": [{**aspect, "mentions": sum(p["aspects"][aspect["code"]] for p in platforms),
                "denominator": low} for aspect in metadata["aspects"]]}

    def reviews(self, params, export=False):
        metadata, _ = self.snapshot()
        filters = self.filters(params, metadata)
        score = params.get("score", ["low"])[0]
        if score not in ["all", "low", "1", "2", "3", "4", "5"]:
            raise ValueError("Bộ lọc số sao không hợp lệ.")
        aspect = params.get("aspect", ["all"])[0]
        bits = {a["code"]: a["bit"] for a in metadata["aspects"]}
        if aspect not in ["all", "unmatched"] + list(bits):
            raise ValueError("Khía cạnh không hợp lệ.")
        raw_query = params.get("q", [""])[0][:160]
        query = query_text(raw_query)
        limit = 5000 if export else min(50, max(1, int(params.get("limit", ["20"])[0])))
        offset = 0 if export else max(0, min(2000000, int(params.get("offset", ["0"])[0])))
        conditions = ["r.platform IN (" + ",".join("?" for _ in filters["platforms"]) + ")",
            "r.month>=?", "r.month<=?"]
        values = [*filters["platforms"], filters["from"], filters["to"]]
        if filters["exclude_spam"]:
            conditions.append("r.spam=0")
        if score == "low":
            conditions.append("r.score<=2")
        elif score != "all":
            conditions.append("r.score=?")
            values.append(int(score))
        if aspect == "unmatched":
            conditions.append("r.aspect_mask=0")
        elif aspect != "all":
            conditions.append("(r.aspect_mask & ?)>0")
            values.append(bits[aspect])
        if query:
            conditions.append("review_fts MATCH ?")
            values.append(query)
        # Keep the selective text index first; otherwise SQLite may probe FTS for
        # every review selected by a broad platform/month filter.
        base = "review_fts CROSS JOIN reviews r ON r.id=review_fts.rowid" if query else "reviews r"
        where = " WHERE " + " AND ".join(conditions)
        with closing(self.connect()) as connection:
            total = connection.execute("SELECT COUNT(*) FROM " + base + where, values).fetchone()[0]
            row_base, row_where, row_values = base, where, list(values)
            if not query:
                year, month = map(int, filters["to"].split("-"))
                next_month = date(year + int(month == 12), 1 if month == 12 else month + 1, 1).isoformat()
                row_base = "reviews r INDEXED BY review_date"
                row_where += " AND r.at>=? AND r.at<?"
                row_values += [filters["from"] + "-01", next_month]
            records = connection.execute("SELECT r.review_key,r.platform,r.month,r.at,r.score,r.content,r.spam,r.aspect_mask FROM "
                + row_base + row_where + " ORDER BY r.at DESC,r.id DESC LIMIT ? OFFSET ?", [*row_values, limit, offset]).fetchall()
        rows = []
        for record in records:
            row = dict(record)
            row["aspects"] = [code for code, bit in bits.items() if row["aspect_mask"] & bit]
            del row["aspect_mask"]
            rows.append(row)
        return {"rows": rows, "total": total, "offset": offset, "limit": limit, "filters": filters}

    def bootstrap(self):
        metadata, _ = self.snapshot()
        streams = [read_json(path) for path in sorted(self.root.glob("stream_batch_*.json"))]
        return {"metadata": metadata, "platforms": PLATFORMS,
            "quality": read_json(self.root / "data_quality.json"),
            "evidence": read_json(self.root / "demo_evidence.json", {}),
            "validation": read_json(self.root / "persisted_validation.json", {}),
            "clusters": read_json(self.root / "cluster_metadata.json", {}), "stream_batches": streams,
            "query_plan": (self.root / "spark_query_plan.txt").read_text(encoding="utf-8")
                if (self.root / "spark_query_plan.txt").exists() else ""}


def handler_class(store):
    class Handler(BaseHTTPRequestHandler):
        def respond(self, body, content_type="application/json; charset=utf-8", status=200, download=None):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            if download:
                self.send_header("Content-Disposition", 'attachment; filename="' + download + '"')
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            parsed = urlsplit(self.path)
            params = parse_qs(parsed.query, keep_blank_values=True)
            try:
                if parsed.path == "/api/health":
                    store.snapshot()
                    data = {"status": "ready"}
                elif parsed.path == "/api/bootstrap":
                    data = store.bootstrap()
                elif parsed.path == "/api/overview":
                    data = store.overview(params)
                elif parsed.path == "/api/reviews":
                    data = store.reviews(params)
                elif parsed.path in ["/api/export.csv", "/api/export-reviews.csv"]:
                    stream = io.StringIO(newline="")
                    writer = csv.writer(stream)
                    if parsed.path == "/api/export.csv":
                        data = store.overview(params)
                        writer.writerow(["platform", "from_month", "to_month", "reviews", "low_rating_reviews", "low_rating_rate", "mean_rating"])
                        for p in data["platforms"]:
                            writer.writerow([p["platform"], data["filters"]["from"], data["filters"]["to"],
                                p["reviews"], p["low"], p["rate"], p["mean"]])
                    else:
                        data = store.reviews(params, export=True)
                        writer.writerow(["review_key", "platform", "at", "score", "aspects_mentioned", "spam_candidate", "content"])
                        for row in data["rows"]:
                            writer.writerow([row["review_key"], row["platform"], row["at"], row["score"],
                                "|".join(row["aspects"]), row["spam"], csv_safe(row["content"])])
                    return self.respond(("\ufeff" + stream.getvalue()).encode("utf-8"), "text/csv; charset=utf-8", download=parsed.path.split("/")[-1])
                elif parsed.path in ["/", "/app.js", "/styles.css"]:
                    filename = "index.html" if parsed.path == "/" else parsed.path[1:]
                    mime = {"index.html": "text/html", "app.js": "text/javascript", "styles.css": "text/css"}[filename]
                    return self.respond((WEB / filename).read_bytes(), mime + "; charset=utf-8")
                else:
                    return self.respond(b'{"error":"Not found"}', status=404)
                self.respond(json.dumps(data, ensure_ascii=False).encode("utf-8"))
            except ValueError as error:
                self.respond(json.dumps({"error": str(error)}, ensure_ascii=False).encode(), status=400)
            except (RuntimeError, FileNotFoundError) as error:
                self.respond(json.dumps({"error": str(error)}, ensure_ascii=False).encode(), status=503)
            except (sqlite3.Error, KeyError) as error:
                print(f"Dashboard data error: {error}", flush=True)
                self.respond('{"error":"Không đọc được snapshot dữ liệu. Kiểm tra job tạo dashboard."}'.encode(), status=503)
    return Handler


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--outputs", type=Path, default=Path(__file__).resolve().parents[1] / "outputs")
    parser.add_argument("--cache", type=Path, help="Optional local serving cache, useful with Docker Desktop bind mounts")
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), handler_class(Store(args.outputs, args.cache)))
    print(f"ReviewLens ready at http://{args.host}:{args.port}", flush=True)
    server.serve_forever()
