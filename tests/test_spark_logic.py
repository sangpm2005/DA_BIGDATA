import csv
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "Run Spark tests inside the project container")
class SparkLogicTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master("local[2]").appName("review-data-contract-tests")
            .config("spark.sql.shuffle.partitions", 2).config("spark.sql.warehouse.dir", "/tmp/spark-tests")
            .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.aspects_file = str(Path(__file__).resolve().parents[1] / "config/aspects.json")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_csv_quoted_newline_quotes_and_comma(self):
        from review_analytics.spark_ops import RAW_COLUMNS, read_raw
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "fixture.csv"
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["" if c == "csv_index" else c for c in RAW_COLUMNS])
                writer.writerow([0, "r1", "display name", "", 'Hàng "tốt",\nnhưng giao lâu',
                    "2", "0", "", "2022-01-01 00:00:00", "", "", "com.shopee.vn"])
            rows = read_raw(self.spark, "file://" + str(path)).collect()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["content"], 'Hàng "tốt",\nnhưng giao lâu')

    def test_quarantine_dedup_and_keep_distinct_identical_text(self):
        from pyspark.sql import functions as F
        from review_analytics.spark_ops import deduplicate_valid, enrich, validation_reasons
        records = [
            ("id1", "com.shopee.vn", "Không hài lòng, giao hàng chậm", "1", "2022-01-01 00:00:00"),
            ("id1", "com.shopee.vn", "Không hài lòng, giao hàng chậm", "2", "2022-01-02 00:00:00"),
            ("id2", "com.shopee.vn", "Không hài lòng, giao hàng chậm", "1", "2022-01-01 00:00:00"),
            ("id3", "com.shopee.vn", "Ứng dụng tốt", "5", "2022-01-01 00:00:00"),
            ("id4", "com.shopee.vn", "", "0", "2022-01-01 00:00:00"),
        ]
        frame = self.spark.createDataFrame(records, ["reviewid", "appid", "content", "score", "at"]) \
            .withColumn("_corrupt_record", F.lit(None).cast("string"))
        prepared = validation_reasons(enrich(frame, self.aspects_file))
        invalid = prepared.filter(F.size("validation_reasons") > 0).collect()
        self.assertEqual(len(invalid), 1)
        self.assertEqual(set(invalid[0]["validation_reasons"]), {"missing_content", "invalid_score"})
        clean = deduplicate_valid(prepared.filter(F.size("validation_reasons") == 0)).collect()
        self.assertEqual(len(clean), 3)
        latest = next(row for row in clean if row["reviewid"] == "id1")
        self.assertEqual(latest["score"], 2)
        self.assertIn("delivery", latest["aspects_mentioned"])
        self.assertIn("không", latest["normalized_content"])


if __name__ == "__main__":
    unittest.main()
