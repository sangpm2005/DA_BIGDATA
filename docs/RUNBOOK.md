# Hướng dẫn chạy và quan sát

Chạy PowerShell tại `D:\Bigdata`. Docker Desktop cần hoạt động với Linux containers. Máy hiện có gần 24 GB RAM; Docker được cấp khoảng 11,5 GiB. Chạy batch rồi streaming; hạn chế chạy nhiều job Spark đồng thời với Airflow.

Với bản clone mới, chạy `scripts/Init-LocalConfig.ps1` trước để tạo `.env` cục bộ. Đặt CSV tải từ Kaggle ở thư mục gốc với tên `vietnamese_ecommerce_review.csv`. Với volume cũ, giữ `.env` khớp thông tin database đang dùng. Không commit mật khẩu hoặc `.env`.

## 1. Audit nguồn, không cần cài thư viện Python

```powershell
py -3.11 scripts/audit_dataset.py
```

## 2. Dựng core và upload HDFS

```powershell
docker compose build spark-client
docker compose up -d namenode datanode spark-master spark-worker-1 spark-worker-2 spark-client
docker compose exec -T spark-client hdfs dfsadmin -report
docker compose exec -T spark-client python scripts/bootstrap_hdfs.py
docker compose exec -T spark-client hdfs dfs -ls /ecommerce/raw
```

NameNode: http://localhost:9870 ; Spark master: http://localhost:8081 . Spark master cần hiển thị **hai worker**, mỗi worker 2 cores. Chờ DataNode được đăng ký và HDFS thoát safe mode trước upload.

Raw upload kiểm tra SHA-256 khi file đã tồn tại. Nếu nguồn khác, script từ chối ghi đè và yêu cầu raw path mới; chạy lại cùng nguồn sẽ bỏ qua upload.

## 3. Test logic xử lý trước chạy đầy đủ

```powershell
docker compose exec -T spark-client python -m unittest discover -s tests -v
```

Tests kiểm tra CSV có dấu nháy/dấu phẩy/dòng mới; Unicode và phủ định; quarantine; giữ review khác ID cùng nội dung; chọn bản ID trùng mới nhất.

## 4. Chạy batch

```powershell
docker compose exec -T spark-client spark-submit --master spark://spark-master:7077 --driver-memory 2g --executor-memory 1g jobs/batch_pipeline.py --stage all
docker compose exec -T spark-client python scripts/verify_outputs.py
```

Có thể chạy từng stage bằng `--stage inspect`, `clean`, `analyze`, `cluster`, `export`. Driver UI http://localhost:4040 chỉ có khi job đang chạy. Stage inspect phải đọc được 1.300.086 record khớp audit độc lập.

`outputs/`: báo cáo chất lượng, kế hoạch Spark SQL, thống kê sàn/tháng/sao/khía cạnh, K-Means và thời gian. CSV xuất chỉ là bảng tổng hợp nhỏ; toàn bộ processed và kết quả phân tán nằm trong HDFS.

```powershell
docker compose exec -T spark-client hdfs dfs -ls /ecommerce/processed/reviews
docker compose exec -T spark-client hdfs dfs -ls /ecommerce/output
docker compose exec -T spark-client hdfs fsck /ecommerce/raw/reviews.csv -files -blocks -locations
```

K-Means mặc định trên mẫu hash 25% nội dung đánh giá thấp đủ dài, không ứng viên spam. Ghi rõ đây là mẫu, không phân cụm toàn dataset. Dùng `--cluster-percent 100` nếu cần thí nghiệm đầy đủ sau khi đo tài nguyên. Chưa có nhãn người đọc thì chưa có precision/recall/F1 khía cạnh.

## 5. Kafka và Structured Streaming

```powershell
docker compose --profile streaming up -d kafka
docker compose exec -T spark-client python jobs/replay_kafka.py
docker compose exec -T spark-client spark-submit --master spark://spark-master:7077 --driver-memory 2g --executor-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 jobs/stream_reviews.py
```

Mặc định phát lại mẫu kiểm tra 1.000 review, phân tầng đủ bốn sàn. CSV gốc có thể dùng với `--input /input/reviews.csv --limit 1000`, nhưng 1.000 dòng đầu chủ yếu thuộc Tiki nên không thích hợp để so sánh sàn.

Consumer chạy hữu hạn với `availableNow`, tối đa 500 offsets/trigger. Dùng `--continuous` cho demo liên tục. Thời gian review giữ nguyên 2013–2022, `ingested_at` là lúc replay hiện tại. Không áp dụng watermark dựa trên “bây giờ” làm mất review lịch sử.

Checkpoint: `/ecommerce/checkpoints/reviews-v1`. Thử chạy producer và consumer lần hai: offset Kafka tăng nhưng review trùng không được tăng số unique trong state. State dedup không watermark tăng theo số ID, phù hợp demo hữu hạn; không tuyên bố thiết kế state không giới hạn dùng được cho production.

```powershell
docker compose exec -T spark-client hdfs dfs -ls /ecommerce/streaming/metrics
docker compose exec -T spark-client hdfs dfs -ls /ecommerce/checkpoints/reviews-v1
docker compose exec -T spark-client python scripts/verify_outputs.py --require-stream
docker compose exec -T spark-client spark-submit --master spark://spark-master:7077 --driver-memory 2g --executor-memory 1g jobs/validate_persisted.py
```

## 6. Airflow

Sau khi runtime đã build:

```powershell
docker compose --profile orchestration build airflow-init
docker compose --profile orchestration up -d postgres kafka
docker compose --profile orchestration run --rm airflow-init
docker compose --profile orchestration up -d airflow-webserver airflow-scheduler
docker compose exec -T airflow-scheduler airflow dags list-import-errors
```

Airflow: http://localhost:8080 ; tài khoản demo tại máy `airflow`; mật khẩu xem `AIRFLOW_ADMIN_PASSWORD` trong `.env`. DAG `ecommerce_dissatisfaction_pipeline` được tạo ở trạng thái pause; bật và Trigger để chạy 9 bước. `schedule=None` vì nguồn lịch sử cố định, không giả vờ có review mới hàng ngày.

Sau khi batch ở bước 4 đã thành công, lưu số đếm trước khi Trigger DAG để đối chiếu:

```powershell
Copy-Item -LiteralPath outputs/data_quality.json -Destination outputs/quality_before_airflow.json
```

Thử trigger hai lần và so sánh `data_quality.json`, lượng dữ liệu processed. Nguồn raw giữ nguyên; batch tính lại kết quả, streaming dùng checkpoint và dedup. Credential demo và cổng chỉ dùng localhost.

Bước 09 còn đọc lại Parquet thật trên HDFS: số review và khóa duy nhất phải bằng báo cáo chất lượng; tổng theo sàn phải khớp; số review streaming phải bằng số khóa duy nhất và tổng metric valid. `outputs/persisted_validation.json` lưu bằng chứng này.

Để chụp bằng chứng cho một DAG đã thành công, thay run ID bằng ID trong Airflow UI:

```powershell
docker compose exec -T airflow-scheduler python /workspace/scripts/capture_evidence.py --run-id validation_20261012
```

Lệnh này lưu trạng thái 9 task, tài nguyên Spark, end offsets Kafka và so sánh các số đếm batch vào `outputs/demo_evidence.json`.

File xuất nhỏ trên máy được ghi qua tệp tạm rồi thay thế nguyên tử. Điều này cho phép Spark và Airflow dùng chung thư mục output dù khác UID, đồng thời tránh công bố file ghi dở khi job thất bại.

## 7. Tạo và mở sản phẩm ReviewLens

Task 07 của DAG hiện tự tạo dữ liệu dashboard. Nếu chạy batch bằng tay, tạo lớp phục vụ sau khi batch thành công:

```powershell
docker compose exec -T spark-client spark-submit --master spark://spark-master:7077 --driver-memory 2g --executor-memory 1g jobs/prepare_dashboard.py
docker compose exec -T spark-client python scripts/build_dashboard_index.py
docker compose exec -T spark-client python scripts/validate_dashboard.py --outputs /exports
docker compose --profile dashboard up -d dashboard
Invoke-RestMethod http://localhost:8090/api/health
```

Mở http://localhost:8090 . Máy hiện tại đã có cube và chỉ mục cho 1.299.873 review, chỉ cần lệnh khởi động dashboard. Giao diện không cần Airflow/Kafka/Spark đang chạy để đọc snapshot đã chuẩn bị; các dịch vụ đó cần khi cập nhật nguồn. Lần đầu Docker sao chép chỉ mục vào volume cache có thể mất vài chục giây.

Theo dõi:

```powershell
docker compose logs --tail 30 dashboard
docker compose exec -T spark-client hdfs dfs -ls /ecommerce/serving/review_index
```

Khi dữ liệu thay đổi, tạo lại cube rồi chỉ mục; generation và số dòng phải khớp trước khi dashboard sẵn sàng. Không sửa CSV tổng hợp bằng tay để làm đẹp biểu đồ. Hướng dẫn thao tác, kịch bản demo và xuất PNG/SVG: `docs/PRODUCT_GUIDE.md`.

Task 09 đối soát mọi ô số đếm trong cube Spark với phép GROUP BY độc lập trên chỉ mục toàn bộ review. Kết quả ở `outputs/dashboard_validation.json`, không chỉ kiểm tra vài dòng mẫu.

Có thể chạy server bằng Python tại Windows thay Docker khi đã có snapshot:

```powershell
py -3.11 dashboard/server.py --port 8090
```

Chỉ chạy một server trên cổng 8090. Snapshot/index là dữ liệu sinh ra, giữ ngoài Git theo `.gitignore`.

## 8. Dừng sau demo

```powershell
docker compose --profile orchestration --profile streaming --profile dashboard stop
```

Volumes giữ HDFS/Kafka/Airflow. Không dùng `down -v` khi cần giữ bằng chứng/chỉ số/checkpoint.

## 9. Nội dung trình bày khi bảo vệ

Giải thích raw → schema → repartition → validation → shuffle dedup → Parquet partition → Spark SQL → kết quả; mở Spark UI và HDFS blocks. Tiếp theo trình bày Kafka partition/offset, microbatch/checkpoint/restart, rồi Airflow dependencies/retry. Cuối cùng mới trình bày khía cạnh và cụm nội dung, cùng những giới hạn suy luận.

