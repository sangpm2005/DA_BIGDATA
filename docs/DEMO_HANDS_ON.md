# Kịch bản chạy thủ công và trình bày đồ án Bigdata

Project: D:\Bigdata. Mục tiêu: thao tác trên hệ thống, chỉ vào kết quả thật và giải thích mối liên hệ giữa dữ liệu, engine xử lý và sản phẩm. Thực hiện lần lượt, dừng khi lệnh lỗi, không chạy các job Spark cùng lúc.

## 0. Trạng thái kiểm tra và chuẩn bị màn hình

Trong phiên chuẩn bị hướng dẫn, Docker CLI trả phiên bản nhưng docker version chờ Engine không trả kết quả; chưa thấy cổng 9870/8081/4040/8080/8090 lắng nghe. Lệnh kiểm tra bị treo đã được dừng, không dừng các dịch vụ Docker khác. Các lệnh demo dưới đây chưa được chạy trên Engine trong phiên này. Số liệu kỳ vọng lấy từ output/log đã lưu, cần đối chiếu lại khi chạy thực hành.

Mở Windows Terminal hoặc PowerShell, hai tab đều tại D:\Bigdata:

- Terminal A: PowerShell điều khiển Docker, HDFS, Kafka và Airflow.
- Terminal B: ban đầu PowerShell, sau đó vào PySpark với dấu nhắc >>>. Chỉ nhập code Python ở dấu nhắc đó.

Mở browser với năm tab, chỉ dùng khi dịch vụ tương ứng đã hoạt động:

| Tab | URL | Cần chỉ vào |
|---|---|---|
| HDFS NameNode | http://localhost:9870 | DataNode, thư mục raw/processed/output và file |
| Spark master | http://localhost:8081 | Hai worker ALIVE, tổng bốn cores, application |
| Spark application | http://localhost:4040 | Executors, Jobs, Stages, SQL khi phiên Spark còn mở |
| Airflow | http://localhost:8080 | Graph/Grid và log của đúng DAG Run |
| ReviewLens | http://localhost:8090 | Bộ lọc, biểu đồ, tra cứu review |

Đặt terminal font khoảng 18–20 và browser zoom 110–125% phù hợp màn chiếu. Một lượt thao tác nên có ba việc: nói mục tiêu → chạy lệnh → chỉ kết quả và giải thích. Source mở sẵn trong editor để dùng khi cần chứng minh logic, không đọc toàn file trước lớp.

## 1. Chuẩn bị runtime trước buổi trình bày

### 1.1. Docker Engine phải sẵn sàng

Terminal A, PowerShell:

```powershell
Set-Location -LiteralPath D:\Bigdata
docker info
```

Phải có thông tin Server, không chỉ phiên bản CLI. Nếu lệnh chờ quá lâu, Ctrl+C; mở Docker Desktop để xem trạng thái Engine. Có thể chọn Restart Docker Desktop trong Troubleshoot khi Engine kẹt, nhưng thao tác đó làm gián đoạn các container đang chạy; thực hiện trong lúc chuẩn bị. Giữ dữ liệu/volumes, không chọn Reset to factory defaults hoặc xóa dữ liệu.

Khi docker info thành công:

```powershell
docker compose up -d namenode datanode spark-master spark-worker-1 spark-worker-2 spark-client
docker compose ps
```

Nếu thiếu image bigdata-review-runtime:local thì build trước:

```powershell
docker compose build spark-client
```

Sau build chạy lại lệnh up. Chờ DataNode đăng ký và HDFS thoát safe mode; không tiếp tục chỉ vì container vừa hiện Up.

```powershell
docker compose exec -T spark-client hdfs dfsadmin -report
docker compose exec -T spark-client hdfs dfs -ls /ecommerce
docker compose exec -T spark-client hdfs dfs -ls /ecommerce/processed/reviews
```

Trạng thái kỳ vọng: một Live DataNode, các thư mục project tồn tại. Mở Spark master, cần hai worker ALIVE, hai cores mỗi worker. Nếu chưa có /ecommerce/raw, dùng các lệnh chuẩn bị/upload ở điểm dừng 1. Nếu chưa có processed hoặc các output, dùng mục 1.2 trước buổi báo cáo.

### 1.2. Chuẩn bị lại dữ liệu nếu output cần thiết chưa có

Không bắt buộc chạy lại khi output hiện có còn phù hợp. Muốn tái tạo toàn batch, chạy lần lượt:

```powershell
docker compose exec -T spark-client python scripts/bootstrap_hdfs.py
docker compose exec -T spark-client spark-submit --master spark://spark-master:7077 --driver-memory 2g --executor-memory 1g jobs/batch_pipeline.py --stage all
docker compose exec -T spark-client python scripts/verify_outputs.py
docker compose exec -T spark-client spark-submit --master spark://spark-master:7077 --driver-memory 2g --executor-memory 1g jobs/prepare_dashboard.py
docker compose exec -T spark-client python scripts/build_dashboard_index.py
docker compose exec -T spark-client python scripts/validate_dashboard.py --outputs /exports
```

Stage all gồm đọc, làm sạch, SQL, phân cụm và export. Hai lệnh serving sau đó tạo cube/index. Đây là công việc chuẩn bị có thể mất nhiều phút, không coi là lệnh nhỏ để chạy bất ngờ khi thầy đang xem. Các lệnh có ghi lại output dẫn xuất; chờ hoàn tất trước khi mở sản phẩm.

Mẫu artifacts/audit/annotation_sample.csv hiện có tại máy. Nếu thiếu audit/sample thì chạy scripts/audit_dataset.py theo RUNBOOK trước phần Kafka.

### 1.3. Chuẩn bị sản phẩm và Airflow

Khi snapshot/index sẵn sàng:

```powershell
docker compose --profile dashboard up -d dashboard
Invoke-RestMethod http://localhost:8090/api/health
```

Cache lần đầu có thể cần thời gian sao chép index. Đợi health thành công, mở ReviewLens và kiểm tra bộ lọc trước giờ trình bày.

Để Airflow sẵn sàng, nếu image chưa có thì build airflow-init theo RUNBOOK. Sau đó:

```powershell
docker compose --profile orchestration up -d postgres kafka
docker compose --profile orchestration run --rm airflow-init
docker compose --profile orchestration up -d airflow-webserver airflow-scheduler
docker compose exec -T airflow-scheduler airflow dags list-import-errors
```

Không Trigger DAG trong lúc đang chạy PySpark hoặc các job bằng tay. Giới hạn max_active_runs của DAG không ngăn được job ngoài Airflow.

## 2. Điểm dừng 1 — HDFS có dữ liệu thật, tương ứng bước 1–2

Terminal A, PowerShell:

```powershell
docker compose exec -T spark-client python scripts/bootstrap_hdfs.py --mode prepare
docker compose exec -T spark-client python scripts/bootstrap_hdfs.py --mode upload
docker compose exec -T spark-client hdfs dfs -ls /ecommerce/raw
docker compose exec -T spark-client hdfs fsck /ecommerce/raw/reviews.csv -files -blocks -locations
```

Lệnh upload có thể báo Source already uploaded with matching SHA-256 nếu nguồn đã có. Đây là cơ chế kiểm tra và bỏ qua upload trùng, không giả vờ vừa upload lại toàn file. Nếu hash khác, dừng để xem nguyên nhân, không ghi đè raw.

Mở NameNode → Utilities → Browse the file system, vào /ecommerce/raw. Chỉ file reviews.csv, sau đó terminal fsck để chỉ block và DataNode.

Theo minh chứng nguồn hiện có: 642.967.981 bytes, năm block, replication=1, một DataNode. Block IDs/IP có thể khác khi dựng lại; không học thuộc chúng.

Nói: “Nguồn CSV được giữ trong vùng raw trên HDFS và kiểm tra SHA-256. File được lưu thành các block; ở cấu hình tại máy có năm block trên một DataNode, replication một. Đây là mô hình thực hành, chưa phải cụm lưu trữ nhiều máy có dự phòng.”

Source mở khi cần: scripts/bootstrap_hdfs.py:17 và :22; infra/hadoop/hdfs-site.xml.

## 3. Điểm dừng 2 — Spark đọc và kiểm tra dữ liệu, tương ứng bước 3–4

### 3.1. Giữ Spark UI mở bằng PySpark tương tác

Terminal B, PowerShell, tại D:\Bigdata:

```powershell
docker compose exec spark-client pyspark --master spark://spark-master:7077 --name ReviewLens-demo-manual --driver-memory 2g --executor-memory 1g --conf spark.cores.max=4 --conf spark.executor.cores=2 --conf spark.sql.shuffle.partitions=8 --conf spark.sql.session.timeZone=Asia/Ho_Chi_Minh
```

Không thêm -T cho phiên tương tác. Chờ dấu nhắc >>>. PySpark đã tạo biến spark; đừng tạo thêm SparkSession khác.

Tại >>>, nhập từng dòng Python, không chép dấu >>>:

```python
spark.sparkContext.setLogLevel("WARN")
root = "hdfs://namenode:9000/ecommerce"
print("application_id =", spark.sparkContext.applicationId)
from pyspark.sql import functions as F
from review_analytics.spark_ops import read_raw, enrich, validation_reasons
raw = read_raw(spark, root + "/raw/reviews.csv")
raw.select("appid", "score", "at", "content").printSchema()
raw.select("appid", "score", "at", "content").show(3, truncate=60)
print("raw_rows =", raw.count())
print("CSV reader partitions =", raw.rdd.getNumPartitions())
```

Kỳ vọng raw_rows=1.300.086, reader partitions=1 với file CSV multiline hiện tại. Việc parser nguồn có một partition khác với số worker đang có; không nói ngay CSV được đọc đồng thời trên cả hai worker.

Mở Spark master để chỉ application ReviewLens-demo-manual. Mở Spark UI → Executors: phân biệt driver với executor; kiểm tra có hai executor chạy trên các worker. Tab Jobs ghi các action vừa chạy. Không cần bấm link worker dùng địa chỉ nội bộ Docker nếu browser Windows không truy cập được; dùng Driver UI để quan sát executor.

### 3.2. Cho thấy phép biến đổi dùng source của đồ án

Tại >>>:

```python
small = raw.select("reviewid", "appid", "content", "score", "at", "_corrupt_record").limit(2000)
prepared = validation_reasons(enrich(small.repartition(8)))
print("Demo processing partitions =", prepared.rdd.getNumPartitions())
prepared.select("platform", "score", "at", "normalized_content", "validation_reasons").show(5, truncate=50)
```

Đây là minh họa biến đổi trên 2.000 record đầu; không phải mẫu đại diện để so sánh sàn, không ghi đè processed toàn bộ. Có repartition để phân phối công việc xử lý. Mở Stages khi action show chạy để quan sát các task thực tế.

Đọc kết quả làm sạch đã lưu toàn dataset và dữ liệu lỗi thật:

```python
reviews = spark.read.parquet(root + "/processed/reviews")
reviews.select("platform", "score", "at", "year", "normalized_content", "aspects_mentioned").printSchema()
reviews.agg(F.count("*").alias("rows"), F.countDistinct("review_key").alias("unique_keys")).show()
bad = spark.read.parquet(root + "/quarantine/reviews")
print("quarantined_rows =", bad.count())
bad.select("score", "validation_reasons").show(8, truncate=False)
```

Kỳ vọng 1.299.873 rows/unique keys và 74 dòng quarantine. Đây là phép đọc lại output thật đang có; chưa tái chạy làm sạch toàn dataset trong phần tương tác này.

Nói: “Raw được đọc bằng schema và parser CSV multiline. Nhóm dùng cùng hàm chuẩn hóa và kiểm tra của source để minh họa xử lý. Kết quả Parquet toàn bộ có kiểu số/ngày, chia theo sàn và năm; đọc lại xác nhận số dòng bằng số khóa duy nhất. Những record lỗi có vùng quarantine và lý do cụ thể.”

Source khi cần: src/review_analytics/spark_ops.py:23, :28, :46; jobs/batch_pipeline.py:45.

## 4. Điểm dừng 3 — Chạy SQL và quan sát xử lý, tương ứng bước 5

Giữ phiên PySpark ở Terminal B. Tại >>>:

```python
reviews.createOrReplaceTempView("reviews")
summary = spark.sql("SELECT platform, COUNT(*) AS reviews, ROUND(100.0 * AVG(CASE WHEN low_rating_proxy THEN 1.0 ELSE 0.0 END), 2) AS low_rating_pct FROM reviews GROUP BY platform ORDER BY platform")
summary.show(truncate=False)
summary.explain("formatted")
```

Kỳ vọng theo snapshot nguồn:

| Sàn | Review | Tỷ lệ 1–2 sao (%) |
|---|---:|---:|
| Lazada | 530.404 | 18,65 |
| Sendo | 179.632 | 14,08 |
| Shopee | 519.191 | 27,55 |
| Tiki | 70.646 | 21,98 |

Mở Spark UI → SQL, chọn execution vừa chạy. Chỉ scan Parquet, aggregate và Exchange/shuffle trong plan nếu có. Mở Stages → chọn stage → xem Tasks; quay Executors để xem số task/dữ liệu đọc. AQE có thể thay đổi số task cuối, không ép mọi stage phải có tám task.

Chạy truy vấn có filter trên cột partition:

```python
filtered = spark.sql("SELECT platform, month, COUNT(*) AS reviews FROM reviews WHERE platform = 'Shopee' AND year = 2022 GROUP BY platform, month ORDER BY month")
filtered.show(truncate=False)
filtered.explain("formatted")
```

Chỉ PartitionFilters trong scan để giải thích pruning theo platform/year. Đây là truy vấn thực hiện tại buổi demo trên dữ liệu HDFS, khác việc chỉ mở CSV kết quả.

Nói: “Spark đang tổng hợp trên Parquet HDFS. GROUP BY phân phối công việc qua các task; kế hoạch cho thấy scan và trao đổi dữ liệu khi cần. Điều kiện trên sàn/năm giúp giới hạn các partition được đọc. Nhóm dùng tỷ lệ cùng số lượng review để so sánh.”

Source khi cần: jobs/batch_pipeline.py:77. Giữ Spark UI mở cho đến khi đã chỉ xong SQL/Stages/Executors.

## 5. Điểm dừng 4 — Model/output và sản phẩm, tương ứng bước 6–7

### 5.1. Kiểm tra model và assignments đã lưu

Tại >>>:

```python
from pyspark.ml.clustering import KMeansModel
model = KMeansModel.load(root + "/models/kmeans")
print("k =", model.getK(), "distance =", model.getDistanceMeasure())
assignments = spark.read.parquet(root + "/output/cluster_assignments_sample")
assignments.groupBy("prediction").count().orderBy("prediction").show()
```

Kỳ vọng k=8, cosine; nhóm đang đọc model/assignments đã lưu, chưa huấn luyện lại model trong buổi demo.

Terminal A, PowerShell:

```powershell
docker compose exec -T spark-client hdfs dfs -ls /ecommerce/models
docker compose exec -T spark-client hdfs dfs -ls /ecommerce/output
$demoCluster = Get-Content -LiteralPath .\outputs\cluster_metadata.json -Raw -Encoding UTF8 | ConvertFrom-Json
$demoCluster | Select-Object k,training_rows,sample_percent,silhouette_cosine | Format-List
Import-Csv -LiteralPath .\outputs\platform_summary.csv | Format-Table platform,reviews,low_rating_reviews,low_rating_rate
```

Theo metadata hiện tại: 61.241 review dùng huấn luyện, mẫu hash 25% tập đủ điều kiện, silhouette khoảng 0,019. Chỉ nói phân cụm khám phá, không gọi đó là accuracy hoặc mô hình tách tốt.

### 5.2. Show sản phẩm để nối kết quả với ứng dụng

Mở ReviewLens http://localhost:8090:

1. Tổng quan: chỉ 1.299.873 review và tỷ lệ sao thấp; nói rõ dữ liệu lịch sử Google Play.
2. Chọn Shopee, từ tháng 2021-01 đến 2022-08, bấm Áp dụng. Snapshot hiện có cho 260.800 review, 81.979 sao thấp, khoảng 31,43%.
3. Mở Khía cạnh, chọn ô Ứng dụng/Shopee để chuyển sang review.
4. Tìm dang nhap, đọc một vài phản hồi; nói biểu đồ giúp khoanh vùng và văn bản giúp kiểm tra ngữ cảnh.
5. Xuất một biểu đồ SVG hoặc bảng CSV nhỏ nếu cần minh chứng báo cáo.

Không hứa số kết quả tìm kiếm luôn bằng 4.764 khi đang giới hạn tháng; con số đó thuộc toàn lịch sử Shopee, 1–2 sao, khía cạnh Ứng dụng.

Nói: “Spark chuẩn bị thống kê và dữ liệu phục vụ từ HDFS. Dashboard đọc snapshot và chỉ mục để lọc/tra cứu, không chạy lại toàn bộ Spark cho mỗi lần bấm. Khía cạnh được đề cập cần đọc trong ngữ cảnh để kiểm tra có thực sự bị chê hay không.”

Tab Luồng dữ liệu trong sản phẩm hiển thị minh chứng đã lưu, không phải bảng theo dõi Engine trực tiếp. Khi replay thêm Kafka, con số offset trực tiếp có thể khác snapshot này.

### 5.3. Đóng phiên PySpark trước phần Kafka/Airflow

Tại >>>, sau khi đã chỉ xong Spark UI:

```python
spark.stop()
exit()
```

Terminal B trở về PowerShell. UI 4040 của phiên này đóng theo application; Spark master vẫn giữ thông tin application đã hoàn tất. Đóng phiên giúp giải phóng cores/RAM cho job streaming và DAG.

## 6. Điểm dừng 5 — Kafka replay và checkpoint, tương ứng bước 8

Terminal A, PowerShell:

```powershell
docker compose --profile streaming up -d kafka
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic ecommerce-reviews
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic ecommerce-reviews --time latest
```

Nếu topic chưa có, chạy producer bên dưới để tạo rồi describe lại. Kỳ vọng ba partition, replication factor một. Ghi nhận end offsets trước replay bằng số thật đang thấy, không ép chúng bằng 662/698/640 của lượt cũ.

Gửi lại mẫu và xem offsets sau gửi:

```powershell
docker compose exec -T spark-client python jobs/replay_kafka.py
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic ecommerce-reviews --time latest
```

Kỳ vọng producer báo published_events=1000. Message tăng offsets dù mang cùng review ID. End offset là vị trí kế tiếp, không phải số unique review; retention/gaps ảnh hưởng cách diễn giải độ dài log.

Terminal B, PowerShell, chạy consumer và chờ kết thúc:

```powershell
docker compose exec -T spark-client spark-submit --master spark://spark-master:7077 --driver-memory 2g --executor-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 jobs/stream_reviews.py
```

Có thể mở Spark UI 4040 khi application này đang chạy. availableNow xử lý phần có sẵn rồi kết thúc; không cần đổi sang --continuous cho demo cơ bản.

Giữ checkpoint /ecommerce/checkpoints/reviews-v1. Nếu nó đã xử lý đủ 1.000 ID của cùng mẫu thì batch mới sẽ có new_unique_events=0; đó là kết quả chống trùng mong đợi. Nếu mới chạy lần đầu, sẽ xuất hiện các batch có unique mới. Không xóa checkpoint để tạo số giống minh chứng cũ.

Terminal A, sau khi consumer hoàn tất:

```powershell
Get-ChildItem -LiteralPath .\outputs -Filter stream_batch_*.json | Sort-Object LastWriteTime -Descending | Select-Object -First 1 | Get-Content -Encoding UTF8
docker compose exec -T spark-client hdfs dfs -ls /ecommerce/checkpoints/reviews-v1
```

Kiểm tra dữ liệu thật bằng validator, sau khi không còn job Spark khác:

```powershell
docker compose exec -T spark-client spark-submit --master spark://spark-master:7077 --driver-memory 2g --executor-memory 1g jobs/validate_persisted.py
Get-Content -LiteralPath .\outputs\persisted_validation.json -Encoding UTF8
```

Validator còn cần processed và platform_summary của nhánh batch. Kỳ vọng streaming_rows=streaming_unique_keys=1000 với cùng mẫu/state; streaming_batches có thể tăng sau mỗi lượt demo.

Nói: “Producer vừa phát lại 1.000 review lịch sử; Kafka tiếp nhận ở offsets mới. Spark đọc các message mới nhưng state nhận diện ID đã có, nên replay không làm tăng review unique trên HDFS. Checkpoint lưu cả tiến độ và state qua lần chạy tiếp.”

Muốn luyện lần nữa, chạy producer → consumer → validator tuần tự. Cả Kafka offsets và batch ID sẽ thay đổi; tổng review unique không tăng nếu cùng ID và checkpoint.

## 7. Điểm dừng 6 — Airflow điều phối và kiểm tra, tương ứng bước 9

### 7.1. Quan sát run đã có

Mở Airflow http://localhost:8080, demo airflow; mật khẩu lấy từ AIRFLOW_ADMIN_PASSWORD trong .env. Chọn DAG ecommerce_dissatisfaction_pipeline và đúng run validation_20261012 nếu metadata cũ vẫn còn. Graph: chỉ cạnh 01 → ... → 09. Grid/Logs: mở task 04 và 09 để chỉ command, output checks và exit code.

Đây là run lịch sử; minh chứng DAG cơ bản và các task 07/09 mở rộng kiểm tra riêng phải trình bày đúng. Không gọi một graph cũ là run vừa Trigger hôm nay.

CLI đọc trạng thái:

```powershell
docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run ecommerce_dissatisfaction_pipeline validation_20261012
```

### 7.2. Nếu muốn chạy mới toàn pipeline

Luyện trước buổi báo cáo; đóng PySpark và chờ job streaming kết thúc. Có thể lưu baseline theo RUNBOOK để đối chiếu số dòng sau chạy:

```powershell
Copy-Item -LiteralPath .\outputs\data_quality.json -Destination .\outputs\quality_before_airflow.json
```

Bật DAG nếu đang pause, Trigger đúng một lần, ghi nhận run ID mới. Airflow tự chạy toàn chuỗi; không đồng thời chạy lại các job bằng tay. Chờ task 09 success rồi xem log và output. Thời gian có thể nhiều phút, nhất là xây chỉ mục; không cam kết hoàn tất trong vài giây.

Khi đủ điều kiện capture_evidence, dùng run ID mới và --output riêng theo hướng dẫn STEP_09 để giữ minh chứng cũ. Không dùng validation_20261012 để đặt tên cho lần chạy mới.

Nói: “Các job có thể chạy thủ công như phần vừa demo, đồng thời được đóng gói trong DAG để tái thực thi có phụ thuộc, log, retry và kiểm tra. Task cuối không chỉ xem nút xanh mà đọc lại dữ liệu HDFS và đối soát dữ liệu sản phẩm.”

Source khi cần: dags/ecommerce_pipeline.py:11 và :36; jobs/validate_persisted.py; scripts/validate_dashboard.py.

## 8. Những lỗi dễ gặp khi luyện

| Hiện tượng | Xử lý |
|---|---|
| docker info treo hoặc không có Server | Kiểm tra trạng thái Docker Engine; chưa chạy tiếp compose |
| HDFS safe mode/no Live DataNode | Chờ đăng ký DataNode, xem logs namenode/datanode |
| Không có processed/reviews | Chuẩn bị batch theo mục 1.2 trước phần truy vấn |
| Gõ code Python nhưng PowerShell báo lỗi | Vào pyspark, chờ >>> rồi nhập Python |
| UI 4040 không mở hoặc bị chuyển 4041 | Kiểm tra application đang chạy; tránh nhiều driver cùng container |
| Có worker nhưng query chờ mãi | Kiểm tra application khác đang chiếm cores/RAM; đóng phiên trước |
| Không tìm được module review_analytics | Dùng đúng container runtime/project mount, không chạy Python tùy ý trên Windows |
| Kafka batch mới toàn 0 unique | Kiểm tra published_events/offsets; nếu replay cùng ID thì đây là kết quả đúng |
| Airflow run queued | Kiểm tra DAG pause, scheduler và các run đang hoạt động |
| Dashboard chưa sẵn sàng | Kiểm tra health, snapshot/generation và cache; chờ chuẩn bị hoàn tất |

## 9. Cách kết thúc và bằng chứng nên giữ

Có thể chụp màn hình bằng Win+Shift+S: HDFS file/block, Spark master workers, Driver UI SQL/Stages/Executors, bảng query terminal, Kafka offsets/metrics, Airflow đúng run và dashboard đã lọc. Kèm thời điểm và application_id/run_id khi lưu; không đặt ảnh cũ dưới nhãn run mới.

Khi kết thúc, thoát PySpark rồi dừng các dịch vụ của project nếu cần:

```powershell
docker compose --profile orchestration --profile streaming --profile dashboard stop
```

Không dùng down -v khi cần giữ HDFS/Kafka/Airflow và checkpoint. Không để một phiên PySpark nhàn rỗi chiếm cores khi cần chạy DAG.

Các thao tác trình diễn truy vấn và kiểm tra là trực tiếp. Model/index có sẵn, ảnh/log cũ và các bảng pipeline trong dashboard phải được gọi là kết quả đã lưu. Sự chuyên nghiệp nằm ở việc chỉ rõ dữ liệu nào, phép xử lý nào và bằng chứng nào hỗ trợ từng kết luận.

Tài liệu đối chiếu: [Spark interactive shell](https://spark.apache.org/docs/3.5.7/quick-start.html), [Spark UI](https://spark.apache.org/docs/3.5.7/monitoring.html), [Docker Desktop Troubleshoot](https://docs.docker.com/desktop/troubleshoot-and-support/troubleshoot/). Source CLI Kafka đã đối chiếu bản 3.9.0.
