# Giải thích cốt lõi Big Data khi bảo vệ

Mở HDFS/Spark/Airflow cùng source code để giải thích bằng một lần chạy thật. Giao diện phân tích và mô hình nội dung là phần bổ sung cho luồng dữ liệu.

## 1. Vì sao đề tài thuộc Big Data?

Tập nguồn có 1,3 triệu review, khoảng 613 MiB. Kích thước này một máy vẫn xử lý được; không nên chỉ dùng dung lượng làm lý do gọi là Big Data. Đồ án thực hành lưu trữ theo block trên HDFS, xử lý theo partition trên các executor, SQL phân tán, luồng Kafka và điều phối có trạng thái. Trình bày khả năng mở rộng của kiến trúc, cùng giới hạn demo trên một máy vật lý.

Hai Spark worker là hai container, mỗi worker 2 core. Một DataNode và replication = 1 phù hợp demo; không đại diện khả năng chịu mất máy của cluster production.

## 2. HDFS làm gì?

NameNode giữ namespace và vị trí block, DataNode lưu byte dữ liệu. CSV gốc được upload với fingerprint SHA-256; pipeline giữ raw, processed, quarantine, output và checkpoint ở các vùng riêng. Lệnh `hdfs fsck ... -files -blocks -locations` cho thấy vị trí block thật, không chỉ danh sách thư mục.

Checksum SHA-256 dùng để nhận biết nguồn không đổi. Checksum/block report của HDFS phục vụ lưu trữ và kiểm tra block; hai loại checksum này có mục đích khác nhau.

## 3. Driver, master, worker và executor khác nhau thế nào?

Driver chạy chương trình và điều phối job. Spark master quản lý tài nguyên standalone. Worker cấp chỗ chạy executor. Executor thực thi task trên partition; master không trực tiếp phân tích nội dung review.

Mở Spark master để thấy hai worker. Log job thể hiện executor được cấp trên cả hai worker. Spark driver UI chỉ hiện khi application đang chạy.

## 4. HDFS chia block rồi tại sao CSV vẫn đọc bằng một task?

Review có thể chứa xuống dòng trong trường được đặt trong dấu nháy. `multiLine=true` đọc đúng record, nhưng một file CSV như vậy không dễ tách đọc theo block độc lập. Audit và Spark đều đọc được 1.300.086 record; reader có một partition.

Sau đọc, pipeline `repartition(8)` để phân phối dữ liệu cho xử lý tiếp, rồi lưu Parquet theo platform/year. Chỉ tăng số worker không tự làm bước parse CSV này song song hơn.

## 5. Transformation và action là gì?

`select`, `filter`, `withColumn` tạo kế hoạch xử lý. `count`, `collect` hoặc ghi file kích hoạt tính toán. Vì Spark đánh giá lười, thời gian của câu `filter` riêng lẻ không phải thời gian xử lý dữ liệu thực tế.

Pipeline persist các DataFrame được dùng nhiều lần với `MEMORY_AND_DISK`, rồi unpersist khi xong. Cache có chi phí và không làm mọi query nhanh hơn; chỉ áp dụng ở phần nhiều action thực sự tái sử dụng.

## 6. Shuffle xảy ra ở đâu?

`repartition` di chuyển dữ liệu sang partition mới. Dedup cần đưa cùng khóa appid/reviewid vào cùng nhóm để chọn bản mới nhất. SQL `GROUP BY platform, month` trao đổi kết quả theo khóa. Kế hoạch SQL có `Exchange hashpartitioning` và `HashAggregate` thể hiện việc này.

Chi phí shuffle gồm CPU, truyền dữ liệu và ghi/đọc đĩa. Tăng số partition vô hạn có thể tạo overhead và file nhỏ; cấu hình 8 partition là lựa chọn cho demo này, không phải hằng số tối ưu cho mọi cluster.

## 7. Vì sao dùng Parquet và partition theo platform/year?

Parquet lưu theo cột, có schema và compression. Query chỉ đọc month và lọc Shopee/2022 có `ReadSchema: struct<month:string>` và `PartitionFilters` trong kế hoạch. Spark có thể bỏ qua các partition của sàn/năm khác.

Partition theo tháng ngay từ đầu có thể tạo nhiều file nhỏ với tập dữ liệu này. Cần đo kích thước file và truy vấn trước khi đổi thiết kế.

## 8. Làm sạch có làm mất dữ liệu không?

Các dòng không hợp lệ vào quarantine với reason; không âm thầm xóa. Nguồn 1.300.086 record = 1.299.873 hợp lệ sau dedup + 139 bản sao ID + 74 quarantine.

71 nội dung rỗng ở raw trở thành 72 sau chuẩn hóa vì có một review chỉ chứa U+200D. Hai dòng sao 0 là không hợp lệ. Nội dung giống nhau giữa các ID khác nhau vẫn được giữ; “ok” có thể là đánh giá của nhiều người.

## 9. Vì sao không làm RFM thật?

Không có giá trị đơn hàng, lịch sử giao dịch hoặc customer ID ổn định. Tên hiển thị không phải customer ID và lượt hữu ích không phải số tiền mua hàng. Thay RFM bằng thống kê review, còn giữ K-Means để thực hành Spark MLlib trên nội dung.

## 10. K-Means và phân tích khía cạnh có phải deep learning không?

Bản hiện tại dùng luật từ khóa cho đề cập khía cạnh và TF-IDF + K-Means trong Spark MLlib để khám phá cụm. Không cần deep learning để chứng minh luồng Big Data. K-Means là phân cụm không giám sát; chỉ số silhouette không phải accuracy nhận diện khía cạnh.

Silhouette ban đầu thấp, khoảng 0,022, và nhiều review sao thấp chưa ánh xạ được khía cạnh. Báo cáo đây là giới hạn quan sát được. Chưa có nhãn người đọc thì không báo cáo precision/recall/F1 khía cạnh.

## 11. Vì sao có Kafka khi dữ liệu là CSV lịch sử?

Producer mô phỏng sự kiện review mới bằng cách phát lại dữ liệu lịch sử. Thời gian nội dung vẫn thuộc 2013–2022; thời gian ingest là lúc replay. Demo này thực hành topic, partition, offset, microbatch và checkpoint, không phải thu thập review mới từ sàn.

## 12. Chạy lại consumer có đọc lại hết dữ liệu không?

Consumer dùng checkpoint HDFS để khôi phục tiến độ và state. Producer tạo event ID ổn định từ khóa review; stateful dedup ngăn review cùng ID làm tăng số review duy nhất khi replay lại.

`foreachBatch` ghi theo batch ID và overwrite để retry cùng batch có thể thay thế kết quả. Không gọi đây là giao dịch exactly-once tổng quát giữa Kafka và mọi sink. State dedup không watermark tăng theo số ID, phù hợp replay hữu hạn; production cần thiết kế retention/upsert phù hợp.

## 13. Airflow làm gì nếu Spark đã chạy được?

Airflow biểu diễn thứ tự, retry, timeout và log của các bước. Job xử lý dữ liệu vẫn chạy trên Spark. DAG có `schedule=None` vì nguồn là một snapshot lịch sử, không giả định mỗi ngày có nguồn mới.

Lần chạy đầu gặp lỗi quyền file output giữa Spark UID root và Airflow UID 50000. Pipeline đã đổi sang ghi tệp tạm và thay thế nguyên tử; chạy tiếp từ task lỗi là một ví dụ điều phối và phục hồi thực tế.

## 14. Kết quả cho phép kết luận điều gì?

Số lượng/tỷ lệ review sao thấp trong dataset Google Play; khía cạnh được nhắc trong tập đó; nhóm nội dung khám phá; khác biệt theo sàn và thời gian. Một review có thể nhắc nhiều khía cạnh nên tổng tỷ lệ đề cập có thể vượt 100%.

Không suy từ review sang tỷ lệ không hài lòng của mọi khách hàng Việt Nam, không coi số sao là nhãn cảm xúc hoàn hảo, và không dùng dữ liệu quan sát này để chứng minh nguyên nhân. “Hàng tốt nhưng giao chậm” cho thấy nhắc một khía cạnh trong review sao thấp chưa xác nhận khía cạnh ấy bị chê.

## 15. Vì sao có thêm dashboard và SQLite?

Sản phẩm giúp người xem nhận biết xu hướng, bấm khía cạnh và đọc phản hồi gốc. Spark tạo bảng tổng hợp theo sàn/tháng/sao trên HDFS, web cộng số đếm nhỏ để đáp ứng bộ lọc nhanh. Chỉ mục FTS5 nhận toàn bộ review xuất từ HDFS để tra cứu theo từ khóa; web không đọc lại CSV nguồn mỗi lần bấm.

Phân biệt xử lý phân tán với phục vụ tương tác: Spark/HDFS thực hiện phần nguồn và tổng hợp, còn SQLite phục vụ tìm kiếm tại máy. Chỉ mục có thể xây lại, không thay Parquet nguồn. Chứng minh bằng `jobs/prepare_dashboard.py`, task 07 và metadata generation/số dòng. Trình bày ma trận khía cạnh cùng phần chưa gán, không để giao diện biến đề cập thành kết luận nguyên nhân.

## Bằng chứng cần mở

- `artifacts/audit/dataset_profile.json`: kiểm tra nguồn độc lập.
- `outputs/cluster_evidence.json`: hai worker và core.
- `outputs/logs/hdfs_blocks.txt`: block HDFS.
- `outputs/data_quality.json`: đối soát xử lý.
- `outputs/spark_query_plan.txt`: pruning, cột đọc và shuffle.
- `outputs/persisted_validation.json`: kiểm tra dữ liệu thật sau lưu.
- `outputs/cluster_metadata.json`: tham số/cụm/silhouette và ví dụ.
- `outputs/logs/tests_after_io_fix.log`: kiểm tra logic.
- Airflow UI và trạng thái task: thứ tự thực thi, lỗi và lần chạy lại.
- ReviewLens http://localhost:8090 và `docs/PRODUCT_GUIDE.md`: demo từ biểu đồ đến phản hồi.
- `outputs/dashboard_preparation.json` / `dashboard_cube.csv`: bảng tổng hợp Spark và phiên dữ liệu.
- `outputs/logs/dashboard_airflow_task07.log`: bước phục vụ chạy qua Airflow.
- `outputs/report_charts/`: hình PNG/SVG có mẫu số và giới hạn để đưa vào báo cáo.

