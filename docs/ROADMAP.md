# Lộ trình triển khai

## Mục tiêu và câu hỏi nghiên cứu

Đề tài: “Phát hiện các khía cạnh gây không hài lòng của khách hàng từ dữ liệu phản hồi trên các sàn thương mại điện tử tại Việt Nam”. Khi diễn giải kết quả, dùng “khía cạnh được nhắc đến trong phản hồi không hài lòng”, vì dữ liệu quan sát không chứng minh nguyên nhân.

1. Lượng phản hồi, phân bố sao và tỷ lệ đánh giá thấp thay đổi thế nào theo nền tảng và tháng?
2. Những khía cạnh nào thường được nhắc trong đánh giá 1–2 sao? Có khác nhau giữa các nền tảng và giai đoạn không?
3. K-Means có phát hiện các nhóm nội dung bổ sung cho bộ khía cạnh thiết kế trước không?
4. Cùng một logic phân tích hoạt động thế nào trên batch và luồng phản hồi phát lại qua Kafka?
5. Hệ thống xử lý dữ liệu thiếu/trùng/lỗi thế nào, có chạy lại được và truy vết kết quả về nguồn không?

## Bám sát 9 bước của giảng viên

| Bước mẫu | Thực hiện cho đề tài | Bằng chứng nghiệm thu |
|---|---|---|
| 1. Chuẩn bị HDFS | Tạo `/ecommerce/raw`, `processed`, `output`, `quarantine`, `models`, `checkpoints`, `streaming` | HDFS UI; lệnh liệt kê; có NameNode/DataNode |
| 2. Upload dữ liệu | Đưa CSV gốc vào raw; lưu checksum và nguồn | Dung lượng/checksum nguồn; `hdfs dfs -ls`; không ghi đè dữ liệu gốc trên Windows |
| 3. Đọc bằng Spark | Đọc CSV từ HDFS bằng schema rõ ràng; xử lý dấu phẩy/dòng mới trong trường có dấu nháy | Tổng record khớp audit; Spark UI thể hiện executors |
| 4. Làm sạch | Chuẩn hóa Unicode/whitespace; kiểm tra thời gian, sao, mã app; quarantine; dedup ID có thứ tự; viết Parquet theo nền tảng/năm | Đối soát raw = valid + quarantined + duplicate ID; file Parquet; giữ phủ định |
| 5. Spark SQL | Thống kê sao, phản hồi/tháng, tỷ lệ sao thấp, khía cạnh được nhắc và mức chưa xác định | SQL, mẫu số công khai, kế hoạch `explain`, partition pruning |
| 6. RFM & K-Means | Thay RFM bằng thống kê phản hồi; giữ TF-IDF + K-Means cho nội dung đánh giá thấp | Không tạo Amount/Monetary giả; seed, k, từ khóa, silhouette, mẫu kiểm tra bằng người |
| 7. Lưu kết quả | Bảng phân tích, chỉ số chất lượng, mô hình, metadata vào HDFS; xuất bảng nhỏ về máy | HDFS output và file tổng hợp; không collect toàn bộ CSV về driver |
| 8. Kafka ingest | Phát lại phản hồi lịch sử → Kafka → Structured Streaming → HDFS Parquet | Topic, offsets, microbatch, checkpoint, restart; ghi rõ là replay |
| 9. Airflow DAG | Điều phối các bước có retry, timeout, log, kiểm tra chất lượng | DAG chạy thành công; chạy lại không nhân đôi dữ liệu batch |

RFM cần Recency/Frequency/Monetary của giao dịch hoặc khách hàng định danh ổn định. `username` là tên hiển thị, `thumbsupcount` là lượt thấy hữu ích; cả hai không thay thế customer ID hay Monetary.

## Các chặng và điều kiện hoàn thành

### Chặng 1 — hiểu dữ liệu và chốt thiết kế

- Kiểm tra toàn bộ CSV, schema, appid, thời gian, sao, thiếu/trùng; fingerprint SHA-256.
- Chọn tiêu chí quarantine và dedup; giữ bản gốc để truy vết.
- Chuẩn bị mẫu phản hồi theo nền tảng × số sao để kiểm tra bằng người.
- Hoàn thành khi có hồ sơ dữ liệu và quyết định được ghi rõ; không chỉ nhìn vài dòng đầu.

### Chặng 2 — HDFS và Spark batch

- Dựng NameNode, DataNode, Spark master và hai worker trong Docker.
- Upload CSV, đọc từ HDFS, làm sạch, Parquet, Spark SQL, xuất bảng thống kê.
- Đối soát số dòng giữa audit và Spark; kiểm tra CSV có newline và dấu nháy; đưa dữ liệu không hợp lệ vào quarantine.
- Chụp Spark UI, số partition, stage/task; giải thích lazy evaluation, action, shuffle, cache và lỗi driver/worker.
- Hoàn thành khi pipeline chạy với dữ liệu đầy đủ, có log và số đếm đối soát.

### Chặng 3 — phân tích khía cạnh và K-Means

- Bộ khía cạnh ban đầu: vận chuyển; sản phẩm; CSKH; ứng dụng; thanh toán; giá/khuyến mãi; đổi trả/hoàn tiền.
- Bản đầu nhận diện **đề cập khía cạnh bằng luật**, ưu tiên minh bạch, dễ kiểm tra. Không tự gọi đây là ABSA đã được xác thực.
- Lọc tập đánh giá thấp, TF-IDF và K-Means; xem từ khóa và phản hồi đại diện, nhận diện cụm chưa ánh xạ.
- So sánh số lượng và tỷ lệ theo sàn/tháng; phân tích chênh lệch quy mô dữ liệu và khía cạnh đa nhãn.
- Đọc mẫu độc lập để đánh giá lỗi; chỉ báo cáo precision/recall/F1 khi có nhãn người đọc. Không cần deep learning để đạt mục tiêu môn học.
- Hoàn thành khi kết quả có bằng chứng phản hồi, tỷ lệ chưa xác định và giới hạn phương pháp.

### Chặng 4 — Kafka và Spark Structured Streaming

- Kafka một broker KRaft phù hợp demo tại máy; producer chỉ gửi trường cần thiết.
- Phát lại một phần CSV có `event_id`, thời gian review gốc, thời gian ingest hiện tại.
- Consumer xử lý microbatch, kiểm tra schema, dedup và ghi HDFS cùng checkpoint.
- Thử dừng/khởi động consumer; quan sát offsets và checkpoint; phân biệt thời gian sự kiện và thời gian xử lý.
- Hoàn thành khi có kết quả luồng và bằng chứng khôi phục; không mô tả là thu thập phản hồi mới từ Google Play.

### Chặng 5 — Airflow và khả năng tái lập

- DAG chạy batch theo đúng thứ tự, retry có giới hạn, failure rõ ràng; demo Kafka theo DAG riêng hoặc bước hữu hạn.
- Thử chạy lại batch, kiểm tra record không tăng vô lý; không xóa checkpoint để “sửa” lỗi.
- Chuẩn bị script demo và bảng giải thích mỗi thành phần; log chạy thật.
- Hoàn thành khi DAG chạy thành công toàn bộ, thao tác lặp lại và xử lý lỗi được chứng minh.

### Chặng 6 — sản phẩm và bàn giao

Đã triển khai ReviewLens tại http://localhost:8090, có biểu đồ/bộ lọc, tra cứu toàn dataset đã làm sạch, xuất CSV/SVG và bộ hình PNG/SVG cho báo cáo. Kịch bản demo cùng giới hạn ứng dụng ở `PRODUCT_GUIDE.md`. Báo cáo Word theo mẫu khoa đã hoàn thành trong `reports/`; push repo chờ địa chỉ do người dùng cung cấp. Đánh giá nhãn người đọc vẫn cần hoàn thiện.

- Giao diện nhỏ để xem bảng/biểu đồ, lọc sàn/thời gian/khía cạnh và phản hồi bằng chứng. Làm sau pipeline.
- Hoàn thiện hướng dẫn cài/chạy, kiến trúc, hạn chế, kết quả và kịch bản bảo vệ.
- Đã viết báo cáo Word từ kết quả đã kiểm chứng, thể hiện rõ giới hạn thực nghiệm; mục lục và đánh số dùng trường/style tự động của Word.
- Khi có repo: kiểm tra `.gitignore`, không commit CSV lớn, dữ liệu nhận dạng, secret hoặc Docker volumes; push source theo yêu cầu.

## Giới hạn cần trình bày khi bảo vệ

- 613 MB/1,3 triệu dòng không tự động chứng minh “Big Data”. Đồ án minh họa kiến trúc lưu trữ và xử lý phân tán; giải thích khả năng mở rộng khi volume/velocity tăng.
- Hai worker trên một máy minh họa phân tán giữa tiến trình/container, không phải thử nghiệm cluster nhiều máy vật lý.
- Một DataNode với replication = 1 không có khả năng chịu mất node như cluster production; demo HDFS bằng block/namespace và luồng đọc/ghi.
- Tỷ lệ đánh giá thấp là tỷ lệ trong tập review Google Play, không phải tỷ lệ khách hàng Việt Nam không hài lòng.
- Một review có thể khen khía cạnh A và chê B; review 1 sao có thể là spam hoặc nội dung tích cực. Sao thấp + nhắc khía cạnh chưa đủ chứng minh khía cạnh đó bị chê.
- Văn bản lặp “ok”, “tốt” giữa nhiều người không được xóa toàn bộ khỏi thống kê. Khử trùng ID để tránh lặp thu thập; tách thí nghiệm với nội dung lặp nếu cần.
- Dữ liệu thiếu nhãn khía cạnh và danh tính khách hàng ổn định; không báo cáo accuracy giả hoặc phân khúc RFM giả.

## Tài liệu kỹ thuật chính thức

- HDFS: https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-common/SingleCluster.html
- Spark CSV: https://spark.apache.org/docs/3.5.7/sql-data-sources-csv.html
- Spark K-Means: https://spark.apache.org/docs/3.5.7/ml-clustering.html
- Spark Kafka: https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html
- Kafka: https://kafka.apache.org/39/getting-started/quickstart/
- Airflow: https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html

