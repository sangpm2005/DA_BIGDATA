# Phiếu thao tác khi trình bày Bigdata

Chi tiết/copy lệnh: DEMO_HANDS_ON.md. Hai terminal tại D:\Bigdata: A điều khiển PowerShell; B vào PySpark cho phần Spark, thoát trước Kafka/Airflow. Các URL dùng khi dịch vụ đã sẵn sàng.

| Điểm dừng | Thao tác | Chỉ vào | Nói một câu |
|---|---|---|---|
| 1 — HDFS, bước 1–2 | Prepare/upload, ls, fsck | http://localhost:9870; file, block, DataNode | Raw có fingerprint, HDFS giữ file theo block; demo một DataNode |
| 2 — Đọc/làm sạch, bước 3–4 | PySpark read_raw, count, minh họa enrich; đọc processed/quarantine | Terminal; http://localhost:4040 Executors/Jobs | Parser multiline đọc nguồn; xử lý có partition; output được kiểm tra bằng số dòng/khóa |
| 3 — SQL, bước 5 | GROUP BY trên Parquet, show, explain | Driver UI SQL/Stages; PartitionFilters | Query thật tổng hợp trên HDFS, có shuffle khi cần và pruning khi lọc partition |
| 4 — Output/sản phẩm, bước 6–7 | Đọc model/assignments; lọc Shopee 2021-01 đến 2022-08; tìm dang nhap | http://localhost:8090; biểu đồ rồi review | Model là phân cụm khám phá; sản phẩm giúp khoanh vùng rồi kiểm tra văn bản |
| 5 — Streaming, bước 8 | Thoát PySpark; lấy offsets trước/sau producer; consumer; validator | Terminal offsets, metrics và HDFS checkpoint | Replay tạo message mới nhưng không tăng review unique khi giữ state |
| 6 — Điều phối, bước 9 | Chọn đúng DAG Run, Graph, Logs task 09 | http://localhost:8080 | DAG quản lý phụ thuộc/retry; kiểm tra cuối đọc dữ liệu thật |

## Số liệu gốc để đối chiếu, không ép kết quả chạy mới giống ảnh cũ

- Raw: 1.300.086 record; file 642.967.981 bytes, năm HDFS block, replication một theo minh chứng đã lưu.
- Processed: 1.299.873 record/khóa duy nhất; quarantine 74; ID trùng loại 139.
- Cân bằng: 1.300.086 = 1.299.873 + 74 + 139.
- Spark: hai worker container, hai cores mỗi worker; phân biệt driver với executor trong UI.
- Sao thấp toàn snapshot: 282.772 review, khoảng 21,75%.
- Shopee 2021-01 đến 2022-08: 260.800 review, 81.979 sao thấp, khoảng 31,43%.
- K-Means: k=8; 61.241 training rows; mẫu hash 25%; silhouette khoảng 0,019, không phải accuracy.
- Streaming cùng mẫu/checkpoint: 1.000 review/khóa unique; offsets và số batch tăng theo lượt gửi.
- Cube/index: 1.910 nhóm × 9 số đếm đã đối soát; toàn index 1.299.873 review.

## Trình tự một thao tác

1. “Tại bước này em kiểm tra/chạy ...”
2. Nhập lệnh, đợi kết quả và chỉ vào đúng số/cột/nút.
3. “Kết quả này cho thấy ...; phạm vi của kết luận là ...”

Thứ tự báo cáo gợi ý: HDFS → Spark → SQL → output/sản phẩm → Kafka → Airflow. Luyện từng điểm dừng trước. Chỉ Trigger mới toàn DAG khi đã đóng các job thủ công và dành đủ thời gian chạy.

Các lưu ý cần nhớ: dữ liệu lịch sử Google Play; CSV multiline reader một partition; hai worker nằm trên cùng máy; dashboard đọc snapshot; cùng Kafka key không tự chống trùng; giữ checkpoint; không gọi log/run cũ là kết quả vừa chạy.
