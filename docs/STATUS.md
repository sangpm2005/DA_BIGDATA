# Trạng thái triển khai

Hồ sơ nộp bài được tổng hợp ngày 28/10/2026; repository DA_BIGDATA được khởi tạo khi đóng gói, không có commit hồi tố. Minh chứng bổ sung ngày 15/10 có 12 microbatch, chín batch không thêm ID mới; dữ liệu streaming vẫn 1.000 dòng/khóa. Cube/index generation 3803e26d88584384995a5fdbcf9fa37a khớp 1.910 nhóm x 9 số đếm. Các phần ghi ngày 12/10 và 14/10 bên dưới giữ nguyên như nhật ký các mốc trước. Bản PDF nộp mới ở docs/submission; kế hoạch nhóm và giới hạn lịch sử Git ở docs/WORK_LOG.md.

Đã hoàn tất và kiểm chứng pipeline kỹ thuật cùng sản phẩm ReviewLens tại http://localhost:8090 ngày 12/10/2026. Phần nhận diện nội dung đang ở mức baseline, chưa xác thực bằng nhãn người đọc.

- Đã audit toàn bộ 1.300.086 record, lưu checksum và mẫu kiểm tra 1.000 review.
- Đã chốt lộ trình theo 9 bước; thay RFM bằng thống kê phản hồi, giữ K-Means.
- Đã viết cấu hình HDFS/Spark hai worker, batch, Kafka replay/consumer và Airflow DAG.
- Bảy test normalization/Spark/ghi artifact đã chạy thành công trong container. Đã đọc đủ dữ liệu bằng Spark từ HDFS, làm sạch, Spark SQL và K-Means.
- Docker ban đầu không khởi động vì socket runtime cũ không truy cập được. Đã lưu các thư mục chỉ chứa socket sang tên backup và khởi động được Docker, không reset hoặc xóa volume.
- Batch hoàn tất: 1.299.873 review sau làm sạch; quarantine 74; khử trùng 139 bản sao ID, số dòng đối soát đúng.
- Kafka/Structured Streaming đã nhận 1.000 review replay và lưu HDFS cùng checkpoint.
- DAG Airflow `validation_20261012` có **9/9 task success**. Lỗi quyền ghi giữa Spark/Airflow đã được sửa bằng ghi tệp tạm và thay thế nguyên tử; đã phục hồi từ task 03.
- Kafka có 2.000 offsets sau hai lượt phát lại; Parquet streaming vẫn có đúng 1.000 review và 1.000 khóa duy nhất. Checkpoint giữ state qua lần khởi động lại; ba microbatch phát lại không tạo review mới.
- Đọc lại Parquet thật trên HDFS: 1.299.873 record và 1.299.873 khóa duy nhất; tổng theo sàn khớp; các số đếm batch không đổi so với trước khi chạy Airflow.
- Baseline K-Means trên 61.241 review trong mẫu hash 25%; silhouette lần đầu khoảng 0,0221, lần chạy qua Airflow khoảng 0,0191. Tỷ lệ sao thấp chưa gán khía cạnh bằng luật khoảng 49–62%. Không coi đây là kết quả nhận diện đã được xác thực.
- Chưa gán nhãn bằng người; chưa có chỉ số độ chính xác khía cạnh, chưa push GitHub.
- Đã bổ sung ReviewLens: biểu đồ xu hướng theo năm/tháng và lượng review, so sánh bốn sàn, năm mức sao, ma trận 7 khía cạnh, độ bao phủ, K-Means và bằng chứng pipeline. Bộ lọc thay đổi đúng mẫu số; bấm khía cạnh mở review thật.
- Spark tạo cube 1.910 dòng từ toàn bộ Parquet HDFS, xuất JSON gzip phân tán; chỉ mục FTS5 chứa đủ 1.299.873 review. Không chỉ tra cứu một tập ví dụ.
- Đã kiểm tra tìm kiếm không dấu, tra cứu từ ma trận, phân trang và tải CSV 4.764 review Shopee về đăng nhập/ứng dụng; xuất SVG đã kiểm tra và có bộ hình PNG/SVG cho báo cáo. Web không dùng CDN.
- 13 tests đạt trong container, gồm kiểm tra mẫu số/bộ lọc, tìm không dấu, snapshot cùng generation, cache và giữ chỉ mục cũ khi rebuild thất bại; 7 tests xử lý ban đầu tiếp tục đạt.
- Đối soát toàn bộ 1.910 ô cube × 9 số đếm với GROUP BY độc lập trên chỉ mục toàn bộ review: tất cả khớp. Task 09 đã bổ sung kiểm tra này.
- Đã hoàn thành `reports/Bao_cao_Bigdata_Nhom_Sang_The_Vi.docx` theo mẫu khoa: môn Bigdata, giảng viên TS. Huỳnh Thái Học, đúng ba thành viên; bỏ trang nhận xét GVHD. Báo cáo 45 trang, 5 chương, 20 bảng và 7 hình, trình bày từ phân tích bài toán đến pipeline, kiểm chứng và sản phẩm.
- Word có mục lục/danh mục tự động, tiêu đề đa cấp, chú thích theo chương và số trang theo section. Đã cập nhật trường bằng Microsoft Word, đối chiếu đủ 27 vị trí hình/bảng với danh mục và kiểm tra toàn bộ trang sau render; mẫu gốc giữ nguyên.

## Bằng chứng

- `outputs/demo_evidence.json`: 9 task, hai worker/tổng 4 core, offsets Kafka và so sánh các số đếm khi chạy lại.
- `outputs/persisted_validation.json`: đối soát Parquet thực tế trên HDFS.
- `outputs/data_quality.json`: raw, quarantine và dedup.
- `outputs/logs/tests_after_io_fix.log`: 7 tests đạt.
- `docs/DEFENSE_GUIDE.md`: giải thích các khái niệm qua code và lần chạy thật.
- `docs/PRODUCT_GUIDE.md`: thao tác sản phẩm, kịch bản demo, kiến trúc phục vụ và giới hạn ứng dụng.
- `outputs/screenshots/`: ảnh sản phẩm; `outputs/report_charts/`: hình cho báo cáo.
- `outputs/logs/dashboard_airflow_task07.log`: chạy riêng task 07 đã mở rộng để tạo snapshot dashboard.
- `outputs/dashboard_validation.json`: toàn bộ số đếm cube khớp chỉ mục, gắn generation của lần chạy.
- `outputs/logs/dashboard_airflow_task09.log`: task 09 mở rộng đã chạy thành công, kiểm tra cả HDFS và lớp phục vụ. Phép audit đã sửa để quét tuần tự, tránh truy cập ngẫu nhiên qua ổ Windows.
- `outputs/dashboard_acceptance.json`: kết quả API, thời gian truy vấn thử, số tests đạt và các thao tác giao diện đã kiểm tra.

## Việc tiếp theo

Kiểm tra mẫu phản hồi độc lập, phân biệt đề cập khía cạnh với cảm xúc tiêu cực tại khía cạnh; cải thiện baseline từ lỗi quan sát được. Sản phẩm, biểu đồ và báo cáo Word đã hoàn thành; chuẩn bị push source khi người dùng cung cấp địa chỉ repo. Tiếp tục giữ trọng tâm HDFS, partition/shuffle, SQL, streaming/checkpoint và điều phối.

## Chuẩn bị nộp tiến độ ngày 14/10/2026

- Đã tạo báo cáo tiến độ 13 trang, có mục lục tự động, bảng kết quả, sơ đồ kiến trúc, biểu đồ, ảnh ReviewLens, khó khăn và phần chưa hoàn thành.
- Phân công theo kế hoạch 5 tuần: mỗi thành viên 5 gói chính và 5 lượt rà soát chéo, tỷ trọng dự kiến 1/3. Các tuần là mốc tương đối; chưa dùng làm nhật ký hay xác nhận đóng góp cá nhân.
- Đã chuẩn bị mô tả để dán vào ô trả lời và bộ PDF/ZIP source, cấu hình, test, minh chứng, bản Word và bảng phân công. CSV gốc, chỉ mục SQLite và Docker volumes không đóng gói; hướng dẫn tái lập được kèm theo.
- Ngày 14/10, API health và overview trả HTTP 200, số liệu khớp snapshot Spark đã lưu. Lớp phục vụ chạy bằng Python trên Windows tại localhost:8090. Docker chưa sẵn sàng ở lần kiểm tra này; bằng chứng HDFS/Spark/Kafka/Airflow vẫn là lần đã lưu ngày 12/10, không coi là trạng thái trực tiếp hôm nay.
- Đã chuẩn bị tệp tại máy; chưa gửi bài trên cổng học tập.

