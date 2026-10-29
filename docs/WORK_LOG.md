# Nhật ký kỹ thuật và phân công năm tuần

Môn Bigdata, giảng viên TS. Huỳnh Thái Học. Nhóm gồm Huỳnh Quang Sang (2001230757, 14DHTH13), Phan Đức Thể (2001230915, 14DHTH13), Đỗ Trọng Vĩ (2001231033, 14DHTH06).

## Nhật ký có minh chứng

| Mốc | Công việc hoặc vấn đề | Minh chứng |
|---|---|---|
| 12/10/2026 | Audit CSV, đưa raw lên HDFS, chạy batch Spark và SQL | raw_inspection.json, data_quality.json, hdfs_blocks.txt, batch_full.log |
| 12/10/2026 | Khác UID Spark/Airflow làm lỗi ghi artifact; dùng file tạm và os.replace rồi chạy tiếp | src/review_analytics/io.py, tests_after_io_fix.log, DAG validation_20261012 |
| 12/10/2026 | Replay hai lượt, giữ checkpoint; dữ liệu streaming không nhân đôi | demo_evidence.json, stream_batch_0..5.json, stream_first.log |
| 12/10/2026 | Mở rộng task 07/09 cho sản phẩm; audit index đổi sang quét tuần tự | dashboard_airflow_task07.log, dashboard_airflow_task09.log, scripts/validate_dashboard.py |
| 15/10/2026 | Sửa kiểm tra Docker trên Windows PowerShell 5.1 khi Start-Process trả ExitCode null | scripts/Prepare-5MinDemo.ps1 và engine_probe.json đã chọn |
| 15/10/2026 | Đối soát HDFS và snapshot bổ sung, cùng tổng review và generation sản phẩm | persisted_validation.json, dashboard_validation.json, dashboard_preparation.json |
| 28/10/2026 | Chuẩn bị hồ sơ nộp PDF, tạo Git và loại mật khẩu khỏi cấu hình công khai | README, .env.example, Init-LocalConfig.ps1 và các commit từ ngày khởi tạo |
| 29/10/2026 | Đưa mã nguồn, output, log và ảnh vào hồ sơ kiểm chứng đầu PDF; thêm chỉ dẫn và kiểm tra tính toàn vẹn artifact | docs/EVIDENCE_GUIDE.md, scripts/check_submission_evidence.py, evidence/audit_20261029.json và PDF bản bổ sung. Không chạy lại toàn pipeline trong lần chỉnh báo cáo này. |

Tên file minh chứng ở trên được lưu trong `evidence/verified_20261028/` theo cùng cấu trúc. Ngày thư mục là ngày chọn và đóng gói bằng chứng; thời điểm thực thi nằm trong chính log/metadata. Có 13 tests đã đạt trong container ở lần kiểm chứng trước; ngày đóng gói chạy lại 11 tests text/IO/dashboard bằng Python tại máy, không coi là chạy lại tests Spark hoặc toàn pipeline.

Git được khởi tạo khi đóng gói bài nộp ngày 28/10/2026. Không có lịch sử commit trước đó để kiểm chứng mức đóng góp cá nhân; không tạo lại hoặc lùi ngày commit. Tác giả commit hiện tại là tài khoản thực hiện đóng gói, không đại diện mọi phần việc trong bảng kế hoạch.

## Phân công cân bằng trong năm tuần

Đây là kế hoạch trách nhiệm theo mốc tuần tương đối, không phải bảng chấm công đã xác nhận. Mỗi người phụ trách năm gói chính và năm lượt kiểm tra chéo, tỷ trọng khối lượng dự kiến 1/3.

| Tuần | Huỳnh Quang Sang | Phan Đức Thể | Đỗ Trọng Vĩ |
|---|---|---|---|
| 1 | Bài toán, phạm vi và chỉ số | Audit nguồn, schema và checksum | Docker, HDFS và Spark hai worker |
| 2 | Vùng HDFS, upload và block | Đọc CSV, chuẩn hóa và quarantine | Dedup, Parquet và đối soát |
| 3 | SQL và thống kê có mẫu số | Luật khía cạnh và phần chưa gán | TF-IDF, K-Means và metadata |
| 4 | Topic Kafka, producer và ID | Streaming, checkpoint và replay | DAG, log, retry và quyền ghi |
| 5 | Cube, index và kiểm tra generation | Biểu đồ, bộ lọc và tra cứu | Báo cáo, minh chứng và demo |

Rà soát chéo: Sang kiểm tra gói của Thể; Thể kiểm tra gói của Vĩ; Vĩ kiểm tra gói của Sang. Cần điều chỉnh hỗ trợ theo độ phức tạp, không chỉ theo số dòng code. Source và số liệu có thể kiểm tra chung; đóng góp cá nhân chỉ được xác nhận khi thành viên có bằng chứng riêng phù hợp.
