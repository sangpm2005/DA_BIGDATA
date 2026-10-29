# Chỉ dẫn kiểm chứng bài nộp cuối kỳ Bigdata

Giảng viên: **TS. Huỳnh Thái Học**. Nhóm: Huỳnh Quang Sang (2001230757), Phan Đức Thể (2001230915), Đỗ Trọng Vĩ (2001231033).

Bản báo cáo bổ sung ngày **29/10/2026** có **63 trang**, đặt hồ sơ minh chứng ngay sau hai trang bìa. Mỗi phần ghép mã nguồn, kết quả đã lưu, giải thích và link đối chiếu. Có thể đọc minh chứng trong PDF mà chưa cần khởi động các dịch vụ.

**[Mở báo cáo cuối kỳ đã bổ sung minh chứng](submission/20261029/Bao_cao_cuoi_ky_Bigdata_bo_sung_minh_chung.pdf)**

Số trang PDF dưới đây đếm từ trang bìa là trang 1. Số La Mã là số in ở chân trang của phần hồ sơ.

| Tiêu chí | Trang PDF | Trang in | Mã nguồn và output đối chiếu |
|---|---:|---|---|
| Ma trận yêu cầu và kết quả | 3 | i | Link nguồn cố định và chỉ dẫn tới từng phần |
| HDFS và ingestion | 4 | ii | [bootstrap_hdfs.py](../scripts/bootstrap_hdfs.py), [fsck có vị trí block](../evidence/verified_20261028/logs/hdfs_blocks.txt) |
| Spark đọc và làm sạch | 5 | iii | [spark_ops.py](../src/review_analytics/spark_ops.py), [batch_pipeline.py](../jobs/batch_pipeline.py), [log Spark](../evidence/verified_20261028/logs/batch_full.log), [data_quality](../evidence/verified_20261028/data_quality.json) |
| Spark SQL và output | 6 | iv | [platform_summary.csv](../evidence/verified_20261028/platform_summary.csv), [physical plan](../evidence/verified_20261028/spark_query_plan.txt) |
| Phân tích và insight | 7 | v | Biểu đồ tỷ lệ theo sàn, mẫu số và [monthly_summary.csv](../evidence/verified_20261028/monthly_summary.csv) |
| Khía cạnh và K-Means | 8 | vi | Heatmap, [aspect_mentions.csv](../evidence/verified_20261028/aspect_mentions.csv), [cluster_metadata](../evidence/verified_20261028/cluster_metadata.json) |
| Kafka và streaming | 9 | vii | [producer](../jobs/replay_kafka.py), [consumer](../jobs/stream_reviews.py), [log producer](../evidence/verified_20261028/logs/kafka_replay.log), [offset và run metadata](../evidence/verified_20261028/demo_evidence.json), [HDFS audit bổ sung](../evidence/verified_20261028/persisted_validation.json) |
| Airflow | 10 | viii | [DAG](../dags/ecommerce_pipeline.py), [9 trạng thái task](../evidence/verified_20261028/demo_evidence.json), [log task 09 mở rộng](../evidence/verified_20261028/logs/dashboard_airflow_task09.log) |
| ReviewLens | 11 | ix | Ảnh giao diện, [ba ảnh đầy đủ](../evidence/verified_20261028/screenshots/), [audit cube và index](../evidence/verified_20261028/dashboard_validation.json), [mã giao diện và API](../dashboard/) |
| Tái lập và quá trình | 12 | x | [README](../README.md), [RUNBOOK](RUNBOOK.md), [nhật ký lỗi và phân công](WORK_LOG.md), [commit](https://github.com/sangpm2005/DA_BIGDATA/commits/main/) |

## Kiểm tra nhanh khi chưa chạy Docker

Từ thư mục repository, dùng Python 3.11:

```powershell
py -3.11 scripts/check_submission_evidence.py
```

Chương trình kiểm tra SHA-256 của 50 artifact, cân bằng số dòng, tổng các bảng SQL, log fsck, trạng thái DAG, offset/microbatch, đối soát sản phẩm và sự tồn tại ảnh. Không cần thêm thư viện Python ngoài thư viện chuẩn. Kết quả kiểm tra đã lưu tại [evidence/audit_20261029.json](../evidence/audit_20261029.json).

Lệnh này kiểm tra tính toàn vẹn và sự nhất quán của minh chứng **đã lưu**. Nó không chạy lại pipeline, không đo hiệu năng mới, không thay thế việc kiểm tra dịch vụ trực tiếp. SHA-256 giúp phát hiện file bị thay đổi, không tự xác thực nguồn gốc thực nghiệm.

## Tái lập luồng đầy đủ

1. Tải CSV từ [Kaggle](https://www.kaggle.com/datasets/hienbm/vietnamese-ecommerce-review); đặt tên và vị trí theo README. Bản tham chiếu có 642.967.981 byte, 1.300.086 record.
2. Tạo `.env` cục bộ bằng `scripts/Init-LocalConfig.ps1`, build runtime và khởi động HDFS/Spark theo [RUNBOOK](RUNBOOK.md).
3. Tạo vùng HDFS, upload, chạy inspect/clean/analyze/cluster/export. Kiểm tra số dòng, Parquet và output SQL.
4. Tạo topic Kafka, replay, chạy consumer, giữ checkpoint và thử phát lại. Kiểm tra riêng số message/offset với số ID duy nhất.
5. Chạy DAG Airflow; tạo cube, chỉ mục ReviewLens và audit. Mở dashboard ở `http://localhost:8090` theo [hướng dẫn sản phẩm](PRODUCT_GUIDE.md).

Các lệnh chi tiết và điều kiện thành công nằm trong RUNBOOK; hồ sơ PDF không thay thế toàn bộ cấu hình chạy.

## Mốc minh chứng và giới hạn

- **12/10:** log thực nghiệm chính; ảnh sản phẩm; run Airflow cơ bản 9/9 thành công; hai lượt replay có tổng end offsets 2.000, sáu microbatch, 1.000 khóa streaming duy nhất. Task 07/09 mở rộng có log riêng; không gộp thành một benchmark end-to-end mới.
- **15/10:** đối soát bổ sung có 12 microbatch, chín batch không thêm ID mới; streaming vẫn 1.000 khóa. Cube 1.910 nhóm với chín số đếm khớp index 1.299.873 review. Silhouette K-Means khoảng 0,0200 thuộc mốc này.
- **28/10:** đóng gói minh chứng, kiểm tra cục bộ, khởi tạo Git và xuất bản bài nộp. Các commit bắt đầu từ ngày này; không có lịch sử Git trước đó để suy ra đóng góp cá nhân.
- **29/10:** bổ sung cách trình bày minh chứng, chỉ dẫn đối chiếu và chương trình kiểm tra artifact. Không gọi đây là ngày chạy lại toàn hệ thống hoặc chụp mới giao diện.

Ảnh ReviewLens là đầu ra từ snapshot batch. Streaming chưa tự cập nhật dashboard. Luật khía cạnh thống kê đề cập từ khóa trong nhóm sao thấp; chưa có tập nhãn độc lập, F1 hoặc bằng chứng quan hệ nhân quả. K-Means chỉ là phần khám phá bổ sung; hướng phân tích có số liệu, bảng và biểu đồ là đầu ra chính.
