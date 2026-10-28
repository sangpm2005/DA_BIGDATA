# Mở source và kết quả sẵn để trình bày trong 5 phút

Mục tiêu: dùng đúng source đang triển khai, chỉ kết quả đã chuẩn bị rồi đi đến sản phẩm. Năm phút là thời lượng trình bày; các job xử lý, streaming và kiểm tra phải chạy xong trước đó. Không chờ toàn pipeline chạy trong lúc thầy đang nghe.

## 1. Chuẩn bị bằng một lệnh

PowerShell tại D:\Bigdata:

```powershell
Set-Location -LiteralPath D:\Bigdata
.\scripts\Prepare-5MinDemo.ps1 -RunPipeline
```

Điều kiện: Docker Desktop Linux containers và docker info có phản hồi; hai image runtime/Airflow đã build, CSV và mẫu audit của đồ án còn tại máy. Script không cài lại Docker hoặc đổi cấu hình bảo mật máy.

Script mở workspace/source, bật hạ tầng, kiểm tra HDFS và hai worker, kiểm tra hash/upload raw. Với -RunPipeline, nó Trigger một run Airflow mới và chờ toàn bộ DAG hoàn thành; tiếp theo đọc lại output HDFS bằng Spark, chuẩn bị các kết quả ngắn và giữ Spark UI để xem. Model/index được tái tạo trong task 06/07 của run này, không phải trong helper quan sát.

Chỉ bắt đầu trình bày sau dòng DEMO READY. Nếu lệnh lỗi hoặc Docker không phản hồi thì chưa có bộ runtime mới sẵn sàng; không dùng thông báo của một lượt cũ để kết luận lượt mới thành công.

Các tùy chọn:

```powershell
# Chỉ kiểm tra Docker Engine, hỗ trợ Windows PowerShell 5.1 và PowerShell 7:
.\scripts\Prepare-5MinDemo.ps1 -CheckOnly

# Chỉ mở source và các kết quả cũ đang có, không chạy job:
.\scripts\Prepare-5MinDemo.ps1 -OpenOnly

# Dùng batch/model/index đã có; chạy lại Kafka replay và kiểm tra HDFS:
.\scripts\Prepare-5MinDemo.ps1
```

Lựa chọn không có -RunPipeline không tạo một run Airflow mới. Khi trình bày Airflow ở chế độ đó phải chọn một run đã có và nói đúng thời điểm của nó.

Nếu thiếu image, build theo RUNBOOK trước khi setup. Nếu chưa có batch output và không chạy -RunPipeline, script sẽ dừng thay vì giả đã có dữ liệu.

## 2. Bố trí sẵn hai cửa sổ

### Cửa sổ 1: VS Code

Workspace HuynhQuangSang.code-workspace chỉnh font 18 và giữ các tab mở. Tên workspace và thư mục hiển thị đều là HuynhQuangSang. Các file source được mở sẵn:

1. scripts/bootstrap_hdfs.py — dòng 16.
2. src/review_analytics/spark_ops.py — dòng 21.
3. jobs/batch_pipeline.py — dòng 45; dùng Ctrl+G để chuyển tới 72, 123, 172.
4. jobs/stream_reviews.py — dòng 31.
5. dags/ecommerce_pipeline.py — dòng 19.

Các JSON kết quả sẵn có: outputs/data_quality.json, cluster_metadata.json và persisted_validation.json. Sau setup thành công có thêm một thư mục riêng outputs/demo_5min/<tag>/ chứa kết quả đọc lại hiện tại, cùng latest.json chỉ đúng phiên đó.

Trong VS Code bấm Ctrl+\ để chia editor thành hai nhóm: bên trái source, bên phải kết quả. Dùng Ctrl+P gõ đường dẫn file cần mở; Ctrl+G gõ số dòng. Dành 1–2 phút trước giờ báo cáo để đặt sẵn hai nhóm, tránh tìm file khi đang nói.

### Cửa sổ 2: Browser

Mở bốn tab theo thứ tự:

| Tab | URL | Chuẩn bị trước khi bấm giờ |
|---|---|---|
| 1 | http://localhost:9870 | Utilities → Browse the file system → /ecommerce/raw |
| 2 | http://localhost:4040/SQL/ | Chọn query tổng hợp hoặc query Shopee/2022; xem được plan |
| 3 | http://localhost:8080 | Đăng nhập airflow; mật khẩu lấy từ AIRFLOW_ADMIN_PASSWORD trong .env, chọn DAG và run mới trong latest.json; mở Graph |
| 4 | http://localhost:8090 | Mở Tổng quan toàn dữ liệu; thử trước nút Khía cạnh và tra cứu |

Spark master http://localhost:8081 có thể mở thêm nếu thầy hỏi worker. Trong 5 phút ưu tiên Driver UI để thấy SQL/Stages/Executors. Driver/executor là các vai trò khác nhau; hai worker chạy dưới dạng container trên cùng máy.

Không cần mở terminal để gõ lệnh trong 5 phút. Terminal setup có thể thu nhỏ sau khi thấy DEMO READY. File kết quả trong editor và UI cho thấy những công việc đã chạy chuẩn bị.

## 3. Các kết quả ngắn của phiên hiện tại

Đọc outputs/demo_5min/latest.json để lấy session_dir và application_id. Trong session_dir:

| File | Chỉ điểm gì |
|---|---|
| 01_hdfs_blocks.txt | File raw, kích thước, block và vị trí lưu thực tế |
| 02_read.json | raw_rows, reader_partitions và schema từ lần đọc hiện tại |
| 03_quality.json | Số dòng/khóa sạch, quarantine và cân bằng nguồn |
| 04_sql.json | Bốn dòng thống kê sàn vừa được Spark tính lại |
| 04_sql_plan.txt | Plan của query lọc Shopee/2022 |
| 05_model.json | Metadata và số assignments hiện có, không fit lại trong helper |
| 06_kafka_offsets.txt | End offsets Kafka lúc chuẩn bị phiên này |
| 06_stream.json | Số dòng/khóa streaming và batch mới nhất đọc lại HDFS |
| 07_airflow_tasks.txt | Trạng thái task của run mới, chỉ có khi dùng -RunPipeline |
| READY.json | Thời điểm, application ID và chế độ quan sát |

jobs/demo_observe.py là helper quan sát: đọc nguồn/output, chạy lại các truy vấn kiểm tra và giữ application sống. Nó không thay thế source pipeline; không ghi lại processed/HDFS/model. Một application giữ UI không có nghĩa job đang tính toán suốt thời gian trình bày: các query có thể đã hoàn tất trước khi bấm giờ.

## 4. Kịch bản đúng 300 giây

| Thời gian | Mở source / màn hình | Chỉ vào và trình bày |
|---|---|---|
| 0:00–0:20 | dags/ecommerce_pipeline.py, dòng 36 | “Đây là luồng xử lý của nhóm: nguồn vào HDFS, Spark xử lý và lưu kết quả; Kafka bổ sung nhánh luồng, Airflow điều phối, sản phẩm phục vụ quan sát.” |
| 0:20–0:50 | bootstrap_hdfs.py, dòng 16 và 21 → NameNode /raw hoặc 01_hdfs_blocks.txt | “Nhóm tạo các vùng dữ liệu và upload CSV có kiểm tra SHA-256. File raw khoảng 613 MiB, năm block theo cấu hình hiện tại; một DataNode, replication một.” |
| 0:50–1:15 | spark_ops.py, dòng 21 → 02_read.json | “PySpark đọc CSV bằng schema tường minh và multiline để giữ review có xuống dòng. Nguồn có 1.300.086 record; reader hiện là một partition, sau đó dữ liệu được phân phối cho xử lý.” |
| 1:15–1:55 | batch_pipeline.py, dòng 45 → 03_quality.json | “Chuẩn hóa, kiểm tra và loại bản sao ID; record lỗi vào quarantine. 1.300.086 = 1.299.873 sạch + 74 lỗi + 139 bản sao ID. Dữ liệu sạch ghi Parquet theo sàn/năm.” |
| 1:55–2:30 | batch_pipeline.py, dòng 72 → 04_sql.json → Spark SQL UI | “Spark SQL tổng hợp theo sàn và thời gian. Đây là bốn dòng kết quả và execution plan có scan/aggregate, Exchange khi cần. Query lọc sàn/năm có partition pruning.” |
| 2:30–2:50 | batch_pipeline.py, dòng 123 → 05_model.json | “Nhóm dùng luật khía cạnh và TF-IDF/K-Means để khám phá nội dung. K bằng tám, huấn luyện trên mẫu hash 25% đủ điều kiện; silhouette thấp, không coi là accuracy.” |
| 2:50–3:10 | batch_pipeline.py, dòng 172 → prepare_dashboard.py nếu cần | “Kết quả chính giữ trên HDFS; bảng tổng hợp nhỏ và dữ liệu phục vụ được chuẩn bị để sản phẩm lọc và tra cứu nhanh.” |
| 3:10–3:40 | stream_reviews.py, dòng 31 và 59 → 06_stream.json | “Kafka phát lại review lịch sử; Spark xử lý microbatch. ID ổn định, state loại trùng và checkpoint giúp chạy tiếp. End offsets tăng nhưng HDFS vẫn có 1.000 review/khóa unique với cùng mẫu.” |
| 3:40–4:00 | dags/ecommerce_pipeline.py, dòng 32 → Airflow Graph | “DAG quy định thứ tự và retry. Task cuối đọc lại HDFS và đối soát dữ liệu sản phẩm; run đã chuẩn bị hoàn tất trước buổi trình bày.” |
| 4:00–5:00 | ReviewLens: Tổng quan → Khía cạnh → Tra cứu | “Từ kết quả xử lý, sản phẩm giúp khoanh vùng và đọc phản hồi thật.” Thao tác một bộ lọc và đọc một review như mục 5. |

Đây là ngân sách thời gian, không phải thời gian benchmark của pipeline. Dùng số thật đang hiển thị; nếu nguồn thay đổi thì không học thuộc số cũ để thay kết quả.

## 5. Một phút cuối: thao tác sản phẩm đã luyện trước

1. Tổng quan: chỉ 1.299.873 review sạch và tỷ lệ 1–2 sao khoảng 21,75% của toàn snapshot.
2. Chỉ chọn Shopee, từ 2021-01 đến 2022-08, bấm Áp dụng. Snapshot hiện tại: 260.800 review, 81.979 sao thấp, khoảng 31,43%.
3. Chọn Khía cạnh, bấm ô Ứng dụng/Shopee để mở phản hồi. Nhập dang nhap, tìm và chỉ một review phù hợp.
4. Nói: “Biểu đồ giúp xác định nhóm phản hồi cần rà soát; văn bản thật giúp kiểm tra ngữ cảnh. Đây là phản hồi ứng dụng Google Play lịch sử, không đồng nhất với giao dịch hoặc toàn bộ khách hàng.”

Nếu thao tác lọc mất thời gian, mở sẵn thêm một tab sản phẩm đã lọc trước bằng UI, rồi chuyển tới tab đó trong buổi trình bày. Không sửa số liệu để tạo kết quả mong muốn. Tab Luồng dữ liệu của sản phẩm là bằng chứng đã lưu, không phải giám sát dịch vụ trực tiếp.

## 6. Những điều phải làm trước khi bấm giờ

- Xác nhận thấy DEMO READY của đúng phiên và các trang đều mở được.
- Source nằm bên trái, kết quả bên phải; font đủ lớn. Luyện Ctrl+G 45/72/123/172 trong batch_pipeline.py.
- Airflow chọn đúng run ID trong latest.json. Nếu full_dag_rerun=false thì run đang xem có thể là lịch sử, phải nói rõ.
- Dừng ở đoạn source quan trọng 5–10 dòng, không cuộn toàn file.
- Không Trigger DAG hoặc mở thêm PySpark khi helper đang giữ bốn cores.
- Chạy thử kịch bản một lần với đồng hồ 5 phút; dành trọn phút cuối cho sản phẩm.

## 7. Kết thúc hoặc muốn chuẩn bị lại

Helper giữ UI tối đa hai giờ theo mặc định, có thể đổi -HoldSeconds khi setup. Giải phóng Spark trước khi Trigger một DAG khác:

```powershell
.\scripts\Stop-5MinObserver.ps1
```

Chờ UI 4040 đóng rồi mới chạy lại Prepare-5MinDemo hoặc một job Spark khác. Script dừng helper bằng file điều khiển của đúng session; các dịch vụ và dữ liệu vẫn giữ.

Nếu Docker đang kẹt, có thể mở source/kết quả đã lưu bằng -OpenOnly và chạy ReviewLens bằng Python tại máy theo RUNBOOK. Đây là phương án trình bày source và kết quả lưu, không được nói HDFS/Spark/Airflow đang online khi chưa kiểm tra được.

Tài liệu đối chiếu: [Spark UI tồn tại theo application](https://spark.apache.org/docs/3.5.7/monitoring.html), [Airflow Graph/Grid](https://airflow.apache.org/docs/apache-airflow/2.10.5/ui.html), [VS Code CLI mở file tại dòng](https://code.visualstudio.com/docs/configure/command-line).
