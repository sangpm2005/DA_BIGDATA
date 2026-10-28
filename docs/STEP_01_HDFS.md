# Bước 1: Chuẩn bị HDFS

Mục tiêu: tạo kho HDFS hoạt động được và các vùng lưu trữ rõ vai trò, để bước 2 có thể đưa CSV vào vùng raw. Bước 1 chưa đọc CSV, chưa làm sạch và chưa phân tích phản hồi.

## 1. Những thành phần đã chuẩn bị

Đồ án sử dụng Hadoop 3.3.6, Java 11 và Linux containers trên Docker Desktop. `infra/Dockerfile` đóng gói runtime và sao chép cấu hình Hadoop vào image. Spark dùng chung image, nhưng không có job Spark chạy trong hàm chuẩn bị thư mục.

- NameNode quản lý tên thư mục, tên file và thông tin ánh xạ block; không lưu nội dung review như DataNode.
- Một DataNode lưu các block khi có file được đưa vào HDFS. Hai Spark worker là thành phần xử lý khác, không phải DataNode.
- `fs.defaultFS=hdfs://namenode:9000` chỉ địa chỉ HDFS cho client bên trong mạng Docker. Cổng 9870 là giao diện web, không phải cổng đọc file HDFS của Spark.
- Volume `namenode_data` gắn với `/hadoop/name`, giữ metadata. Volume `datanode_data` gắn với `/hadoop/data`, giữ block. Khởi động lại container không đồng nghĩa xóa volume.
- Script entrypoint kiểm tra `/hadoop/name/current/VERSION` trước khi format NameNode. Khởi động thông thường dùng lại trạng thái đã khởi tạo.
- `dfs.replication=1` phù hợp cấu hình một DataNode trong thực hành. Đây chưa phải bằng chứng chịu lỗi của cụm nhiều máy. `dfs.permissions.enabled=false` là cấu hình demo tại máy, chưa triển khai phân quyền HDFS.

Đường dẫn `/ecommerce/raw` là đường dẫn logic trong HDFS. Nó khác thư mục Windows `D:\Bigdata`, và cũng khác thư mục vật lý `/hadoop/data` trong container.

## 2. Những vùng thư mục đã tạo

| Đường dẫn | Vai trò |
|---|---|
| `/ecommerce/raw` | Nguồn ban đầu, giữ nguyên để đối chiếu và xử lý lại |
| `/ecommerce/processed` | Dữ liệu đã làm sạch, ghi Parquet ở bước sau |
| `/ecommerce/output` | Kết quả phân tích và bảng tổng hợp |
| `/ecommerce/quarantine` | Record không hợp lệ, lưu để kiểm tra lý do |
| `/ecommerce/models` | Mô hình và artifact phục vụ phân cụm |
| `/ecommerce/checkpoints` | Tiến độ và state của Structured Streaming |
| `/ecommerce/streaming` | Dữ liệu và metric của luồng replay |

Ba vùng raw, processed và output bám yêu cầu của pipeline. Bốn vùng còn lại phục vụ kiểm tra chất lượng, phân cụm và streaming. Các tên vùng là quy ước của đồ án; không phải bảy thư mục mặc định Hadoop bắt buộc có.

## 3. Source cần mở và giải thích

1. `compose.yaml`, dòng 43: NameNode, DataNode, volumes, healthcheck và cổng web.
2. `infra/hadoop/core-site.xml`, dòng 3: địa chỉ HDFS mặc định cho client.
3. `infra/hadoop/hdfs-site.xml`, dòng 3: nơi lưu metadata, block, replication và cấu hình demo.
4. `infra/entrypoint.sh`, dòng 4: khởi tạo và chạy daemon HDFS.
5. `scripts/bootstrap_hdfs.py`, dòng 11: wrapper gọi CLI Hadoop thật, `check=True` làm script thất bại nếu lệnh Hadoop lỗi.
6. Cùng file, dòng 16: hàm `prepare()` tạo bảy vùng bằng `hdfs dfs -mkdir -p`.
7. `dags/ecommerce_pipeline.py`, dòng 19: task `01_prepare_hdfs` gọi script với `--mode prepare`; task upload là bước 02 riêng.

Đoạn code trọng tâm:

```python
BASE = "/ecommerce"

def prepare():
    hdfs("-mkdir", "-p", *[f"{BASE}/{zone}" for zone in
        ["raw", "processed", "output", "quarantine", "models", "checkpoints", "streaming"]])
```

`BASE` là vùng gốc của đồ án; vòng lặp ghép bảy đường dẫn. `-p` tạo cả thư mục cha khi cần và cho phép chạy lại khi thư mục đã tồn tại. Chạy lại không xóa hoặc ghi đè nội dung trong các vùng. Không dùng chế độ mặc định `all` khi muốn minh họa riêng bước 1, vì `all` còn gọi hàm upload của bước 2.

## 4. Trình tự demo khi Docker và các container đã sẵn sàng

Mở PowerShell tại thư mục dự án. Các lệnh dưới đây là hướng dẫn demo, chưa được chạy lại trong buổi giải thích này.

```powershell
docker compose ps namenode datanode spark-client
docker compose exec -T spark-client hdfs dfsadmin -report
docker compose exec -T spark-client hdfs dfsadmin -safemode get
docker compose exec -T spark-client python scripts/bootstrap_hdfs.py --mode prepare
docker compose exec -T spark-client hdfs dfs -ls /ecommerce
```

Giải thích `spark-client`: đây là container có sẵn Hadoop CLI và source của dự án. Lệnh đang gọi HDFS; không có nghĩa là đang chạy phân tích Spark.

Kết quả cần quan sát:

- NameNode/DataNode chạy; NameNode healthy là một chỉ báo ban đầu.
- `dfsadmin -report` có `Live datanodes (1)`, DataNode trạng thái `Normal` và dung lượng khả dụng.
- Safe mode là OFF để NameNode cho phép thay đổi namespace. Chờ hệ thống tự sẵn sàng nếu chưa OFF; không dùng lệnh ép thoát safe mode để che lỗi.
- Script prepare trả exit code 0, và `hdfs dfs -ls /ecommerce` có đủ bảy thư mục. Script có thể không in thông báo khi thành công.
- Chạy lại prepare rồi liệt kê lần nữa để minh họa tính chạy lại an toàn.

NameNode healthcheck trong Compose chỉ chạy `hdfs dfs -ls /`; nó không đủ để xác nhận DataNode sẵn sàng ghi dữ liệu. Vì vậy phải kiểm tra thêm DataNode, safe mode và kết quả tạo thư mục. Task success riêng lẻ cũng chưa đủ thay thế các kiểm tra đó.

Mở http://localhost:9870 khi Docker đang hoạt động. Trang Overview cho thấy Live Nodes; trang Utilities → Browse the file system có thể xem `/ecommerce`. Nên chụp cả kết quả terminal và trang web; giữ thời điểm chụp.

## 5. Minh chứng đã lưu của đồ án

- `outputs/logs/hdfs_report.txt`: báo cáo có một Live DataNode, trạng thái Normal. Thời điểm Last contact trong file là 11/10/2026 UTC, tương ứng sáng 12/10/2026 tại Việt Nam.
- Trong log chuẩn bị, `Num of Blocks: 0` là bình thường khi chưa upload dữ liệu. Năm block của CSV thuộc minh chứng bước 2, không phải yêu cầu bước 1.
- `outputs/demo_evidence.json`: task `01_prepare_hdfs` của run `validation_20261012` có state `success`.

Đây là minh chứng của lần chạy đã lưu. Không nói các container đang chạy hiện tại nếu chưa kiểm tra trực tiếp. Lần kiểm tra ngày 14/10, Docker chưa sẵn sàng; dashboard hoạt động bằng Python trên Windows không chứng minh HDFS hiện đang online.

## 6. Đoạn trình bày khoảng một phút

“Ở bước chuẩn bị HDFS, nhóm xây dựng kho lưu trữ cho các bước xử lý dữ liệu. Nhóm dùng Hadoop 3.3.6 trên Docker, gồm một NameNode quản lý metadata và một DataNode lưu block. Hai volume riêng giữ metadata và dữ liệu qua lần khởi động lại container. Client kết nối tới hdfs://namenode:9000. Nhóm viết script Python gọi lệnh Hadoop để tạo vùng raw, processed và output; bổ sung quarantine, models, checkpoints và streaming phục vụ các bước sau. Script dùng mkdir -p nên chạy lại không xóa dữ liệu đã có. Nhóm kiểm tra DataNode đã đăng ký, HDFS thoát safe mode và các vùng được tạo thành công. Có báo cáo HDFS và task 01 thành công của lần chạy đã lưu. Cấu hình một DataNode, replication bằng 1 đáp ứng bài thực hành tại máy; nhóm chưa kết luận khả năng chịu lỗi nhiều máy.”

## 7. Câu hỏi cần tự trả lời trước khi sang bước 2

1. NameNode và DataNode khác nhau ở điểm nào?
2. Vì sao `/ecommerce/raw` không phải là thư mục Windows?
3. Vì sao dùng volume và không format NameNode mỗi lần khởi động?
4. Nếu script chạy thành công nhưng không có Live DataNode, đã đủ sẵn sàng upload chưa?
5. Vì sao hai Spark worker không làm replication HDFS thành 2?

Tài liệu đối chiếu: [Kiến trúc HDFS Hadoop 3.3.6](https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html), [Các lệnh HDFS](https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-hdfs/HDFSCommands.html).
