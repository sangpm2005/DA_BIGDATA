# Bước 2: Upload dữ liệu vào HDFS

Mục tiêu: đưa nguyên file CSV nguồn vào vùng raw trên HDFS, ghi nhận phiên bản nguồn và kiểm tra file cùng các block đã được lưu. Bước 1 chuẩn bị kho và thư mục; bước 2 đưa dữ liệu vào kho. CSV được giữ nguyên byte; đọc schema bằng Spark và làm sạch thuộc các bước sau.

## 1. Đầu vào và đầu ra của đồ án

| Vị trí | Đường dẫn | Ý nghĩa |
|---|---|---|
| Máy Windows | `D:\Bigdata\vietnamese_ecommerce_review.csv` | File đã tải về |
| Bên trong container client | `/input/reviews.csv` | Cùng file nguồn được Docker bind mount để client đọc |
| HDFS | `/ecommerce/raw/reviews.csv` | Bản được sao chép vào kho HDFS |
| HDFS | `/ecommerce/raw/source_sha256.txt` | Giá trị SHA-256 của file nguồn lúc upload |

`compose.yaml`, dòng 11 khai báo:

```yaml
- ./vietnamese_ecommerce_review.csv:/input/reviews.csv:ro
```

Phần trước dấu hai chấm là file trên máy, phần giữa là nơi container nhìn thấy file, `ro` là chỉ đọc. Bind mount giúp client truy cập file Windows, chưa đưa file vào HDFS. Việc sao chép vào HDFS xảy ra khi gọi `hdfs dfs -put`.

File nguồn đã kiểm chứng có 642.967.981 byte, khoảng 613,18 MiB; hồ sơ audit ghi 1.300.086 record CSV, dữ liệu Google Play của bốn ứng dụng Shopee, Lazada, Sendo và Tiki. HDFS lưu byte và block, không tự hiểu nội dung review hoặc đếm record CSV.

## 2. Source và logic upload

Mở `scripts/bootstrap_hdfs.py`, hàm `upload(source)` tại dòng 21. Hàm có ba phần:

1. Đọc file nguồn theo từng khối 1 MiB để tính SHA-256. Việc tính hash không cần nạp toàn bộ CSV vào RAM.
2. Kiểm tra `/ecommerce/raw/reviews.csv` đã tồn tại hay chưa. Nếu đã có, so SHA-256 nguồn với giá trị trong `source_sha256.txt`; khớp thì bỏ qua upload, khác hoặc thiếu manifest thì báo lỗi.
3. Khi chưa có file đích, dùng `hdfs dfs -put` để upload, rồi ghi manifest SHA-256 lên HDFS.

Đoạn băm nguồn:

```python
digest = hashlib.sha256()
with source.open("rb") as handle:
    while chunk := handle.read(1024 * 1024):
        digest.update(chunk)
fingerprint = digest.hexdigest()
```

Đoạn kiểm tra chạy lại:

```python
exists = hdfs("-test", "-e", destination, check=False).returncode == 0
if exists:
    stored = hdfs("-cat", manifest, check=False, capture=True)
    if stored.returncode != 0 or stored.stdout.strip() != fingerprint:
        raise ValueError("HDFS raw already exists with a different or missing fingerprint; use a versioned raw path")
    print("Source already uploaded with matching SHA-256", flush=True)
    return
```

Lệnh upload trọng tâm:

```python
hdfs("-put", str(source), destination)
```

Với input và cấu hình hiện tại, tương đương:

```bash
hdfs dfs -put /input/reviews.csv /ecommerce/raw/reviews.csv
```

Không có cờ `-f` ép ghi đè. `check=True` trong wrapper làm script thất bại nếu lệnh upload lỗi. Task `02_upload_csv` tại `dags/ecommerce_pipeline.py`, dòng 20 gọi script với `--mode upload`.

Chạy lại cùng nguồn không tạo thêm bản CSV và không nhân đôi số review. Đây là chống upload lại cùng file; chưa phải khử trùng các record trong dữ liệu.

## 3. SHA-256 giúp gì và kiểm tra đến đâu?

SHA-256 là dấu vân tay tính từ nội dung byte. Đổi tên file không đổi hash; sửa nội dung sẽ làm hash thay đổi với xác suất trùng cực nhỏ. Mục tiêu ở đây là nhận diện nguồn, không phải mã hóa nội dung review.

Giá trị nguồn đã lưu trong `artifacts/audit/dataset_profile.json`:

```text
9d333c550cb141aa0f3e9f12af43e8e407809031b073b046b61607f84b39bdf9
```

Giới hạn cần nói đúng: script tính hash file nguồn và lưu hash vào manifest. Khi chạy lại, nó đọc manifest để so với nguồn; chưa tự đọc lại toàn bộ CSV trên HDFS để tính SHA-256. Vì vậy manifest khớp là kiểm tra nguồn theo giá trị đã lưu, chưa phải phép đối soát byte độc lập của file đích.

Muốn kiểm tra đầy đủ hơn, tính SHA-256 trực tiếp từ file HDFS theo lệnh ở phần demo. Không so `hdfs dfs -checksum` trực tiếp với SHA-256 nguồn, vì checksum HDFS không mặc định là SHA-256 toàn file.

Vùng raw được bảo vệ khỏi ghi đè bởi logic script. Cấu hình thực hành đang tắt phân quyền HDFS, nên không tuyên bố raw đã được khóa bằng ACL hoặc cơ chế bất biến của hệ thống.

## 4. HDFS lưu CSV thành những block nào?

Minh chứng `outputs/logs/hdfs_blocks.txt` của lần chạy ngày 12/10/2026 theo giờ Việt Nam ghi:

| Block | Kích thước byte | Replica thực tế |
|---|---:|---:|
| 0 | 134.217.728 | 1 |
| 1 | 134.217.728 | 1 |
| 2 | 134.217.728 | 1 |
| 3 | 134.217.728 | 1 |
| 4 | 106.097.069 | 1 |

Bốn block đầu đầy 128 MiB; block cuối chứa phần còn lại. Tổng là `4 × 134.217.728 + 106.097.069 = 642.967.981` byte. Đây là cơ chế lưu trữ của HDFS; nhóm không tự cắt CSV thành năm file.

Client vẫn nhìn thấy một file `/ecommerce/raw/reviews.csv`. Cả năm block nằm trên một DataNode của cấu hình hiện tại. Năm block không có nghĩa năm máy, năm file CSV hoặc năm Spark task. Replication bằng 1 nghĩa mỗi block có một bản sao.

`fsck` đã ghi `HEALTHY`, `Missing blocks: 0`, `Corrupt blocks: 0`. Điều này xác nhận trạng thái block theo kiểm tra HDFS, chưa xác nhận chất lượng nội dung CSV hay khả năng chịu lỗi khi mất DataNode duy nhất.

## 5. Trình tự demo

Các lệnh dưới đây là hướng dẫn, chưa được chạy lại trong buổi giải thích này. Cần Docker và HDFS sẵn sàng từ bước 1. Mở PowerShell tại thư mục dự án.

Kiểm tra nguồn trên máy:

```powershell
Get-Item -LiteralPath .\vietnamese_ecommerce_review.csv | Select-Object Name,Length
Get-FileHash -LiteralPath .\vietnamese_ecommerce_review.csv -Algorithm SHA256
```

Upload bằng script, sau đó kiểm tra file và manifest:

```powershell
docker compose exec -T spark-client python scripts/bootstrap_hdfs.py --mode upload
docker compose exec -T spark-client hdfs dfs -ls /ecommerce/raw
docker compose exec -T spark-client hdfs dfs -stat '%b' /ecommerce/raw/reviews.csv
docker compose exec -T spark-client hdfs dfs -cat /ecommerce/raw/source_sha256.txt
docker compose exec -T spark-client hdfs fsck /ecommerce/raw/reviews.csv -files -blocks -locations
```

`-stat '%b'` lấy kích thước byte; `fsck` có `-files -blocks -locations` để hiển thị file, block và nơi lưu. Sau upload dự kiến raw có CSV và manifest, kích thước CSV 642.967.981 byte, năm block và trạng thái HEALTHY.

Lần đầu script in `Uploaded source; SHA-256 ...`. Khi file đã có cùng manifest, in `Source already uploaded with matching SHA-256`. Trường hợp thứ hai là kết quả chạy lại bình thường, không phải thiếu bước upload.

Đọc lại file HDFS để kiểm tra hash độc lập, khi cần trình bày phép đối soát toàn vẹn:

```powershell
docker compose exec -T spark-client bash -o pipefail -c 'hdfs dfs -cat /ecommerce/raw/reviews.csv | sha256sum'
```

Phép pipe chạy bên trong Linux container để giữ nguyên byte. `pipefail` giúp lệnh báo thất bại nếu việc đọc HDFS lỗi. Nó quét toàn bộ CSV, không sửa hoặc tải CSV thành file mới. So giá trị trả về với `Get-FileHash` và manifest; ba giá trị phải bằng nhau trước khi nói đã đối soát byte file đích. Chưa có log của phép SHA-256 đọc lại này trong minh chứng đang lưu; không mô tả nó đã chạy thành công.

Giao diện: mở http://localhost:9870, Utilities → Browse the file system → `/ecommerce/raw`. Show `reviews.csv`, kích thước, block size và replication; terminal `fsck` thể hiện vị trí từng block rõ hơn.

Không dùng `wc -l` để khẳng định số record review: CSV có trường văn bản chứa xuống dòng, nên số dòng vật lý có thể khác số record. Audit nguồn dùng parser CSV; đọc bằng Spark sẽ được giải thích ở bước 3.

## 6. Source và minh chứng cần mở

- `compose.yaml:11`: nguồn Windows được mount vào client, chế độ chỉ đọc.
- `scripts/bootstrap_hdfs.py:21`: hash, kiểm tra file đã tồn tại, upload và manifest.
- `dags/ecommerce_pipeline.py:20`: task 02 tự động gọi chế độ upload.
- `artifacts/audit/dataset_profile.json`: nguồn, kích thước, SHA-256 và số record audit.
- `outputs/logs/hdfs_blocks.txt`: kích thước đích, năm block, replication, HEALTHY và không có block thiếu/hỏng.
- `outputs/demo_evidence.json`: task `02_upload_csv` của run `validation_20261012` đã success. Task thành công có thể là nhánh bỏ qua vì nguồn đã upload; không suy ra mỗi lần success là một lần sao chép 613 MiB mới.

Chụp ảnh source trọng tâm, kết quả terminal và giao diện raw kèm thời điểm. Những file minh chứng trên là của lần chạy đã lưu; buổi giải thích này chưa xác nhận trạng thái HDFS trực tiếp.

## 7. Đoạn trình bày khoảng một phút

“Sau khi chuẩn bị HDFS, nhóm đưa nguyên CSV nguồn vào vùng raw. Docker cho container client đọc file từ máy bằng bind mount chỉ đọc. Script tính SHA-256 để ghi nhận nguồn, kiểm tra file đích đã tồn tại hay chưa, rồi gọi hdfs dfs -put để upload khi cần. Chạy lại cùng nguồn sẽ bỏ qua upload; nguồn khác hoặc thiếu manifest thì báo lỗi để tránh ghi đè nhầm. Bản trên HDFS có 642.967.981 byte, được lưu thành năm block, mỗi block một replica trên DataNode hiện tại. Nhóm có log fsck HEALTHY, không có block thiếu hay hỏng, cùng task 02 thành công. Manifest ghi hash nguồn; nếu cần khẳng định file HDFS giống nguyên byte nguồn, nhóm phải thực hiện thêm phép đọc lại và so SHA-256. Ở bước này nhóm giữ nguyên CSV để các bước xử lý tiếp theo có nguồn đối chiếu.”

## 8. Câu hỏi tự kiểm tra

1. Mount CSV vào container khác upload CSV vào HDFS như thế nào?
2. Vì sao dùng vùng raw và giữ nguyên file nguồn?
3. Chạy upload hai lần cùng nguồn có nhân đôi số review không?
4. Năm block có nghĩa năm DataNode hoặc năm Spark task không?
5. Manifest khớp và fsck HEALTHY đã đủ để khẳng định SHA-256 toàn file HDFS khớp nguồn chưa?

Tài liệu đối chiếu: [Hadoop File System Shell](https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-common/FileSystemShell.html), [HDFS Commands Guide](https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-hdfs/HDFSCommands.html).
