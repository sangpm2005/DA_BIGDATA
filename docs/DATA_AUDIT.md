# Kiểm tra toàn bộ dữ liệu gốc

Thực hiện ngày 12/10/2026 (giờ Việt Nam), bằng `scripts/audit_dataset.py`, đọc record CSV đúng quy tắc quoting thay vì đếm số dòng vật lý.

| Chỉ số | Kết quả |
|---|---:|
| Dung lượng | 642.967.981 byte, khoảng 613,18 MiB |
| Record | 1.300.086 |
| Thời gian nhỏ nhất | 2013-06-10 01:04:59 |
| Thời gian lớn nhất | 2022-08-27 10:44:03 |
| Record ngoài khoảng 2013–08/2022 | 0 |
| Thiếu content | 71 |
| Sao không hợp lệ | 2, đều bằng 0 |
| Thời gian không parse được | 0 |
| Record sai số trường CSV | 0 |
| Dòng lặp khóa appid + reviewid | 139 |
| Dòng lặp nội dung chuẩn hóa trong cùng app | 638.855 |
| Ứng viên quảng cáo/link theo luật ban đầu | 88 |

SHA-256 nguồn: `9d333c550cb141aa0f3e9f12af43e8e407809031b073b046b61607f84b39bdf9`.

Các số dưới đây tính trên **raw**, trước quarantine và dedup; cần phân biệt với kết quả Spark sau làm sạch.

| Nền tảng | App ID | Review raw | Sao trung bình hợp lệ | Tỷ lệ 1–2 sao trong số có sao hợp lệ |
|---|---|---:|---:|---:|
| Shopee | com.shopee.vn | 519.297 | 3,7724 | 27,55% |
| Lazada | com.lazada.android | 530.491 | 4,1119 | 18,65% |
| Sendo | com.sendo | 179.643 | 4,2060 | 14,08% |
| Tiki | vn.tiki.app.tikiandroid | 70.655 | 3,9958 | 21,98% |

Schema: index CSV, reviewid, username, userimage, content, score, thumbsupcount, reviewcreatedversion, at, replycontent, repliedat, appid.

## Quyết định từ dữ liệu thực tế

1. Giữ CSV gốc, lưu fingerprint, upload HDFS raw. Không cần thu thập bổ sung để bắt đầu.
2. Quarantine thiếu nội dung/sao lỗi/thời gian/app lỗi; lưu lý do và đối soát số dòng.
3. Khử trùng khóa review, không xóa hàng loạt nội dung giống nhau vì các review “ok”, “tốt” có thể thuộc nhiều người khác nhau.
4. Flag 88 review chỉ là ứng viên theo luật rất hẹp, không có nghĩa dataset chỉ có 88 spam. Xem mẫu và phân tích độ nhạy.
5. Không dùng username làm customer ID, không dùng thumbsupcount làm tiền giao dịch.
6. Loại username và userimage khỏi dữ liệu phân tích/xuất mẫu. Văn bản vẫn có thể chứa thông tin người viết tự đưa vào; kiểm tra trước khi công khai ví dụ.
7. Dataset không có nhãn khía cạnh chuẩn. Mẫu 1.000 review đã tạo có nhãn trống để người đọc kiểm tra.

Chi tiết máy đọc: `artifacts/audit/dataset_profile.json`. Mẫu nhãn: `artifacts/audit/annotation_sample.csv`. Khối lượng theo tháng: `artifacts/audit/monthly_volume.csv`.

## Đối soát sau làm sạch bằng Spark

Spark đọc đúng 1.300.086 record từ HDFS. Sau chuẩn hóa Unicode, có thêm một review chỉ chứa ký tự vô hình U+200D, nên số nội dung rỗng tăng từ 71 lên 72. Đây là thay đổi có giải thích được, không phải mất dòng do parser.

Quarantine 74 record = 72 nội dung rỗng sau chuẩn hóa + 2 sao không hợp lệ; khử trùng 139 bản sao ID; còn **1.299.873 review**. Đối soát: `1.300.086 = 74 + 139 + 1.299.873`.

