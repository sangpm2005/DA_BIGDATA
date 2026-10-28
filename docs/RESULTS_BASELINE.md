# Kết quả đã kiểm chứng và giới hạn baseline

Tập nguồn là snapshot đánh giá ứng dụng TMĐT 2013–08/2022. Các số dưới đây tính sau làm sạch và dedup ID, không phải thống kê khách hàng hiện tại.

## Luồng kỹ thuật

1.300.086 record raw → quarantine 74 + bỏ 139 bản sao ID → 1.299.873 review trong Parquet HDFS. Tổng số khóa review duy nhất và tổng thống kê theo sàn đều bằng số review này.

Hai worker Spark có tổng 4 core. Airflow có 9/9 task thành công. Bảy kiểm tra tự động đạt, gồm CSV quoting/newline, chuẩn hóa, quarantine/dedup và ghi artifact khi gián đoạn.

Sau hai lần replay, Kafka có 2.000 offsets; streaming Parquet vẫn có 1.000 review duy nhất. Sáu microbatch được lưu, trong đó ba batch của lượt lặp có 0 review mới. Đây là bằng chứng phục hồi checkpoint và chống trùng trong demo hữu hạn.

## Thống kê sau làm sạch

| Nền tảng | Review | Review 1–2 sao | Tỷ lệ 1–2 sao |
|---|---:|---:|---:|
| Shopee | 519.191 | 143.040 | 27,55% |
| Lazada | 530.404 | 98.905 | 18,65% |
| Sendo | 179.632 | 25.299 | 14,08% |
| Tiki | 70.646 | 15.528 | 21,98% |

Các tỷ lệ là tín hiệu sao thấp trong tập review. Quy mô, giai đoạn và người viết giữa các sàn khác nhau; bảng này không đủ để xếp hạng mức hài lòng của toàn bộ khách hàng.

## Khía cạnh được nhắc trong tập sao thấp

Theo luật hiện tại, ứng dụng và vận chuyển là hai nhóm được nhắc nhiều nhất ở cả bốn sàn. Tỷ lệ đề cập ứng dụng trong tập sao thấp lần lượt khoảng Shopee 24,80%, Lazada 14,48%, Sendo 18,00%, Tiki 20,06%. Tỷ lệ đề cập vận chuyển khoảng Shopee 14,31%, Lazada 14,32%, Sendo 13,89%, Tiki 19,56%.

Đây là **đề cập**, chưa phải cảm xúc tiêu cực theo từng khía cạnh. Review có thể khen một khía cạnh và chê khía cạnh khác. Các nhãn có thể cùng xuất hiện trong một review.

Tỷ lệ sao thấp chưa có nhãn khía cạnh: Shopee 52,56%; Lazada 62,15%; Sendo 55,23%; Tiki 48,86%. Cần đọc mẫu để phân biệt phản hồi chung không nêu khía cạnh với phản hồi bị luật bỏ sót; không ép tất cả review vào bảy nhóm.

## K-Means

K = 8, seed = 42, TF-IDF âm tiết unigram/bigram, khoảng cách cosine. Mẫu hash 25% từ review sao thấp phù hợp bộ lọc, cuối cùng có 61.241 vector khác 0. Mô hình và feature pipeline được lưu vào HDFS.

Silhouette lần chạy đầu khoảng 0,0221 và lần chạy qua Airflow khoảng 0,0191. Các cụm phân tách yếu; chỉ dùng để khám phá và kiểm tra bằng phản hồi đại diện. Không diễn giải silhouette thành độ chính xác nhận diện khía cạnh hoặc bằng chứng nguyên nhân.

## Phần cần hoàn thiện về nội dung nghiên cứu

Đọc và gán nhãn mẫu độc lập; đánh giá đề cập khía cạnh và polarity theo khía cạnh; xem lỗi, cải thiện luật và báo cáo phần chưa xác định. Nhãn trong mẫu 1.000 review vẫn để trống. Chưa có precision/recall/F1 khía cạnh; không công bố chỉ số được tạo từ chính luật dùng để dự đoán.

Nguồn kết quả: `outputs/platform_summary.csv`, `aspect_mentions.csv`, `unmatched_low_ratings.csv`, `cluster_metadata.json`, `demo_evidence.json`, `persisted_validation.json`.
