# Kiểm tra bằng người và nhãn đánh giá

`artifacts/audit/annotation_sample.csv` là mẫu phân tầng 4 nền tảng × 5 mức sao × 50 review = 1.000 dòng. Nhãn hiện để trống; đây không phải bộ ground truth đã có sẵn.

Mục đích chính là kiểm tra phương pháp phát hiện phàn nàn/khía cạnh. Không cần huấn luyện deep learning. Có thể bắt đầu với 200–300 review để rà luật; giữ phần test riêng nếu muốn báo cáo metrics cuối cùng.

## Các trường người đọc điền

- `complaint_gold`: `yes`, `no`, `uncertain`. Đọc nội dung, không lấy số sao làm đáp án. Có mô tả trải nghiệm tiêu cực/không hài lòng cụ thể hoặc phàn nàn chung thì `yes`.
- `aspects_gold`: nhiều nhãn phân cách bởi `|`: `delivery`, `product`, `support`, `app`, `payment`, `promotion`, `returns`. Chỉ gán khi có bằng chứng trong câu; nếu không đủ thông tin thì để trống và ghi notes.
- `aspect_polarities_gold`: ví dụ `delivery:negative|product:positive`; mỗi khía cạnh dùng `negative`, `positive`, `neutral`, `mixed`, `uncertain`.
- `spam_gold`: `yes`, `no`, `uncertain`; chỉ đánh giá dựa trên nội dung, không lấy flag ứng viên làm đáp án.
- `annotator`, `notes`: người đọc và lý do cho trường hợp khó.

## Bộ khía cạnh

| Nhãn | Bao gồm |
|---|---|
| delivery | Thời gian, phí vận chuyển, shipper, mất/giao sai/thiếu hàng |
| product | Chất lượng hàng, hàng giả, khác mô tả, đóng gói, người bán |
| support | Tổng đài, tư vấn, thái độ, xử lý khiếu nại |
| app | Crash/lag, giao diện, đăng nhập, tìm kiếm, cập nhật, lỗi thao tác |
| payment | Thanh toán, thẻ, ví, bị trừ tiền, phương thức thanh toán |
| promotion | Giá, mã giảm giá, voucher, sale, điều kiện khuyến mãi |
| returns | Đổi trả, hoàn tiền, hủy đơn và quy trình liên quan |

“Hàng tốt nhưng giao quá lâu”: product:positive và delivery:negative. “Không bị lỗi nữa”: không tự gán app:negative. “App tệ” có thể complaint=yes và app:negative. “Tệ” không đủ để xác định một trong bảy khía cạnh. Quảng cáo vay tiền dù 1 sao không phải bằng chứng phàn nàn về sàn.

## Cách dùng mẫu

`split` được chia theo hash nội dung chuẩn hóa để cùng văn bản không đi vào các split khác nhau, kể cả giữa sàn. Đây là split gợi ý, không thay cho kiểm tra near-duplicate và coverage. Chỉ dùng train/validation khi điều chỉnh luật; giữ test đến cuối.

Mẫu phân tầng không theo tỷ trọng số sao trong dữ liệu gốc. Tỷ lệ trên mẫu không phải tỷ lệ toàn dataset; metrics có thể báo cáo theo sàn/sao và macro, hoặc dùng trọng số xác suất lấy mẫu nếu cần ước lượng quần thể. Không báo cáo metric khi nhãn còn trống. Nếu có hai người đọc, gán độc lập một phần mẫu và đối chiếu bất đồng trước khi chốt.

