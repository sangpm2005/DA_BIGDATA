# Minh chứng chọn lọc cho bài nộp

Thư mục được đóng gói ngày 28/10/2026; không có nghĩa mọi job chạy vào ngày đó.

- `demo_evidence.json` và log ngày 12/10 chứng minh DAG cơ bản 9/9 success và thử replay hai lượt với sáu batch, tổng end offsets 2.000. Task 07/09 mở rộng có log riêng.
- `persisted_validation.json`, `dashboard_validation.json` và metadata hiện tại phản ánh đối soát bổ sung ngày 15/10: 12 batch, chín batch không thêm unique; HDFS vẫn giữ 1.000 review/khóa streaming. Generation sản phẩm 3803e26d88584384995a5fdbcf9fa37a.
- Metadata K-Means mới có silhouette khoảng 0,0200. Log/report ban đầu có khoảng 0,0191; đây là các mốc khác nhau, không phải accuracy.
- Biểu đồ/ảnh cũ giữ theo lần tạo đã có; không gọi chúng là ảnh giám sát trực tiếp ngày 28/10.
- Cluster metadata công khai bỏ ví dụ văn bản; log có tham số mật khẩu được che nếu phát hiện. Raw, tập annotation có văn bản và index không được đưa vào thư mục này.

`MANIFEST_SHA256.json` ghi hash chính các bản công khai sau khi đã che hoặc tổng hợp nếu cần. Kiểm chứng nội dung theo source và README chính; không dùng một ảnh rời làm bằng chứng cho mọi chức năng.
