# ReviewLens — sản phẩm quan sát và rà soát phản hồi

Mở **http://localhost:8090**. Sản phẩm giúp người phụ trách trải nghiệm ứng dụng, vận chuyển hoặc CSKH tìm những nhóm phản hồi cần đọc trước, so sánh theo nền tảng/giai đoạn và xuất bằng chứng để thảo luận cải thiện dịch vụ.

Ứng dụng thực tế là quy trình **quan sát → khoanh vùng → đọc phản hồi → đề xuất kiểm tra**. Ví dụ: chọn Shopee năm 2021–08/2022, nhìn xu hướng sao thấp, mở khía cạnh ứng dụng rồi tìm “dang nhap” để rà soát trải nghiệm đăng nhập. Kết quả là một danh sách tình huống và số lượng phản hồi hỗ trợ việc điều tra; quyết định cải thiện còn cần kiểm chứng bằng dữ liệu vận hành và con người.

## Các màn hình

| Màn hình | Quan sát và thao tác | Ý nghĩa |
|---|---|---|
| Tổng quan | Lượng review, tỷ lệ 1–2 sao, trung bình sao, độ bao phủ; xu hướng theo năm/tháng; tỷ lệ từng sàn; phân bố năm mức sao | Nhận biết giai đoạn và chủ đề cần rà soát; đọc tỷ lệ cùng mẫu số |
| Khía cạnh | Ma trận 7 khía cạnh × sàn, bấm một ô mở phản hồi; có phần chưa gán; 8 cụm K-Means | Khoanh vùng và kiểm tra các giả thuyết nội dung |
| Tra cứu phản hồi | Từ khóa có/không dấu, sàn, số sao, khía cạnh, phân trang 20 review; xuất tối đa 5.000 dòng | Đối chiếu biểu đồ với văn bản thật trên toàn dataset đã làm sạch |
| Luồng dữ liệu | Raw/quarantine/dedup/clean; 9 task Airflow; offset Kafka và unique trên HDFS; microbatch; kế hoạch Spark SQL | Chứng minh nguồn gốc và quá trình xử lý của số liệu |

Bộ lọc sàn/tháng/ứng viên spam thay đổi mẫu số của toàn bộ biểu đồ thống kê. Bộ lọc riêng ở phần tra cứu áp dụng cho danh sách review. Phần K-Means hiện dùng mô hình trên mẫu hash 25% toàn snapshot đủ điều kiện, được ghi rõ và không tính lại theo bộ lọc giao diện. Màn hình pipeline hiển thị bằng chứng đã lưu của toàn lần chạy, không phải trạng thái trực tiếp của dịch vụ.

Tìm kiếm nhiều từ yêu cầu tất cả các từ xuất hiện trong review, chấp nhận không dấu; không yêu cầu các từ nằm cạnh nhau thành một cụm. Kết quả vẫn cần đọc để kiểm tra ý nghĩa trong ngữ cảnh.

## Kịch bản demo 5–7 phút

1. Mở Tổng quan: chỉ ra 1.299.873 review đã làm sạch, 282.772 review 1–2 sao (21,75%), trung bình 3,98 sao. Giải thích đây là tập Google Play lịch sử.
2. So sánh bốn sàn: Shopee 27,55%; Lazada 18,65%; Sendo 14,08%; Tiki 21,98%. Đọc số review đi kèm để tránh so sánh số tuyệt đối giữa tập khác quy mô.
3. Chỉ giữ Shopee, đặt Từ tháng `2021-01`, Đến tháng `2022-08`, bấm Áp dụng. Kết quả kiểm chứng: 260.800 review, 81.979 sao thấp, tỷ lệ 31,43%. Chuyển theo năm/tháng hoặc số review để thấy quy mô quan sát.
4. Mở Khía cạnh, bấm ô Ứng dụng của Shopee để mở review. Nhập `dang nhap`, bấm Tìm phản hồi. Đọc vài phản hồi và nhận xét điều gì được chứng minh, điều gì chỉ là giả thuyết. Đổi bộ lọc để so sánh với Vận chuyển.
5. Xuất thống kê CSV hoặc review CSV. Tải biểu đồ SVG để chèn vào slide. Nút In / Lưu PDF dùng chức năng in của trình duyệt đối với trang đang xem.
6. Mở Luồng dữ liệu: đối soát 1.300.086 = 1.299.873 + 139 + 74. Chỉ ra 2.000 offset sau hai replay nhưng HDFS vẫn có 1.000 review duy nhất; giải thích checkpoint. Mở Airflow/Spark master để minh họa thực thi thực tế.
7. Kết thúc bằng liên hệ giữa sản phẩm và Big Data: Spark tổng hợp trên executor; dashboard đọc kết quả phục vụ quan sát. Một biểu đồ đẹp tự nó không chứng minh xử lý phân tán.

Với bộ lọc toàn bộ lịch sử và riêng Shopee, tìm `dang nhap` + 1–2 sao + Ứng dụng cho 4.764 review. Số này giảm khi rút gọn thời gian nên phải đọc phạm vi đang chọn.

## Xuất hình cho báo cáo

Đã tạo trong `outputs/report_charts/`, mỗi hình có PNG 200 dpi và SVG:

- `01_platform_low_rating`: tỷ lệ đánh giá thấp và mẫu số của từng sàn.
- `02_annual_trend_and_volume`: xu hướng tỷ lệ cùng lượng review theo năm; năm 2013/2022 chỉ có một phần năm.
- `03_aspect_platform_heatmap`: tỷ lệ đề cập khía cạnh trên cùng mẫu số sao thấp của mỗi sàn.

`chart_manifest.json` lưu generation và checksum nguồn. Tạo lại từ bảng tổng hợp Spark:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-visuals.txt
.\.venv\Scripts\python.exe scripts/render_report_charts.py
```

Các biểu đồ tĩnh này mặc định dùng toàn bộ sàn/giai đoạn và giữ ứng viên spam. Biểu đồ SVG tải trực tiếp trong dashboard dùng bộ lọc hiện tại. Ảnh chụp sản phẩm đã kiểm tra nằm ở `outputs/screenshots/`.

## Nguồn dữ liệu của sản phẩm

`jobs/prepare_dashboard.py` đọc Parquet processed trên HDFS bằng Spark. Job tạo cube theo sàn/tháng/sao/cờ spam và 7 khía cạnh (1.910 dòng trong snapshot hiện tại), đồng thời xuất toàn bộ review dưới dạng JSON gzip phân tán trên HDFS. Chỉ collect bảng tổng hợp có giới hạn 10.000 dòng; không collect 1,3 triệu văn bản về driver.

`scripts/build_dashboard_index.py` đọc luồng JSON gzip từ HDFS để tạo SQLite FTS5 tại máy. Đây là **chỉ mục phục vụ tra cứu**, có thể tái tạo; nguồn phân tích chính vẫn là Parquet HDFS. Chuẩn hóa không dấu chỉ áp dụng cột tìm kiếm, văn bản hiển thị vẫn giữ nguyên. Dashboard dùng Python/HTML/CSS/JS và chạy offline sau khi chuẩn bị dữ liệu.

Cube, metadata và chỉ mục phải cùng generation và cùng số dòng. Snapshot thiếu/đang thay đổi trả thông báo chưa sẵn sàng; không trộn kết quả giữa hai lần chạy. Docker giữ một bản cache chỉ mục trong volume Linux để tránh chi phí đọc ngẫu nhiên qua ổ Windows; cache tự làm mới khi generation thay đổi. Lần mở đầu sau cập nhật có thể chờ sao chép chỉ mục khoảng 400 MB, những lần tìm tiếp theo dùng cache. Tên và avatar người viết không đưa vào chỉ mục.

Airflow task 07 đã được mở rộng để tạo cube và chỉ mục trước task Kafka. Chạy task 07 qua Airflow CLI được kiểm tra riêng; pipeline 9/9 task trước đó và bước phục vụ mới đều có log.

Task 09 đối soát cả Parquet thật trên HDFS và 1.910 ô cube × 9 số đếm với GROUP BY độc lập trên chỉ mục. Phép audit toàn bộ chỉ mục dùng quét bảng tuần tự để tránh đọc ngẫu nhiên trên Docker bind mount Windows; việc tìm kiếm tương tác vẫn dùng chỉ mục và cache. `outputs/dashboard_validation.json` và `dashboard_acceptance.json` lưu kết quả kiểm chứng.

## Giới hạn ứng dụng

Dữ liệu dừng ở 08/2022, nên sản phẩm hiện minh họa phân tích lịch sử, không đánh giá thị trường hiện tại. Review ứng dụng không đồng nhất với giao dịch đã mua hàng hoặc toàn bộ khách hàng Việt Nam. Một người có thể viết nhiều review.

Sao 1–2 là tín hiệu thay thế cho không hài lòng. Khía cạnh được nhắc chưa xác nhận bị chê, một review có thể vừa khen vừa chê nhiều khía cạnh. Luật hiện phủ khoảng 44,05% review sao thấp; phần còn lại công khai là chưa gán. Silhouette K-Means khoảng 0,019 thể hiện phân tách yếu, không phải accuracy. Cần gán nhãn mẫu độc lập trước khi tuyên bố chất lượng nhận diện hoặc đưa ra khuyến nghị vận hành chắc chắn.

Để dùng với nguồn mới: lưu snapshot raw mới, xác thực schema và phạm vi thời gian, chạy lại pipeline rồi công bố snapshot phục vụ. Việc thu thập dữ liệu mới và tích hợp hệ thống của sàn chưa thuộc bản demo này.
