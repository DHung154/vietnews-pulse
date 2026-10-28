# Demo đồ án – 1 phút 45 giây

## Chuẩn bị trước buổi báo cáo

Mở Docker Desktop, Tailscale nếu nhóm cần đọc chung, rồi chạy PowerShell tại E:\BigData:

```powershell
./scripts/start_presentation.ps1
```

Script khởi động sáu container HDFS/Kafka/Airflow đã có, giữ config VPN và volume. Chờ services healthy và HDFS hết safe mode trước khi báo cáo. Trang http://localhost:8504 mặc định là **Demo nhanh · 1 phút 45 giây**. Dashboard chính ở http://localhost:8502 tiếp tục dùng snapshot hiện có.

## Thao tác trình diễn

1. Một thành viên chia sẻ toàn màn hình nếu cần cho người xem thấy terminal riêng. Phóng to trình duyệt hoặc dùng F11.
2. Giữ chọn **Mở 1 terminal thật**, bấm nút bắt đầu kiểm tra một lần. Worker chạy nền; cửa sổ terminal riêng đọc log và lệnh thực tế. Đóng cửa sổ này không dừng Spark.
3. Sáu cảnh tự chuyển trong **105 giây**. Tạm dừng/chuyển cảnh chỉ điều khiển kịch bản, không điều khiển tiến độ job. Chuyển tab tạm ngừng đồng hồ để không bỏ qua phần đang thuyết trình.
4. Đóng terminal rồi muốn xem tiếp: bấm **Mở terminal** ở cạnh **Copy log**. **Phát kịch bản** cũng mở lại terminal khi ô lựa chọn đang bật. Cả hai dùng cùng phiên, không chạy lại Spark. Nếu terminal cũ vẫn mở, hệ thống giữ cửa sổ đó.
5. Sau khi phiên xử lý kết thúc, nút bắt đầu kiểm tra tạo một phiên mới có output riêng. Refresh trang không tự chạy job. Khi xong, terminal giữ lại cho đến khi nhấn Enter.

## Nội dung theo sáu cảnh

| Mốc | Cảnh | Nội dung trình bày |
|---|---|---|
|00:00–00:12|Bài toán|Thu thập và phân tích tin tức tiếng Việt; truy ngược kết quả về bài gốc.|
|00:12–00:27|Thu thập|15 feed từ ba nguồn. Kiểm tra HDFS trực tiếp, xử lý lại raw RSS đã lưu để tránh chờ mạng báo.|
|00:27–00:57|Làm sạch|Spark đọc HDFS, chuẩn hóa, khử trùng, ghi và đọc lại Parquet. Chỉ hiện số đếm sau khi job xuất kết quả thành công.|
|00:57–01:17|Phân tích|Spark SQL tổng hợp theo nguồn trên dữ liệu vừa sạch. Sentiment và phân cụm có kết quả đã lưu riêng.|
|01:17–01:32|Điều phối|Kiểm tra topic Kafka và run Airflow. Chín task toàn snapshot là kết quả lịch sử có ngày chạy.|
|01:32–01:45|Kết quả|Đối soát SQLite rồi mở dashboard, thử bộ lọc và một bài gốc. Phân biệt toàn snapshot với poll dùng trình diễn.|

Đồng hồ đo thời lượng trình bày. Sơ đồ chuyển động minh họa kiến trúc; trạng thái, log và số đếm lấy từ output thật. Không đánh dấu job thành công theo đồng hồ. Nếu máy chậm hoặc dịch vụ lỗi, kết quả thực tế có thể chưa hoàn tất khi kịch bản kết thúc. Diễn tập trên đúng máy trước buổi báo cáo.

## Phạm vi và minh chứng

- Toàn snapshot **13.388 bài**, chín task: `runtime/nine_steps/full_2a5e58de5cb434258ff9/`. Kết quả ngày 16/10, nguồn đóng băng 08/10. Không train lại trong demo.
- Raw poll trên `hdfs:///news/lab/airflow_181ed4cba01a3cab/raw/rss`: kết quả trước đây **809 observations → 778 bài, 31 bản trùng**. Đây là RSS thật đã lưu, không phải crawl mới hoặc toàn corpus.
- Mỗi phiên Spark live tạo `/news/presentations/talk_<timestamp>_<id>/...` và `runtime/presentation/<session_id>/`. Kiểm tra checksum nguồn trước/sau, output Parquet, số bài và unique keys.
- Kafka/Airflow live là lệnh quan sát; không trigger toàn DAG trong lượt demo ngắn. Spark ETL live dùng `local[2]` trên một máy.
- SQLite production chỉ được đọc để kiểm tra integrity/count/model, không publish. Tỷ lệ có dự đoán không phải độ chính xác. Coverage và trạng thái thử nghiệm giữ theo dữ liệu thực tế.

## Chạy lại và kiểm tra

```powershell
python -m unittest tests.test_presentation tests.test_demo_console tests.test_demo_runner -q
node tests/test_presentation_client.cjs
```

Mỗi phiên lưu `state.json`, `terminal.log`, log từng lệnh và `etl.json`; `terminal.json` lưu PID cửa sổ đọc log. Khóa publish/worker chặn chạy chồng. Backend chỉ nhận action cố định và session ID hợp lệ, không nhận lệnh shell từ trình duyệt.

Ngân sách kiểm tra là 105 giây; Spark ETL tối đa 80 giây và mỗi lệnh quan sát tối đa 8 giây, giới hạn tiếp theo theo thời gian còn lại. Dọn container khi timeout có thể tốn thêm vài giây. Chỉ dừng container duy nhất thuộc phiên đó, không dừng HDFS/Kafka/Airflow hoặc xóa output dở. Lỗi trả về trạng thái `partial`, không thay bằng kết quả cũ.

Nếu worker đã mất, backend nhận diện phiên bị ngắt để cho phép bắt đầu lượt mới. Chỉ đóng terminal không gây tình trạng này vì worker chạy độc lập. Lịch Airflow đang có giữ nguyên; không đổi lịch hoặc retention trong chức năng trình diễn.
