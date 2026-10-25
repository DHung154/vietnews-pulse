# Demo đồ án - 3 phút 30 giây

## Chuẩn bị trước buổi báo cáo (không tính vào 3-4 phút)

Mở Docker Desktop, Tailscale nếu nhóm cần đọc chung, rồi chạy từ PowerShell tại E:\BigData:

```powershell
./scripts/start_presentation.ps1
```

Script chỉ start sáu container HDFS/Kafka/Airflow đã có, giữ config VPN/volume; không build hoặc tạo dataset. Nếu Docker chưa sẵn sàng/thiếu container thì báo lỗi. Không dừng process đang dùng8504. Các job scheduler đang bật trước đó giữ nguyên; script không pause/unpause DAG. Chờ services healthy, kiểm tra HDFS hết safe mode, mở http://localhost:8504. Trang mặc định **Báo cáo3–4phút**. Chạy diễn tập một lần trước, đóng terminal diễn tập bằng Enter khi đã xem xong. Không chờ Docker khởi động trong lúc thầy đang tính giờ.

Dashboard chính http://localhost:8502 dùng snapshot đã có. Nếu chưa mở, chạy lệnh `start_dashboard.ps1` trong reports/dashboard_refresh_20261008.md với đúng serving/bulletin. Không đổi pointer chỉ để demo.

## Trình diễn online

Chỉ một thành viên chia sẻ **toàn màn hình trình diễn** (không chỉ tab browser) nếu muốn thầy nhìn thấy terminal riêng. Đóng/ẩn thông báo và cửa sổ cá nhân trước; không chia sẻ mật khẩu/token. Thành viên đó thao tác, người khác không cùng bấm Start.

1. Phóng to browser, bấm **Toàn màn hình** trong demo nếu trình duyệt cho phép. Nếu iframe bị chặn fullscreen, dùng F11 thủ công.
2. Giữ chọn **Mở1terminal thật**, bấm **Bắt đầu phiên demo đúng1lần**. Mở một console mới và worker thật; không phải video log gõ sẵn. Các lệnh Docker chạy ẩn bên dưới, chỉ console này hiện ra. Khi xong terminal giữ lại đến khi nhấn Enter. Có thể Alt+Tab thủ công sang terminal5-10giây rồi trở lại trang.
3. Kịch bản chạy210giây. **Tạm dừng** chỉ dừng lời dẫn/chuyển động, KHÔNG dừng Spark; ←/→ hoặc sáu nút chuyển công đoạn không chạy thêm job. Load/refresh trang không tự khởi chạy lại.
4. Cuối buổi mở dashboard chính để chỉ bộ lọc và một bài gốc. Nếu chỉ chia sẻ cửa sổ browser, dùng terminal nhúng bên phải vì người xem sẽ không thấy cửa sổ console khác.

## Kịch bản lời nói

| Mốc | Hiển thị | Câu nói trọng tâm |
|---|---|---|
|00:00-00:25|Bài toán|“Hệ thống theo dõi xu hướng tin tức tiếng Việt, lưu được nguồn gốc và truy ngược bài báo. Đây là dây chuyền RSS→HDFS→Spark→dashboard.”|
|00:25-00:55|Thu thập/HDFS|“15feed,3nguồn. Raw đã lưu được giữ nguyên trên HDFS. Phiên này đọc lại một poll RSS08/10, không gọi đó là crawl mới.”|
|00:55-01:40|Spark ETL live|“Spark đang chuẩn hóa và khử trùng bằng hàm của hệ thống. Kết quả live là809observations→778bài,31bản trùng trong lần diễn tập. Các số chỉ được hiện khi output phiên mới thành công.”|
|01:40-02:20|SQL/model|“SQL tổng hợp nguồn trên dữ liệu vừa sạch. Model sentiment giữ phiên bản đang có, vẫn thử nghiệm. Clustering là nghiên cứu bổ sung, không khẳng định đó là sự kiện đúng.”|
|02:20-02:55|Kafka/Airflow|“Đây là lệnh đọc topic và run Airflow trực tiếp. Chín task toàn snapshot đã chạy trước, có log ngày16/10; không phải chín task vừa chạy xong trong3phút.”|
|02:55-03:30|Kết quả|“Snapshot đầy đủ13.388bài, demo live một poll riêng. SQLite integrity và unique keys được kiểm tra; cuối cùng xem dashboard và bài gốc. Coverage incomplete và chất lượng sentiment chưa được nghiệm thu.”|

Trang có lời dẫn rút gọn dưới mỗi cảnh. Đồng hồ là **thời lượng kịch bản**, không phải phần trăm xử lý. Sơ đồ/packet là minh họa; thanh SQL chỉ vẽ count từ output live thật. Nếu máy chậm hoặc service lỗi, có trạng thái lỗi/đang chạy, không tự lấp bằng số cũ.

## Hai lớp bằng chứng, không trộn số

- Full snapshot13.388 và chín task: `runtime/nine_steps/full_2a5e58de5cb434258ff9/`, kết quả16/10 đã lưu; source frozen08/10. Không train lại sentiment trong demo.
- Poll raw809: `hdfs:///news/lab/airflow_181ed4cba01a3cab/raw/rss`, thu08/10. Spark live tạo `/news/presentations/talk_<timestamp>_<id>/...` và `runtime/presentation/<id>/`; source checksum trước/sau được kiểm tra. Đây là xử lý lại raw thật, không phải synthetic, không phải full corpus/live RSS network.
- Kafka/Airflow live là **lệnh quan sát**, không trigger toàn DAG. Phần ETL live làm read/normalize/dedup/SQL/write/read trong một Spark session local[2]. Không tuyên bố vừa chạy Kafka ingest hoặc model training nếu chỉ đọc status.
- SQLite live đọc production snapshot chỉ đọc; không publish. 100% prediction không phải accuracy; không đổi coverage incomplete.

## Evidence và chạy lại

Mỗi click tạo session mới; khóa launch chặn lượt đang queued/running. CLI cố định, không nhận lệnh/path từ browser. JSON, log từng lệnh, terminal.log và canonical/etl.json giữ trong runtime/presentation/<session>. Session ID hiển thị cuối trang, timestamps đổi giờ Việt Nam. Timeout ETL100giây; chỉ dừng container thuộc chính session khi hết hạn, không kill HDFS/Kafka/Airflow hoặc xóa output dở. Các probe khác8-15giây; status partial nếu một lệnh lỗi. Do máy/network không đảm bảo SLA, hãy diễn tập trên đúng máy trước báo cáo.

Nếu worker bị hệ điều hành kill, queued/running có thể lưu lại: kiểm tra đúng PID/container sở hữu trước khi xử lý, không xóa pointer/lock để chạy chồng. Terminal kết thúc đã nhấn Enter chỉ đóng cửa sổ, không xóa evidence. Lịch hourly Airflow có thể tiếp tục replay snapshot cố định và tốn dung lượng; chưa có retention tự động, không sửa lịch ngầm trong tính năng này.
