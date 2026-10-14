# Kịch bản demo trước giảng viên (8–10 phút)

## Demo đồ án — một nút chạy thật (cập nhật14/10)

### Xem từng công đoạn bằng animation2D

- **Theo lượt chạy thật**: tự chọn task đang running trong Airflow, đổi cảnh khi task thay đổi (poll8giây). Lỗi thì dừng ở bước lỗi; tất cả success thì hiện cảnh đối soát hoàn tất, không tiếp tục chuyển động giả.
- **Xem minh họa từng công đoạn**: chọn1trong9cảnh để giải thích HDFS, fileRSS, schemaJSON, bộ lọc dedup, SQL, TF-IDF/KMeans, lưu/inference, Kafka partition và đối soát. Đây là phát animation minh họa, không chạy lại job. Nhãn này luôn hiện ngay trên cảnh.
- Checkbox **Bật chuyển động minh họa** để tắt/bật; tôn trọng prefers-reduced-motion của trình duyệt. Hình/gói tin/cột/điểm là sơ đồ chức năng, không phải telemetry từng dòng, tốc độ, vector model thực hoặc % trong task.
- Số liệu bên dưới cảnh chỉ lấy từ output task đã success của run đang theo dõi. Chưa có output thì ghi chưa có, không lấy số của run cũ. Mở expander để xem sơ đồ tổng thể9trạm và bảng thời gianVN.

1. Mở http://localhost:8504, giữ chế độ **Chạy toàn pipeline**.
2. Bấm **Chạy demo đồ án** một lần. Airflow nhận run_id mới; trang tự cập nhật8giây/lần, hoặc bấm Cập nhật tiến trình. Không cần nhập lệnh terminal.
3. Chỉ vào bảng dây chuyền: lấy file RSS đã thu→HDFS→Spark→làm sạch→SQL→K-Means→sentiment/SQLite→Kafka→đối soát. Khi clean hoàn thành sẽ có count/trùng; khi save hoàn thành sẽ có10bài mẫu và sentiment của chính run này; khi9task thành công mới hiện hoàn tất.
4. Nếu cần giải thích sâu, chuyển **Xem chi tiết9bước**. Phần đó cố định evidence08/10, không phải kết quả run vừa chạy.

Đầu vào nút hiện là poll RSS thật ngày08/10 có manifest809observations; đây là xử lý lại dữ liệu thật, **không crawl mới**. Coverage/as_of giữ đúng bản input; output/checkpoint/topic/modelKMeans/SQLite mới riêng. Không train/thay model sentiment, không thay pointer/dashboard8502. Run vẫn tiếp tục trong Airflow nếu đóng trình duyệt. Current run lưu runtime/demo_control/current.json nên mở lại trang vẫn theo dõi được.

Chống trigger chồng bằng lock và kiểm tra Airflow queued/running; max_active_runs=1. Nếu trigger timeout, giữ run_id và đọc lại trạng thái trước thử tiếp. Nếu task lỗi, không thay bằng output cũ. Muốn khởi động service theo phần bên dưới/runbook, trang không tự bật Docker hoặc đổi HDFS VPN.

Mở http://localhost:8504 trên máy trình diễn. Chọn từng bước ở sidebar. Phân biệt JSON evidence ngày08/10 với nút kiểm tra live có timestamp/exitcode. Không nói evidence cũ là kết quả vừa tính lại. Trang không thực thi lệnh tùy ý, không train, không upload hoặc publish.

| Thời gian | Chọn bước và thao tác | Giải thích cho thầy |
|---|---|---|
| 1phút | Chuẩn bị HDFS → kiểm tra chỉ đọc; mở NameNode | Một NameNode,3DataNode logic trên một máy; không phải3máy vật lý |
| 1phút | Đưa RSS vào HDFS / Spark đọc HDFS | Manifest15file thật809observations; nút live liệt kê file HDFS, không phải chạy lại Spark |
| 1phút | Làm sạch/khử trùng | 809→778canonical+31trùng+0invalid; giữ latest content/category riêng |
| 1phút | Spark SQL | Xem sql.json:77dailyrows,15trendrows,0mismatch với DataFrame; cửa sổ6giờ đã kết thúc |
| 1phút | TF-IDF/K-Means | 5cụm, lưu/tải model mismatch0; silhouette thấp0.0117, chưa khẳng định cụm tốt/cùng sự kiện; RFM không áp dụng tin tức |
| 1phút | Lưu/sentiment → mở dashboard8502 | Xem5bài từ SQLite lab chỉ đọc; prediction không phải human gold; dashboard toàn corpus là scope khác |
| 1phút | Kafka | Bảng lần đầu809→replay1618 nhưng canonical778; restart cùng checkpoint không tăng; nút live describe topic3partition |
| 1–2phút | Airflow → kiểm tra live, mở8088 | Xem9task của run manual_review_20261008T100700Z, log/duration từng task; không Clear task cũ |

## Muốn chạy toàn bộ thật ngay buổi báo cáo

Chuẩn bị trước theo docs/NINE_STEPS_RUNBOOK.md. Trigger DAG vietnews_nine_steps bằng conf có source_manifest, coverage_manifest,as_of thật, run_id mới. Mở Grid/Graph/Logs trong Airflow chờ từng task. Không vừa trigger rồi nhận JSON trên trang demo là output run mới: trang hiện cố định run đã kiểm chứng. Có thể mở folder scope mới để đối chiếu sau khi hoàn thành.

## Khởi động

- HDFS đang chia sẻ VPN: phải dùng `docker compose -f compose.yaml -f compose.hdfs-team.yaml up -d namenode datanode datanode2 datanode3` với HDFS_TEAM_BIND_IP đúng adapter thật. Không chạy compose.yaml đơn lẻ vì sẽ rollback chia sẻ.
- Kafka/Airflow: `docker compose -f compose.nine-steps.yaml up -d --no-build airflow`. Chỉlocalhost8088; mật khẩu theo runbook, không đưa lên slide.
- Demo: `python -m streamlit run dashboard/demo.py --server.address 127.0.0.1 --server.port 8504 --server.headless true` (dùng terminal ẩn khi launcher).
- Dashboard8502 dùng scripts/start_dashboard.ps1 với serving/bulletin khớp snapshot; không đổi pointer để tiện demo.

Nếu nút live báo lỗi/timeout, nói rõ service chưa sẵn sàng; không che lỗi bằng evidence cũ. Chưa có benchmark full-corpus/distributed, chưa chứng minh sentiment chính xác, coverage vẫn incomplete. Không dùng mô phỏng transaction/Kafka animation làm evidence của dữ liệu tin thật.
