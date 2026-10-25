# Đồng đội đọc HDFS chung qua Tailscale

**Bổ sung bản đầy đủ14/10:** đã có13.388bài của snapshotdashboard08/10 tại `/news/shared/rss_live_v2/snapshot_20261008T090238Z_81c6a670/{clean,enriched}`. Scriptteam_read_hdfs.py mới mặc địnhfull; `--dataset lab` mới đọc778. Lệnh gửi đồng đội gọn trong TEAM_DATA_FULL.md. Những mô tả778bài phía dưới là gói lab cũ.

## Cập nhật 14/10/2026 — đã bật trên máy Hưng

IP VPN thật: **100.126.77.31**. Người dùng xác nhận tailnet chỉ nhóm tin cậy. Docker/HDFS đã khởi động,3DataNode healthy, các cổng8020/9866/9868/9869 chỉ bind IP này (UI vẫn localhost). Client Docker riêng trên máy Hưng đã đọc qua IP VPN: **778 bài/778 khóa**, exit0; chưa thay thế bằng chứng từ máy đồng đội.

Thành viên thay `$serverVpn` trong ví dụ bằng `100.126.77.31`. Snapshot778bài là poll lab, không toàn corpus dashboard. Phần mô tả08/10 bên dưới là trạng thái trước khi bật.

Gói gửi nhóm `gui_dong_doi.zip` chứa script và hướng dẫn, không chứa dataset/secret. Giải nén rồi chép team_read_hdfs.py vào scripts/ của repo trên máy thành viên trước khi chạy ví dụ. Nếu repo chưa có image: `docker build -t bigdata-spark-dev -f docker/Dockerfile.dev .` (cần clone code và dependencies theo README).

Chỉ dùng VPN riêng nhóm. Máy Hưng chưa có Tailscale/IP VPN ở lượt kiểm tra08/10/2026: cấu hình **chưa kích hoạt**. HDFS chưa có Kerberos: script không ghi dữ liệu nhưng không enforce read-only; người có kết nối có thể giả username. Tailnet ACL cần giới hạn các thành viên tin cậy tới máy Hưng TCP8020,9866,9868,9869. Không mở Internet/LAN trường, không port-forward router, không tự sửa firewall/hosts/power settings.

## Máy Hưng

1. Cài/đăng nhập Tailscale, mời nhóm hoặc chia sẻ máy trong tailnet; không gửi auth key vào repo. Lấy IP thật bằng `tailscale ip -4`; xác nhận IP có trên adapter máy.
2. Xác nhận ACL chỉ cho nhóm. Không có job HDFS đang ghi. Lệnh dưới recreate bốn container HDFS và tạm gián đoạn job; giữ nguyên volumes/project, không format/down -v. Trước khi chạy cần đối soát đường dẫn snapshot và count.

```powershell
cd E:\BigData
$env:HDFS_TEAM_BIND_IP = '<IP Tailscale thật của máy Hưng>'
docker compose -f compose.yaml -f compose.hdfs-team.yaml config --quiet
docker compose -f compose.yaml -f compose.hdfs-team.yaml up -d namenode datanode datanode2 datanode3
docker compose exec -T namenode hdfs dfsadmin -report
```

Sau khi đã bật chia sẻ, luôn dùng cả hai file Compose khi cập nhật bốn service HDFS. Dùng chỉ compose.yaml sẽ rollback về localhost và cổng cũ. Không cần bật Spark worker để client local[2] đọc HDFS.

Nếu máy này cần Docker config riêng, thêm `--config E:/BigData/runtime/docker-config` sau `docker`.

NameNode8020; ba DataNode có cổng nội bộ=ngoài lần lượt9866/9868/9869. Chỉ đổi host port sẽ sai vì NameNode quảng bá cổng nội bộ. Hostname datanode/datanode2/datanode3 giữ mạng Docker core; client ánh xạ ba tên về cùng IP VPN trong container riêng. Cổng UI vẫn localhost.

## Máy thành viên

Tailscale kết nối được máy Hưng; Docker Linux và image `bigdata-spark-dev` build theo README (Python3.10,Java17,PySpark3.5.7). Nhận scripts/team_read_hdfs.py; không nhận state/checkpoint/password. Chạy từ repo:

```powershell
$serverVpn = '<IP Tailscale thật của máy Hưng>'
$clientScript = (Resolve-Path scripts/team_read_hdfs.py).Path
docker run --rm --entrypoint python --add-host "datanode:$serverVpn" --add-host "datanode2:$serverVpn" --add-host "datanode3:$serverVpn" --mount "type=bind,source=$clientScript,target=/app/team_read_hdfs.py,readonly" bigdata-spark-dev /app/team_read_hdfs.py --server $serverVpn
```

Không sửa hosts của Windows. Spark local[2] xử lý trên máy thành viên, đọc metadata NameNode và block trực tiếp từ DataNode. Một số Docker Desktop không route được VPN host: lưu lỗi TCP, xử lý mạng riêng trước; không tự đổi firewall/route. Trong VS Code/PyCharm có thể chỉnh script rồi chạy bằng lệnh Docker này.

Scope frozen778 bài `/news/lab/airflow_181ed4cba01a3cab/clean`, không phải13.388 bài dashboard. Script kiểm tra4endpoint, unique keys/count, chỉ show5 bài. Đây là dữ liệu thật, không human gold. Lưu stdout/exit code từ máy thành viên mới có evidence remote. Count kỳ vọng cố định chỉ áp dụng snapshot này.

## Rollback

```powershell
docker compose -f compose.yaml up -d namenode datanode datanode2 datanode3
```

Trả localhost/cổng9866 nội bộ, giữ volumes. Kiểm tra3DataNode live và đọc/count snapshot trước khi kết luận phục hồi. Không dùng down -v. Chưa tuyên bố remote read thành công trước khi chạy máy khác.

Căn cứ hostname routing: [Hadoop3.3.6 multihoming](https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-hdfs/HdfsMultihoming.html).
