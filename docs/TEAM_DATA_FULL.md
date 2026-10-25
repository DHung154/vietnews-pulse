# Đọc bản đầy đủ13.388bài trên HDFS chung

Bản frozen rss_live_v2 khớp dashboard snapshot08/10/2026, đã đưa lên HDFS14/10. Không phải crawl mới nhất, không gồm154bài legacy rss_live. Sentiment là model prediction thử nghiệm, không human gold. Bản lab778bài vẫn giữ nguyên.

## Gửi Huy

Nhận team_read_hdfs.py mới trong gói này, thay script cũ (bản cũ cố định778bài). Kết nối Tailscale đã được chia sẻ máyHưng100.126.77.31. Docker imagebigdata-spark-dev như lần đọc778bài trước.

Giải nén vào thư mục riêng, mở PowerShell tại thư mục chứa team_read_hdfs.py rồi chạy:

```powershell
$clientScript = (Resolve-Path team_read_hdfs.py).Path
docker run --rm --entrypoint python --add-host datanode:100.126.77.31 --add-host datanode2:100.126.77.31 --add-host datanode3:100.126.77.31 --mount "type=bind,source=$clientScript,target=/app/team_read_hdfs.py,readonly" bigdata-spark-dev /app/team_read_hdfs.py --server 100.126.77.31 --dataset full
```

Kỳ vọng article_count=13388,unique_keys=13388,scope=fullrss_live_v2dashboard snapshot08/10. Chỉ show5dòng mẫu, không phải chỉ có5bài. Mặc định script mới chọnfull; muốnlab thêm --dataset lab.

## Đường dẫn dùng trong code PySpark của nhóm

```python
# Giữ cấu hình hostname mapping như lệnh Docker bên trên.
spark.conf.set('spark.hadoop.dfs.client.use.datanode.hostname', 'true')
base = 'hdfs://100.126.77.31:8020/news/shared/rss_live_v2/snapshot_20261008T090238Z_81c6a670'
clean = spark.read.parquet(base + '/clean')
enriched = spark.read.parquet(base + '/enriched')
```

clean có dữ liệu đã chuẩn hóa/khử trùng; enriched thêm sentiment/probability/model provenance. Không cần gửi13kfile về máy. Tránh ghi output vào hai thư mục chia sẻ này. Thư mục có manifest.json đối chiếu vớiSQLite. HDFS simple auth không enforce read-only mạnh; chỉ nhóm tin cậy được kết nối.

Không restartHDFS bằng compose.yaml đơn lẻ vì máy chủ đang dùng compose.hdfs-team.yaml choVPN. Không sửa firewall/volume/hosts để làm test xanh; máyHưng phải bật Docker/Tailscale và khôngsleep. Chạy trên máy thành viên và gửi stdout/exitcode để có evidence remote; thử từ container trên máyHưng chưa thay được bước đó.
