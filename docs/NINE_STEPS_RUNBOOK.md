# Lab 9 bước cho dữ liệu tin tức (mở rộng, kích hoạt thủ công)

Ngày quyết định: 08/10/2026. Người dùng đã xác nhận bổ sung Kafka và Airflow. Đây là cấu hình mở rộng tách khỏi core trong IMPLEMENTATION_PLAN, không phải thay kế hoạch cũ hoặc thay dashboard production. RFM dành cho giao dịch khách hàng nên **không áp dụng cho bài báo**; bước 6 thay bằng TF-IDF + K-Means phân cụm văn bản thử nghiệm. Sentiment vẫn dùng model đã deploy, không train lại trong DAG.

## Các bước tương ứng

| Bước | Task Airflow | Kết quả |
|---|---|---|
| 1. Chuẩn bị HDFS | prepare | HDFS scope `/news/lab/<run_id>` mới; kiểm tra immutable manifest/hash |
| 2. Upload | upload | Sao raw đã đối chiếu checksum vào HDFS; giữ nguyên raw nguồn |
| 3. Spark đọc HDFS | read | Schema RAW_SCHEMA hiện có, đối soát số observations |
| 4. Làm sạch | clean | Normalize/dedup hiện có; clean/quarantine Parquet riêng |
| 5. Spark SQL | sql | `spark.sql()` cho volume theo ngày VN và trend; đối chiếu với DataFrame core |
| 6. Thay RFM bằng clustering tin | cluster | CountVectorizer + IDF + Normalizer + K-Means; lưu/tải model, assignments, top terms/mẫu |
| 7. Lưu kết quả | save | Enriched/gold HDFS; SQLite phục vụ riêng, model sentiment hiện tại |
| 8. Kafka ingest | kafka | Raw JSON → Kafka → Structured Streaming → HDFS; replay/restart checkpoint, canonical không tăng trùng |
| 9. Airflow DAG | toàn DAG + verify | Chín task nối tuần tự, trạng thái/log có thật; kiểm tra output và nguồn không đổi |

Bước 8 kiểm chứng đường Kafka thay thế bằng **cùng một poll thật**; không cộng đường file và đường Kafka vào production hoặc báo thành nhiều bài mới. Kafka producer có semantics at-least-once; checkpoint giữ offset, canonical dedup giữ khóa bài. Không tuyên bố toàn bộ publishing exactly-once. Topic riêng `vietnews-lab-<run_id>`; single broker, replication=1, mặc định 3 partition. HDFS dùng replication=3 nếu ba DataNode hoạt động.

## Cài/chạy trên Docker Linux hiện có

Yêu cầu: core Compose đã build `bigdata-spark-dev`, network `bigdata_default`, HDFS healthy; đủ RAM cho Kafka (768 MB limit) và Airflow/Spark (3 GB limit). Không tự đổi RAM Docker, power settings hoặc dừng dịch vụ khác. Build image riêng có Airflow 2.10.5 theo constraints Python3.10; không cài Airflow vào Python host. Java17/PySpark3.5.7 tái sử dụng từ base Spark. Không mount Docker socket, không thêm database server: Airflow standalone dùng SQLite/SequentialExecutor **chỉ cho lab**, không phải cấu hình production/HA.

```powershell
cd E:\BigData
docker compose -f compose.nine-steps.yaml config --quiet
docker compose -f compose.nine-steps.yaml build airflow
docker compose -f compose.nine-steps.yaml up -d
docker compose -f compose.nine-steps.yaml ps
docker compose -f compose.nine-steps.yaml exec -T airflow airflow dags list-import-errors
docker compose -f compose.nine-steps.yaml exec -T airflow airflow dags list
```

Docker login/config cần dùng cấu hình thật của máy; lượt triển khai hiện tại dùng thêm `docker --config E:/BigData/runtime/docker-config ...`. Không commit auth config. Image Kafka đã pin digest Linux amd64 thực tế; máy kiến trúc khác cần xác minh digest tương ứng trước.

Hai init service chỉ chỉnh owner cho volume Kafka lab và thư mục/lock `runtime/nine_steps` do lab tạo; không đổi quyền HDFS, raw, model hoặc hệ điều hành. Airflow chạy user50000, không-root. Các CLI lab dùng chung `.lab_writer.lock`; dùng cùng user Airflow để tránh tạo lại lock root-owned.

UI Airflow: http://localhost:8088, chỉ bind localhost. User standalone `admin`; xem mật khẩu được sinh trong volume Airflow bằng lệnh sau (không ghi vào báo cáo hoặc commit):

```powershell
docker compose -f compose.nine-steps.yaml exec -T airflow cat /opt/airflow/standalone_admin_password.txt
```

## Trigger một poll thật

Trong UI chọn DAG `vietnews_nine_steps`, bật DAG rồi Trigger bằng JSON conf. `schedule=None`, `catchup=False`, `max_active_runs=1`, không tự tạo lịch. Có thể chọn lại immutable manifest mới nhất nhưng phải thực sự tồn tại, có raw hash khớp. Dùng coverage explicit ngày VN đã kết thúc; profile coverage bằng runner core trước, không giả complete.

```json
{
  "source_manifest": "/app/evidence/d02/collect_manifest_20261008T084057Z_3ef803fd.json",
  "coverage_manifest": "/app/evidence/dashboard_refresh_20261008T084631Z_r2/coverage.json",
  "as_of": "2026-10-08T08:55:08.792652+00:00"
}
```

Ví dụ dùng poll/as_of của snapshot đã có để đối chiếu, không hứa poll mới nhất. CLI cũng hỗ trợ `airflow dags trigger --conf '<JSON>' vietnews_nine_steps`; PowerShell cần truyền JSON bằng subprocess argument list hoặc quoting phù hợp, không ghép dữ liệu người dùng vào shell command.

Mỗi dag_run tạo run_id `airflow_<sha256(run_id)[:16]>`. Output tại `E:/BigData/runtime/nine_steps/<run_id>`; gồm plan/input hashes, từng stage JSON, Kafka progress, canonical/replay, model clustering HDFS, score manifest và serving SQLite. Raw/model/evidence core mount read-only. Không đổi `NEWS_SERVING_ROOT` của dashboard chính 8502.

Nếu stage thất bại: xem log task và giữ mọi output để audit. Mặc định retries=0 và output hoàn thành từ chối ghi đè; trigger **run mới** sau khi sửa nguyên nhân. Không xóa raw/checkpoint/volume để retry. Kafka restart test trong stage8 tái sử dụng **đúng** topic/output/checkpoint của run đó; run mới phải dùng topic/checkpoint khác.

Normalizer được materialize/cache trên executor trước khi tách clean/quarantine, vẫn dùng nguyên dedup core. Lượt thử đã gặp stage union treo (không còn task active); cache giải quyết ca canonical đã chạy, nhưng chưa xác định được nguyên nhân gốc của scheduler. Đối chiếu model clustering dùng full join theo article_id, không collect corpus lên driver.

## Kiểm tra và shutdown

```powershell
python -m unittest discover -s tests -p test_nine_steps.py -q
docker run --rm --entrypoint python --mount type=bind,source=E:/BigData,target=/app --env NEWS_SPARK_TESTS=1 bigdata-spark-dev -m unittest discover -s tests -p test_nine_steps.py -q
docker compose -f compose.nine-steps.yaml stop
```

Stop giữ volumes; **không** dùng `down -v`, không sửa production checkpoints. Nếu muốn demo SQLite mới, dùng launcher hiện có trên port riêng sau khi tạo bulletin khớp snapshot; không tự publish tập một poll đè dashboard toàn corpus.

K-Means/silhouette chỉ đo phân cụm nội bộ, không đo accuracy sentiment, không chứng minh cùng sự kiện. Cần người review top terms và mẫu trước khi đặt tên chủ đề. Với một poll, không kết luận chất lượng trên toàn corpus. Human gold, coverage đạt 95%, benchmark M8 và crash/broker restart là các việc riêng.

## Căn cứ kỹ thuật

- [Kafka Docker chính thức](https://kafka.apache.org/40/getting-started/docker/).
- [Spark 3.5.7 Kafka integration](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html): connector `spark-sql-kafka-0-10_2.12:3.5.7`; dependency resolve tại build, không mỗi lần DAG khởi động.
- [Airflow 2.10.5 quick start](https://airflow.apache.org/docs/apache-airflow/2.10.5/start.html): constraints, standalone và giới hạn SQLite/SequentialExecutor. Phiên bản pin phục vụ tương thích lab, không khẳng định là bản mới nhất hoặc đủ an toàn để mở Internet.
