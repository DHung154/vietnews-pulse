# Replay toàn snapshot theo lịch Airflow

## Phạm vi

DAG `vietnews_full_snapshot` dùng snapshot canonical đóng băng `snapshot_20261008T090238Z_81c6a670`, 13.388 bài, không phải lấy RSS mới hoặc chạy ETL từ toàn raw. `as_of` cố định theo snapshot; không diễn giải biểu đồ là hôm nay. Model `tfidf_lr_weak_v3_20260917` được giữ nguyên. Không cập nhật serving pointer dashboard chính.

Code: `airflow/dags/vietnews_full_snapshot.py`, `scripts/full_snapshot_pipeline.py`; cấu hình: `config/full_snapshot.json`.

## Chuẩn bị

1. Làm theo README để build image Spark/Airflow và bật HDFS/Spark/Kafka. Không `down -v` hoặc format NameNode.
2. Chuẩn bị input HDFS trong config, kiểm tra bằng `hdfs dfs -ls`, `-count`, `fsck`. Count bài do Spark đọc xác minh; số file HDFS không phải số bài.
3. Chuẩn bị model thật dưới `runtime/m04/models/<version>` và coverage JSON có `window_end_basis=explicit_end_local` dưới `evidence/`. Các artifact này không kèm repo. Cấu hình hiện tại dùng coverage kết thúc 15/10/2026 và trạng thái incomplete.
4. Không sửa config/coverage trong một lượt đang chạy: pipeline khóa fingerprint và từ chối thay đổi.

```powershell
docker --config runtime/docker-config compose -f compose.nine-steps.yaml config --quiet
docker --config runtime/docker-config compose -f compose.nine-steps.yaml up -d --no-deps --no-build airflow
docker --config runtime/docker-config exec vietnews-lab-airflow-1 airflow dags list-import-errors --output json
```

Lệnh `--no-build` chỉ phù hợp máy đã build image. Clone mới xem build trong runbook 9 bước. Wrapper khởi động kiểm tra thư mục bind mount, giữ PID file stale bằng tên version trước khi chạy `airflow standalone`; từ chối đụng PID đang là webserver/gunicorn. Không sửa database, volume hoặc mật khẩu.

## Chạy và kiểm tra

Lịch cron `0 * * * *`, timezone `Asia/Ho_Chi_Minh`, `catchup=False`, `max_active_runs=1`, chín task nối tiếp, retry1 sau một phút. Mỗi task timeout30 phút. DAG dùng local[2] trong Airflow; standalone benchmark là phép thử riêng trên cùng máy.

```powershell
docker --config runtime/docker-config exec vietnews-lab-airflow-1 airflow dags unpause vietnews_full_snapshot
docker --config runtime/docker-config exec vietnews-lab-airflow-1 airflow dags trigger -r demo_FULL_ID_MOI vietnews_full_snapshot
docker --config runtime/docker-config exec vietnews-lab-airflow-1 airflow dags list-runs -d vietnews_full_snapshot --output json
docker --config runtime/docker-config exec vietnews-lab-airflow-1 airflow tasks states-for-dag-run vietnews_full_snapshot demo_FULL_ID_MOI --output json
```

Thay run ID mỗi lượt; không clear task đã ghi output để ghi đè. `full_<sha256(run_id)[:20]>` là namespace của lượt. Nếu muốn dừng các lượt tương lai, người vận hành dùng `airflow dags pause vietnews_full_snapshot`; không xóa output. Lịch đã được bật trong lượt kiểm chứng 16/10, không phải automation desktop.

## Chín task và bằng chứng

| Task | Kiểm tra |
|---|---|
| prepare | Đọc HDFS đủ article_id riêng; lưu schema, HDFS checksums và model hashes |
| kafka | Producer canonical JSON → topic riêng → Structured Streaming availableNow → HDFS observations/checkpoint |
| read | Đọc trực tiếp observations HDFS; parse JSON FAILFAST |
| dedup | Khử trùng theo core; full join unique keys đối chiếu mọi cột trừ snapshot_id |
| sql | Spark SQL aggregation; đối chiếu category trend với core |
| cluster | TF-IDF512/PCA16/K-Means8; model riêng, không phải sentiment training |
| score | Dùng model sentiment đang có, không đổi training/model production |
| serve | SQLite riêng: integrity, counts, model và coverage thật |
| verify | 13.388 unique keys, input/model không đổi, production_publish_performed=false |

HDFS: `/news/full_runs/<full_id>/<task>/attempt_<n>`; host: `runtime/nine_steps/<full_id>/`. Mỗi retry ghi scope attempt mới; stage JSON chỉ ghi khi thành công. Không tái dùng checkpoint từ đường dẫn khác.

Lượt kiểm chứng lưu tại `evidence/completion_full_20261016_v1/`: `summary.json`, `tasks_*.log`, `task_logs_*/task_id=*/attempt=*.log`, `full_*/{plan,prepare,kafka,read,dedup,sql,cluster,score,serve,verify}.json`, `commands.json`. Ba lượt đều 9/9 success, gồm scheduled + hai manual. Attempt dedup đầu của scheduled bị stall; có observation/owned-process intervention trong `dedup_stall.json`; retry attempt2 thành công. Không bỏ log lỗi.

## Giới hạn vận hành

Mỗi giờ replay cùng snapshot và tăng topic/output/checkpoint; chưa có retention/cleanup tự động. Theo dõi đĩa trước khi để chạy dài. Một broker, một NameNode, một máy vật lý, simple auth: không HA hoặc benchmark đa máy. Không mở cổng ra Internet. Coverage thiếu vẫn incomplete; 100% prediction không phải accuracy. Nhóm sự kiện và sentiment vẫn thử nghiệm.
