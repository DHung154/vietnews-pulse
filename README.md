# VietNews Pulse

**Theo dõi xu hướng và phân tích cảm xúc tin tức tiếng Việt sử dụng Apache Spark.**

Hệ thống thu thập RSS, chuẩn hóa và khử trùng bài báo, phân tích biến động qua hai cửa sổ 6 giờ đã kết thúc, dự đoán sentiment và hiển thị bằng Streamlit. Sentiment mô tả **nội dung tiêu đề và mô tả bài báo**, không phải cảm xúc độc giả hoặc thái độ công chúng.

## 1. Chức năng và kiến trúc

```text
RSS → raw JSONL → Spark ETL / streaming observations
    → clean snapshot → inference + analytics Parquet
    → SQLite snapshot + current.json → Streamlit dashboard
```

- Registry theo feed, state độc lập, conditional HTTP, retry hữu hạn và khóa writer.
- Chuẩn hóa URL/text/time, `article_id` ổn định, khử trùng xuyên batch qua snapshot; giữ nội dung mới nhất và provenance.
- Historical và live tách `dataset_id`; historical thiếu ngày đăng giữ null, không lấy ngày import thay thế.
- Trend so hai cửa sổ 6 giờ đã đóng theo giờ Việt Nam; số nền bằng 0 phân biệt với tăng trưởng phần trăm.
- Bộ lọc dataset/ngày/nguồn/chủ đề/sentiment/tìm kiếm; biểu đồ thời gian, sentiment, từ khóa và danh sách bài có phân trang.
- Bản tin biến động có bài báo làm bằng chứng. Gom sự kiện là heuristic cần review; cùng chủ đề chưa chắc cùng sự kiện.
- Coverage theo scheduled slot; nhiều poll cùng slot không bù outage. Thiếu dữ liệu phải hiện `incomplete`.
- Dashboard chỉ đọc snapshot; không train lại model hoặc tải toàn corpus để hiển thị một trang.

HDFS dùng cho lưu trữ/kiểm thử cluster. Spark `local[2]` và standalone cluster là hai chế độ khác nhau. Dự án không dùng MongoDB, Kafka, Airflow, FastAPI hoặc React.

## 2. Phạm vi repo và giới hạn

Repo chứa code, test, cấu hình và fixture nhỏ. **Không chứa dữ liệu crawl, Kaggle/UIT corpus, model đã train, SQLite serving, nhãn, checkpoint, credentials, plan/prompt hoặc báo cáo review nội bộ.** Clone repo không tự có dữ liệu production trên dashboard.

- `data_sample/news_sample.jsonl`: fixture kiểm thử, không phải evidence crawl thật.
- `data_sample/sentiment_train.jsonl`: sentiment synthetic, không phải nhãn người.
- UIT-VSFC là feedback sinh viên ngoài miền tin báo; kết quả trên tập này không chứng minh chất lượng tin tức.
- Nhãn LLM/nhãn yếu phải lưu riêng, không chuyển thành human gold. Giữ tooling kiểm tra provenance/thử nghiệm, không phân phối artifact nhãn AI.
- Chưa có đủ human news gold để kết luận accuracy thật. Tỷ lệ có prediction không phải độ chính xác.
- Ngày 01/10/2026: kiểm tra offline 69 pass, 1 Spark skip; coverage 14 ngày vẫn incomplete. Docker máy phát triển lỗi khởi động nên lượt refresh Spark mới còn bị chặn. Không tuyên bố nghiệm thu toàn bộ pipeline.

## 3. Yêu cầu

- Git, Python **3.10** (`>=3.10,<3.11` theo `pyproject.toml`).
- Spark 3.5.7, NumPy 1.26.4: `requirements.lock.txt`.
- Streamlit 1.39.0: `requirements.dashboard.lock.txt`.
- Java 17 nếu chạy Spark native Linux; Dockerfile đã cài Java trong container.
- Docker Engine/Compose hoạt động để chạy container/HDFS. Windows dùng Linux containers/WSL2.
- Internet để cài dependencies, kéo image và đọc RSS. Dung lượng/RAM phụ thuộc corpus; cluster mẫu chỉ có một worker, không phải cấu hình production/HA.

`docker version` phải trả phần **Server**, không chỉ Client. Không reset Docker hoặc xóa volumes để làm smoke xanh.

## 4. Cài đặt

### Windows — PowerShell

```powershell
git clone https://github.com/DHung154/vietnews-pulse.git
cd vietnews-pulse
py -3.10 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.lock.txt
.\.venv\Scripts\python.exe -m pip install -r requirements.dashboard.lock.txt
.\.venv\Scripts\python.exe -m pip install -e . --no-deps
```

Không cần activate venv hoặc thay execution policy toàn máy. Với các lệnh `python` bên dưới, dùng `.\.venv\Scripts\python.exe` để bảo đảm đúng môi trường.

### Linux

```bash
git clone https://github.com/DHung154/vietnews-pulse.git
cd vietnews-pulse
python3.10 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.lock.txt -r requirements.dashboard.lock.txt
python -m pip install -e . --no-deps
```

Chạy Spark native cần `java -version` và `JAVA_HOME` trỏ Java 17. Trên Windows, Spark có thể đọc được nhưng lỗi ghi Parquet/model vì native Hadoop/winutils; ưu tiên Docker/Linux.

## 5. Test và smoke

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -q
.\.venv\Scripts\python.exe -m compileall -q src scripts tests dashboard
```

Test Spark opt-in có thể skip; đọc lý do. Offline/AppTest không thay bằng chứng Spark/HDFS.

Build image dùng chung cho dev và cluster, tại gốc repo:

```powershell
docker build -t bigdata-spark-dev -f docker/Dockerfile.dev .
docker compose -f compose.dev.yaml build
docker compose -f compose.dev.yaml run --rm spark-dev
```

Smoke đọc fixture, ghi Parquet và đọc lại. Output `runtime/m01/smoke_parquet`, manifest `evidence/m01/container_smoke_manifest.json`. Nếu output đã tồn tại, dùng đường dẫn version mới qua CLI `scripts/smoke.py --help`; không xóa artifacts cũ để test pass.

## 6. Thu thập RSS thật

Xem registry `config/rss_sources.yaml` trước khi gọi mạng. URL/status có thể thay đổi; cấu hình không phải bằng chứng feed đang healthy.

```powershell
python scripts/collect_rss.py --config config/rss_sources.yaml --raw-root runtime/data_collection_v2/raw/rss --state runtime/data_collection_v2/state.json --lock runtime/data_collection_v2/collector.lock --evidence evidence/d02/collect_manifest.json --request-gap 5
```

- Raw lưu theo nguồn/feed/ngày ở `runtime/data_collection_v2/raw/rss`.
- `evidence/d02/collect_manifest.json` là latest; mỗi run còn có manifest immutable riêng.
- Observations lặp không phải bài độc nhất. Canonical count chỉ có sau dedup.
- Không trộn historical raw vào input streaming RSS; không tắt TLS verification.

### Lịch 10 phút

Wrapper chỉ chạy **một lượt**, supervisor deadline 540 giây:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/run_collector.ps1 -PythonExecutable "$PWD\.venv\Scripts\python.exe"
```

Task Scheduler: kiểm tra task cũ trước khi tạo; action gọi wrapper/Python bằng đường dẫn tuyệt đối, Start in là thư mục repo; trigger mỗi 10 phút; không khởi động instance mới khi instance trước còn chạy (`IgnoreNew`). Chọn tài khoản/điều kiện đăng nhập phù hợp, không mặc định SYSTEM. Máy sleep/offline không thu RSS; resume không bù toàn bộ bài đã trôi khỏi feed.

Cron Linux (thay đường dẫn thực):

```cron
*/10 * * * * cd /path/vietnews-pulse && /path/vietnews-pulse/.venv/bin/python scripts/collector_runner.py
```

Log per-run: `runtime/data_collection_v2/logs/runs/runner_<id>.json`, `.stdout.log`, `.stderr.log`. JSON còn `started` cần đối chiếu process/event logs; lock file tồn tại không chứng minh OS lock bị giữ. Không dùng task exit code thay coverage.

### Coverage

```powershell
python scripts/profile_coverage.py --manifests evidence/d02 --config config/rss_sources.yaml --output evidence/d04/coverage_check.json --required-days 14 --min-success-rate 0.95 --end-local YYYY-MM-DD
```

Thay `YYYY-MM-DD` bằng **ngày Việt Nam đã kết thúc**. Đủ 14 ngày lịch không đồng nghĩa đạt 95% slot thành công; lịch sử ngoài window không bù ngày thiếu.

## 7. RSS → snapshot → serving → dashboard

Lệnh refresh dưới đây dùng raw và manifests đã tạo ở bước 6. Output riêng; thay hậu tố `first_run` khi cần version mới. Chế độ **không model** cho sentiment unavailable/null, không tự sinh nhãn:

```powershell
docker compose -f compose.dev.yaml run --rm --entrypoint python spark-dev scripts/refresh.py --raw runtime/data_collection_v2/raw/rss --observations runtime/demo_first_run/observations --checkpoint runtime/demo_first_run/checkpoint --runtime-root runtime/demo_first_run/jobs --serving-root serving/demo_first_run --source-manifest evidence/d02/collect_manifest.json --coverage-manifest evidence/d04/coverage_check.json --max-files-per-trigger 100 --evidence evidence/demo_first_run/refresh.json --master "local[2]"
```

Nếu có model đã lưu, thêm `--model runtime/<duong-dan-model>`. Model version/provenance phải khớp artifact; không thay model production chỉ để demo. Refresh ghi outputs vào host mounts, không mất khi container `--rm`. Đây là local Spark, không mặc nhiên đi qua HDFS.

### Train fixture để kiểm tra save/load

```powershell
docker compose -f compose.dev.yaml run --rm --entrypoint python spark-dev scripts/train_model.py --input data_sample/sentiment_train.jsonl --dataset-id demo_sentiment --model runtime/demo_fixture/model_v1 --model-version demo_fixture_v1 --evidence evidence/demo_fixture/train.json --master "local[2]"
```

TF-IDF + Logistic Regression là baseline machine learning, **không phải deep learning**. Chỉ fit train; validation chọn cấu hình; test đánh giá sau. Dataset thật cần split/group chống leakage trước train. Metric fixture không phải chất lượng tin tức thật.

### Mở Streamlit

```powershell
$env:NEWS_SERVING_ROOT = "$PWD\serving\demo_first_run"
.\.venv\Scripts\python.exe -m streamlit run dashboard/app.py --server.headless true --server.port 8502 --browser.gatherUsageStats false
```

Mở **http://localhost:8502/**. Root mặc định `serving`; app đọc `current.json` và SQLite, ưu tiên dataset `rss_live_v2` nếu có. Chưa tạo snapshot phải báo unavailable, không sinh số liệu minh họa. Model version lấy từ snapshot, không hardcode model đã deploy.

Linux:

```bash
NEWS_SERVING_ROOT="$PWD/serving/demo_first_run" python -m streamlit run dashboard/app.py --server.headless true --server.port 8502
```

`NEWS_EVENT_BULLETIN_PATH` trỏ artifact bulletin được tạo bởi `scripts/prepare_event_bulletin.py`; xem `--help`, dùng đúng snapshot. Coverage thiếu phải giữ cảnh báo; grouping/sentiment thử nghiệm không được trình bày như kết luận đã được con người xác minh.

## 8. HDFS + Spark standalone: ghi, restart và đọc lại

Sau khi build image `bigdata-spark-dev`:

```powershell
docker compose -f compose.yaml config
docker compose -f compose.yaml up -d namenode datanode spark-master spark-worker
docker compose -f compose.yaml ps
docker compose -f compose.yaml run --rm cluster-runner --mode write
docker compose -f compose.yaml restart namenode datanode spark-master spark-worker
docker compose -f compose.yaml run --rm cluster-runner --mode verify
```

Chờ services healthy trước verify. Fixture ghi/đọc tại `hdfs:///tmp/news_pipeline_m1/cluster_smoke/parquet`; manifest `evidence/m01/cluster_smoke_manifest.json`. Nếu write từ chối output cũ, truyền `--storage` scope HDFS mới, dùng cùng scope khi verify. Không gọi smoke fixture này là kiểm chứng hoàn chỉnh RSS → HDFS → Dashboard.

UI: NameNode `http://localhost:9870`, DataNode `http://localhost:9864`, Spark master `http://localhost:8080`, worker `http://localhost:8081`. HDFS replication=1, không HA.

Tắt bằng `docker compose -f compose.yaml stop`. **Không dùng `down -v`, format NameNode hoặc xóa raw/checkpoint để làm test xanh.**

## 9. Dataset và nhãn

- Kaggle: tải theo quyền truy cập/điều khoản; kiểm tra schema/mapping trước `scripts/import_history.py`. Thiếu ngày đăng giữ null.
- UIT-VSFC: `scripts/adapt_uit_vsfc.py`; loader/dependencies/dataset riêng không nằm trong cài đặt core mặc định.
- RSS title/description không mặc nhiên là toàn văn bài báo.
- [Hướng dẫn sentiment](docs/NEWS_SENTIMENT_LABELING_GUIDE.md), [review nhóm sự kiện](docs/event_group_review.md): hai người gán độc lập, xử lý bất đồng, freeze gold và kiểm tra provenance.
- `scripts/evaluate_gold.py` chặn gold chưa chốt, predictions thiếu và article/event group xuyên split. Không giả human agreement bằng nhãn LLM.

Không phân phối lại corpus/toàn văn khi chưa có căn cứ quyền sử dụng. Repo chưa có license riêng; không mặc định được cấp phép MIT.

## 10. Cấu trúc

| Thư mục | Vai trò |
|---|---|
| `src/news_pipeline/` | Identity, normalization, dedup, ML, analytics, coverage, serving/locks |
| `scripts/` | CLI collect/import/snapshot/stream/train/score/export/refresh/validation |
| `dashboard/` | Streamlit và truy vấn snapshot chỉ đọc |
| `config/` | RSS registry, category và historical mapping |
| `docker/`, `compose*.yaml` | Linux runtime và cluster Hadoop/Spark |
| `data_sample/` | Fixture nhỏ |
| `tests/` | Unit/regression, integration opt-in |
| `docs/` | Hướng dẫn nhãn và nhóm sự kiện |

Runtime, evidence, serving, annotations, corpus và hồ sơ plan/review nội bộ bị loại khỏi Git nhưng không xóa trên máy. Các CLI có `--help`; luôn ghi lệnh, exit code, counts và giới hạn khi báo cáo một lần chạy.

## 11. Xử lý lỗi

- **8502 connection refused:** Streamlit chưa chạy/đã thoát; kiểm tra process/port.
- **Dashboard trống:** chưa có `current.json` hoặc sai `NEWS_SERVING_ROOT`; chạy refresh/export trước.
- **Docker chỉ có Client/startup error:** khắc phục Engine trước; test offline không thay evidence cluster.
- **Parquet/model lỗi trên Windows:** chạy Linux container, không sửa global OS config để né lỗi.
- **Output tồn tại:** dùng version mới, giữ dữ liệu cũ.
- **SSL/DNS/feed error:** xem lỗi theo feed và TLS diagnostics; không tắt verification hoặc đổi manifest lỗi thành healthy.
- **Coverage incomplete:** thiếu slot/ngày/feed hoặc HTTP lỗi; không hiển thị mất dữ liệu như số 0 đáng tin cậy.
- **100% có prediction:** là coverage dự đoán, không phải accuracy 100%.
