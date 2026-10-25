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
- Cập nhật 04/10/2026: Docker/HDFS/Spark đã chạy lại. Một lượt RSS thật đi qua Spark standalone → HDFS → inference/analytics → SQLite → dashboard riêng: 797 observations, 764 bài sau khử trùng; replay và đọc lại sau restart giữ nguyên số bài. Đây không phải nghiệm thu toàn corpus. Kiểm tra Windows: 85 pass, 2 Spark skip; hai test Spark chạy riêng trên Linux: 2/2 pass. Coverage 20/09–03/10 vẫn `incomplete`; chưa có đánh giá trên main news gold do người gán.

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

Task Scheduler: kiểm tra task cũ trước khi tạo; Start in là thư mục repo, trigger mỗi 10 phút, `IgnoreNew`. Để không bật console, action gọi `C:\Windows\System32\wscript.exe` với arguments `//B //NoLogo "E:\BigData\scripts\run_collector_hidden.vbs" "C:\path\to\python.exe"` (thay đường dẫn repo/Python thật). Launcher gọi chính wrapper hiện có ở chế độ ẩn và giữ exit code; logs vẫn lưu bình thường. Chọn tài khoản/điều kiện đăng nhập phù hợp, không mặc định SYSTEM. Máy sleep/offline không thu RSS; resume không bù toàn bộ bài đã trôi khỏi feed.

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

### Kiểm chứng RSS thật trên cluster

`scripts/verify_hdfs_pipeline.py` tái sử dụng stream/snapshot/inference/analytics/export hiện có, nhận một immutable collector manifest và coverage manifest. Chạy trong Linux container nối mạng Compose, với output mới dưới `runtime/`; không đổi serving pointer của dashboard chính. Chế độ `--mode verify` dùng lại đúng input HDFS/checkpoint của lượt thành công sau restart.

`scripts/verify_recovery_fixture.py` kiểm tra thêm 10 bài, replay, late event, khóa writer và publish lỗi trong namespace HDFS riêng. **Mọi input của script này là synthetic**, không cộng vào tin thật hoặc gold.

Xem [kịch bản demo và lệnh chạy lại](docs/DEMO_RUNBOOK_20261004.md). Trên máy phát triển hiện có dashboard chính cổng 8502 và bản demo một lượt RSS/HDFS riêng cổng 8503; máy clone mới cần tự tạo input/model/output, không có sẵn các artifact này.

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

### Lab mở rộng Kafka/Airflow (không thay core)

Đã bổ sung code/cấu hình cho bài lab 9 bước theo dữ liệu tin tức: HDFS → Spark ETL/SQL → TF-IDF/K-Means thử nghiệm → lưu kết quả, và Kafka → Structured Streaming do Airflow điều phối thủ công. Xem [runbook 9 bước](docs/NINE_STEPS_RUNBOOK.md) để build/chạy/trigger và kiểm tra evidence. Chỉ xem là integration đã chạy khi có task logs/manifests thật; code hiện diện không phải nghiệm thu. RFM không áp dụng cho tin tức, clustering không thay model sentiment hiện tại.

- **8502 connection refused:** Streamlit chưa chạy/đã thoát; kiểm tra process/port.
- **Dashboard trống:** chưa có `current.json` hoặc sai `NEWS_SERVING_ROOT`; chạy refresh/export trước.
- **Docker chỉ có Client/startup error:** khắc phục Engine trước; test offline không thay evidence cluster.
- **Parquet/model lỗi trên Windows:** chạy Linux container, không sửa global OS config để né lỗi.
- **Output tồn tại:** dùng version mới, giữ dữ liệu cũ.
- **SSL/DNS/feed error:** xem lỗi theo feed và TLS diagnostics; không tắt verification hoặc đổi manifest lỗi thành healthy.
- **Coverage incomplete:** thiếu slot/ngày/feed hoặc HTTP lỗi; không hiển thị mất dữ liệu như số 0 đáng tin cậy.
- **100% có prediction:** là coverage dự đoán, không phải accuracy 100%.

## 12. Hồ sơ kiểm chứng bổ sung ngày 16/10/2026

### Demo báo cáo online 3-4 phút (cập nhật21/10)

Sau khi Docker Engine sẵn sàng: `./scripts/start_presentation.ps1`, chờ các service healthy rồi mở http://localhost:8504. Chế độ mặc định có sáu cảnh, lời dẫn210giây, pause/chuyển cảnh/fullscreen, terminal thật mở một lần khi bấm Start. Spark live xử lý lại một poll rawRSS trên HDFS vào scope mới; Kafka/Airflow chỉ kiểm tra trạng thái. Full snapshot/model dùng evidence đã lưu và ghi rõ ngày, không giả task hoàn tất theo timer. Xem [hướng dẫn trình diễn và lời nói theo mốc](docs/DEMO_3_MINUTES.md). Chuẩn bị services trước buổi nói; thời gian Docker khởi động không nằm trong3-4phút. Không cần thêm framework, package hoặc backend mới.

Hướng dẫn vận hành được công khai trong [runbook toàn snapshot](docs/FULL_SNAPSHOT_RUNBOOK.md). Nhật ký, bảng đối chiếu evidence và kế hoạch nội bộ giữ trên máy, không kèm repo.

- `scripts/benchmark_snapshot.py`: nhận `--master`, `--input` HDFS, `--output` thư mục mới dưới `/app/runtime`, `--run-id` mới và `--repeats 3`. Chạy trong image Spark/Linux trên cùng Docker network; standalone cần driver hostname truy cập được từ worker. Đo **đọc/tổng hợp/ghi-đọc Parquet canonical**, không phải benchmark toàn pipeline ETL/ML. Một máy không chứng minh scale-out nhiều máy.
- `python -m scripts.analyze_completion_snapshot --database <snapshot.sqlite> --output evidence/<version_moi>/analysis --end-local YYYY-MM-DD`: SQLite chỉ đọc, ngày cuối phải là ngày Việt Nam đã kết thúc. Xuất coverage, CSV slot thiếu/lỗi, JSON phân bố và SVG; không publish dashboard.
- `python -m scripts.audit_repo_secrets --output evidence/<version_moi>/security.json`: quét nguồn Git/history và root `.env*`, chỉ lưu fingerprint/vị trí ứng viên, không in secret. Không có pattern match không chứng minh tuyệt đối an toàn.
- `python scripts/check_completion_step1.py --output evidence/<version_moi>/checks`: unit tests, compile và Docker read; trả khác 0 nếu có check lỗi. Trên Windows, Script Host cần quyền thực thi hợp lệ; không sửa registry/quyền toàn cục để né lỗi sandbox.

Corpus, annotations, model và evidence không có sẵn sau clone. Script benchmark mặc định kiểm tra snapshot 13.388 bài; chỉ chạy khi đã chuẩn bị đúng HDFS input, không dùng fixture để giả count đó. Các wrapper `run_completion_benchmark.py`, `cluster_snapshot_sweep.py`, `capture_completion_services.py`, `package_completion_results.py` đóng gói **lượt đo cố định 16/10**, từ chối output đã có, không phải scheduler production. Khi tái hiện dùng CLI tham số/output version mới; không xóa output cũ.

### Kết quả tiếp nối 16/10: DAG toàn snapshot

Đã chạy DAG mới `vietnews_full_snapshot` ba lượt thành công, mỗi lượt chín task và 13.388 bài; một lượt do scheduler tạo. Lịch `0 * * * *`, timezone Việt Nam, `catchup=False`, `max_active_runs=1`, retry một lần. Đây là **replay snapshot canonical cố định 08/10 qua Kafka → HDFS → Spark → SQLite riêng**, không phải collector lấy tin mới. Không publish vào dashboard production, không train hoặc thay model sentiment.

Xem [runbook toàn snapshot](docs/FULL_SNAPSHOT_RUNBOOK.md) trước khi bật lịch; input HDFS, model và coverage phải được chuẩn bị thật. Mỗi giờ tạo topic/output/checkpoint mới, cần theo dõi dung lượng. DAG cũ `vietnews_nine_steps` giữ manual.

- Import Markdown 100 mẫu pilot giữ nguyên nhãn/text/evidence; ghi tên Võ Đình Hưng theo xác nhận của người dùng, chưa coi pilot là test độc lập hoặc human gold đã freeze. Pool mở rộng 400 mẫu chưa gán.
- Crash batch Parquet trên 1.000 dòng synthetic: kill khi thấy file staging, không có `_SUCCESS`, retry scope mới đọc đủ 1.000 dòng; không suy thành khôi phục streaming/publish production.
- Clone công khai commit `c6ba43eecd90f06fa9f94f9aed4acc1440636b2b`: cài venv/dependencies thành công nhưng hai test phụ thuộc artifact cục bộ bị lỗi. Sau bản vá fixture ở `dashboard/demo.py`, `tests/test_demo_console.py`, `tests/test_demo_runner.py`: 80 test, 79 pass và 1 skip. Bản sửa fixture được đưa vào lần cập nhật code này; chưa chạy lại clone sạch từ commit mới.
- Workspace hiện tại: 114 test, 110 pass, 4 Spark opt-in skip; compile thành công. Chưa công bố accuracy/macro-F1 mới khi chưa có tập test độc lập hợp lệ.

Kiểm tra trước khi xuất bản ngày25/10/2026:120test,116pass/4Sparkopt-in skip, compile thành công. Docker Engine chưa chạy tại thời điểm kiểm tra, vì vậy không coi lần kiểm tra code này là một lần kiểm chứng pipeline live mới. Raw, annotations, model, serving và evidence cần chuẩn bị riêng, không có sẵn sau clone.
