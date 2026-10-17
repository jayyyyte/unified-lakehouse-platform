# MOSAIC — Build Log (Nhật ký xây dựng)

**Ghi chép theo ngày và theo milestone: đã xây gì, kiểm chứng thế nào, và mỗi lần chạy thực sự đo
được gì.**

Last updated: 2026-10-16 · Covers M0–M4 (M4 complete)

> Thuật ngữ khó tra ở
> [PROBLEM.md, Phụ lục C](PROBLEM.md#phụ-lục-c--bảng-thuật-ngữ-glossary).*
>
> *Bố cục đã được sắp lại: mỗi milestone có cùng khung **Mục tiêu → Đầu việc (cách làm, output, kết
> quả) → Số đo → Phát hiện → Lỗi tự bắt được**. Nội dung và số liệu giữ nguyên bản gốc (commit
> `bf167cc`); phần M0/M1 được bổ sung chi tiết từ CHANGES-FROM-GR1, ADR và git history.*

---

## Mục lục

0. [Cách đọc tài liệu này](#0-cách-đọc-tài-liệu-này)
1. [Dòng thời gian tổng quan](#1-dòng-thời-gian-tổng-quan)
2. [M0 — Problem spec, skeleton, stack](#2-m0--problem-spec-skeleton-stack)
3. [M1 — Sources, seed corpus, load generator, CDC live](#3-m1--sources-seed-corpus-load-generator-cdc-live)
4. [M2 — CDC restart safety và WAL guard](#4-m2--cdc-restart-safety-và-wal-guard)
5. [M3 — Medallion on Iceberg](#5-m3--medallion-on-iceberg)
6. [Sau M3 — giới hạn bộ nhớ Trino](#6-sau-m3--giới-hạn-bộ-nhớ-trino-2026-10-1316)
7. [M4 — Embedding pipeline (kênh chậm) và đăng ký dataset](#7-m4--embedding-pipeline-kênh-chậm-và-đăng-ký-dataset)
8. [Deliverables: bản đồ file theo milestone](#8-deliverables-bản-đồ-file-theo-milestone)
9. [Environment facts đã xác lập](#9-environment-facts-đã-xác-lập)
10. [Command reference](#10-command-reference)
11. [Độ phủ KPI của GR1 Table 4.4](#11-độ-phủ-kpi-của-gr1-table-44)
12. [Những gì CHƯA được kiểm chứng](#12-những-gì-chưa-được-kiểm-chứng)
13. [Run ledgers](#13-run-ledgers)

---

## 0. Cách đọc tài liệu này

Ba tài liệu mô tả trạng thái dự án và **không chồng lấn** nhau:

| Tài liệu | Trả lời câu hỏi | Thay đổi khi nào |
|---|---|---|
| [STATUS.md](STATUS.md) | *Bây giờ ta đang ở đâu?* | mỗi phiên làm việc (bị ghi đè) |
| [CHANGES-FROM-GR1.md](CHANGES-FROM-GR1.md) | *Bản build khác thiết kế GR1 thế nào, vì sao?* | khi phát hiện một điểm lệch |
| **file này** | *Đã xây gì, theo thứ tự nào, mỗi lần chạy đo được gì?* | **chỉ thêm vào** (append-only) |

STATUS.md là **ảnh chụp** và bị ghi đè. File này là **sổ cái** (*ledger*): mục mới được thêm vào,
mục cũ không viết lại, nên quỹ đạo phát triển vẫn còn nguyên khi ảnh chụp đã trôi đi. Khi bảo vệ
GR2, đây là nơi lấy tư liệu cho luận điểm *"chúng tôi đã làm, và đây là bằng chứng"*.

**Nguồn gốc các mục:**
- **M0, M1** được **dựng lại** từ STATUS.md, CHANGES-FROM-GR1.md và các ADR mà các phiên trước viết
  ngay lúc làm.
- **M2, M3** là **ghi chép trực tiếp**: mọi con số đều do một lệnh chạy thật sinh ra (M2:
  2026-09-07/08; M3: tới 2026-10-05), và tái hiện được từ các ledger máy đọc được trong
  `experiments/results/`.

**Quy tắc của file:** một dòng chỉ được ghi *verified* nếu lệnh **đã thật sự chạy** và output **đã
được đọc**. Thứ gì đã viết mà chưa chạy thì ghi **unexecuted**, đúng chữ STATUS.md dùng. Số liệu
trích từ output thật, không bao giờ trích từ kỳ vọng.

---

## 1. Dòng thời gian tổng quan

| Ngày | Sự kiện | Milestone |
|---|---|---|
| 2026-08-24 | First commit: stack, PROBLEM.md, khung repo; ADR 0001–0004 được viết | M0 **đóng** |
| 2026-08-25 | Lần chạy thật đầu tiên: Connect chết vì Avro converter → chuyển sang JsonConverter (ADR 0005); sửa logic health check (`compose ps` giấu container đã thoát) | M0 → M1 |
| 2026-08-28 | `cdc-verify` pass trên cả hai nguồn; masking được chứng minh trên dữ liệu thật; phát hiện A6, A7 | M1 **đóng** |
| 2026-09-06 | Commit load generator, công cụ verify CDC, `REPLICA IDENTITY FULL` vào repo | (M1, commit) |
| 2026-09-07/08 | Harness restart-safety + WAL guard; WAL guard **FAIL** → tìm ra A9 → sửa → PASS | M2 **đóng 09-08** |
| 2026-09-12 | Phát hiện Kafka retention đã xoá topic `categories` (A10, lần 1) | M3 bắt đầu |
| 2026-09-12..23 | Spark image, Nessie lưu bền, hai smoke test đường ghi, Bronze job (còn thiếu idempotency key) | M3 |
| ~2026-09-25 | Docker Desktop restart → cả stack dừng 10 ngày | — |
| 2026-10-05 | Viết idempotency key; mọi topic đã bị retention xoá (A10, lần 2) → resnapshot; chứng minh exactly-once, đo latency, dbt Silver/Gold, reconcile | M3 **đóng 10-05** |
| 2026-10-13 | Đo được: build incremental 0 dòng vẫn tốn 45–61 s ở `gold.orders`, đẩy heap Trino lên 6+ GiB | sau M3 |
| 2026-10-16 | Giới hạn bộ nhớ Trino (`-Xmx3G`, `mem_limit 6g`, `MALLOC_ARENA_MAX=2`), đo lại | sau M3 |
| 2026-10-16 | Chốt 13 quyết định M4 trước khi code (ADR 0006); commit M3 (`c24134a`); khôi phục checkpoint Bronze bị rách (D17); dựng embedding pipeline + registry; phát hiện A13; `embed-verify --with-refresh` 7/7 PASS, **L0 tái hiện end-to-end** | M4 **đóng 10-16** |

| | Milestone | State | Date closed | Evidence |
|---|---|---|---|---|
| **M0** | Problem spec, skeleton, stack definition | **done, verified** | 2026-08-24 | 10/10 services healthy; `doctor` |
| **M1** | Sources, seed corpus, load generator, CDC live | **done, verified** | 2026-08-28 | `cdc-verify` pass cả hai nguồn; số dòng loadgen khớp chính xác |
| **M2** | CDC ingestion + restart-safety demo | **done, verified** | **2026-09-08** | `restart-safety` PASS, `wal-guard` PASS (sau một bản sửa do chính M2 phát hiện ra) |
| **M3** | Medallion on Iceberg (Spark Bronze, dbt-trino Silver/Gold) | **done, verified** | **2026-10-05** | `bronze-verify exactly-once` + `replay` PASS; đo KPI latency; `reconcile` khớp tuyệt đối sau MERGE dưới tải |
| **M4** | Trino federation + embedding pipeline | **done, verified** | **2026-10-16** | `embed-verify --with-refresh` 7/7 PASS: counts, semantics, recall (plan đã kiểm), guard, no drift, **L0 stale hit**, L0 fixed; `register` 14 dataset |
| M5 | N1 Freshness Registry | có schema, chưa có collector | — | — |
| M6 | N2 Retrieval + fusion engine | chưa bắt đầu | — | — |
| M7 | Experiments E1–E4 | chưa bắt đầu | — | — |
| M8 | N3 governance, `jobmarket` domain, console | chưa bắt đầu | — | — |

---

## 2. M0 — Problem spec, skeleton, stack

**Đóng:** 2026-08-24 · **Nguồn:** dựng lại từ CHANGES-FROM-GR1 (C1, C7, D1–D6, A2–A5) và ADR.

### 2.1 Mục tiêu

Có một **bài toán được phát biểu rõ** (GR1 chưa có), một **khung repo** tách lõi chung khỏi domain,
và một **stack Docker chạy được trên một máy**, khoẻ mạnh thật chứ không phải chỉ "đã khởi động".

### 2.2 Đầu việc

| # | Đầu việc | Cách làm | Output | Kết quả |
|---|---|---|---|---|
| 1 | Phát biểu bài toán | Viết một câu query bị sai (L0), đặt metric đo nó (stale-hit rate@10, nDCG@10), và dựng cái thang L0→L3 | `docs/PROBLEM.md` | Đây là xương sống của mọi việc sau (C1) |
| 2 | Tách lõi khỏi domain | `platform/` = lõi chung, `domains/<domain>/` = mọi thứ biết "product" là gì | cấu trúc thư mục | (C7) |
| 3 | Cổng kiểm tra trừu tượng | Lệnh quét `platform/` tìm 12 từ thuộc *cả hai* domain | `mosaic.py check-abstraction` | PASS: "scanned 6 files under platform/ for 12 domain terms" |
| 4 | Task runner | Máy không có `make`, nên viết `mosaic.py` chỉ dùng thư viện chuẩn Python, chạy được Windows/macOS/Linux | `mosaic.py` | (D1) |
| 5 | Stack Docker | 10 service ở profile mặc định (`mysql`, `postgres`, `kafka`, `schema-registry`, `kafka-connect`, `minio`, `minio-init`, `nessie`, `spark`, `trino`); `opa`/`marquez` (profile `gov`), `airflow` (`orch`), `api` (`console`) là tuỳ chọn | `docker/compose.yaml` | (D3): stack đầy đủ của Appendix B không vừa một máy |
| 6 | `up` đợi **healthy** | `compose up -d` trả về ngay khi container *start*, trong khi Kafka Connect nhận TCP rất lâu trước khi nhận connector. `up` đợi tới khi mọi service healthy | `mosaic.py up` | (D2) |
| 7 | Tham số hoá port | Cổng 9001 trên máy bị một container khác (`lakehouse-control-plane`) chiếm. Mọi port publish ra host đặt trong `docker/.env`; MinIO console chuyển sang 9101 | `docker/.env` | (D4); port bên trong container không đổi |
| 8 | DB metadata riêng | `mosaic_platform` với 6 bảng `platform.*`: `datasets`, `watermarks`, `watermark_history`, `column_tags`, `column_lineage`, `query_audit`. Đặt riêng vì DB domain là nguồn CDC, để chung thì bảng platform sẽ bị capture và làm bẩn luồng thay đổi | `sql/platform.sql` | (D5) |
| 9 | 4 Trino catalog | `iceberg` (qua Nessie), `mysql`, `postgresql`, `platform`; catalog thứ 4 cho phép join watermark với chính dữ liệu nó mô tả trong một câu query | `docker/trino/catalog/*.properties` | tất cả query được |
| 10 | Sửa image tag hỏng | `debezium/connect:2.7` không tồn tại → `2.7.3.Final`; `projectnessie/nessie:0.90.4` không có trên Docker Hub → `ghcr.io/...` | compose | (A3, A4) |
| 11 | Healthcheck Nessie | Image Nessie không có curl/wget nên healthcheck HTTP fail mãi, chặn Trino và Spark khởi động. Bỏ healthcheck, dependent dùng `service_started`, probe từ host | compose | (A5) |
| 12 | Quyết định kiến trúc | ADR 0001 (OPA thay Ranger), 0002 (Marquez thay DataHub), 0003 (dbt-trino cho Silver/Gold), 0004 (masking native trong connector) | `docs/adr/` | 2026-08-24 |
| 13 | Báo cáo trạng thái | Một lệnh kiểm mọi thứ | `mosaic.py doctor` | — |

### 2.3 Kiểm chứng

`python mosaic.py up` → **10/10 service healthy**; `doctor` pass mọi check; `check-abstraction`
PASS.

### 2.4 Phát hiện & lỗi tự bắt được (xảy ra quanh 2026-08-25, lần chạy thật đầu tiên)

- **A2 / ADR 0005:** worker Kafka Connect **chết ngay khi khởi động** vì
  `io.confluent.connect.avro.AvroConverter` không có trong image. Chuyển sang `JsonConverter`,
  `schemas.enable=false`.
- **`docker compose ps` giấu container đã thoát** (STATUS finding 5): một service crash biến mất khỏi
  bảng đếm health, dẫn tới một lần báo sai "all healthy". `mosaic.py` giờ so với
  `compose config --services` và đọc `ps -a` kèm exit code.

---

## 3. M1 — Sources, seed corpus, load generator, CDC live

**Đóng:** 2026-08-28 (code commit vào repo 2026-09-06) · **Nguồn:** dựng lại từ CHANGES-FROM-GR1
(A1, A6, A7, C9, D6–D10) và ADR 0004.

### 3.1 Mục tiêu

Có hai database nguồn chứa **dữ liệu mẫu có ngữ nghĩa thật**, CDC chạy trên cả hai và **chứng minh
được** là PII đã bị che. Thêm một công cụ sinh tải liên tục và một **công tắc tạo lỗi L0** theo yêu
cầu.

### 3.2 Đầu việc

| # | Đầu việc | Cách làm | Output | Kết quả |
|---|---|---|---|---|
| 1 | Schema nguồn | Dùng nguyên văn DDL của GR1 Appendix A, đặt dưới `domains/` để "thêm domain chỉ chạm vào `domains/`" đúng theo nghĩa đen | `domains/ecommerce/ddl/{mysql,postgres}.sql` | MySQL 3 bảng, PostgreSQL 6 bảng (D6) |
| 2 | Debezium connectors | Masking **ngay trong connector**: `column.mask.hash.SHA-256.with.salt...` cho email/phone, `column.exclude.list` cho `raw_card_number`, `national_id`. **Không** dùng `unwrap`, để giữ envelope. Đặt rõ `decimal.handling.mode=string` (mặc định cho ra bytes base64) và `time.precision.mode=connect` | `domains/ecommerce/connectors/*.json` | (A1, D7, D8) |
| 3 | Seed corpus generator | Mô tả sản phẩm được **ghép từ bộ từ vựng theo từng category** (không dùng lorem ipsum). Tên, mô tả và category lá được sinh **cùng nhau** để luôn khớp nhau. Kết quả **tất định** theo `(scale, seed, now)`, ghi trong `manifest.json` | `tools/seed/` | 86,314 dòng / 7 bảng ở scale `small`. Kiểm: query "waterproof hiking backpack" trả về dry bag, bếp cắm trại, gậy trekking, không có đồ bếp (C9) |
| 4 | Bulk loader | `LOAD DATA INFILE` cần quyền global `FILE`. Tài khoản `cdc` cố ý chỉ có quyền replication tối thiểu, nên nạp bằng `root`. **Không** cấp `FILE` cho `cdc` | `tools/seed/load.py` | (D9) |
| 5 | Load generator | Ghi OLTP liên tục (đặt hàng, thanh toán, đổi trạng thái, trừ kho, nhập kho). Đo **số dòng đã commit** trên một connection riêng, throttle theo con số đó, in riêng *sent* và *sustained* | `tools/loadgen/` | ~**100 ops/s sustained** (bị giới hạn bởi fsync mỗi commit dưới Docker Desktop) |
| 6 | Công tắc L0 | `stockout --top 3` cho các sản phẩm đang đứng đầu hết hàng; `restock --last 3` nhập lại. Mỗi sự kiện ghi kèm timestamp commit ở nguồn | `mosaic.py stockout/restock`, `stockout_events.jsonl` | Tạo ra đúng lỗi L0 theo yêu cầu |
| 7 | Verify CDC | Rút kỳ vọng **từ chính cấu hình connector**, lấy mẫu ở **đuôi** topic (không lấy từ đầu, xem 3.5), kiểm envelope, kiểm mask trên **cả before lẫn after**, kiểm cột bị loại **vắng mặt hẳn** | `tools/cdc/verify.py`, `mosaic.py cdc-verify` | PASS cả hai nguồn |
| 8 | Phiên DB dài | Phiên psql/mysql sống lâu qua `compose exec -T` để công cụ không phải mở connection mỗi câu | `tools/dbsession.py` | — |
| 9 | Replica identity | `REPLICA IDENTITY FULL` trên `users`, `products`, `inventory` (xem A6 bên dưới) | DDL PostgreSQL | (D10) |

### 3.3 Số đo (cdc-verify, 2026-08-28, 300 message mỗi topic, sau một lần `loadgen`)

| Khẳng định | Bằng chứng |
|---|---|
| Envelope nguyên vẹn | mọi message có `op` / `before` / `after` / `source`; không có `ExtractNewRecordState` |
| `after` đã mask | kiểm 754 giá trị đã mask trên `orders`, 674 trên `users`, tất cả là hex digest |
| **`before` đã mask** | **502 before image trên cả hai nguồn, tất cả đã mask** |
| Cột bị loại vắng mặt | `payments.raw_card_number` và `users.national_id` vắng mặt trong 637 image, tức là không có key, không chỉ là null |
| Load generator | 5,401 ops / 240 s, chênh số dòng khớp chính xác trên 4 bảng (lần chạy 2026-10-05) |

### 3.4 Phát hiện của M1

- **A6: PostgreSQL không có before image.** Với replica identity mặc định, `products` và `inventory`
  mỗi bảng có 300 event `op=u` nhưng **0 before image**, trong khi MySQL `orders` có 79/79. Sửa bằng
  `REPLICA IDENTITY FULL`. Sau khi sửa, lệnh `stockout` cho ra
  `before: {stock_qty: 192}` → `after: {stock_qty: 0}`, đúng sự kiện L0 kèm trạng thái trước đó.
- **A7: hash bị cắt theo độ rộng cột.** `buyer_phone VARCHAR(32)` chỉ nhận 32 ký tự hex (nửa digest
  SHA-256), không có log nào báo. `verify.py` đọc độ rộng cột và kỳ vọng `min(64, width)`.
- **Bẫy MySQL `ERROR 1045`**: thiếu quyền `FILE` bị báo y như sai mật khẩu. Ba đường kết nối khác
  đều xác thực được; chỉ riêng câu `LOAD DATA INFILE` lỗi. Tốn một vòng debug.

### 3.5 Lỗi tự bắt được trong M1

| Lỗi | Triệu chứng | Sửa |
|---|---|---|
| Lấy mẫu topic `--from-beginning` | Toàn bộ mẫu là initial snapshot (`op=r`, `before: null`), nên phép kiểm before-image **pass một cách vô nghĩa** dù đọc bao nhiêu message | Lấy mẫu từ **đuôi** topic |
| Generator đọc giờ hệ thống ở cấp module | Cùng seed cho ra timestamp khác nhau mỗi lần, phá tính tất định mà E3/E4 cần | Bỏ, thêm một check so sánh từng byte để canh |
| Tên/mô tả sinh độc lập | Ra những thứ như *"Minimalist Kettle"* (ấm đun) nhưng mô tả lại là nồi; truy hồi đúng mà trông như sai | Sinh tên, mô tả, category cùng lúc |
| Nhập kho vô điều kiện | Nhập cho sản phẩm bán chạy bất kể còn bao nhiêu: tồn kho phình lên 2,664 so với seed 500, **không gì có thể hết hàng tự nhiên** | Chỉ nhập khi tồn kho thấp |
| Nhập kho tức thì | Phần sản phẩm cố ý để trống trong seed bị lấp đầy trong vòng một phút | Thêm thời gian chờ `RESTOCK_LEAD_SECONDS`. **Khoảng chờ này chính là cửa sổ mà bảng xếp hạng cũ tự tin sai** |
| Báo tốc độ gửi | Một lần báo 549 ops/s trong khi MySQL vẫn đang commit phần việc đó **152 s sau**; một lần khác **mất âm thầm 5,327 orders** khi client bị kill lúc đang xả hàng đợi | Đo số dòng đã commit, in *sent* và *sustained* riêng; cuối mỗi lần chạy đếm lại số dòng ở cả hai engine |

Ghi chú thiết kế: load generator **không bao giờ chạm vào `products.name` hay
`products.description`**. Đầu vào của kênh ngữ nghĩa được giữ đứng yên có chủ đích, để mọi stale hit
chỉ có thể quy cho sự bất đối xứng độ tươi chứ không phải do text bị đổi dưới chân embedding.

---

## 4. M2 — CDC restart safety và WAL guard

**Đóng:** 2026-09-08 · **Ghi chép trực tiếp.** Mọi số liệu M2 lấy từ lần chạy cuối cùng với cấu hình
và harness đã sửa: ledger entry 7/7 trong `experiments/results/restart_events.jsonl`.

### 4.1 Mục tiêu (STATUS.md đặt ra trước khi bắt đầu)

> 1. Kill `kafka-connect` giữa chừng khi `loadgen` đang chạy, restart nó, và cho thấy
>    `_connect_offsets` chạy tiếp đúng chỗ đã dừng: **không gap, không trùng ngoài mức
>    at-least-once**.
> 2. Test tương tự cho replication slot của PostgreSQL: xác nhận `confirmed_flush_lsn` tiến lên,
>    và heartbeat giữ cho nó tiếp tục tiến khi các bảng capture đang nhàn rỗi. Đây là **WAL-bloat
>    guard của GR1 §4.4, chưa từng được thử dưới một lần đình trệ thật**.

Cả hai đã xong. Mục tiêu thứ hai **FAIL ngay lần chạy đầu**, và đó là kết quả có giá trị nhất của
milestone này.

### 4.2 Đầu việc

| File | Thay đổi | Vì sao |
|---|---|---|
| `tools/cdc/restart.py` | **mới** | Harness, có hai sub-command: `restart`, `wal-guard` |
| `mosaic.py` | +2 lệnh | `restart-safety`, `wal-guard`, theo pattern uỷ quyền `_tool()` sẵn có |
| `domains/ecommerce/ddl/postgres.sql` | thêm `cdc_heartbeat` vào `mosaic_pub` | bản sửa cho finding A9 |
| `domains/ecommerce/connectors/postgres-cdc.json` | thêm `public.cdc_heartbeat` vào `table.include.list` | cùng bản sửa: connector phải *capture* chính bảng nó ghi heartbeat vào |
| `experiments/results/restart_events.jsonl` | **mới** | ledger append-only, cùng dạng với `stockout_events.jsonl` |

**Các cam kết thiết kế của harness.** Mỗi cam kết tồn tại để chặn một cách cụ thể để "báo thành
công mà không chứng minh được gì":

- **Không nêu tên domain nào.** Danh sách bảng và topic lấy từ `topic.prefix` + `table.include.list`
  của connector (dùng lại `expectations()` của `tools/cdc/verify.py`). Cột khoá tự sinh lấy từ catalog
  sống (`information_schema` trên MySQL, `pg_attrdef` trên PostgreSQL). Trỏ harness vào connector của
  domain khác là nó đo domain đó.
- **Chứng minh không-gap một cách chính xác, không thống kê.** Mỗi bảng có khoá tự sinh ghi một
  dải id **liên tục**. Harness khoanh dải đó lúc kill và lúc ngừng ghi, rồi yêu cầu **mọi id** trong
  dải phải có trên topic. Thiếu một id là run fail và in ra đúng id đó.
- **Trùng lặp là bằng chứng, không phải lỗi** (xem A8).
- **"Đã đuổi kịp" được phát hiện máy móc**: end-offset các topic đứng yên qua 3 lần poll liên tiếp,
  không nhìn bằng mắt, và chỉ đo trên topic của *workload*. Topic heartbeat là traffic nền vĩnh viễn
  nên bị loại; để nó lại từng làm một lần chạy báo "recovery" sai là 214 s (lỗi #6 ở 4.6).

**Quy trình từng bước:**

| # | Bước | Lệnh / hành động | Kết quả |
|---|---|---|---|
| 1 | Nắm trạng thái | `python mosaic.py doctor` | Docker Desktop đang tắt; bật lên, `up` → **10/10 healthy** |
| 2 | Thăm dò offsets API | `curl :8083/connectors/<n>/offsets` | chạy được; trả về binlog `file`/`pos`/`gtids` và PG `lsn_commit`, tốt hơn tự parse `_connect_offsets` |
| 3 | **Tìm ra sự thật mà thiết kế xoay quanh** | grep log worker tìm `offset.flush.interval.ms` | **60000**: offset flush theo đồng hồ, không theo record |
| 4 | Cú pháp vị trí MySQL | `SHOW MASTER STATUS` vs `SHOW BINARY LOG STATUS` | bản 8.0.46 dùng cái đầu; cái sau là cú pháp 8.4 và báo lỗi |
| 5 | Công cụ đo slot | `pg_replication_slots` + `pg_wal_lsn_diff` | mọi cột có đủ trên PG 15 |
| 6 | Cách poll rẻ | `kafka-get-offsets.sh --topic-partitions a,b,c` | một lần exec trả về mọi topic (`--topic-list` **không có** trong bản này) |
| 7 | Viết harness | `tools/cdc/restart.py` | — |
| 8 | Smoke test phần suy luận | import `topology()` | 7 bảng capture, 6 có khoá tự sinh, mọi topic đúng |
| 9 | Chạy thí nghiệm 1 | `python mosaic.py restart-safety` | **PASS**, nhưng một dòng bằng chứng không chứng minh được gì → sửa (lỗi #1) |
| 10 | Chạy lại thí nghiệm 1 | như trên | **PASS**, mọi dòng đều có giá trị |
| 11 | Chạy thí nghiệm 2 | `python mosaic.py wal-guard --idle 130` | **FAIL**: guard không có tác dụng |
| 12 | Chẩn đoán | xem `pg_publication_tables` | `cdc_heartbeat` **không** có trong `mosaic_pub` |
| 13 | Sửa | `ALTER PUBLICATION` + sửa DDL + sửa connector + đăng ký lại | publication giờ có 5 bảng |
| 14 | Chạy lại thí nghiệm 2 | `python mosaic.py wal-guard --idle 130` | **PASS** |
| 15 | Kiểm lại bằng chứng M1 | `python mosaic.py cdc-verify` | **PASS**: 807 UPDATE event, 807 before image; topic `cdc_heartbeat` mới có 67 event `op=c`, tức bản sửa thấy được end-to-end |

### 4.3 Thí nghiệm 1: restart dưới tải

```
python mosaic.py restart-safety --rate 25 --warmup 20 --stall 45 --drain 20
```

*(Tải 25 ops/s; chạy bình thường 20 s; giết worker và để chết 45 s; xả thêm 20 s.)*

Chế độ `kill` = **SIGKILL**, có chủ đích. `docker stop` gửi SIGTERM, cho Connect kịp flush offset
trước khi tắt. Một lần tắt êm chứng minh được **ít hơn hẳn**, vì hành vi replay từ offset cũ chính là
thứ cần test.

**Chứng minh độ phủ:** mọi id đã commit ở nguồn trong lúc sập đều có mặt trên topic.

| Bảng capture | dải id đã ghi | số dòng | event trên topic | missing | duplicates |
|---|---|---|---|---|---|
| `ecommerce_tx.orders` | 27000..28054 | 1,055 | 1,844 | **0** | 36 |
| `ecommerce_tx.order_items` | 52728..54617 | 1,890 | 2,427 | **0** | 48 |
| `ecommerce_tx.payments` | 23612..24282 | 671 | 879 | **0** | 20 |
| `public.users` | 5614..5708 | 95 | 211 | **0** | 6 |
| `public.products` | — | 0 | 222 | — | không có insert trong cửa sổ |
| `public.categories` | — | 0 | 0 | — | không có insert trong cửa sổ |
| `public.inventory` | — | — | 2,716 | — | không có khoá tự sinh (PK là FK) |

*(Số event trên topic nhiều hơn số dòng vì một dòng có thể được insert rồi update trong cửa sổ.
Cột "duplicates" ở M2 sau này được đính chính, xem 5.6.)*

**Thời gian và offset:**

| Đại lượng | Giá trị |
|---|---|
| số thao tác gửi khi CDC đang chết | 1,760 |
| thời gian sập | 45 s (SIGKILL) |
| connector báo `RUNNING` sau restart | 8.3 s |
| việc ghi tiếp tục trong lúc hồi phục | 28.4 s |
| **đuổi kịp hoàn toàn sau restart** | **30.3 s** (1.9 s sau lần ghi cuối) |
| offset đã commit vượt qua điểm kill | 72.8 s sau restart (chu kỳ flush là 60 s) |
| offset MySQL, lúc kill → sau | `mysql-bin.000007:1238339` → `:2399951` |
| GTID MySQL, lúc kill → sau | `…:1-61738` → `…:1-63546` (1,808 transaction) |
| offset PostgreSQL, lúc kill → sau | `lsn 70936688` → `lsn 72250344` |
| slot trong lúc sập | `confirmed_flush` **ĐỨNG YÊN**; retained WAL 772,480 → 1,327,160 byte |
| tổng duplicate event | 110 |

**Kết luận: PASS.** 4 bảng được chứng minh không có gap qua một lần SIGKILL worker.

### 4.4 Thí nghiệm 2: WAL-accumulation guard

```
python mosaic.py wal-guard --idle 130
```

Có ba pha, vì guard chỉ có ý nghĩa khi **so sánh**:
1. nhàn rỗi, worker sống: flush **phải tiến** (heartbeat là thứ đẩy nó);
2. nhàn rỗi, worker chết: flush **phải đứng**;
3. restart: flush **phải hồi phục**.

Nếu chỉ chạy pha 1 thì chỉ thấy một con số đang chạy, chứ không thấy *cái gì* làm nó chạy.

Cửa sổ nhàn rỗi phải dài hơn chu kỳ flush 60 s, nếu không thì một vị trí flush đứng yên chỉ chứng
minh được là đồng hồ chưa kịp điểm. Harness từ chối chạy nếu `--idle` quá nhỏ.

**Trước khi sửa: FAIL.** 130 s nhàn rỗi với connector *khoẻ mạnh*:

| Mẫu | `confirmed_flush_lsn` | `current_wal_lsn` | retained WAL |
|---|---|---|---|
| bắt đầu | `0/40D3EA8` | `0/4175C28` | 2,216,816 |
| +130 s | `0/40D3EA8` *(không đổi)* | `0/4176308` *(đã tiến)* | 2,218,576 |

WAL tiến lên trong khi vị trí confirmed-flush đứng yên **chính là** tình trạng giữ WAL không giới
hạn mà GR1 §4.4 nói heartbeat sẽ ngăn chặn.

**Sau khi sửa: PASS.**

| Pha | Quan sát | Kết luận |
|---|---|---|
| nhàn rỗi, worker sống | `confirmed_flush` tiến **688,776 byte** | ok |
| nhàn rỗi, worker chết | `confirmed_flush` tiến 0 byte | đứng yên đúng như kỳ vọng |
| sau restart | `confirmed_flush` tiến 4,488 byte | ok |
| retained WAL hiện tại | **4,544 byte** (trước khi sửa là 704,880) | có giới hạn |

| nhàn rỗi, connector **sống** | mức tiến `confirmed_flush` trong 130 s |
|---|---|
| bảng heartbeat nằm ngoài publication | **0 byte** |
| bảng heartbeat được publish *(bản sửa)* | **688,776 byte** |

**Một lưu ý trung thực mà harness giờ tự in ra.** Retained WAL *không* tăng trong pha sập, và đó là
đúng chứ không phải kết quả yếu. Câu `INSERT` heartbeat **do chính connector phát ra**, nên kill
worker là dừng luôn người ghi duy nhất, và database trở nên nhàn rỗi thật sự. Guard bảo vệ trường
hợp *tập bảng capture nhàn rỗi bên cạnh một database bận rộn* (pha 1 đo đúng trường hợp đó). Guard
**không thể bảo vệ gì khi chính consumer đang chết**.

### 4.5 Phát hiện của M2

Cả hai thuộc CHANGES-FROM-GR1 Part A và nên vào báo cáo GR2.

#### A8: Restart safety là at-least-once kèm cửa sổ replay, không phải exactly-once

GR1 Table 4.3 mô tả restart safety bằng một dòng: *"offset stored in the Kafka Connect offsets
topic"*. Câu đó đúng nhưng chưa đủ. **Connect flush source offset theo đồng hồ**:
`offset.flush.interval.ms = 60000`, mặc định của Connect. Vì vậy offset đã lưu chậm hơn record cuối
cùng đã phát tới một phút, và một lần sập sẽ phát lại mọi thứ đã commit kể từ đó.

Đo được: **110 duplicate event** qua một lần SIGKILL 45 s, và **0** dòng bị mất. Cửa sổ replay bị
chặn bởi chu kỳ flush, **không phải bởi độ dài lần sập**: sập 5 giây hay 5 phút thì cũng phát lại
khoảng 60 s công việc như nhau.

*Vì sao quan trọng cho báo cáo:* một harness chấm trùng lặp là thất bại sẽ báo **false negative**
cho một cơ chế đang chạy đúng thiết kế. Phía downstream, đây cũng là lý do câu của GR1 §4.2
*"deduplicate using the source offset as an idempotency key"* là **bắt buộc** chứ không phải trang
trí, và Bronze của M3 phải thực sự implement nó.

#### A9: WAL-bloat guard của GR1 §4.4 không có tác dụng với cấu hình như viết

Heartbeat vẫn bắn đúng (503 dòng trong `cdc_heartbeat`, 503 message trên
`__debezium-heartbeat.mosaic.postgres`) nhưng **không đẩy được `confirmed_flush_lsn`**, vì
**`heartbeat.action.query` ghi vào một bảng nằm ngoài publication**. `pgoutput` chỉ giải mã bảng
*đã publish*, nên WAL của heartbeat không bao giờ thành change event, Debezium không có LSN mới để xác
nhận, và vị trí flush đứng im trong khi WAL tích tụ.

Bản sửa là thêm một bảng vào hai chỗ: `mosaic_pub` và `table.include.list`. Hiệu quả: từ 0 byte lên
688,776 byte flush trong cùng cửa sổ 130 s.

*Vì sao quan trọng:* đây là **cùng một dạng** với A6. Cả hai là biện pháp mà GR1 nêu trong văn xuôi
nhưng **âm thầm vô hiệu** cho tới khi có thứ gì đó đo nó. Không cái nào sinh ra lỗi, cảnh báo hay
dòng log. Cả hai chỉ được phát hiện nhờ xây phép đo. Đó là một câu trả lời đứng vững cho câu hỏi
*"giai đoạn implement thực sự đóng góp gì?"*

### 4.6 Lỗi tìm thấy trong chính harness của phiên này

Ghi lại vì dự án này đã ba lần bị "cắn" bởi lỗi đo lường báo thành công giả: `docker compose ps` báo
sai "all healthy"; load generator báo tốc độ gửi; mẫu topic `--from-beginning` khiến phép kiểm
before-image pass vô nghĩa. Đây là phần M2 bổ sung vào danh sách đó, và tất cả đều được bắt **trước**
khi milestone được tuyên bố xong.

| # | Lỗi | Triệu chứng | Sửa |
|---|---|---|---|
| 1 | Đọc offset đã commit **ngay trong cửa sổ flush của nó** | In `at kill:` và `after:` giống hệt nhau, nên dòng bằng chứng chính không chứng minh gì mà trông như một kết quả. Thăm dò tay sau đó cho thấy offset *đã* tiến | Poll tới khi offset vượt điểm kill và **báo mất bao lâu**: thời gian chờ trở thành phép đo (30.4 s ở lần pass) |
| 2 | Ngủ cố định đúng một chu kỳ flush (lần sửa đầu cho #1) | Vẫn đọc quá sớm: có lần flush rơi vào ~97 s sau restart chứ không phải ~60 s như chu kỳ gợi ý | Thay bằng cách poll ở trên |
| 3 | `query_rows(sep="|")` cho PostgreSQL | `PostgresSession` đọc với `-F "\t"`, nên mọi dòng sẽ bị parse thành một field duy nhất, và cả mẫu slot lẫn việc suy ra cột khoá đều âm thầm trả về rỗng | Dùng separator tab mặc định |
| 4 | Check "nhàn rỗi" đếm cả bảng heartbeat | Khi `cdc_heartbeat` đã được capture (bản sửa A9), chính các lần ghi của nó bị coi là "nhiễm" và làm fail mọi lần `wal-guard` | Loại bảng được nêu trong `heartbeat.action.query`, parse từ config đã khai báo |
| 5 | Kết luận không yêu cầu offset phải di chuyển | Một worker sống lại mà không tiêu thụ gì vẫn sẽ pass: topic im lặng, không id nào thiếu trong dải đã khoanh | Thêm `_offset_progressed()` vào điều kiện pass |
| 6 | **Bản sửa A9 làm hỏng metric hồi phục.** Khi `cdc_heartbeat` được capture, topic của nó có một event mỗi 10 s *mãi mãi*, mà bộ phát hiện "đứng yên" lại theo dõi mọi topic | Một lần chạy báo **214.59 s** "recovery", tức harness ngồi đợi một heartbeat do chính nó đưa vào tập capture. Con số thật là ~30 s | Chỉ xả các topic của workload; loại bảng của `heartbeat.action.query` |
| 7 | Tính thời gian offset tiến từ lúc *bắt đầu poll* chứ không từ lúc restart | In "committed offset caught up **0.03 s** after the restart". Poll bắt đầu sau 214 s xả, lúc đó offset đã tiến từ lâu. Vô nghĩa, và còn tô đẹp kết quả | Poll offset *bên trong* vòng xả và tính cả hai từ `t_restart`. Giờ báo 72.8 s, khớp với chu kỳ flush 60 s |

---

## 5. M3 — Medallion on Iceberg

**Đóng:** 2026-10-05 · **Ghi chép trực tiếp:** mọi số bên dưới do một lệnh chạy hôm đó sinh ra, tái
hiện được từ `experiments/results/{bronze,lakehouse,restart,snapshot}_events.jsonl`.

### 5.1 Mục tiêu

Xây ba tầng medallion trên Iceberg:
- **Bronze**: Spark Structured Streaming đọc mọi topic Debezium, giữ nguyên envelope, **exactly-once**
  dù Kafka giao at-least-once.
- **Silver**: dbt-trino, một bảng "trạng thái hiện tại" cho mỗi bảng nguồn, **khớp nguồn sống từng
  dòng**.
- **Gold**: dbt-trino, các bảng tổng hợp theo GR1 §4.3, có test chất lượng (thay Great Expectations).
- Đo KPI **end-to-end latency** của GR1 Table 4.4.

### 5.2 Giai đoạn 1 (2026-09-12..23): hạ tầng và đường ghi

*(Ghi trong bảng verified của STATUS.md lúc đó; tóm tắt lại ở đây.)*

| Đầu việc | Output |
|---|---|
| Image Spark có sẵn jar, pin bản Iceberg trùng với bản Trino dùng | `docker/spark/` → `mosaic/spark:3.5.1-iceberg1.5.2` |
| Cho Nessie lưu bền (RocksDB trên volume `nessie-data`), sống qua restart | compose |
| Hai smoke test đường ghi (Spark ghi Iceberg, Trino đọc được và ngược lại) | — |
| Bronze job, **trừ** idempotency key (để `NotImplementedError`) | `pipelines/bronze/bronze_stream.py` |
| Phát hiện **A10 lần 1** (2026-09-12): topic `categories` đã bị retention xoá sạch | CHANGES-FROM-GR1 A10 |

### 5.3 Giai đoạn 2 (2026-10-05): đầu việc đã xây

| Thành phần | Làm gì |
|---|---|
| `idempotency_key()` trong `bronze_stream.py` | SHA-256 trên `to_json(struct(topic, op, record_key, source.file/pos/row/lsn/txId/ts_ms, before, after))`. Đây là mảnh cuối cùng Bronze stream còn thiếu |
| `_ingest_ts` là hằng số theo từng batch | thay cho `current_timestamp()`: ổn định kể cả khi một partition đã cache bị tính lại, và được đóng dấu vào snapshot summary dưới tên `mosaic.ingest_ts_us`, để mỗi dòng nối được với snapshot đã publish nó |
| `--reset-checkpoint` đổi tên | checkpoint được cất sang chỗ khác, không bao giờ bị xoá |
| `tools/bronze/verify.py` | `exactly-once` (replay phía Connect), `replay` (replay phía Spark), `latency` (KPI Table 4.4) |
| Sửa `tools/cdc/restart.py` | duplicates = các source event **giống hệt nhau**; ghi dải key từng bảng vào ledger |
| `pipelines/transform/` | dbt project không chứa domain: `assemble.py` (sinh Silver + chép Gold), macro `cdc_current_state`, source Bronze có freshness, quy tắc đặt tên schema |
| `domains/ecommerce/gold/` | `orders`, `users`, `product_sales`, `user_category_affinity` + tests |
| `docker/dbt/`, service compose `dbt` | dbt-core 1.8.8 + dbt-trino 1.8.3, profile `transform` |
| `tools/lakehouse/reconcile.py` | so Silver với nguồn sống qua Trino federation |
| `tools/trino.py` | Trino REST client chỉ dùng thư viện chuẩn |
| `tools/cdc/snapshot.py` | ad-hoc BLOCKING snapshot (`resnapshot`) |

### 5.4 Quy trình từng bước (2026-10-05)

1. **Trạng thái khi bắt đầu.** Cả stack `Exited (255)` đã 10 ngày (do Docker Desktop restart).
   `mosaic-spark` chạy lần cuối ngày 2026-09-23 và thoát với mã 1 vì `NotImplementedError` của cái
   key chưa viết. Nhưng nó đã kịp kết nối Nessie, tạo bảng và subscribe 8 topic, nên **phần ống nối
   đã được chứng minh từ trước**.
2. **Viết key; `up`.** Spark lỗi ngay batch đầu: `Cannot fetch offset 2278 ... cdc_heartbeat ... Some
   data may have been lost`. Kiểm Kafka: **mọi topic bảng có earliest == latest**, tức retention đã
   xoá hết khi stack đang tắt (**A10 lần 2**). Bronze: 0 dòng, 0 snapshot, nên checkpoint
   (`offsets/0..1`, `commits/0`) không chứa gì đáng giữ. Lệnh xoá checkpoint bị permission policy của
   phiên từ chối, nên checkpoint được **đổi tên** thành `bronze.stale-2026-09-23`, và
   `--reset-checkpoint` được sửa để cũng làm như vậy.
3. **Stream khoẻ** lúc 04:20:35 UTC. `resnapshot`: cả 7 bảng phát lại đúng số dòng ở nguồn,
   **114,709 `op=r` trong 40.4 s**. Bronze xả về backlog 0; kiểm bằng *Trino* (engine khác với engine
   đã ghi): với mỗi bảng, `n = distinct keys = distinct event_id = source rows`.
4. **Replay phía Connect.** `restart-safety --mode kill`: PASS, 0 missing, **183 replayed events**.
   Sau đó `bronze-verify exactly-once` (5.5).
5. **Replay phía Spark** hai lần (5.5). **Latency** (5.6).
6. **dbt, build đầu tiên:** 4 bảng Silver nguồn PostgreSQL build xong và pass mọi test; 3 bảng nguồn
   MySQL lỗi `integer overflow`, vì `date_add` của Trino ép tham số về số nguyên 32-bit mà epoch
   millis không vừa. Viết lại dùng `from_unixtime_nanos`.
7. **Build thứ hai:** Silver ổn, các bảng PostgreSQL giờ đi qua nhánh MERGE incremental. `gold.orders`
   lỗi `ICEBERG_TOO_MANY_OPEN_PARTITIONS (174 > 100)` vì partition spec của GR1 §4.3. Nâng giới hạn
   writer lên 5000, restart Trino. **Build thứ ba: 78/78.**
8. **Reconcile** trên Silver vừa build: khớp tuyệt đối. **Negative control** với múi giờ nguồn sai:
   mọi dòng MySQL đều khác.
9. **MERGE dưới tải:** `loadgen --rate 25 --duration 240` chạy nền, ba lần `dbt build` trong lúc đó,
   một lần `stockout`. Rồi cho hệ đứng yên, build lần cuối, reconcile: khớp tuyệt đối (5.7).

### 5.5 Bronze exactly-once

**Phía Connect**: `bronze-verify exactly-once` trên lần chạy restart-safety lúc 04:32 UTC:

| Bảng | key ghi trong cửa sổ sập | insert event trên Kafka | dòng `op=c` ở Bronze | distinct |
|---|---|---|---|---|
| `ecommerce_tx.orders` | 28405..29492 (1,088) | 1,107 | 1,088 | 1,088 |
| `ecommerce_tx.order_items` | 55258..57237 (1,980) | 2,010 | 1,980 | 1,980 |
| `ecommerce_tx.payments` | 24491..25209 (719) | 724 | 719 | 719 |
| `public.users` | 5722..5816 (95) | 95 | 95 | 95 |

Phép kiểm này **cố ý độc lập** với key của chính Bronze: một insert chỉ xảy ra một lần ở nguồn, nên
`op=c` thứ hai cho cùng một key chỉ có thể là replay, và thiếu một cái chỉ có thể là gap.

**Phía Spark**: `bronze-verify replay` (SIGKILL stream dưới tải 20 ops/s, sập ~30 s):

| Lần | Thời điểm sập | Chạy lại batch đang dở | Kiểm cửa sổ |
|---|---|---|---|
| 1 | giữa batch 70, chưa commit gì | 2,987 vào, 2,987 ghi | 624 / 1,137 / 440 / 70 insert, đều chính xác |
| 2 | **sau khi Iceberg commit batch 77, trước khi checkpoint commit** | **673 vào, 0 ghi, 673 bị bỏ**; vẫn 1 snapshot cho batch 77 | 536 / 1,033 / 371 / 46, đều chính xác |

Lần 2 là đúng trường hợp mà docstring của Bronze tuyên bố sẽ hấp thụ được, và một phép append ngây
thơ thì sẽ ghi trùng. Nó **xảy ra tự nhiên**, nên nhánh withheld-commit của công cụ (viết ra để ép
trường hợp này) chưa bao giờ phải kích hoạt. Log của Spark: `batch 77: 673 in, 673 written` →
restart → `batch 77: 673 in, 0 written, 673 duplicate(s) dropped`.

### 5.6 End-to-end latency (GR1 Table 4.4)

`bronze-verify latency`, 120 s ở 20 ops/s (2,400 ops), trigger 10 s. "Khả dụng" = thời điểm commit
của Iceberg snapshot đã publish dòng đó (nối chính xác qua `_ingest_ts`); loại các dòng `op=r`.

| Nguồn | events | p50 | p95 | p99 | max | capture p50 / p95 | landing p50 / p95 |
|---|---|---|---|---|---|---|---|
| MySQL | 5,144 | 7.76 s | 11.66 | 12.29 | 13.21 | 0.26 / 0.48 | 7.50 / 11.37 |
| PostgreSQL | 2,791 | 7.99 s | 12.03 | 12.91 | 13.51 | 0.49 / 0.87 | 7.50 / 11.51 |

*Capture* = commit ở nguồn → record trên Kafka; *landing* = Kafka → snapshot commit. Debezium dưới
1 giây ở cả hai nguồn; **trigger chiếm phần lớn** (thời gian chờ phân bố đều 0–10 s cộng khoảng 2.5 s
xử lý batch). Vì vậy KPI này chỉnh được bằng `BRONZE_TRIGGER_SECONDS`, đánh đổi là sinh ra nhiều
file nhỏ hơn.

**Đính chính A8 (phát hiện trong M3):** cột "duplicates" của M2 đếm *số event trên mỗi key vượt quá
cái đầu tiên*, nên lẫn cả UPDATE hợp lệ (một order đi từ `pending → paid` là hai source event chứ
không phải một event giao hai lần), và bỏ qua các bảng không có khoá tự sinh. M3 đổi metric thành
"**cùng một source event được giao hơn một lần**", khớp theo op, vị trí nguồn, `source.ts_ms` và cả
hai image. Lần chạy tương đương ở M3 đo được **183 replay trên sáu bảng** (54 là insert bị replay).
Kết luận không đổi.

### 5.7 Silver và Gold

Số dòng được MERGE incremental trong và sau bài test tải:

| Model | build 2 | build 3 | final |
|---|---|---|---|
| `silver.orders` | 577 | 660 | 2,621 |
| `silver.inventory` | 278 | 300 | 724 |
| `silver.users` | 73 | 54 | 349 |
| `silver.order_items` / `payments` / `products` | | | 3,795 / 1,363 / 173 |
| `gold.orders` | 577 | 660 | 2,621 |

Build 1 MERGE 0 dòng vì nó chạy trước khi các commit đầu tiên của generator tới được Bronze.

**Reconcile cuối cùng (lúc hệ đứng yên):** 0 missing / 0 extra / 0 differing ở cả 7 bảng (36,678
orders, 70,056 items, 29,827 payments, 6,479 users, 1,000 products, 48 categories, 1,000
inventory), trên mọi cột không bị mask (cột bị mask: `buyer_email`, `buyer_phone`, `email`,
`phone`).

**Gold so với số tổng hợp sống (ad hoc, qua federation):** `gold.orders` (số dòng item, số đơn vị,
status) so với MySQL: 36,678 orders, 0 missing/extra/differing. `product_sales.stock_qty` so với
`inventory` sống: 0/1,000 khác.

**L0 quan sát được ngay trong stack:** trong lúc chạy tải, sản phẩm 68 về tồn kho 0 ở nguồn lúc
05:01:49; Bronze có thông tin này 2 s sau; Silver/Gold vẫn ghi 9 cho tới lần build kế tiếp.

**Tác dụng phụ trên Iceberg:** `silver.orders` sau 4 lần MERGE có 4 data file, 5 position-delete
file, 785 position delete (theo snapshot summary; `$files` của Trino 451 không liệt kê delete file).
`gold.product_sales` giữ 6 snapshot qua các lần build (`CREATE OR REPLACE`). `SHOW STATS` sau
ANALYZE post-hook: NDV của `order_id` là 36,678.

**Đo partitioning của GR1 §4.3** (finding A12): `gold.orders` có 2,944 phân vùng, 2,968 file, 12.6
dòng/file. `ANALYZE` mất 8.25 s so với 0.25 s trên `silver.orders` không phân vùng (33 lần); full
scan 1.28 s so với 0.09 s (14 lần). GR1 Listing 4.2: 100 dòng trong 1.61 s.

### 5.8 Phát hiện của M3

| # | Phát hiện | Ở đâu |
|---|---|---|
| A10 | Kafka retention xoá lịch sử CDC trước khi Bronze kịp nạp, hai lần | CHANGES-FROM-GR1 |
| A11 | MySQL `DATETIME` lệch 7 h; việc chuẩn hoá chuyển từ SMT sang Silver | CHANGES-FROM-GR1 |
| A12 | Partitioning `gold.orders` của §4.3 được thiết kế cho quy mô khác | CHANGES-FROM-GR1 |
| A8 (đính chính) | Đếm trùng của M2 lẫn replay với update hợp lệ | CHANGES-FROM-GR1 A8 |
| D15 | Seed và load generator dùng hai bộ từ vựng vùng khác nhau | CHANGES-FROM-GR1 |
| — | Trino không ghi được thuộc tính snapshot summary; `$files` giấu delete file | STATUS finding 12 |
| — | Silver phải sắp theo vị trí nguồn, không theo thời gian | STATUS finding 17 |

### 5.9 Lỗi tự bắt được trong M3

1. **Metric trùng lặp của M2** (đính chính A8 ở trên). Phát hiện khi thiết kế phép kiểm Bronze, vì
   phép chứng minh theo dải insert cần một con số có nghĩa là *replay*.
2. **Một lời cảnh báo sai, bị chính dữ liệu bắt.** Công cụ latency ban đầu in rằng thời gian nguồn
   MySQL chỉ có độ phân giải 1 giây, suy ra từ các dòng snapshot. Nhưng số capture mâu thuẫn với điều
   đó (p50 0.26 s không thể tồn tại nếu bị cắt tới 1 s). Query Bronze cho thấy 19,029/19,045 insert
   MySQL có thời gian dưới giây; chỉ các dòng `op=r` mới là giây chẵn. Cảnh báo được sửa trước khi
   lọt vào bất kỳ tài liệu nào.
3. **`decimal` qua REST.** Lần chạy latency đầu tiên crash khi in `max()`, vì giao thức REST của
   Trino trả decimal dưới dạng chuỗi. Cast sang `double` trong SQL; lần chạy được **thực hiện lại**,
   không vá kết quả cũ.
4. **Giữ negative control trong ledger.** Dòng 2 của `lakehouse_events.jsonl` là lần chạy cố ý sai
   múi giờ (`MISMATCH`). Đó là bằng chứng rằng phép reconcile *có thể* fail.

### 5.10 Những gì M3 còn để lại chưa kiểm chứng

- Nhánh withheld-commit của `bronze-verify replay` (chưa cần tới, xem 5.5).
- Iceberg maintenance: chưa có compaction, snapshot expiry hay dọn orphan file.
- Chưa có lịch dbt; Silver/Gold chỉ refresh khi chạy `python mosaic.py dbt`.
- Mọi thứ ở quy mô `small`.

---

## 6. Sau M3 — giới hạn bộ nhớ Trino (2026-10-13/16)

| Ngày | Việc | Kết quả |
|---|---|---|
| 2026-10-13 | Đo một lần build incremental **không thay đổi dòng nào** | vẫn tốn 45–61 s cho `gold.orders`; một câu lệnh quét 27,142 split cho 95 MiB. Đây là thứ đẩy heap Trino lên 6+ GiB (hệ quả của A12) |
| 2026-10-13 | Chẩn đoán | Image Trino tự đặt heap bằng 80% bộ nhớ nó thấy (`MaxRAMPercentage=80`). Không có giới hạn container thì đó là 80% của cả Docker VM (12.3 GiB), và G1 cứ phình heap ra thay vì thu gom (6–7 GiB cho một live heap 0.4 GiB) |
| 2026-10-16 | Sửa: `docker/trino/jvm.config` pin `-Xmx3G`; `mem_limit: 6g` làm chốt chặn; `MALLOC_ARENA_MAX=2` (glibc mặc định mở tới 128 arena trên 16 CPU) | Đỉnh `dbt build` incremental 6.20 → **2.59 GiB**; ghi 2,944 phân vùng 7.04 → **3.90 GiB**; đỉnh kernel 7.39 → **3.93 GiB**; 0 OOM; 78/78 pass. Riêng `MALLOC_ARENA_MAX`: off-heap 1.47 → 0.87 GiB |

Bài học đo lường: phải nhìn **RSS hoặc `memory.peak`** của container, không nhìn memory pool của Trino.
Pool chỉ đạt đỉnh 19 MiB trong khi container ngồi ở 6 GiB.

---

## 7. M4 — Embedding pipeline (kênh chậm) và đăng ký dataset

**Đóng:** 2026-10-16 · **Ghi chép trực tiếp.** Mọi quyết định thiết kế được hỏi và chốt **trước khi**
viết code, và ghi đầy đủ lý do ở [ADR 0006](adr/0006-m4-embedding-pipeline.md). Các mã `M4-xx` dưới đây
trỏ vào ADR đó.

### 7.1 Mục tiêu

Dựng **kênh chậm** của L0, tức một vector index trên pgvector, sao cho nó **cũ theo đúng cách L0 mô
tả**: không chỉ "già", mà **sai** về những thứ đã đổi sau lần refresh. Đồng thời đăng ký mọi dataset vào
`platform.datasets`, làm điểm bàn giao cho N1 (M5).

Câu hỏi then chốt (ADR 0006, phần Context): embedding chỉ mã hoá ý nghĩa của mô tả, mà mô tả thì load
generator cố ý không bao giờ sửa. Nếu chỉ "embed mô tả" thì index cũ 6 tiếng giống hệt index mới, và E4
sẽ đo một hiệu ứng không tồn tại. Lời giải (M4-A1): index chỉ chứa sản phẩm **còn hàng lúc refresh**,
kèm **ảnh chụp** `stock_qty`/`price` tại thời điểm đó.

### 7.2 Giai đoạn 0: dọn đường (2026-10-16)

| Việc | Kết quả |
|---|---|
| Commit toàn bộ code M3 chưa từng được commit (M4-D1) | `c24134a`: Bronze stream, image Spark/dbt, Gold models, giới hạn Trino, signal channel |
| Bật lại stack sau khi Docker Desktop dừng cứng | 9/10 healthy; **Bronze stream chết**: `Failed to read log file .../offsets/1891. Incomplete log file` |
| Chẩn đoán checkpoint | `offsets/1890`, `offsets/1891`, `commits/1890`, `commits/1891` đều **0 byte**: rename đã xảy ra nhưng nội dung chưa từng được flush xuống đĩa trước khi VM dừng. Cặp nguyên vẹn gần nhất: 1889 |
| Khôi phục (D17) | Cất bốn file rỗng sang `checkpoints/bronze.torn-2026-10-16/` (không xoá). Spark chạy lại từ batch 1890: **"2 already in Bronze"**, idempotency key chặn đúng phần đã ghi. Exactly-once được chứng minh thêm một lần, lần này từ sự cố thật |

### 7.3 Đầu việc

| # | Đầu việc | Cách làm | Output | Kết quả |
|---|---|---|---|---|
| 1 | Spec của domain (M4-B2) | `retrieval.yaml` khai báo entity, 12 dataset với SLA, và index (nguồn, khoá, cột text, predicate `eligible`, cột snapshot, đích, model, HNSW, SLA). Validator viết tay, báo lỗi chỉ đúng key | `domains/ecommerce/retrieval.yaml`, `platform/spec/retrieval_spec.py` | spec sai (index trỏ tới dataset chưa khai báo) bị từ chối, thông báo chỉ đúng chỗ |
| 2 | Gold catalog (M4-B2a) | Model mới: text (tên, mô tả, tên category lá) + hai fact nhanh | `domains/ecommerce/gold/product_catalog.sql` | 1,000 dòng, 946 còn hàng = nguồn sống 946; dbt toàn project **84/84 PASS** |
| 3 | Image jobs (M4-C4) | Python 3.11, torch CPU, sentence-transformers 3.0.1, psycopg 3; model bake sẵn ở revision `1110a243…`, `HF_HUB_OFFLINE=1` | `docker/jobs/`, service `jobs` (profile `jobs`) | 498 MB; build ~4 phút |
| 4 | Refresh index (M4-A1, A3, C2) | guard → đọc Gold pin snapshot (một câu lệnh, kèm watermark) → embed → build bảng mới + HNSW → swap trong một transaction | `pipelines/embeddings/index.py` | xem 7.4 |
| 5 | Đường đọc (query, recall) | top-k cosine kèm snapshot + `refreshed_at`; recall HNSW vs exact, **EXPLAIN từng đường đi** | `pipelines/embeddings/ann.py` | phát hiện A13, xem 7.5 |
| 6 | Chốt chặn N3 (M4-B3) | đọc `platform.column_tags`, từ chối nếu cột text mang `sensitive`; in số tag đang biết | `platform/governance/guard.py` | negative control pass |
| 7 | Đăng ký dataset (M4-B4) | probe thật từng dataset, tất cả hoặc không gì cả, xoá dataset không còn khai báo | `platform/freshness/register.py`, `platform/freshness/datasets.yaml` | 14 dataset; spec lệch thì không đăng ký gì |
| 8 | Gate mở rộng (M4-B2) | quét thêm `pipelines/embeddings/`; miễn trừ đúng cụm `ORDER BY` | `mosaic.py` (`GATED_DIRS`) | bắt được *"inner **product**"* ngay lần đầu |
| 9 | Kiểm chứng (M4-D3) | sáu kiểm tra có răng, cộng `--with-refresh` | `tools/embeddings/verify.py`, `mosaic.py embed-verify` | **7/7 PASS** |

### 7.4 Số đo: refresh

Ở quy mô `small`, 946 dòng đủ điều kiện trên 1,000 (ledger `embedding_events.jsonl`, event `refresh`):

| Giai đoạn | Thời gian |
|---|---|
| đọc Gold qua Trino (pin snapshot) | 0.10–1.18 s |
| nạp model | 2.9–3.2 s |
| embed 946 text trên CPU | 4.6–4.9 s (~205 text/s) |
| `COPY` + build HNSW + swap | 0.12 + 0.14–0.17 + 0.01–0.04 s |
| **tổng** | **8.1–9.3 s** |

**`refreshed_at` theo watermark hoạt động như thiết kế (M4-A3).** Lần refresh đầu đóng dấu
`2026-10-05 05:03:20 UTC`, tức "cũ 992,310 s (11.5 ngày) ngay lúc ra đời", vì nguồn không thay đổi gì từ
hôm đó. Sau khi stockout đi qua Bronze và Gold được build lại, `refreshed_at` nhảy lên
`2026-10-16 16:45:52`, và index chỉ cũ 33.6 s lúc ra đời (4 s tới Bronze, ~30 s dbt). Còn **refresh lại
mà không build lại Gold thì không tươi hơn chút nào**: lần cuối giữ nguyên `refreshed_at`, tuổi lúc ra
đời tăng từ 64 lên 91 s.

### 7.5 Số đo: đường đọc

Trên 100 vector truy vấn lấy mẫu từ index, k = 10, sau khi pin access path:

| Đường đi | p50 | p95 | recall@10 | plan đã kiểm |
|---|---|---|---|---|
| exact | 0.58–0.85 ms | 0.73–1.43 ms | 1.0 (định nghĩa) | sequential scan |
| HNSW `ef_search=10` | 0.34–0.37 ms | 0.41–0.47 ms | 1.0000 | index scan |
| HNSW `ef_search=40` | 0.53–0.54 ms | 0.69–0.80 ms | 1.0000 | index scan |
| HNSW `ef_search=100` | 0.57–0.71 ms | 0.76–0.90 ms | 1.0000 | index scan |

Ở 946 dòng, recall 1.0 là điều dự kiến *theo cấu tạo*; đánh đổi thật là việc của E3 ở quy mô `full`.

### 7.6 Số đo: kiểm chứng `embed-verify --with-refresh` (95.8 s)

| Kiểm tra | Kết quả |
|---|---|
| counts | index 946 = ledger 946 = số đủ điều kiện đếm lại ở **đúng** Iceberg snapshot đã pin; `refreshed_at` đồng nhất và = `max(_src_ts)` của snapshot đó |
| semantics | "waterproof hiking backpack" 10/10 Outdoor & Hiking; "wireless noise cancelling headphones for commuting" 10/10 Audio & Headphones; "cast iron skillet for searing" 10/10 Kitchen & Dining |
| recall | 1.0 ở `ef_search=40`, plan cả hai đường đều đúng |
| guard | gắn `sensitive` lên `description` → refresh **exit 2 REFUSED**, index không đổi; gỡ tag → exit 0 |
| no drift | trước khi kéo công tắc: 0/10 kết quả mâu thuẫn với nguồn sống |
| **L0** | **stockout kết quả #1 (sản phẩm 342, tồn kho sống 160 → 0); index vẫn trả nó ở #1 với `stock_qty=160`; stale-hit rate@10 = 1/10** |
| L0 fixed | thay đổi tới Bronze sau 4 s → dbt `+product_catalog` → refresh: sản phẩm 342 không còn trong top-10; stale-hit 0/10. Sau đó restock về 160 |

**Đây là lần đầu L0 được tái hiện end-to-end trên stack thật.** Trước M4, lỗi chỉ quan sát được gián tiếp
(STATUS: "Silver/Gold vẫn ghi 9 trong khi nguồn sống là 0").

### 7.7 Phát hiện của M4

| Mã | Phát hiện | Ghi ở |
|---|---|---|
| A13 | Ở 946 vector, planner chọn sequential scan (cost 230) thay vì HNSW (cost 417). Không pin thì "ANN" thực chất là exact, và thí nghiệm Track 2 của §4.5 cho recall 1.0 cùng đường cong `ef_search` phẳng, **vô nghĩa** | CHANGES-FROM-GR1 A13 |
| D17 | Checkpoint Spark trên named volume không bền khi Docker Desktop dừng cứng: bốn file metadata 0 byte | CHANGES-FROM-GR1 D17 |
| (M5) | **Bài toán nguồn nhàn rỗi.** Bronze trễ 20 s nhờ heartbeat CDC, còn Silver/Gold "trễ" 11 ngày chỉ vì nguồn đứng yên. Watermark event-time không phân biệt được "đứng yên" với "kẹt" | ADR 0006 M4-A3; CHANGES-FROM-GR1 C2 |

### 7.8 Lỗi tự bắt được trong M4

| Lỗi | Bắt bằng gì | Sửa |
|---|---|---|
| Comment *"inner **product**"* trong runner | gate mở rộng, lần chạy đầu | đổi thành "dot similarity" |
| "HNSW" chạy sequential scan | phép kiểm plan trong `query` (`plan uses hnsw index: False`) | pin access path (A13) |
| Registry in watermark của dataset vector theo giờ +07 dưới nhãn "UTC" | đọc output: 12:03:20 so với 05:03:20 của mọi dòng khác | đổi sang UTC trước khi in; lag vốn đã đúng |
| Giả định `_src_ts` của Bronze là `timestamp` không múi giờ | `with_timezone()` báo `FUNCTION_NOT_FOUND` lúc `--with-refresh`; `DESCRIBE` cho thấy là `timestamp(6) with time zone` | bỏ ép kiểu; sửa docstring đã viết sai |

### 7.9 Những gì M4 còn để lại chưa kiểm chứng

- Mọi thứ ở quy mô `small`; planner ở 100k vector có tự chọn HNSW không thì **chưa kiểm**.
- Lan truyền tag N3 (M8); guard hiện chỉ chặn tag được ghi trực tiếp.
- Watermark chưa được ghi vào `platform.watermarks` (việc của collector M5); `register` chỉ in bản xem trước.
- Baseline post-filter của E4 (M4-A2) mới là cam kết, chưa có code.

---

## 8. Deliverables: bản đồ file theo milestone

Chỉ liệt kê file có thật trong cây thư mục.

| Milestone | Path | Loại | Làm gì |
|---|---|---|---|
| M0 | `docker/compose.yaml` | stack | triển khai 10 service trên một máy, profile `gov`/`orch`/`console` |
| M0 | `docker/trino/catalog/*.properties` | config | 4 catalog Trino: `iceberg` (Nessie), `mysql`, `postgresql`, `platform` |
| M0 | `sql/platform.sql` | schema | `mosaic_platform`: 6 bảng `platform.*` không chứa domain (N1 + N3) |
| M0 | `mosaic.py` | tooling | task runner (máy không có `make`) |
| M0 | `docs/PROBLEM.md` | spec | cái thang trừu tượng L0–L3 |
| M1 | `domains/ecommerce/ddl/{mysql,postgres}.sql` | schema | GR1 Appendix A, cộng `REPLICA IDENTITY FULL` (A6) |
| M1 | `domains/ecommerce/connectors/*.json` | config | connector Debezium với masking native (ADR 0004) |
| M1 | `tools/seed/` | tooling | sinh corpus tất định, có ngữ nghĩa thật + bulk loader |
| M1 | `tools/loadgen/` | tooling | ghi OLTP liên tục + công tắc stock-out (tạo ra L0) |
| M1 | `tools/cdc/verify.py` | tooling | chứng minh masking/envelope, kỳ vọng rút từ JSON connector |
| M1 | `tools/dbsession.py` | tooling | phiên psql/mysql sống lâu qua `compose exec -T` |
| **M2** | **`tools/cdc/restart.py`** | **tooling** | **harness restart-safety + WAL-guard (artifact chính của M2)** |
| **M2** | **`experiments/results/restart_events.jsonl`** | **data** | **ledger máy đọc được của mọi lần chạy M2** |
| **M3** | **`pipelines/bronze/bronze_stream.py`** | **pipeline** | **mọi topic Debezium → `iceberg.bronze.cdc_events`, envelope nguyên vẹn, có idempotency key** |
| **M3** | **`docker/spark/`** | **image** | **`mosaic/spark:3.5.1-iceberg1.5.2`, jar có sẵn, Iceberg pin trùng bản của Trino** |
| **M3** | **`pipelines/transform/`** | **dbt project** | **không chứa domain: `assemble.py` sinh Silver, macro `cdc_current_state`, source Bronze** |
| **M3** | **`domains/ecommerce/gold/`** | **models** | **4 model Gold + tests, gồm các check chéo nguồn của GR1 §4.3** |
| **M3** | **`docker/dbt/`** | **image** | **`mosaic/dbt:1.8` (dbt-core 1.8.8, dbt-trino 1.8.3)** |
| **M3** | **`tools/bronze/verify.py`** | **tooling** | **Bronze exactly-once (cả hai nguồn replay) + latency commit→Bronze** |
| **M3** | **`tools/lakehouse/reconcile.py`** | **tooling** | **Silver so với nguồn sống từng dòng** |
| **M3** | **`tools/cdc/snapshot.py`, `tools/trino.py`** | **tooling** | **BLOCKING snapshot theo yêu cầu; Trino REST client thư viện chuẩn** |
| sau M3 | `docker/trino/jvm.config` | config | giới hạn heap Trino `-Xmx3G` |
| **M4** | **`domains/ecommerce/retrieval.yaml`** | **spec** | **hợp đồng domain: entity, dataset + SLA, index (nguồn, text, `eligible`, snapshot, đích, model)** |
| **M4** | **`domains/ecommerce/gold/product_catalog.sql`** | **model** | **"Gold-layer product catalog" của GR1 §4.1: nguồn của index** |
| **M4** | **`platform/spec/retrieval_spec.py`** | **core** | **load + validate `retrieval.yaml`** |
| **M4** | **`platform/runtime/db.py`** | **core** | **Trino REST (kèm kiểu cột) + psycopg cho job trong stack** |
| **M4** | **`platform/governance/guard.py`** | **core** | **chốt chặn N3 cho đầu vào embedding** |
| **M4** | **`platform/freshness/register.py`, `datasets.yaml`** | **core** | **probe + đăng ký dataset vào `platform.datasets`** |
| **M4** | **`pipelines/embeddings/`** | **pipeline** | **refresh (publish nguyên tử), query, recall; domain-free, được gate kiểm** |
| **M4** | **`docker/jobs/`** | **image** | **`mosaic/jobs:1`: torch CPU, sentence-transformers, model pin revision** |
| **M4** | **`tools/embeddings/verify.py`** | **tooling** | **sáu kiểm tra nghiệm thu M4, kết thúc bằng tái hiện L0** |
| **M4** | **`docs/adr/0006-m4-embedding-pipeline.md`** | **ADR** | **mọi quyết định M4 kèm lý do** |

---

## 9. Environment facts đã xác lập

Bổ sung vào danh sách gotcha của CLAUDE.md. Mỗi dòng đã tốn ít nhất một lần chạy hỏng hoặc gây hiểu
nhầm.

| Sự thật | Hệ quả |
|---|---|
| `offset.flush.interval.ms = 60000` trên worker này | không bao giờ đọc offset đã commit trong ~60 s sau restart rồi coi là bằng chứng |
| MySQL ở đây là **8.0.46** → `SHOW MASTER STATUS` | `SHOW BINARY LOG STATUS` là cú pháp 8.4, báo lỗi cú pháp |
| `kafka-get-offsets.sh` **không có** `--topic-list` trong bản này | dùng `--topic-partitions a,b,c`: một lần exec cho mọi topic, nhờ vậy mới poll mỗi 2 s được |
| `PostgresSession` đọc với `-F "\t"` | `query_rows()` trên PG phải dùng separator tab mặc định, không dùng `|` |
| `heartbeat.action.query` của Debezium chỉ có ích nếu bảng của nó **được capture** | nếu không thì WAL guard vô hiệu và im lặng (A9) |
| Người ghi heartbeat **chính là connector** | kill worker là dừng luôn heartbeat, nên pha sập không thấy WAL tăng trên database đang nhàn rỗi |
| Docker Desktop dừng thì cả stack dừng | xảy ra giữa phiên; named volume còn nguyên, `up` là khôi phục |
| Stack dừng quá 7 ngày thì quay lại với topic CDC rỗng | Kafka retention chạy theo giờ thật ngay khi broker khởi động (A10) |
| `date_add` của Trino ép tham số về int 32-bit | dùng `from_unixtime_nanos(ms * 1000000)` |
| REST của Trino trả `decimal` dạng chuỗi | cast sang `double` trong SQL |
| Trino giới hạn 100 partition writer | nâng lên 5000 cho `gold.orders` (D14) |
| Image Trino lấy heap = 80% bộ nhớ nhìn thấy | pin `-Xmx3G` + `mem_limit` + `MALLOC_ARENA_MAX=2` (mục 6) |
| Docker Desktop dừng cứng có thể để lại file checkpoint Spark **0 byte** | stream chết với `Incomplete log file`; cất các cặp offset/commit rỗng, chạy lại từ cặp nguyên vẹn gần nhất (D17) |
| Thư mục `platform/` trùng tên module `platform` của thư viện chuẩn | trong container nó được mount thành `mosaic_platform`; torch/transformers import `platform` |
| Planner của PostgreSQL không dùng HNSW ở ~1k vector | pin access path cho mỗi đường đi, kiểm bằng `EXPLAIN` (A13) |
| `_src_ts` ở mọi tầng là `timestamp(6) with time zone` trong Trino | Spark `TIMESTAMP` thành Iceberg `timestamptz`; so sánh thẳng với `from_iso8601_timestamp` |
| psycopg trả `timestamptz` theo múi giờ session (+07 ở đây) | `.astimezone(utc)` trước khi in hoặc so sánh chuỗi |

---

## 10. Command reference

| Lệnh | Milestone | Mục đích |
|---|---|---|
| `python mosaic.py doctor` | M0 | báo cáo trạng thái một phát |
| `python mosaic.py up / down / clean` | M0 | vòng đời stack; `up` đợi **healthy**; `clean` xoá cả volume |
| `python mosaic.py check-ports` | M0 | xung đột port trên host, kèm chủ sở hữu |
| `python mosaic.py check-abstraction` | M0 | cổng kiểm tra độ thuần của lõi (L3) |
| `python mosaic.py generate` / `python -m tools.seed.load` | M1 | sinh và nạp corpus |
| `python mosaic.py connectors` | M1 | đăng ký connector Debezium cho một domain |
| `python mosaic.py cdc-verify [--require-update]` | M1 | chứng minh masking/envelope trên topic |
| `python mosaic.py loadgen --rate N --duration S` | M1 | ghi OLTP liên tục |
| `python mosaic.py stockout / restock` | M1 | công tắc lỗi L0 |
| **`python mosaic.py restart-safety [--mode kill\|stop\|pause]`** | **M2** | **kill Connect dưới tải, chứng minh không gap** |
| **`python mosaic.py wal-guard [--idle N]`** | **M2** | **heartbeat / WAL-accumulation guard** |
| **`python mosaic.py resnapshot`** | **M3** | **BLOCKING ad-hoc snapshot, kiểm với số dòng nguồn** |
| **`python mosaic.py bronze`** | **M3** | **Bronze stream còn sống không, backlog, batch gần nhất** |
| **`python mosaic.py bronze-verify exactly-once\|replay\|latency`** | **M3** | **các phép chứng minh Bronze và KPI end-to-end** |
| **`python mosaic.py dbt [-- args]`** | **M3** | **Silver + Gold (assemble rồi dbt)** |
| **`python -m tools.lakehouse.reconcile`** | **M3** | **Silver == nguồn sống, từng dòng** |
| **`python mosaic.py embed refresh\|status\|query\|recall [-- args]`** | **M4** | **index của kênh chậm: rebuild, trạng thái, top-k, recall HNSW vs exact** |
| **`python mosaic.py register [--dry-run]`** | **M4** | **probe + đăng ký dataset vào `platform.datasets`** |
| **`python mosaic.py embed-verify [--with-refresh]`** | **M4** | **nghiệm thu M4, kết thúc bằng tái hiện L0** |

---

## 11. Độ phủ KPI của GR1 Table 4.4

GR1 định nghĩa năm KPI CDC để đo "khi pipeline được implement ở GR2". Hiện **ba KPI đã đo**, một KPI
đo một phần, một KPI chưa bắt đầu.

| KPI | Trạng thái | Giá trị / vướng mắc |
|---|---|---|
| **End-to-end latency (commit nguồn → Iceberg Bronze)** | **đã đo (M3)** | p50 **7.8 s** MySQL / **8.0 s** PostgreSQL, p99 12.3 / 12.9 s; capture p50 0.26 / 0.49 s, phần còn lại là trigger 10 s |
| Throughput dưới tải liên tục | một phần (M1) | ~100 ops/s sustained, bị giới hạn bởi fsync mỗi commit dưới Docker Desktop |
| **Restart recovery time** | **đã đo (M2)** | **30.3 s** để đuổi kịp hoàn toàn sau SIGKILL 45 s, với việc ghi vẫn tiếp tục 28.4 s trong lúc hồi phục |
| DDL handling (ảnh hưởng của `ALTER TABLE` ở downstream) | chưa bắt đầu | giới hạn đã ghi ở ADR 0005 (`schemas.enable=false` không thấy được thay đổi kiểu thuần tuý) |
| **WAL lag (chỉ PostgreSQL)** | **đã đo (M2)** | tái hiện được flush đứng yên + WAL phình theo yêu cầu; guard đã sửa và đo lại (0 → 688,776 byte) |

*(Bản gốc ghi "Two are now measured" nhưng bảng liệt kê ba KPI đã đo; ADVISOR-BRIEFING cũng nói
"three of GR1's five". Số đúng là **ba**.)*

---

## 12. Những gì CHƯA được kiểm chứng

Nêu rõ, theo quy tắc báo cáo của dự án.

- **Bản sửa DDL được áp bằng tay lên database đang chạy** (`ALTER PUBLICATION`). Phần sửa trong
  `domains/ecommerce/ddl/postgres.sql` chỉ có hiệu lực với volume mới. Lưu ý tương tự áp dụng cho
  `REPLICA IDENTITY FULL`.
- Nhánh withheld-commit của `bronze-verify replay` (xem 5.5).
- Iceberg maintenance chưa tồn tại; dbt chưa có lịch.
- Các profile `gov`, `orch`, `console` chưa từng được bật.
- Mọi thứ vẫn ở quy mô `small` (~86k dòng seed cộng phần tăng từ load generator).
- M4: planner ở 100k vector có tự chọn HNSW không; lan truyền tag N3; ghi watermark vào
  `platform.watermarks` (M5); baseline post-filter của E4 (xem 7.9).

---

## 13. Run ledgers

Máy đọc được, append-only, mỗi dòng một object JSON. Đây là thứ E1–E4 sẽ đọc lại.

| File | Ghi bởi | Nội dung |
|---|---|---|
| `experiments/results/stockout_events.jsonl` | `stockout` / `restock` / `loadgen` | mọi lần hết hàng (tự nhiên hoặc thủ công) kèm timestamp commit ở nguồn |
| `experiments/results/restart_events.jsonl` | `restart-safety` / `wal-guard` | kết luận từng lần chạy, số gap và trùng, mọi mốc thời gian, offset lúc kill và sau đó, toàn bộ chuỗi mẫu replication slot; từ M3 có thêm dải key từng bảng |
| `experiments/results/snapshot_events.jsonl` | `resnapshot` | số `op=r` từng bảng so với số dòng nguồn |
| `experiments/results/bronze_events.jsonl` | `bronze-verify` | các lần chạy exactly-once, Spark-replay và latency |
| `experiments/results/lakehouse_events.jsonl` | `tools.lakehouse.reconcile` | số missing / extra / differing từng bảng |
| `experiments/results/embedding_events.jsonl` | `embed refresh` / `embed recall` / `embed-verify` | mỗi refresh (snapshot đã pin, số dòng, `refreshed_at`, tuổi lúc ra đời, thời gian từng giai đoạn, model revision, kết quả guard, kể cả lần bị từ chối), mỗi lần đo recall, mỗi lần nghiệm thu |

Tính đến 2026-09-08, ledger restart có 5 entry: 3 `restart-safety` (pass, inconclusive, pass) và 2
`wal-guard` (fail, pass). **Các lần thất bại được giữ có chủ đích.** Cặp fail/pass của `wal-guard`
*chính là* bằng chứng cho A9, và entry `inconclusive` là lần chạy đã làm lộ ra lỗi harness #1 ở 4.6.
