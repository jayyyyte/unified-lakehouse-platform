# Implementation status (Trạng thái implement)

**Last updated:** 2026-10-16 | **Current milestone:** M4 **done, verified** → M5 next | **Next action:** M5: collector của N1 Freshness Registry (ghi `platform.watermarks`), giải bài toán nguồn nhàn rỗi bằng heartbeat CDC

Cách nhanh nhất để xem trạng thái thật của hệ thống: `python mosaic.py doctor`.

> *Thuật ngữ khó tra ở [PROBLEM.md, Phụ lục C](PROBLEM.md#phụ-lục-c--bảng-thuật-ngữ-glossary).*

**File này để làm gì.** Đây là thứ đầu tiên một phiên làm việc mới phải đọc sau
[CLAUDE.md](../CLAUDE.md). Nhiệm vụ của nó là tránh phải **suy luận lại** những gì đã làm xong. File
này là một **snapshot** và bị ghi đè liên tục; còn lịch sử theo thời gian thì nằm ở
[BUILD-LOG.md](BUILD-LOG.md).

**Quy tắc cập nhật:**
- Chỉ chuyển một milestone sang **done** khi bước kiểm chứng của nó **đã thật sự chạy và pass**.
- Thứ gì đã viết mà chưa chạy là **unverified**, và phải ghi rõ như vậy.
- Phát hiện không hiển nhiên ghi vào mục *Findings*, đừng chỉ để trong commit message.

---

## Milestones

| | Milestone | Nội dung | Trạng thái |
|---|---|---|---|
| **M0** | Problem spec, skeleton, stack definition | Viết PROBLEM.md, dựng khung repo, định nghĩa stack Docker | **done, verified** |
| **M1** | Sources, seed corpus, load generator | DB nguồn, bộ dữ liệu mẫu, công cụ sinh tải | **done, verified**: CDC chạy thật trên cả hai nguồn, masking đã chứng minh, load generator và "công tắc" stock-out hoạt động |
| **M2** | CDC ingestion + restart-safety demo | Chứng minh CDC không mất dữ liệu khi sập | **done, verified**: không có gap khi SIGKILL dưới tải; WAL guard của GR1 §4.4 được chạy thử, phát hiện **không có tác dụng**, sửa, đo lại |
| **M3** | Medallion on Iceberg | Spark ghi Bronze, dbt-trino ghi Silver/Gold | **done, verified**: Bronze exactly-once với *cả hai* nguồn replay; đo được KPI latency end-to-end; Silver khớp nguồn sống từng dòng sau các MERGE incremental dưới tải; Gold khớp các số tổng hợp từ nguồn sống |
| **M4** | Trino federation + embedding pipeline | Kênh chậm (vector index) + đăng ký dataset | **done, verified**: index chỉ chứa thực thể đủ điều kiện lúc refresh kèm ảnh chụp fact nhanh; **L0 tái hiện end-to-end**; 14 dataset đã đăng ký. Quyết định: [ADR 0006](adr/0006-m4-embedding-pipeline.md) |
| M5 | N1 Freshness Registry | | schema + `platform.datasets` đã có (M4); collector chưa bắt đầu |
| M6 | N2 Retrieval + fusion engine | | chưa bắt đầu |
| M7 | Experiments E1–E4 | | chưa bắt đầu |
| M8 | N3 governance, `jobmarket` domain, console | | chưa bắt đầu |

**Đọc nhanh:** phần "đường ống dữ liệu" (nguồn → CDC → Bronze → Silver → Gold) và **kênh chậm**
(vector index) đã xong và được chứng minh; lỗi L0 giờ **tái hiện được bằng một lệnh**. Phần "đóng góp
mới" (Freshness Registry, fusion, governance đầy đủ, domain thứ hai) mới có nền móng: spec domain,
`platform.datasets`, và chốt chặn N3 chưa có lan truyền.

---

## What is verified working (đã kiểm chứng bằng chạy thật)

Mọi dòng dưới đây được xác nhận bằng **một lần chạy thật**, không phải bằng cách đọc code.

**Stack**: `python mosaic.py up` cho 10/10 service healthy; `doctor` pass mọi check (2026-10-05),
gồm cả check mới `bronze stream`.

| Hạng mục | Bằng chứng | Diễn giải |
|---|---|---|
| MySQL CDC prerequisites | `binlog_format=ROW`, `binlog_row_image=FULL`, `gtid_mode=ON` | MySQL đã bật đủ điều kiện để Debezium đọc binlog đầy đủ cả dòng trước lẫn sau |
| PostgreSQL CDC prerequisites | `wal_level=logical`, publication `mosaic_pub`, pgvector đã cài | PostgreSQL giải mã WAL được; có publication; có extension vector |
| Domain schemas | MySQL 3 bảng; PostgreSQL 6 bảng | Đúng schema của GR1 Appendix A |
| Platform metadata DB | `mosaic_platform` với đủ 6 bảng `platform.*` | DB riêng cho metadata của nền tảng (N1, N3) |
| Nessie | API v2 ở cổng 19120; lưu `ROCKSDB` trên volume `nessie-data`, sống qua restart | Catalog Iceberg bền vững, restart không mất danh sách bảng |
| Trino catalogs | `iceberg` (Nessie), `mysql`, `postgresql`, `platform`, tất cả query được | Federation hoạt động |
| Abstraction gate | `check-abstraction` pass (M0: quét 6 file; phạm vi mở rộng ở M4, xem dòng bên dưới) | `platform/` không chứa từ vựng domain |
| Debezium connectors | cả hai `RUNNING`, có khai báo Kafka signal channel | Đang stream thay đổi |
| PII masking | `cdc-verify` pass trên cả hai nguồn, cả ảnh **before** lẫn **after** | Không có plaintext PII nào trên Kafka |
| Load generator | 5,401 ops trong 240 s, chênh lệch số dòng khớp **chính xác** trên cả 4 bảng (2026-10-05) | Công cụ sinh tải ghi đúng những gì nó báo |
| Restart safety (M2, chạy lại ở M3) | SIGKILL Connect 45 s dưới tải: **0 missing**, **183 replayed events** (khớp theo danh tính sự kiện), đuổi kịp 30.2 s sau restart | Không mất sự kiện nào; có trùng do replay, đúng như at-least-once |
| WAL guard (M2) | Mức tiến của flush trong 130 s nhàn rỗi: 0 byte (lỗi) → 688,776 byte (đã sửa) | Heartbeat giờ thực sự giữ WAL không phình |
| **Bronze stream (M3)** | `mosaic-spark` chạy `bronze_stream.py` làm tiến trình chính; healthy = query đang active + có progress trong < 60 s; 8 topic → `iceberg.bronze.cdc_events` | Luồng Bronze sống và có tiêu chí "khoẻ" rõ ràng |
| **Bronze bootstrap (M3)** | sau `resnapshot`, Bronze chứa đúng số dòng nguồn của mỗi bảng dưới dạng `op=r` (`n = distinct keys = distinct event_id`), 114,709 dòng, 0 `_src_ts` null | Nạp lại toàn bộ từ đầu và khớp tuyệt đối |
| **Bronze exactly-once, phía Connect (M3)** | với các insert nằm trong khoảng id bị khoanh lúc test restart, Kafka có 1,107 / 2,010 / 724 / 95 insert event cho 1,088 / 1,980 / 719 / 95 key; Bronze chứa **đúng một bản ghi mỗi key** | Kafka nhận trùng, Bronze khử trùng hoàn toàn |
| **Bronze exactly-once, phía Spark (M3)** | `docker kill` stream rơi đúng vào khe giữa lúc Iceberg commit batch 77 và lúc checkpoint commit; khi restart, batch 77 chạy lại: **673 vào, 0 ghi, 673 bị bỏ**; vẫn chỉ có 1 Iceberg snapshot cho batch đó | Trường hợp nguy hiểm nhất của streaming đã xảy ra thật, và không sinh trùng |
| **End-to-end latency (M3, GR1 Table 4.4)** | từ commit ở nguồn tới Iceberg Bronze snapshot, 120 s ở 20 ops/s: MySQL p50 **7.76 s** / p95 11.66 / p99 12.29 / max 13.21; PostgreSQL p50 **7.99 s** / p95 12.03 / p99 12.91 / max 13.51. Riêng capture (commit → Kafka) p50 0.26 s MySQL, 0.49 s PostgreSQL; phần còn lại là trigger 10 s | Debezium chưa tới 1 giây; phần lớn độ trễ là do Spark đợi trigger 10 s |
| **Silver (M3)** | 7 model sinh tự động, `MERGE` incremental; sau 4 lần build dưới tải đồng thời, `reconcile` cho **0 missing / 0 extra / 0 differing** ở cả 7 bảng so với nguồn sống (36,678 orders, 70,056 items, 29,827 payments, 6,479 users, 1,000 products, 48 categories, 1,000 inventory) | Silver là bản sao chính xác của nguồn |
| Silver negative control | cùng phép kiểm nhưng cố tình đặt sai múi giờ nguồn: mọi dòng MySQL đều khác, PostgreSQL 0 | Phép kiểm tra **có khả năng** báo lỗi, tức là nó "có răng" |
| **Gold (M3)** | `orders` (MERGE incremental, phân vùng theo GR1 §4.3), `users` (`bucket(user_id, 32)`), `product_sales`, `user_category_affinity`; 78/78 dbt node pass, gồm cả hai test tham chiếu chéo nguồn | Tầng Gold đầy đủ, đúng spec |
| Gold vs live sources | `gold.orders` so với số tổng hợp MySQL: 36,678 orders, 0 missing/extra/differing; `product_sales.stock_qty` so với tồn kho sống: 0/1,000 khác (lúc hệ đứng yên) | Gold đúng khi không có ghi mới |
| GR1 Listing 4.2 | câu query federation `iceberg.gold.orders` × `postgresql.public.users` chạy được, 100 dòng, 1.61 s | Ví dụ federation trong báo cáo GR1 chạy được thật |
| ANALYZE post-hook | `SHOW STATS` trên `gold.orders`: NDV `order_id` = 36,678 | Thống kê cho optimizer được cập nhật sau mỗi lần ghi |
| **Embedding index (M4)** | `embed refresh`: 946/1,000 sản phẩm còn hàng từ `gold.product_catalog` (pin Iceberg snapshot), MiniLM-L6-v2 @ `1110a243`, ~205 text/s CPU, tổng 8–9 s; publish nguyên tử (build bảng mới + HNSW, swap trong một transaction); `refreshed_at` = `max(_src_ts)` của snapshot đã đọc | Kênh chậm tồn tại, và độ cũ của nó là độ cũ *thật* của dữ liệu nó đọc |
| **M4 nghiệm thu** | `embed-verify --with-refresh` 7/7 PASS (2026-10-16): counts khớp ở đúng snapshot; 3 query ngữ nghĩa đều 10/10 đúng category gốc; recall@10 = 1.0 với plan đã kiểm; guard từ chối khi gắn tag; 0 drift trước công tắc | Mọi kiểm tra đều có khả năng fail |
| **L0 end-to-end (M4)** | stockout kết quả #1 của "waterproof hiking backpack" (sản phẩm 342, sống 160 → 0): index **vẫn trả nó ở #1 với `stock_qty=160`**, stale-hit rate@10 = 1/10. Đưa qua Bronze (4 s) → dbt → refresh: biến mất, 0/10 | Lỗi cốt lõi của đề tài giờ là sự kiện quan sát được, không còn là lời kể |
| **Dataset registry (M4)** | `register`: 14 dataset (1 Bronze, 7 Silver, 5 Gold, 1 vector) trong `platform.datasets`, mỗi cái được probe; negative control (bảng không tồn tại + cột sai) → không đăng ký gì, exit 1 | Điểm bàn giao cho M5 |
| **Abstraction gate (M4)** | quét `platform/` **và** `pipelines/embeddings/` (25 file), miễn trừ đúng `ORDER BY`; negative control `"orders"` vẫn bị bắt | Runner embedding domain-free được máy kiểm |
| Trino memory bound | `-Xmx3G` (`docker/trino/jvm.config`) + `mem_limit: 6g` + `MALLOC_ARENA_MAX=2`, 2026-10-16. Cùng workload, trước → sau khi giới hạn: đỉnh `dbt build` incremental 6.20 → **2.59 GiB**, ghi 2,944 phân vùng 7.04 → **3.90 GiB**, đỉnh kernel 7.39 → **3.93 GiB**; 0 lần OOM; 78/78 pass. Riêng `MALLOC_ARENA_MAX` giảm off-heap 1.47 → 0.87 GiB | Trino trước đó "ăn" hết RAM của Docker VM; nay đã bị chặn trần mà không hỏng gì |

## Written but NOT yet executed (đã viết nhưng CHƯA chạy)

- **Nhánh withheld-commit** của `bronze-verify replay`: nhánh code cố tình dời file `commits/N` đi
  để ép Spark chạy lại một batch. Cả hai lần chạy test đều tự rơi vào các trạng thái mà Spark tự xử
  lý được (một lần giữa batch, một lần đúng vào khe commit đã ghi / checkpoint chưa ghi), nên nhánh
  này **chưa bao giờ được kích hoạt**.
- **Iceberg maintenance** (`optimize`, `expire_snapshots`, `remove_orphan_files`) **chưa tồn tại**.
  Mỗi MERGE của Silver thêm position-delete files (đã có 5 file trên `silver.orders` sau 4 lần build),
  mỗi micro-batch Bronze thêm một snapshot, và chưa có gì gộp chúng lại. GR1 giao việc này cho
  Airflow, nhưng profile `orch` (Airflow) chưa từng được bật.
- **dbt chưa có lịch chạy**: Silver/Gold chỉ cập nhật khi có người chạy `python mosaic.py dbt`. Vì
  vậy độ cũ của Gold hiện **do người vận hành quyết định**, cho tới khi có orchestration.
- Các profile `gov`, `orch`, `console` **chưa từng được bật**. File `docker/api.Dockerfile` chưa tồn
  tại, nên `console` sẽ build lỗi cho tới M6/M8. (Image `docker/jobs/` của M4 có sẵn mọi dependency mà
  API của M6 cần; cân nhắc dùng lại.)
- **Index chưa có lịch refresh**, có chủ đích (ADR 0006, M4-C1): chu kỳ refresh là biến thí nghiệm.
- **Planner ở quy mô `full`** (100k vector) có tự chọn HNSW không: chưa kiểm. Đường đọc đã pin access
  path nên kết quả không phụ thuộc vào câu trả lời, nhưng A13 nói "chưa kiểm".

## Current runtime state (trạng thái đang chạy)

Stack đang **up**, profile mặc định, 10/10 healthy, Bronze stream active với backlog 0; `doctor` pass
mọi check, gồm dòng mới `embedding index`. Silver và Gold được build lần cuối (toàn bộ, 84/84) lúc
2026-10-16 16:47 UTC. Index: 946 dòng, `refreshed_at` 2026-10-16 16:46:34 UTC. Không có gì tự động
refresh cả ba. `platform.datasets` có 14 dòng; `platform.watermarks` vẫn rỗng (M5).

Lần stockout/restock của `embed-verify` (sản phẩm 342) là thay đổi nguồn duy nhất kể từ 2026-10-05; sản
phẩm đó đã được trả về tồn kho 160, và Gold + index đã được build lại sau đó nên cả ba nhất quán.

Số dòng hiện tại (seed corpus + phần tăng thêm do load generator): orders 36,678, order_items 70,056,
payments 29,827, users 6,479, products 1,000, categories 48, inventory 1,000.

Checkpoint của Bronze đã được **cất sang chỗ khác, không xoá**, ngày 2026-10-05:
`spark-state:/opt/mosaic/state/checkpoints/bronze.stale-2026-09-23`. Lý do: offset trong checkpoint
trỏ vào dữ liệu đã bị Kafka retention xoá, và bảng tương ứng thì rỗng. Có thể xoá khi không còn ai
cần. Ngày 2026-10-16 thêm `checkpoints/bronze.torn-2026-10-16/`: bốn file offset/commit **0 byte** của
batch 1890–1891, bị rách khi Docker Desktop dừng cứng (finding 19).

`mosaic_pub` publish 5 bảng: 4 bảng domain + `cdc_heartbeat` (finding 10).

---

## Findings worth carrying into the report (phát hiện nên đưa vào báo cáo GR2)

> Bản đầy đủ, có bằng chứng và gợi ý cách trình bày: [CHANGES-FROM-GR1.md](CHANGES-FROM-GR1.md).
> Mã trong ngoặc (A6, A7…) là mã mục trong file đó.

1. **Chuỗi SMT masking ở GR1 Appendix C không thể chạy như đã viết.** Muốn mask được thì phải làm
   phẳng envelope, mà làm phẳng thì phá đúng cái envelope Bronze cần giữ. Xem
   [ADR 0004](adr/0004-connector-native-masking.md).
2. **Hai image tag trong GR1 Appendix B không tồn tại** (`debezium/connect:2.7`, và
   `projectnessie/nessie:0.90.4` trên Docker Hub).
3. **MySQL báo thiếu quyền `FILE` bằng lỗi `ERROR 1045 Access denied`**, trông y hệt sai mật khẩu.
4. **Avro converter trong GR1 Appendix B không load được**: image không có class đó, worker chết
   ngay khi khởi động ([ADR 0005](adr/0005-json-converter.md)).
5. **`docker compose ps` giấu các container đã thoát**, từng dẫn tới một kết luận sai là "all
   healthy".
6. **PostgreSQL không phát before image với replica identity mặc định** (A6): mọi UPDATE đều có
   `before: null`.
7. **Debezium cắt ngắn hash mask theo độ rộng cột nguồn** (A7): cột `VARCHAR(32)` chỉ nhận 32 ký tự
   hex.
8. **Load generator báo "tốc độ gửi" là đang nói sai**: tốc độ commit thực tế chỉ khoảng 100 ops/s.
9. **"Restart safety" thực chất là at-least-once kèm một cửa sổ replay** (A8). *Đính chính ở M3:*
   con số "110 duplicates" của M2 đếm số sự kiện trên mỗi key, nên lẫn cả các UPDATE hợp lệ; đếm
   đúng theo danh tính sự kiện thì một lần chạy tương đương có 183 replay. Kết luận không đổi.
10. **WAL-bloat guard ở GR1 §4.4 không có tác dụng với cấu hình như viết** (A9): heartbeat ghi vào
    bảng không nằm trong publication.
11. **Kafka retention đã xoá lịch sử CDC trước khi Bronze kịp nạp, hai lần** (A10). Ngày
    2026-09-12: topic `categories` rỗng. Ngày 2026-10-05, sau 10 ngày tắt máy: *mọi* topic bảng đều
    rỗng, và checkpoint Bronze trỏ vào offset đã bị xoá (`OffsetOutOfRange`). Cả hai lần đều khôi
    phục bằng BLOCKING ad-hoc snapshot, và Bronze nạp lại chính xác.
12. **Trino không ghi được thuộc tính tuỳ biến vào Iceberg snapshot summary.** Spark thì ghi được
    (Bronze đóng dấu `max(_src_ts)` và `ingest_ts_us` vào mỗi commit). Vì vậy watermark của
    Silver/Gold phải đọc **từ dữ liệu**. Thêm nữa: **bảng `$files` của Trino 451 không liệt kê
    delete file**; position delete của MERGE chỉ thấy được trong snapshot summary
    (`total-position-deletes`). N1 phải dùng summary.
13. **Bronze đạt exactly-once trên một nguồn chỉ at-least-once** (kết quả tích cực, M3). Một
    idempotency key duy nhất, là SHA-256 trên op, record key, vị trí nguồn, `source.ts_ms` và cả hai
    image, hấp thụ được *cả* replay của Connect *lẫn* việc Spark chạy lại batch. Hai phương án thay
    thế đều **không** dùng được: hash cả envelope thất bại vì `ts_ms` của envelope là thời điểm xử
    lý, mỗi lần replay một khác; hash cả khối `source` thất bại vì trường `sequence` của PostgreSQL
    được khởi tạo lại từ offset đã lưu sau mỗi restart.
14. **MySQL `DATETIME` đến lệch 7 tiếng** (A11), vì giờ địa phương bị mã hoá như thể là UTC.
    `TimestampConverter` trong GR1 hoá ra là thứ **thiết yếu** (*load-bearing*). Vì các SMT của A1 đã
    bị bỏ, giờ Silver làm việc chuyển đổi. Negative control chứng minh điều này.
15. **Cách phân vùng `gold.orders` ở GR1 §4.3 được thiết kế cho một quy mô khác** (A12): 2,944
    phân vùng, trung bình 12.6 dòng mỗi phân vùng; phải nâng giới hạn writer của Trino; `ANALYZE` chậm
    33 lần và full scan chậm 14 lần so với bảng không phân vùng. Tạm giữ như spec; **quyết định còn
    để ngỏ** (xem Open questions). Đo ngày 2026-10-13: một lần build incremental thay đổi **0 dòng**
    vẫn tốn 45–61 s cho `gold.orders` (một câu lệnh quét 27,142 split cho 95 MiB dữ liệu), và chính
    nó đẩy heap Trino lên 6+ GiB trước khi heap bị giới hạn.
16. **Seed và load generator dùng hai bộ từ vựng vùng miền khác nhau** (D15): một bên dùng tên thành
    phố, bên kia dùng `north`/`central`/…, nên các số tổng hợp theo vùng bị tách đôi. Đã sửa trong
    generator; các dòng cũ giữ năm giá trị vùng lớn.
17. **Silver phải sắp thứ tự theo vị trí trong log nguồn, không theo thời gian** (thiết kế M3). Một
    dòng đọc từ snapshot mang **thời điểm đọc**, thời điểm này có thể muộn hơn commit của một UPDATE
    nằm *sau* nó trong log. Nếu sắp theo thời gian, bản UPDATE mới hơn có thể bị bản snapshot cũ đè
    lên. Đã kiểm tra cho từng nguồn: `source.pos` của MySQL là riêng cho từng binlog event (orders
    và items trong cùng một GTID có vị trí khác nhau), `lsn` của PostgreSQL là riêng cho từng thay
    đổi.
18. **Planner bỏ qua index HNSW ở quy mô prototype** (A13, M4). Ở 946 vector, sequential scan + sort có
    cost 230, còn HNSW có cost 417. Query "ANN" không pin thì thực chất là exact, nên thí nghiệm HNSW vs
    exact của GR1 §4.5 sẽ cho recall 1.0 và đường cong `ef_search` phẳng, **vô nghĩa**. Đã pin access
    path cho từng đường đi, và mỗi lần đo đều `EXPLAIN` để kiểm.
19. **Checkpoint Spark bị rách khi Docker Desktop dừng cứng** (D17, M4). Bốn file offset/commit 0 byte;
    stream chết với `Incomplete log file`. Cất chúng đi và chạy lại từ cặp nguyên vẹn gần nhất. Idempotency
    key chặn đúng 2 dòng đã ghi: **exactly-once được chứng minh thêm một lần, lần này từ sự cố thật**.
20. **Bài toán nguồn nhàn rỗi** (M4, việc của M5). Bronze trễ 20 s nhờ heartbeat CDC, trong khi mọi bảng
    Silver/Gold "trễ" 11 ngày chỉ vì nguồn không có thay đổi. Index build hôm nay vì thế "cũ 11.5 ngày lúc
    ra đời". Watermark event-time **không phân biệt được "đứng yên" với "kẹt"**; collector của N1 phải kết
    hợp heartbeat (A9) để biết connector đã đọc tới "bây giờ".
21. **Kênh chậm phải "sai", không chỉ "già"** (M4, thiết kế, ADR 0006 M4-A1). Embedding chỉ mã hoá mô tả, mà
    mô tả không bao giờ đổi (C9). Index cũ chỉ *sai* nếu nó chụp lại tập thực thể đủ điều kiện cùng các fact
    nhanh lúc refresh. Đây là điều kiện để E4 đo được một hiệu ứng có thật.

## Open questions (câu hỏi còn mở)

- **Partition spec của `gold.orders` (A12).** Spec `day x bucket(region, 16)` của GR1 cho ra file
  khoảng 13 dòng ở quy mô này. Giữ nguyên để trung thành với spec, hay chuyển sang phân vùng theo
  tháng? Nên chốt trong thí nghiệm E2 ở quy mô `full`. **Đây là quyết định của bạn.**
- **Kafka retention (A10).** Giữ mặc định 7 ngày (vì Bronze mới là system of record), hay đặt
  `retention.ms=-1` cho các topic CDC để một lần tắt máy lâu không thể xoá lịch sử chưa được tiêu
  thụ?
- **Quy mô.** Mọi thứ vẫn ở mức `small` (~36k orders). GR1 §10.1 muốn 10M+ dòng cho E2. Ước tính
  nạp `full` mất khoảng 40 phút; giờ hình dạng pipeline đã cố định nên đáng làm trước M7.
- **Phân vùng Silver (D12).** Silver hiện là các bảng trạng thái hiện tại, không phân vùng; chỉ xem
  lại nếu có consumer cần pruning.
- **Nguồn nhàn rỗi (finding 20).** Khi nguồn đứng yên, N1 nên báo watermark là gì: `max(_src_ts)` thuần
  tuý (trung thực về dữ liệu, nhưng báo "cũ" cho một hệ khoẻ), hay nâng lên tới mốc heartbeat gần nhất
  mà connector đã xác nhận (đúng hơn về "đã biết tới đâu")? Cần chốt ở đầu M5.

---

## Tooling added for future sessions (công cụ đã có)

- `python mosaic.py doctor`: báo cáo trạng thái một phát, giờ có cả Bronze stream.
- `python mosaic.py check-ports` / `check-abstraction` / `cdc-verify` / `loadgen` / `stockout` /
  `restock` / `restart-safety` / `wal-guard`: xem CLAUDE.md.
- `python mosaic.py resnapshot`: chạy Debezium BLOCKING snapshot cho mọi bảng được capture qua
  Kafka signal channel, rồi kiểm số lượng `op=r` với số dòng ở nguồn.
- `python mosaic.py bronze`: Bronze stream còn sống không, backlog, tổng số, batch gần nhất.
- `python mosaic.py bronze-verify exactly-once`: lấy lần chạy `restart-safety` gần nhất và kiểm
  tới tận Bronze: mỗi key trong khoảng bị khoanh có đúng một insert event.
- `python mosaic.py bronze-verify replay`: SIGKILL Bronze stream dưới tải, ép batch đang chạy dở
  phải chạy lại, chứng minh không trùng và không mất.
- `python mosaic.py bronze-verify latency`: KPI end-to-end của GR1 Table 4.4, chính xác tới từng
  dòng (`_ingest_ts` nối mỗi dòng với snapshot đã publish nó).
- `python mosaic.py dbt [-- <args>]`: build Silver/Gold. Image chạy `assemble.py` trước để sinh
  Silver từ cấu hình connector và chép `domains/*/gold/` vào.
- `python -m tools.lakehouse.reconcile`: so Silver với nguồn sống từng dòng. Chạy khi hệ đứng yên.
- `tools/trino.py`: Trino REST client chỉ dùng thư viện chuẩn; có `query()`, `scalar()`.
- `python mosaic.py embed refresh|status|query|recall [-- args]` (M4): index của kênh chậm, chạy trong
  image `jobs`. Ví dụ: `embed query -- --text "tent" --k 5 [--exact]`.
- `python mosaic.py register [--dry-run]` (M4): probe + đăng ký dataset vào `platform.datasets`.
- `python mosaic.py embed-verify [--with-refresh]` (M4): sáu kiểm tra nghiệm thu, kết thúc bằng tái hiện
  L0; `--with-refresh` cho thấy stale hit biến mất sau Gold + refresh. Tự restock sau khi chạy.
- Các ledger trong `experiments/results/`: `restart_events`, `snapshot_events`, `bronze_events`,
  `lakehouse_events`, `stockout_events`, `embedding_events` (`.jsonl`, append-only, **cố ý giữ cả lần thất bại**;
  dòng 2 của `lakehouse_events` là lần chạy negative control sai múi giờ có chủ đích).
- `docs/BUILD-LOG.md`, `docs/ADVISOR-BRIEFING.md`, `docs/CHANGES-FROM-GR1.md`, `CLAUDE.md`.

## Immediate next steps (việc tiếp theo)

M4 đã đóng (2026-10-16). Tiếp theo là M5, N1 Freshness Registry:

1. **Chốt câu hỏi nguồn nhàn rỗi trước** (open question, finding 20): watermark khi nguồn đứng yên là
   `max(_src_ts)` hay mốc heartbeat đã xác nhận.
2. **Ba collector** ghi `platform.watermarks` + `watermark_history` cho mọi dòng trong `platform.datasets`:
   - CDC: offset Connect + `source.ts_ms`, cộng heartbeat;
   - Table: Bronze đọc từ snapshot summary (Spark ghi `max(_src_ts)`), Silver/Gold đọc **từ dữ liệu**
     (Trino không ghi được summary, finding 12);
   - Vector: `max(refreshed_at)` của index (đồng nhất theo thiết kế, ADR 0006 M4-C2).
   `register` đã có hàm probe dùng lại được (`platform/freshness/register.py`).
3. M6 sẽ mở rộng `retrieval.yaml` với `channels` + `fusion`. Nhớ cam kết **baseline post-filter** cho E4
   (ADR 0006, M4-A2).
4. Cân nhắc các open question ở trên trước E2; nâng quy mô lên `full` trước M7, sau đó kiểm planner có tự
   chọn HNSW ở 100k vector không (A13).

### Caveats to carry forward (lưu ý mang theo)

- Bản sửa publication (finding 10) và `REPLICA IDENTITY FULL` được áp **bằng tay lên database đang
  chạy**. Phần sửa trong file DDL chỉ có hiệu lực với volume mới tạo.
- Kafka đang chứa một lần re-snapshot (2026-10-05) chồng lên lịch sử streaming. Silver sắp thứ tự
  theo vị trí nguồn nên nhiều `op=r` cho cùng một key là vô hại; **đừng "sửa" chúng**.
- Khi `loadgen` đang chạy, phần sản phẩm hết hàng có sẵn trong seed sẽ được nhập lại trong vài
  phút. **Muốn tạo lỗi theo yêu cầu thì dùng `stockout`.**
- Gold chỉ tươi bằng lần chạy `dbt` gần nhất. Trong bài test tải M3, nguồn sống cho thấy sản phẩm 68
  có tồn kho 0, trong khi Silver/Gold vẫn ghi 9. Đó chính là **sự bất đối xứng L0, quan sát được
  ngay trong stack này**.
- **Refresh index không làm nó tươi hơn Gold** (M4-A3): `refreshed_at` là watermark của Gold đã đọc. Muốn
  index tươi thì `python mosaic.py dbt` trước, rồi `embed refresh`.
- `product_embeddings` được **tạo lại** ở mỗi refresh (DROP + RENAME). Đừng thêm FK, grant hay index tay
  lên nó: mất sau lần refresh kế tiếp. Muốn đổi định nghĩa thì sửa `retrieval.yaml`.
- Trên volume PostgreSQL **mới**, `product_embeddings` không tồn tại cho tới lần `embed refresh` đầu tiên
  (DDL không còn tạo nó); `doctor` báo `--` chứ không FAIL.
- Truy vấn ANN phải **pin access path** (A13). Đừng bỏ `SET LOCAL enable_seqscan = off` trong
  `pipelines/embeddings/ann.py`: ở quy mô nhỏ planner sẽ lặng lẽ chạy exact.
