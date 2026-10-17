# MOSAIC — Problem Statement and Abstraction Ladder

> *Bảng thuật ngữ và thứ tự đọc các tài liệu nằm ở [Phụ lục](#phụ-lục-a--thứ-tự-đọc-tài-liệu) cuối file.*

> **Vì sao có file này.** Đây là "chương một còn thiếu" của bản thiết kế GR1. `GR1_report.md` mô
> tả **kiến trúc** rất kỹ, nhưng chưa bao giờ nói rõ **một vấn đề cụ thể** mà nền tảng tồn tại để
> giải quyết. Mọi thứ được xây trong repo này đều treo vào vấn đề nêu ở đây.

Cấu trúc tài liệu là một **cái thang 4 bậc** (*four-rung ladder*). Mỗi bậc có một sản phẩm viết ra
(*artifact*) và một **phép thử máy móc** (*mechanical test*), tức là chạy lệnh ra PASS/FAIL chứ
không dựa vào lời khẳng định. Ta bắt đầu từ **một câu query bị sai trong một domain**, rồi leo dần
lên cho tới khi phần lõi của nền tảng không còn biết mình đang chạy cho domain nào.

| Bậc | Câu hỏi bậc này trả lời | Artifact | Phép thử |
|---|---|---|---|
| **L0** | Hôm nay, cụ thể cái gì đang hỏng? | chính mục này | lỗi **tái hiện được theo yêu cầu** |
| **L1** | Mô hình hoá trường hợp hỏng đó thế nào? | `domains/ecommerce/` | lỗi **đo được** bằng số |
| **L2** | Phần nào trong đó *không* liên quan tới thương mại? | `platform/` | lỗi **được sửa** |
| **L3** | Sự trừu tượng là thật hay chỉ là lời nói? | `domains/jobmarket/` | domain thứ hai chạy được, **không sửa code** |

---

## L0 — One concrete failure (một lỗi cụ thể)

Câu query:

> **"Show the 10 products most relevant to shopper X, right now."**
> *("Hiện 10 sản phẩm phù hợp nhất với khách hàng X, ngay lúc này.")*

Để trả lời tốt, cần **ba loại thông tin** nằm ở **ba hệ thống khác nhau**, với **ba mức độ tươi**
chênh nhau rất xa:

| Thông tin cần | Nằm ở đâu | Độ cũ điển hình | Vì sao cũ như vậy |
|---|---|---|---|
| Mức liên quan **ngữ nghĩa** với câu hỏi | `product_embeddings` (pgvector, vector 384 chiều) | **hàng giờ** | bị giới hạn bởi lịch chạy lại embedding |
| Sở thích mua hàng, xếp hạng doanh thu, độ gần đây | `iceberg.gold.*` | **vài phút** | bị giới hạn bởi độ trễ CDC + chu kỳ batch |
| Sản phẩm **có mua được không**? (tồn kho, giá, còn bán không) | MySQL / PostgreSQL, dữ liệu sống | **vài giây** | đây chính là nguồn sự thật |

Cách làm thông thường: chạy bảng xếp hạng SQL, chạy bảng xếp hạng vector, rồi gộp hai cái bằng
**Reciprocal Rank Fusion (RRF)**. RRF là *baseline* chuẩn cho đúng dạng bài toán này (Cormack,
Clarke & Buettcher, SIGIR 2009).

### The defect (khiếm khuyết)

**RRF coi mọi bảng xếp hạng đầu vào là tươi như nhau.** Điểm RRF chỉ phụ thuộc vào **vị trí** của
item trong từng bảng xếp hạng:

```
RRF(d) = SUM_i  1 / (k + rank_i(d))
```

*(d là một item, ví dụ một sản phẩm; rank_i(d) là thứ hạng của d trong kênh i; k là hằng số, thường
là 60. Item đứng #1 ở một kênh được cộng 1/61, đứng #2 được cộng 1/62, v.v.)*

Trong công thức này **không có số hạng nào** nói về việc bảng xếp hạng được tính *lúc nào*, hay dữ
liệu đằng sau nó *cũ bao nhiêu*. Từ đó ra lỗi cụ thể:

> **Một sản phẩm đã hết hàng từ năm phút trước vẫn đứng #1.**

Kênh ngữ nghĩa (*semantic channel*) đặt nó ở đó. Kênh ngữ nghĩa thì đã cũ sáu tiếng, nên không biết
sản phẩm đã hết. Và điểm mấu chốt là: **hàm fusion không có cách nào phạt kênh đó vì nó không
biết.** Trong khi đó, kênh giao dịch (*transactional channel*), tươi tới từng giây, đã phản ánh
việc hết hàng, nhưng lại bị một "ý kiến cũ" có trọng số ngang bằng lấn át.

Lỗi này không chỉ xảy ra với hết hàng. Giá vừa đổi, tin đăng vừa bị gỡ, người bán vừa bị khoá cũng
gặp đúng chuyện đó. Nói chung, **mọi thông tin thay đổi nhanh hơn kênh chậm nhất** trong fusion đều
có thể sinh lỗi này.

### Why this is not simply "refresh the embeddings faster"

*(Vì sao không chỉ đơn giản là "chạy lại embedding thường xuyên hơn")*

Embedding lại cả catalogue rất tốn, và càng tốn khi catalogue càng lớn. Khoảng thời gian giữa hai
lần refresh là một **quyết định ngân sách**, không phải sơ suất. Sự bất đối xứng về độ tươi là
**mang tính cấu trúc** (*structural*), không phải do cấu hình sai. Mọi hệ thống thật nào kết hợp
một semantic index tính theo batch với một transactional store dạng streaming đều mắc phải nó.

Vì vậy câu hỏi không phải là *làm sao xoá bỏ* sự bất đối xứng, mà là **làm sao để hàm fusion *biết*
về nó**.

### How the failure is measured (đo lỗi thế nào)

Có hai metric, cả hai đều so với một **oracle**. Oracle là bảng xếp hạng lý tưởng tại thời điểm
query *t*, tính **trực tiếp trên database OLTP sống** với cùng logic chấm điểm. Oracle tươi tuyệt
đối theo định nghĩa, nên dùng được làm đáp án chuẩn (*ground truth*).

- **stale-hit rate@10** *(metric chính)*: tỉ lệ item trả về trong top-10 mà **vị trí của nó dựa trên
  dữ liệu mà nguồn sống đã phủ nhận**, ví dụ được xếp cao nhưng thực ra đã hết hàng. Đây là metric
  gọi đúng tên con bug.
- **nDCG@10 vs oracle** *(metric phụ)*: chống lại các "bản sửa suy biến" (*degenerate fix*). Một
  policy chặn hết mọi thứ sẽ có stale-hit rate hoàn hảo bằng 0 nhưng nDCG rất tệ. Kết quả chỉ có ý
  nghĩa khi **cả hai metric cùng tiến đúng hướng**.

---

## L1 — Model the concrete case (mô hình hoá trường hợp cụ thể)

Hình thức hoá bài toán e-commerce để lỗi không còn là một giai thoại mà thành thứ đo được:

- **Entities and sources** (thực thể và nguồn): giữ nguyên GR1 Appendix A.
  - **MySQL 8.0** giữ dữ liệu giao dịch (`orders`, `order_items`, `payments`), capture qua **binlog**.
  - **PostgreSQL 15** giữ master data (`users`, `products`, `categories`, `inventory`), capture qua
    **WAL**, *và* đồng thời làm vector store nhờ **pgvector**.
- **Two ranking channels** (hai kênh xếp hạng): một **SQL channel** chạy trên `iceberg.gold.*`, và
  một **ANN channel** chạy trên `product_embeddings`.
- **Two freshness signals** (hai tín hiệu độ tươi): `last_cdc(d)` (lần cuối dữ liệu nguồn của d đi
  qua CDC) và `last_refresh(d)` (lần cuối embedding của d được tính lại), theo **GR1 Eq. 5.1**.
- **The fusion function**: **GR1 Eq. 5.2**, trong đó hàm suy giảm (*decay*) `g(.)` được để dạng
  **pluggable** (cắm được nhiều dạng), vì tìm ra dạng đúng của nó **chính là** phần việc thí nghiệm.

**Deliverable của L1**: một pipeline khiến câu query **trả lời được** *và* khiến lỗi **tái hiện
được theo yêu cầu**. Cụ thể là có một "công tắc" (lệnh `python mosaic.py stockout`) làm hết hàng
sản phẩm đang đứng #1 để chạy lại query và quan sát.

---

## L2 — Abstract it (trừu tượng hoá)

Không có gì ở trên thực sự gắn với thương mại. Bỏ hết từ vựng nghiệp vụ đi thì còn lại một **bất
biến** (*invariant*):

> Một **retrievable entity** (thực thể truy hồi được) được xếp hạng bởi *N* **channels**. Mỗi channel
> được tạo ra (*materialised*) bởi một pipeline nào đó, và mỗi pipeline có một **watermark** đo
> được. Vì vậy, fusion đúng phải là hàm của **cả điểm số lẫn watermark**, không chỉ điểm số.

Từ câu này rút ra **ba artifact trung lập với domain** (*domain-neutral*). Ba artifact này chính là
nền tảng.

### 1. Freshness Registry — `platform/freshness/` (N1)

Một **mặt phẳng watermark** (*watermark plane*) duy nhất, query được, phủ mọi dataset trong hệ
thống (bảng Iceberg, vector index, nguồn sống), ở cả mức dataset lẫn mức từng entity.

Các mảnh ghép **đã tồn tại sẵn nhưng chưa bao giờ được nối lại**: Iceberg có snapshots, Debezium có
offsets, catalog có lineage. Nhưng không cái nào trả lời được câu *"dữ liệu đằng sau **câu trả lời
cụ thể này** cũ bao nhiêu?"*. Không có câu trả lời đó thì **không thể tính được GR1 Eq. 5.1**. Đó là
lý do Registry được xây **trước** fusion engine chứ không phải sau.

### 2. Declarative retrieval spec — `platform/spec/`, `domains/*/retrieval.yaml`

Mỗi domain khai báo trong YAML: entity nào được xếp bởi những channel nào. Với mỗi channel ghi
engine, query template, registry key (khoá tra watermark trong Registry), freshness SLA, cộng với
fusion policy. **Bộ chạy (runner) không biết gì về domain**; chỉ file YAML mang tri thức nghiệp vụ.

### 3. Tag-driven governance — `platform/governance/` (N3)

Chính sách PII được gắn vào **tag** trong catalog thay vì **tên cột**, và tag lan truyền theo đường
source → Bronze → Silver → Gold → embedding. **Một cột đã gắn tag nhạy cảm thì không được dùng làm
đầu vào cho embedding.**

GR1 §1.1 nêu chuyện **PII rò rỉ qua embedding** là một khoảng trống làm động lực cho đề tài, nhưng
hai lớp masking ở GR1 §4.6 đều dừng ở ranh giới dữ liệu quan hệ (bảng). Bậc L2 lấp nốt khoảng trống
mà chính nền tảng còn để hở.

---

## L3 — Prove the abstraction rather than assert it

*(Chứng minh sự trừu tượng thay vì chỉ khẳng định)*

Một domain thứ hai không liên quan gì là `jobmarket` (tin tuyển dụng và ứng viên), chạy trên
**đúng những binary đó**. Thêm domain này chỉ cần thêm DDL nguồn và một file `retrieval.yaml`,
không gì khác.

Nó có **cùng hình dạng** nhưng **không dùng chung từ nào**: mô tả công việc là kênh ngữ nghĩa chậm;
"còn tuyển không", số lượng tuyển, điều chỉnh lương là các thông tin giao dịch nhanh. Lỗi cũng là
cùng một lỗi và dễ hình dung y như vậy: **gợi ý một vị trí đã tuyển xong từ một giờ trước**.

Hai phép thử máy móc quyết định sự trừu tượng có thật hay không:

```bash
# 1. Lõi chung không được chứa từ vựng domain nào.
python mosaic.py check-abstraction

# 2. Domain thứ hai chạy được trên cùng stack đang chạy.
python mosaic.py test-domain --domain jobmarket
```

Nếu cả hai đều pass, câu trả lời cho câu hỏi của giảng viên *"đây là một platform hay một
tool?"* không còn là lời khẳng định về **ý định** nữa, mà trở thành một **quan sát về hành vi**.
**Đó là toàn bộ mục đích của cái thang.**

---

## What this buys, restated plainly (tóm lại, cái thang mang lại gì)

GR1 đã chỉ ra ba khoảng trống (*gaps*). Cái thang khớp với chúng như sau:

| Khoảng trống trong GR1 | Bậc giải quyết |
|---|---|
| (i) chưa có kiến trúc tham chiếu sẵn sàng cho benchmark với **multi-source CDC** | L1: pipeline, cộng các thí nghiệm E1–E3 |
| (ii) chưa có **governance thống nhất** cho cả dữ liệu có cấu trúc **lẫn** dữ liệu vector | L2: tag lan truyền tới tận embedding |
| (iii) chưa có **hybrid top-*k* fusion có tính tới độ tươi** | L0/L1: lỗi và cách đo nó; L2: cách sửa |

Ngoài ra cái thang còn thêm một điều mà báo cáo GR1 không hề claim: **bằng chứng rằng kết quả là một
platform**, dưới dạng một domain thứ hai chạy trên nó mà không phải sửa gì.

---
---

## Phụ lục A — Thứ tự đọc tài liệu

| # | File | Đọc để biết |
|---|---|---|
| 1 | **PROBLEM.md** (file này) | Dự án giải quyết vấn đề gì; cái thang L0→L3 |
| 2 | Phụ lục B, C bên dưới | Bức tranh kiến trúc + thuật ngữ |
| 3 | [STATUS.md](STATUS.md) | Dự án đang ở đâu: milestone nào xong, cái gì đã kiểm chứng |
| 4 | [BUILD-LOG.md](BUILD-LOG.md) | Nhật ký xây dựng từng milestone: đầu việc, cách làm, output, số đo |
| 5 | [CHANGES-FROM-GR1.md](CHANGES-FROM-GR1.md) | Bản implement khác thiết kế GR1 ở đâu và vì sao |
| 6 | [adr/](adr/) | 5 quyết định kiến trúc (Ranger, DataHub, Spark-Silver, SMT, Avro) |
| 7 | [ADVISOR-BRIEFING.md](ADVISOR-BRIEFING.md) | Kịch bản buổi gặp giảng viên hướng dẫn |

---

## Phụ lục B — Bức tranh kiến trúc: dữ liệu chảy thế nào

```
 NGUỒN (OLTP, dữ liệu sống)                CAPTURE                      LAKEHOUSE (Iceberg trên MinIO, catalog Nessie)
 ─────────────────────────                 ───────                      ─────────────────────────────────────────────
 MySQL 8.0                                                               Bronze           Silver              Gold
   orders, order_items, payments ─ binlog ─┐                             (Spark           (dbt-trino)         (dbt-trino)
                                           ├─► Debezium ─► Kafka ──────► Structured  ─►   7 bảng "trạng   ─►  4 bảng tổng hợp:
 PostgreSQL 15                             │   (Kafka      topics        Streaming)       thái hiện tại"      orders, users,
   users, products, categories, ─ WAL ─────┘    Connect)                 cdc_events       (dedup, ép kiểu,    product_sales,
   inventory  (+ pgvector)                                               (envelope        latest-wins)        user_category_
                                                                         nguyên vẹn)                          affinity

 Trino = tầng federation: một câu SQL join được  iceberg.* + mysql.* + postgresql.* + platform.*

 [M4] Embedding pipeline: text sản phẩm ─► vector 384 chiều ─► product_embeddings (pgvector)   ◄── kênh "chậm"
 [M5] N1 Freshness Registry: watermark của mọi dataset (các bảng platform.*)
 [M6] N2 Retrieval + fusion engine: kênh SQL + kênh ANN ─► fuse có tính tới freshness
 [M8] N3 Governance: tag PII lan theo lineage; jobmarket domain; console demo
```

**Đã chạy và kiểm chứng (tới M4):** mọi thứ từ nguồn tới Gold, cộng kênh chậm (vector index). **L0
tái hiện được bằng một lệnh** (`python mosaic.py embed-verify`), xem
[ADR 0006](adr/0006-m4-embedding-pipeline.md) về việc kênh chậm "sai" thế nào chứ không chỉ "già".
**Chưa làm (M5–M8):** Freshness Registry, fusion engine, thí nghiệm E1–E4, governance đầy đủ,
domain thứ hai, console.

---

## Phụ lục C — Bảng thuật ngữ (Glossary)

### C.1 Nguồn dữ liệu & CDC

| Thuật ngữ | Giải thích |
|---|---|
| **OLTP** | *Online Transaction Processing*: database nghiệp vụ, nơi ứng dụng ghi đơn hàng, tồn kho… theo thời gian thực. Ở đây là MySQL và PostgreSQL, và đây là **source of truth**. |
| **CDC** (*Change Data Capture*) | Bắt mọi thay đổi (INSERT/UPDATE/DELETE) của database **bằng cách đọc log nội bộ của nó**, thay vì định kỳ query lại cả bảng. Nhanh, và không đè tải lên nguồn. |
| **binlog** | *Binary log* của MySQL, nhật ký mọi thay đổi. `binlog_format=ROW` + `binlog_row_image=FULL` nghĩa là log ghi **toàn bộ dòng trước và sau** khi đổi. |
| **GTID** | *Global Transaction ID* của MySQL. Mỗi transaction có một số định danh toàn cục (dạng `uuid:1-63546`), dùng để biết đã đọc tới transaction nào. |
| **WAL** | *Write-Ahead Log* của PostgreSQL, tương đương binlog. `wal_level=logical` cho phép giải mã WAL thành thay đổi ở mức dòng. |
| **Logical replication slot** | Một "dấu trang" PostgreSQL giữ cho mỗi consumer (ở đây là Debezium). PostgreSQL **không xoá WAL** cho tới khi consumer xác nhận đã đọc, nên consumer chết lâu thì WAL phình to và có thể làm đầy đĩa. |
| **Publication** (`mosaic_pub`) | Danh sách bảng PostgreSQL được phát ra qua logical replication. Bảng không có trong publication thì **không được giải mã**. |
| **pgoutput** | Plugin giải mã WAL chuẩn của PostgreSQL; chỉ giải mã các bảng có trong publication. |
| **LSN** | *Log Sequence Number*, vị trí (byte) trong WAL. `confirmed_flush_lsn` là vị trí consumer đã xác nhận xử lý xong, `current_wal_lsn` là vị trí WAL hiện tại. Hiệu của hai số là lượng **retained WAL** (WAL đang bị giữ lại). |
| **Replica identity** | Cấu hình PostgreSQL quyết định khi UPDATE thì WAL ghi gì về **dòng cũ**: `DEFAULT` chỉ ghi primary key, `FULL` ghi cả dòng. |
| **Heartbeat** | Debezium định kỳ (mỗi 10 s) ghi một dòng vào bảng `cdc_heartbeat` để tạo "thay đổi giả", giúp `confirmed_flush_lsn` tiếp tục tiến lên khi các bảng nghiệp vụ không có ai ghi. |

### C.2 Debezium & Kafka

| Thuật ngữ | Giải thích |
|---|---|
| **Debezium** | Bộ connector CDC mã nguồn mở. Đọc binlog/WAL và đẩy mỗi thay đổi thành một message vào Kafka. |
| **Kafka** | Hàng đợi message bền vững (*durable streaming buffer*). Ở đây chạy chế độ **KRaft**, không cần ZooKeeper. |
| **Kafka Connect / worker / connector** | *Kafka Connect* là framework chạy connector; *worker* là tiến trình (container `kafka-connect`); *connector* là cấu hình cho một nguồn cụ thể (`mysql-cdc`, `postgres-cdc`). |
| **Topic** | Một "kênh" trong Kafka. Mỗi bảng được capture có một topic riêng, ví dụ `mosaic.mysql.ecommerce_tx.orders`. |
| **Offset** | Vị trí đọc. Trong Kafka, offset là vị trí trong topic. Với Debezium, *source offset* là vị trí trong log nguồn (binlog file/pos hoặc LSN). Connect lưu source offset vào topic `_connect_offsets` để khi restart biết đọc tiếp từ đâu. |
| **`offset.flush.interval.ms`** | Chu kỳ Connect ghi offset xuống, ở đây là **60 000 ms**. Offset được lưu **theo đồng hồ**, không phải sau từng record, và đây là gốc của finding A8. |
| **Change envelope** | "Phong bì" của một sự kiện Debezium, gồm `op` (loại thao tác), `before` (dòng trước khi đổi), `after` (dòng sau khi đổi), `source` (metadata: file/pos/lsn, `ts_ms` = thời điểm commit ở nguồn). |
| **`op`** | `c` = create (insert), `u` = update, `d` = delete, `r` = read (dòng phát ra trong lúc **snapshot**, không phải thay đổi thật). |
| **Snapshot (Debezium)** | Đọc toàn bộ bảng hiện có rồi phát thành các sự kiện `op=r`. *Initial snapshot* chạy lần đầu; *ad-hoc BLOCKING snapshot* là chạy lại theo yêu cầu (lệnh `resnapshot`). Đừng nhầm với *Iceberg snapshot* ở dưới. |
| **Signal channel** | Kênh (ở đây là một Kafka topic) để gửi lệnh cho connector đang chạy, ví dụ "snapshot lại các bảng này". |
| **SMT** (*Single Message Transform*) | Phép biến đổi nhẹ của Kafka Connect áp lên từng message: `MaskField`, `ReplaceField`, `ExtractNewRecordState` (`unwrap`: làm phẳng envelope, chỉ giữ `after`), `TimestampConverter`. |
| **Converter** | Cách Connect tuần tự hoá message: `AvroConverter` (nhị phân, cần **Schema Registry**) hoặc `JsonConverter` (JSON tự mô tả). `schemas.enable=false` nghĩa là bỏ khối schema đi kèm mỗi message. |
| **Retention** | Kafka xoá dữ liệu cũ hơn `log.retention.hours` (mặc định 168 h = 7 ngày). |

### C.3 Lakehouse & xử lý

| Thuật ngữ | Giải thích |
|---|---|
| **Lakehouse** | Kết hợp *data lake* (lưu file rẻ trên object storage) với tính năng của *data warehouse* (bảng, transaction, SQL). |
| **Medallion (Bronze/Silver/Gold)** | Ba tầng tinh chế. **Bronze** = dữ liệu thô, giữ nguyên envelope (để audit và replay). **Silver** = đã dedup, ép kiểu, mask; là "trạng thái hiện tại" của từng bảng nguồn. **Gold** = đã join, tổng hợp, phân vùng, sẵn sàng cho phân tích và xếp hạng. |
| **Apache Iceberg** | *Table format*: biến một đống file Parquet trên object storage thành một "bảng" có transaction, schema và lịch sử phiên bản. |
| **Iceberg snapshot / snapshot summary** | Mỗi lần commit tạo một phiên bản (snapshot) mới của bảng. *Summary* là metadata key-value đi kèm mỗi commit; dự án ghi `max(_src_ts)` vào đó để biết độ tươi. |
| **Position-delete files / merge-on-read** | Khi `MERGE`/`UPDATE`, Iceberg không viết lại file dữ liệu mà ghi thêm file "xoá dòng ở vị trí X", lúc đọc mới gộp lại. Ghi nhanh, nhưng file tích tụ dần nên cần **compaction**. |
| **Maintenance** | `optimize` (gộp file nhỏ), `expire_snapshots` (xoá phiên bản cũ), `remove_orphan_files`. Chưa làm. |
| **Partition / `day()` / `bucket(n)`** | Chia bảng thành nhóm file để query bỏ qua phần không cần (*pruning*). `day(created_at)` cho mỗi ngày một phân vùng; `bucket(region, 16)` băm cột region vào 16 ngăn. Quá nhiều phân vùng nhỏ thì chậm (finding A12). |
| **Nessie** | *Catalog* cho Iceberg, nơi lưu thông tin "bảng X hiện trỏ tới metadata file nào". Có branch giống git. |
| **MinIO** | Object storage tương thích S3, chạy local; nơi chứa các file Parquet thật. |
| **Trino** | Query engine SQL phân tán. Vai trò **federation**: một câu SQL đọc cùng lúc Iceberg, MySQL, PostgreSQL. Mỗi nguồn là một *catalog* (`iceberg`, `mysql`, `postgresql`, `platform`). |
| **`ANALYZE` / NDV** | Thu thập thống kê bảng cho *cost-based optimizer* của Trino. NDV = *number of distinct values*. |
| **Spark Structured Streaming** | Engine xử lý luồng: đọc Kafka liên tục, cứ mỗi **micro-batch** (kích hoạt 10 s một lần, gọi là *trigger*) thì ghi một lần vào Bronze. |
| **Checkpoint** | Thư mục Spark lưu "đã xử lý tới offset nào, batch nào đã commit". Khi restart, Spark đọc checkpoint để chạy tiếp. |
| **dbt** | Công cụ viết transformation bằng SQL + Jinja. Mỗi *model* là một câu SELECT, được dbt biên dịch thành `CREATE TABLE`/`MERGE`; có `tests:` để kiểm tra chất lượng. **dbt-trino** là adapter chạy dbt trên Trino. |
| **Incremental model / `MERGE INTO`** | Chỉ xử lý dữ liệu mới kể từ lần build trước rồi `MERGE` vào bảng đích, thay vì build lại toàn bộ. |
| **Post-hook** | Câu SQL dbt chạy ngay sau khi build một model; ở đây là `ANALYZE`. |
| **Latest-wins** | Khi một khoá có nhiều sự kiện, giữ sự kiện **mới nhất theo vị trí trong log nguồn**. |
| **Soft delete (`_is_deleted`)** | Không xoá dòng, chỉ đánh dấu là đã xoá. |

### C.4 Độ tin cậy & đo lường

| Thuật ngữ | Giải thích |
|---|---|
| **At-least-once** | Mỗi sự kiện được giao **ít nhất một lần**: không mất, nhưng có thể trùng. |
| **Exactly-once** | Mỗi sự kiện có mặt ở đích **đúng một lần**. Bronze đạt được exactly-once dù nguồn chỉ là at-least-once, nhờ *idempotency key*. |
| **Idempotency key** | Khoá định danh duy nhất của một sự kiện: ghi lần hai với cùng khoá thì bị bỏ qua. Ở Bronze, khoá là SHA-256 của (topic, op, record key, vị trí nguồn, `ts_ms`, before, after). |
| **Replay** | Sự kiện bị giao lại sau restart, do offset được lưu trễ. |
| **SIGKILL vs SIGTERM** | SIGKILL (`docker kill`) giết tiến trình ngay, không kịp dọn dẹp. SIGTERM (`docker stop`) cho tắt êm, nên Connect kịp flush offset. Test dùng SIGKILL vì đó mới là trường hợp xấu cần chứng minh. |
| **Gap** | Sự kiện bị **mất**. Mục tiêu của restart-safety là 0 gap. |
| **Quiescence / caught up** | Hệ thống đã "đuổi kịp": end-offset của các topic đứng yên qua 3 lần poll liên tiếp. |
| **p50 / p95 / p99** | Phân vị. p50 là trung vị (50% sự kiện nhanh hơn mức này); p99 nghĩa là 99% sự kiện nhanh hơn mức này. |
| **Negative control** | Cố tình chạy với cấu hình **sai** để chứng minh phép kiểm tra **có khả năng** báo lỗi. Phép kiểm tra luôn PASS thì vô nghĩa. |
| **Ledger** | File `.jsonl` append-only trong `experiments/results/`. Mỗi lần chạy thêm một dòng JSON, **kể cả lần thất bại**. |
| **Reconcile** | So từng dòng của Silver với nguồn sống và đếm missing / extra / differing. |
| **KPI (GR1 Table 4.4)** | 5 chỉ số CDC mà GR1 yêu cầu đo: end-to-end latency, throughput, restart recovery time, DDL handling, WAL lag. |

### C.5 Retrieval

| Thuật ngữ | Giải thích |
|---|---|
| **Embedding** | Vector số (ở đây 384 chiều, model `all-MiniLM-L6-v2`) biểu diễn ngữ nghĩa của một đoạn text. Hai text gần nghĩa thì cho hai vector gần nhau. |
| **pgvector / HNSW** | Extension PostgreSQL để lưu và tìm vector. HNSW là cấu trúc index cho tìm gần đúng nhanh (`m=16, ef_construction=64`). |
| **ANN** | *Approximate Nearest Neighbor*: tìm các vector gần nhất theo kiểu gần đúng, đổi một chút độ chính xác lấy tốc độ. |
| **Channel** | Một nguồn sinh ra danh sách xếp hạng: kênh SQL (trên Gold) và kênh ANN (trên embedding). |
| **Fusion / RRF** | Gộp nhiều bảng xếp hạng thành một. RRF chấm điểm bằng Σ 1/(k + thứ hạng), chỉ dựa vào vị trí. |
| **Freshness / staleness / watermark** | Độ tươi / độ cũ / mốc "dataset đã cập nhật tới lúc nào". |
| **SLA** | Ở đây là ngưỡng độ cũ tối đa chấp nhận được. |
| **Oracle, stale-hit rate@10, nDCG@10** | Xem mục L0 ở trên. |

### C.6 Governance

| Thuật ngữ | Giải thích |
|---|---|
| **PII** | *Personally Identifiable Information*: email, số điện thoại, CMND, số thẻ… |
| **Masking Layer 1 / Layer 2** | Layer 1 che PII **trước khi vào Kafka** và không thể đảo ngược. Layer 2 che **lúc query**, tuỳ theo role (Trino + policy engine). |
| **Salted SHA-256** | Băm giá trị cùng một chuỗi bí mật (*salt*). Không đảo ngược được, nhưng cùng input cho cùng hash, nên vẫn join và đếm distinct được. |
| **Tag / lineage / propagation** | *Tag* là nhãn gắn vào cột (`sensitive`…). *Lineage* là đồ thị "cột này sinh ra từ cột nào". *Propagation* là việc nhãn tự lan theo lineage qua các tầng. |
| **OPA / Rego** | *Open Policy Agent* là policy engine; Rego là ngôn ngữ viết policy. Thay cho **Apache Ranger**. |
| **OpenLineage / Marquez** | OpenLineage là chuẩn phát sự kiện lineage; Marquez là server nhận và hiển thị lineage. Thay cho **DataHub**. |

### C.7 Thuật ngữ quy trình của dự án

| Thuật ngữ | Giải thích |
|---|---|
| **GR1 / GR2** | Graduation Research 1 (thiết kế, đã xong) / Graduation Research 2 (implement, đang làm). |
| **Milestone M0–M8** | Các mốc implement (xem STATUS.md). |
| **ADR** | *Architecture Decision Record*: tài liệu ngắn ghi một quyết định kiến trúc, bối cảnh và hệ quả của nó. |
| **Finding** | Phát hiện về chỗ thiết kế GR1 **không chạy được như đã viết**. Đây là sản phẩm có giá trị của giai đoạn implement. |
| **Substitution** | Thay một thành phần bằng thứ nhẹ hơn nhưng giữ nguyên **cam kết (guarantee)**. |
| **Verified / unverified** | "Verified" chỉ khi lệnh **đã thật sự chạy** và output đã được đọc. Viết xong mà chưa chạy là unverified. |
| **`_src_ts` / `_ingest_ts`** | Hai cột bắt buộc trên mọi dòng Silver/Gold: thời điểm commit ở nguồn / thời điểm vào lakehouse. Là nguyên liệu của Freshness Registry. |
| **Profile (compose)** | Nhóm service bật theo tuỳ chọn: mặc định (core), `gov`, `orch` (Airflow), `console`, `transform` (dbt). |
