# Changes from the GR1 Report (Những thay đổi so với báo cáo GR1)

**Bản ghi đầy đủ về việc bản implement khác `GR1_report.md` ở đâu, và tại sao.**

Last updated: 2026-10-16 · Covers milestones M0–M4 (M4 complete)

> *Bản song ngữ: diễn giải tiếng Việt, giữ thuật ngữ tiếng Anh và giữ nguyên trích dẫn gốc từ GR1
> (để dẫn lại khi viết báo cáo). Thuật ngữ khó tra ở
> [PROBLEM.md, Phụ lục C](PROBLEM.md#phụ-lục-c--bảng-thuật-ngữ-glossary).*

> **Đã đối chiếu nguồn.** Mọi khẳng định ở đây đã được kiểm với **`GR1_report.pdf`** (49 trang),
> không chỉ với bản chuyển đổi Markdown. Số trang bên dưới là số trang PDF. Bản `GR1_report.md` đã
> được xác nhận độc lập là trung thực (khớp 98% độ dài văn bản, mọi đoạn cấu hình được trích khớp PDF
> từng byte). Xem *Phụ lục: về hai bản sao của báo cáo* ở cuối.

---

## How to read this document (cách đọc)

GR1 là một **nghiên cứu thiết kế**; repo này là **bản implement** của thiết kế đó. Mỗi khi bản
implement gặp thực tế, sẽ xuất hiện các loại khác biệt. Tài liệu này tách chúng ra rõ ràng, vì khi
bảo vệ, mỗi loại mang ý nghĩa rất khác nhau:

| Phần | Loại thay đổi | Ý nghĩa |
|---|---|---|
| **A** | **Findings**: GR1 nói một điều *không thể thực thi như đã viết* | Không phải lỗi cần giấu. Đây là **sản phẩm** của giai đoạn implement, và là phần giá trị nhất của tài liệu. |
| **B** | **Substitutions**: thay một thành phần bằng thứ tương đương nhẹ hơn | Đánh đổi kỹ thuật có chủ đích. **Cam kết (guarantee) giữ nguyên**, chỉ công cụ khác. |
| **C** | **Additions**: những thứ GR1 hoàn toàn không có | Nơi chứa tính mới (*novelty*). GR1 đề xuất; ở đây xây. |
| **D** | **Operational changes**: cấu trúc, công cụ, quy ước | Mang tính cơ học. Không ảnh hưởng tới các claim của kiến trúc. |
| **E** | **Unchanged**: những gì GR1 đặc tả và được giữ nguyên | Khoanh vùng phần thay đổi, để người đọc thấy thiết kế phần lớn vẫn đứng vững. |
| **F** | **Defects inside GR1 itself**: tham chiếu hỏng, mâu thuẫn trong văn bản | Sửa trong báo cáo trước khi nộp GR2. |

Mỗi mục theo cùng một khuôn: **GR1 nói gì → ta làm gì → vì sao → bằng chứng → ảnh hưởng tới các
claim của GR1.** Nếu thay đổi có ADR thì ADR chứa lập luận đầy đủ và được link tới.

**Một câu để nhớ:** kiến trúc của GR1 qua được giai đoạn implement gần như nguyên vẹn. Thứ *không*
qua được là một số **cấu hình cụ thể** trong Appendix B và C, và việc phát hiện ra chúng chính là mục
đích của GR2.

---

## Summary of every change (tóm tắt mọi thay đổi)

| # | Thay đổi | Loại | ADR |
|---|---|---|---|
| A1 | Chuỗi SMT masking ở Appendix C không thể thoả §4.2 và §4.6 cùng lúc | Finding | [0004](adr/0004-connector-native-masking.md) |
| A2 | Avro converter của Appendix B không có trên classpath của image Connect, nên worker chết khi khởi động | Finding | [0005](adr/0005-json-converter.md) |
| A3 | Image tag `debezium/connect:2.7` không tồn tại | Finding | — |
| A4 | Image tag `projectnessie/nessie:0.90.4` không có trên Docker Hub | Finding | — |
| A5 | Image Nessie không chạy được HTTP healthcheck (không có curl/wget) | Finding | — |
| A6 | PostgreSQL **không phát before image** với replica identity mặc định, nên với Appendix A như hiện tại thì envelope §4.2 không thể đạt được cho nguồn này | Finding | [0004](adr/0004-connector-native-masking.md) |
| A7 | Debezium **âm thầm cắt hash mask theo độ rộng cột nguồn**: cột hẹp cho ra digest không đầy đủ | Finding | [0004](adr/0004-connector-native-masking.md) |
| A8 | **Restart safety là at-least-once kèm cửa sổ replay**, không phải exactly-once. Connect flush offset theo đồng hồ, điều mà tóm tắt một dòng ở Table 4.3 không nói | Finding | — |
| A9 | **WAL-bloat guard của §4.4 vô hiệu**: heartbeat ghi vào bảng ngoài publication, nên `pgoutput` không giải mã nó và `confirmed_flush_lsn` không bao giờ tiến | Finding | — |
| A10 | **"Replay capability" của §4.2 hết hạn theo Kafka retention**: hai lần không bootstrap được Bronze từ topic; sau 10 ngày dừng máy thì mọi topic bảng đều rỗng | Finding | — |
| A11 | **MySQL `DATETIME` đến lệch 7 tiếng**: Debezium mã hoá giờ địa phương không có múi giờ như thể là UTC; việc chuẩn hoá mà §4.4 giao cho SMT `TimestampConverter` phải chuyển sang Silver | Finding | (xem A1) |
| A12 | **Partitioning `gold.orders` của §4.3 quá mịn cho prototype**: 2,944 phân vùng, trung bình 12.6 dòng; build vượt giới hạn writer mặc định của Trino; `ANALYZE` chậm 33 lần và full scan chậm 14 lần so với cùng dữ liệu không phân vùng | Finding | — |
| A13 | **Ở quy mô prototype, planner của PostgreSQL bỏ qua index HNSW**: một query "ANN" không pin access path thực chất là tìm chính xác, nên phép so sánh HNSW vs exact của §4.5 (Track 2) cho kết quả vô nghĩa nếu không pin từng đường đi | Finding | [0006](adr/0006-m4-embedding-pipeline.md) |
| B1 | Apache Ranger → Open Policy Agent | Substitution | [0001](adr/0001-opa-instead-of-ranger.md) |
| B2 | DataHub → OpenLineage + Marquez | Substitution | [0002](adr/0002-marquez-instead-of-datahub.md) |
| B3 | Spark (Silver/Gold) + Great Expectations → dbt-trino | Substitution | [0003](adr/0003-dbt-trino-for-silver-gold.md) |
| B4 | Avro + Confluent Schema Registry → JsonConverter | Substitution | [0005](adr/0005-json-converter.md) |
| C1 | Một bài toán được phát biểu rõ và cái thang trừu tượng 4 bậc (`PROBLEM.md`) | Addition | — |
| C2 | **N1** Freshness Registry: mặt phẳng watermark query được | Addition | — |
| C3 | **N2** Fusion engine có tính tới độ tươi, implement §5.3 | Addition | — |
| C4 | **N3** Governance theo tag, đi tới tận vector index | Addition | — |
| C5 | **N4** Định tuyến query theo freshness SLA (stretch) | Addition | — |
| C6 | Domain thứ hai (`jobmarket`) làm bằng chứng máy móc cho tính tổng quát | Addition | — |
| C7 | Tách `platform/` ÷ `domains/`, có cổng CI cưỡng chế | Addition | — |
| C8 | Quy ước watermark `_src_ts` / `_ingest_ts` | Addition | — |
| C9 | Seed generator và load generator là deliverable chính thức | Addition | — |
| D1 | Task runner `mosaic.py` (không có `make`) | Operational | — |
| D2 | `up` đợi *healthy*, không chỉ *started* | Operational | — |
| D3 | Compose profiles để footprint mặc định vừa một máy | Operational | — |
| D4 | Mọi port publish ra host đều tham số hoá | Operational | — |
| D5 | Metadata của platform nằm ở DB riêng + catalog Trino thứ 4 | Operational | — |
| D6 | DDL domain và connector chuyển vào `domains/<domain>/` | Operational | — |
| D7 | Đặt rõ `decimal.handling.mode` và `time.precision.mode` | Operational | — |
| D8 | Không dùng `ExtractNewRecordState` ở connector | Operational | (xem A1) |
| D9 | Bulk load MySQL chạy bằng `root`, không bằng `cdc` | Operational | — |
| D10 | `REPLICA IDENTITY FULL` trên `users`, `products`, `inventory` | Operational | (xem A6) |
| D11 | Silver được **sinh tự động** cho mỗi bảng capture; Gold nằm ở `domains/<domain>/gold/` | Operational | (xem C6) |
| D12 | Silver = một bảng trạng thái hiện tại không phân vùng cho mỗi bảng capture, soft delete | Operational | — |
| D13 | dbt chạy trong container riêng (`mosaic/dbt:1.8`), sau profile `transform` | Operational | [0003](adr/0003-dbt-trino-for-silver-gold.md) |
| D14 | Nâng `iceberg.max-partitions-per-writer` của Trino từ 100 lên 5000 | Operational | (xem A12) |
| D15 | Load generator lấy vùng miền từ bộ từ vựng của seed | Operational | — |
| D16 | `product_embeddings` do pipeline tạo (không còn trong DDL nguồn), **bỏ FK** tới `products`, thêm cột snapshot + `embedded_at`; thêm Gold `product_catalog` làm nguồn | Operational | [0006](adr/0006-m4-embedding-pipeline.md) |
| D17 | Checkpoint Bronze trên named volume bị **rách** (file 0 byte) khi Docker Desktop dừng cứng; khôi phục bằng cách cất file rách sang chỗ khác | Operational | — |

---

# Part A — Findings: những đặc tả GR1 không thể thực thi như đã viết

Đây là những chỗ mà làm đúng từng chữ của GR1 thì ra một hệ thống **không khởi động được**, hoặc
**không làm điều báo cáo nói**. **Mỗi phát hiện đều tìm ra bằng cách chạy thật, không phải bằng cách
đọc.** Nên trình bày nổi bật trong báo cáo GR2.

---

## A1. The SMT masking chain contradicts the Bronze envelope requirement

*(Chuỗi SMT masking mâu thuẫn với yêu cầu giữ envelope ở Bronze)*

**Đây là phát hiện quan trọng nhất của tài liệu.** Nó là một **mâu thuẫn nội tại** thật sự trong
GR1, không phải một lỗi cấu hình.

### What GR1 says (GR1 nói gì)

Hai yêu cầu, ở hai mục khác nhau:

> **§4.2** — *"The Bronze tier stores the unmodified Debezium change envelope, including
> the before/after row state, the operation type, the source offset or log sequence number,
> and the event timestamp, **with no transformation applied**, in order to preserve full
> auditability and replay capability."*

Tức là: Bronze phải lưu **nguyên vẹn** envelope (before/after, loại thao tác, offset/LSN, timestamp),
**không biến đổi gì**, để giữ khả năng audit và replay.

> **§4.6 / Table 4.5** — Layer-1 masking applies `MaskField` / `ReplaceField`
> *"at ingestion, before the Kafka topic"*, so that raw PII *"never persists to any storage
> tier."*

Tức là: masking lớp 1 phải chạy **trước khi vào Kafka**, để PII thô **không bao giờ** được lưu ở bất
kỳ tầng nào.

Appendix C implement yêu cầu thứ hai bằng chuỗi transform này:

```json
"transforms": "unwrap,maskPII,dropSensitive,ts",
"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
"transforms.maskPII.type": "org.apache.kafka.connect.transforms.MaskField$Value",
"transforms.maskPII.fields": "buyer_email,buyer_phone"
```

### Why it cannot work (vì sao không thể chạy)

Một change event của Debezium là **lồng nhau**. Các cột nghiệp vụ không nằm ở cấp ngoài cùng mà nằm
*bên trong* `before` và `after`:

```json
{
  "op": "u",
  "before": { "buyer_email": "old@example.com", "total_amount": "100.00" },
  "after":  { "buyer_email": "new@example.com", "total_amount": "150.00" },
  "source": { "file": "mysql-bin.000001", "pos": 1234, "ts_ms": 1699123456789 }
}
```

`MaskField$Value` và `ReplaceField$Value` của Kafka Connect **chỉ chạm được field cấp ngoài cùng**;
chúng không với tới `after.buyer_email`. Đó chính là lý do Appendix C đặt `unwrap` **đầu tiên**:
`ExtractNewRecordState` làm phẳng event, chỉ giữ lại ảnh `after`, để `MaskField` có thứ mà nó chạm
được.

Nhưng làm phẳng thì **vứt bỏ `before`, `op`, và cả cấu trúc envelope**. Vì vậy:

> Chạy chuỗi của Appendix C sẽ cho ra một Bronze **không thể** chứa envelope nguyên vẹn mà §4.2 đòi
> hỏi. Thoả §4.6 như đã viết thì §4.2 trở thành bất khả.

### The second, quieter problem (vấn đề thứ hai, khó thấy hơn)

Kể cả bỏ qua mâu thuẫn trên, cách SMT còn **làm rò rỉ đúng thứ PII nó định chặn**. `MaskField` chạy
sau `unwrap`, nên nó chỉ thấy ảnh `after`. Giả sử bằng cách nào đó envelope được giữ lại, thì **ảnh
`before` của mọi UPDATE sẽ mang giá trị cũ chưa mask đi thẳng vào Bronze.** Đổi email của một người
dùng, và địa chỉ cũ bị ghi vào kho lưu trữ vĩnh viễn ở dạng rõ.

### What we do instead (ta làm gì thay thế)

Chuyển masking từ SMT của Kafka Connect **vào chính connector Debezium**, và bỏ hẳn `unwrap`.

| Việc cần làm | Appendix C (SMT) | Ở đây (connector-native) |
|---|---|---|
| Che một giá trị | `MaskField$Value` | `column.mask.hash.SHA-256.with.salt.<salt>` |
| Loại một cột | `ReplaceField$Value` | `column.exclude.list` |
| Chuẩn hoá timestamp | `TimestampConverter$Value` | `time.precision.mode=connect`, cast ở Silver |
| Làm phẳng | ở connector | ở bước Bronze→Silver |

Cấu hình thật đang nằm trong [`domains/ecommerce/connectors/mysql-cdc.json`](../domains/ecommerce/connectors/mysql-cdc.json):

```json
"column.mask.hash.SHA-256.with.salt.mosaicPiiSalt":
    "ecommerce_tx.orders.buyer_email,ecommerce_tx.orders.buyer_phone",
"column.exclude.list": "ecommerce_tx.payments.raw_card_number"
```

### Why this fixes both problems at once (vì sao sửa được cả hai)

Masking native chạy **trước khi envelope được dựng**: connector đọc binlog/WAL, mask cột, *rồi mới*
xây event. Hệ quả:

1. **Cả `before` lẫn `after` đều được mask**, nên lỗ rò khi UPDATE được bịt.
2. **Envelope được giữ nguyên vẹn**, nên §4.2 thoả được.
3. Masking vẫn không đảo ngược được và vẫn chạy trước mọi Kafka topic, nên ngữ nghĩa Layer-1 của §4.6
   và Table 4.5 không đổi.

### One deliberate improvement (một cải tiến có chủ đích)

`MaskField` với giá trị thay thế rỗng của GR1 **phá huỷ** giá trị. Ở đây dùng **SHA-256 có salt**:
cũng không đảo ngược được, nhưng giữ cột **join được và đếm được**. Nhờ vậy Silver/Gold vẫn trả lời
được câu *"tuần này có bao nhiêu người mua khác nhau"* mà không bao giờ thấy địa chỉ email nào.

Các field mà GR1 nói **không bao giờ được lưu**, là `users.national_id` và
`payments.raw_card_number`, thì bị **loại hẳn** chứ không băm, vì một hash có salt vẫn là một hình
thức lưu trữ.

### Impact on GR1's claims (ảnh hưởng tới claim của GR1)

Mô hình masking hai lớp của Table 4.5 **giữ nguyên về bản chất**; chỉ cơ chế thay đổi. Cam kết trong
báo cáo ("irreversible, never persisted") giờ **thực sự đúng**, điều trước đây không đúng.

**Giới hạn đã biết:** salt đang nằm trong cấu hình connector, điều không chấp nhận được ở production.
Cách sửa (một Kafka Connect config provider đọc từ secret store) được nêu tên, chưa implement. Nên
ghi rõ điều này trong báo cáo.

### Status

**Verified (2026-08-28).** *(Bản gốc vẫn ghi "Written, not yet exercised", đó là trạng thái trước
M1 và đã lỗi thời.)* `python mosaic.py cdc-verify` trên dữ liệu thật: 502 before image trên cả hai
nguồn đều đã mask; cột bị loại vắng mặt trong 637 image. Chi tiết và ví dụ JSON ở
[ADR 0004, mục Verification](adr/0004-connector-native-masking.md).

---

## A2. The Avro converter is not on the Connect image's classpath

*(Avro converter không có trong image Connect)*

### What GR1 says

Appendix B cấu hình Connect worker như sau, và Appendix C lặp lại cho từng connector:

```yaml
KEY_CONVERTER: io.confluent.connect.avro.AvroConverter
VALUE_CONVERTER: io.confluent.connect.avro.AvroConverter
CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
```

### What actually happens (thực tế)

Worker **chết ngay lúc khởi động**, trước khi phục vụ được bất kỳ request nào:

```
ERROR  Stopping due to error   [org.apache.kafka.connect.cli.AbstractConnectCli]
org.apache.kafka.common.config.ConfigException: Invalid value
io.confluent.connect.avro.AvroConverter for configuration key.converter:
Class io.confluent.connect.avro.AvroConverter could not be found.
```

Image `debezium/connect` **không** đóng gói Avro converter của Confluent, vì converter này theo giấy
phép Confluent và được phát hành riêng. Vậy **không có cách nào Appendix B đã từng được chạy như
viết**.

### What we do instead

Dùng `org.apache.kafka.connect.json.JsonConverter` với `schemas.enable=false` cho cả key và value.

Hai phương án khác đã được cân nhắc và loại:

| Phương án | Vì sao không |
|---|---|
| Tự build image kèm jar Avro của Confluent | Khoảng 8 jar phụ thuộc bắc cầu phải pin bằng tay, dễ vỡ khi đổi version. Tệ hơn: wire format của Confluent có một byte "magic" + schema id ở đầu, và `from_avro` của Spark bản OSS **không tích hợp Schema Registry**, nên Bronze job sẽ cần ABRiS hoặc tự giải mã. |
| Converter Apicurio (`ENABLE_APICURIO_CONVERTERS=true`) | Có sẵn trong image nên không phải tải về, nhưng thay Confluent Schema Registry bằng **Apicurio Registry**, một service Appendix B không hề nêu, và vẫn để nguyên gánh nặng wire format Avro cho Spark. |

### What is kept, and what is lost (giữ được gì, mất gì)

**Giữ được.** Envelope Debezium (`op`, `before`, `after`, `source`) được giữ nguyên. `schemas.enable=false`
chỉ bỏ *khối schema* đi kèm mỗi message của Connect, **không** bỏ envelope. §4.2 không bị ảnh hưởng.

**Có thể nói là tốt hơn cho Bronze.** §4.2 biện minh cho Bronze bằng khả năng audit và replay. JSON
tự mô tả đọc được bằng bất kỳ công cụ nào mà không cần tra registry. Ngược lại, một payload Avro nhị
phân có schema nằm ở một service khác là bằng chứng audit *yếu hơn*, vì nếu mất service đó thì không
đọc được.

**Mất.** Mã hoá nhị phân gọn, và việc registry cưỡng chế tương thích schema. Ở quy mô một node và 10M
dòng thì cả hai đều không thiết yếu.

**Mất một phần, cần nói thẳng.** Với `schemas.enable=false`, việc **thêm hoặc bỏ cột** vẫn thấy được
qua thay đổi tập field trong payload, nên KPI DDL-handling của GR1 Table 4.4 vẫn đo được cho trường
hợp thông thường. Nhưng **đổi kiểu dữ liệu** (ví dụ `INT` → `BIGINT`) mà không đổi tập field thì
**không quan sát được**. Nếu thí nghiệm DDL cần độ trung thực về kiểu, bật
`value.converter.schemas.enable=true` sẽ khôi phục nó, đổi lại message to hơn khoảng 10 lần.

**Schema Registry vẫn được deploy nhưng nằm ngoài đường dữ liệu.** Nó vẫn ở trong stack để quay lại
Avro chỉ là đổi cấu hình, không phải deploy lại. **Đừng mô tả nó là load-bearing trong báo cáo**, vì
hiện nó không mang traffic nào.

**Đảo ngược được:** một dòng trong config worker, hai dòng mỗi connector.

Lập luận đầy đủ: [ADR 0005](adr/0005-json-converter.md).

---

## A3. `debezium/connect:2.7` does not exist

**GR1 Appendix B:** `image: debezium/connect:2.7`

**Thực tế:**
```
failed to resolve reference "docker.io/debezium/connect:2.7":
docker.io/debezium/connect:2.7: not found
```

Debezium **chỉ publish tag ở mức patch**: `2.7.0.Final`, `2.7.1.Final`, `2.7.2.Final`,
`2.7.3.Final`. Không có tag "cuốn chiếu" `2.7`.

**Sửa:** `debezium/connect:2.7.3.Final`, bản patch mới nhất của 2.7, nên version major.minor mà GR1
nêu được giữ đúng.

---

## A4. `projectnessie/nessie:0.90.4` is not on Docker Hub

**GR1 Appendix B:** `image: projectnessie/nessie:0.90.4`

**Thực tế:** tag không resolve được. Liệt kê Docker Hub thì thấy `projectnessie/nessie` **ngừng cập
nhật ở 0.76.6**, vì dự án đã chuyển việc publish container sang GitHub Container Registry.

**Sửa:** `ghcr.io/projectnessie/nessie:0.90.4`. Đã kiểm có mặt bằng cách liệt kê toàn bộ 350 tag trên
GHCR. **Version GR1 chọn là đúng; chỉ registry là sai.**

---

## A5. The Nessie image cannot run an HTTP healthcheck

**GR1 không nêu điều này**, nhưng nó suy ra trực tiếp từ stack của Appendix B, và đáng ghi lại vì nó
gây ra một lỗi *im lặng*.

Nessie được build trên một base image tối giản, **không có `curl` lẫn `wget`**. Một healthcheck kiểu
`curl -fsS http://localhost:19120/api/v2/config` vì vậy **fail mãi mãi**, không phải vì Nessie hỏng mà
vì chương trình để probe không tồn tại.

**Vì sao quan trọng:** container bị đánh dấu unhealthy vĩnh viễn, và mọi service có
`depends_on: nessie: {condition: service_healthy}`, gồm **Trino và Spark, hai consumer quan trọng
nhất**, sẽ không bao giờ khởi động. Stack trông như bị treo mà không có lỗi gì hữu ích.

**Sửa:** không đặt healthcheck cho Nessie; các service phụ thuộc dùng `condition: service_started`;
kiểm tra sẵn sàng được làm **từ host**, nơi có curl. Đã kiểm:

```
$ curl -fsS http://localhost:19120/api/v2/config
{ "defaultBranch": "main", "maxSupportedApiVersion": 2, "specVersion": "2.1.0", ... }
```

---

## A6. PostgreSQL sends no before image, so §4.2's envelope cannot hold for that source

*(PostgreSQL không gửi before image, nên envelope của §4.2 không thể đạt cho nguồn này)*

**GR1 §4.2** yêu cầu Bronze lưu *"the unmodified Debezium change envelope, including the
before/after row state."* **GR1 Appendix A.2** đưa ra DDL PostgreSQL nhưng không đặt replica
identity, nên mọi bảng giữ mặc định của PostgreSQL.

**Mặc định đó nghĩa là gì:** với một `UPDATE`, PostgreSQL chỉ ghi *primary key* của dòng cũ vào WAL.
Logical decoding không còn gì khác để đưa ra, nên Debezium phát:

```json
{"op": "u", "before": null, "after": { ... }}
```

**`before` là null trên mọi UPDATE của PostgreSQL.** Quan sát trực tiếp:
`mosaic.postgres.public.products` và `.inventory` mỗi topic trả về 300 event `op=u` với **0 before
image**, trong khi `mosaic.mysql.ecommerce_tx.orders` (cùng nền tảng, cùng họ connector) trả về 79
update event với 79 before image. `binlog_row_image=FULL` của MySQL (Appendix B *có* đặt) không có
cấu hình tương ứng phía PostgreSQL trong Appendix A.

**Vì sao quan trọng, theo hai nghĩa:**

1. Yêu cầu envelope của §4.2 đơn giản là **không thể thoả** cho nửa PostgreSQL của hệ thống. Bronze
   sẽ lưu một envelope vĩnh viễn thiếu một nửa mà không ai nhận ra, vì `before: null` trông như "hợp
   lệ là không có" chứ không giống "thiếu khả năng".
2. **Claim trung tâm của ADR 0004 trở nên không chứng minh được trên nguồn này.** Lập luận chống SMT
   masking là nó làm rò ảnh *before* của các UPDATE PII. Trên một nguồn không phát before image thì
   không phân biệt được hai cách tiếp cận, nên bằng chứng phải lấy từ MySQL, hoặc phải đổi replica
   identity.

**Sửa:** `ALTER TABLE ... REPLICA IDENTITY FULL` trên ba bảng mà trạng thái trước thực sự được dùng
tới: `users` (có PII, để claim masking kiểm được), `inventory` (thông tin thay đổi nhanh mà lỗi L0
xoay quanh) và `products` (lịch sử giá). Đã có trong `domains/ecommerce/ddl/postgres.sql`.

**Chi phí, nói thẳng:** `FULL` ghi cả dòng cũ vào WAL ở mỗi UPDATE và DELETE, nên khối lượng WAL tăng
theo độ rộng dòng. Đó là lý do chỉ áp cho ba bảng được nêu tên chứ không áp cho cả schema.

**Bằng chứng sau khi sửa**, dùng đúng công tắc L0 từ đầu tới cuối, `python mosaic.py stockout`:

```json
{"op": "u",
 "before": {"product_id": 29, "stock_qty": 192, "warehouse": "SIN-01"},
 "after":  {"product_id": 29, "stock_qty": 0,   "warehouse": "SIN-01"},
 "source_ts_ms": 1787889547921}
```

Đó là sự kiện L0 với trạng thái trước còn nguyên và một timestamp commit ở nguồn, đúng thứ N1 sẽ đọc
làm `last_cdc(d)`.

---

## A7. Connector-native masking truncates the hash to the source column's width

*(Masking native cắt hash theo độ rộng cột nguồn)*

**GR1 không nêu điều này.** Appendix C dùng SMT; hành vi này thuộc cơ chế native đã thay thế chúng
(ADR 0004). Ghi lại vì nó im lặng, và vì phép kiểm hiển nhiên ("đây có phải hash SHA-256 không?") sẽ
báo **sai là fail** nếu không biết điều này.

Hai cột, một luật mask (`column.mask.hash.SHA-256.with.salt.mosaicPiiSalt`):

| Cột nguồn | Kiểu khai báo | Hash phát ra |
|---|---|---|
| `orders.buyer_email` | `VARCHAR(255)` | `43883ab2…08082`: **64 ký tự hex**, digest đầy đủ |
| `orders.buyer_phone` | `VARCHAR(32)` | `1e3ebf29…2736c`: **32 ký tự hex**, nửa digest |
| `users.phone` (PostgreSQL) | `TEXT` | 64 ký tự hex, digest đầy đủ |

Debezium cắt hash cho vừa độ rộng khai báo của cột nó thay thế. Không có cảnh báo nào được log; giá
trị vẫn *trông như* một hash.

**Ý nghĩa và không ý nghĩa.** Giá trị vẫn là digest một chiều có salt, nên không có plaintext nào tới
Kafka và cam kết của Table 4.5 vẫn đúng. Nhưng độ mạnh giờ phụ thuộc vào độ rộng: một cột PII
`VARCHAR(10)` sẽ cho digest 40 bit, và với một giá trị ít entropy, định dạng cố định như số điện thoại
trong nước, mức đó nằm trong tầm brute-force của ai đang giữ salt. **Độ mạnh của mask là thuộc tính
của DDL nguồn, không phải của cấu hình masking.** Đó đúng là loại ràng buộc ngầm nên báo cho người
review biết thay vì để họ tự tìm ra.

**Xử lý ở đây:** `tools/cdc/verify.py` đọc `character_maximum_length` từ `information_schema` lúc
kiểm và kỳ vọng `min(64, width)` ký tự hex, nên một mask bị cắt được *báo ra* chứ không âm thầm chấp
nhận:

```
NOTE: buyer_phone is narrower than a SHA-256 digest, so the mask is truncated
      to 32 hex chars (128 bits)
```

`buyer_phone` được cố ý để nguyên `VARCHAR(32)`: giữ schema Appendix A trung thực, 128 bit không phải
điểm yếu thực tế, và phát hiện này có ích cho báo cáo hơn bản sửa. Mở rộng cột bị mask lên 64 ký tự là
cách khắc phục khi cần độ mạnh digest đầy đủ.

---

## A8. "Restart safety" is at-least-once with a replay window, not exactly-once

**Đo ở M2.** Đây là sự hiệu chỉnh về **mức độ**, không phải về bản chất, nhưng mức độ đó chính là thứ
tầng downstream phải được xây để chịu được.

### What GR1 says

> **Table 4.3** — Restart safety: *"Offset stored in the Kafka Connect offsets topic"*
> (MySQL) / *"LSN stored in the replication slot"* (PostgreSQL).

> **Table 4.1** — Debezium is chosen partly for *"restart safety achieved through Kafka-stored
> source offsets"*.

Cả hai đều đúng, nhưng không câu nào nói offset được ghi **khi nào**.

### What actually happens

Offset được ghi **theo đồng hồ**:

```
offset.flush.interval.ms = 60000        # đọc từ worker đang chạy; là mặc định của Connect
```

Không ghi sau từng record, và cũng không ghi khi sập, vì một SIGKILL không cho cơ hội flush gì cả. Vậy
offset trong `_connect_offsets` chậm hơn record cuối cùng tới một phút, và khi restart Debezium chạy
tiếp từ vị trí cũ đó, **đọc lại mọi thứ đã commit kể từ đó**.

### Evidence (bằng chứng)

`python mosaic.py restart-safety --rate 25 --warmup 20 --stall 45 --drain 20`, worker bị dừng bằng
**SIGKILL** (không phải `docker stop`, vì `docker stop` flush offset trước khi tắt nên chứng minh được
ít hơn hẳn):

| Bảng capture | dải id ghi trong lúc sập | số dòng | missing | duplicates |
|---|---|---|---|---|
| `ecommerce_tx.orders` | 27000..28054 | 1,055 | **0** | 36 |
| `ecommerce_tx.order_items` | 52728..54617 | 1,890 | **0** | 48 |
| `ecommerce_tx.payments` | 23612..24282 | 671 | **0** | 20 |
| `public.users` | 5614..5708 | 95 | **0** | 6 |

Offset đã commit có chạy tiếp và tiến lên: `mysql-bin.000007:1238339 → :2399951` (GTID
`…:1-61738 → …:1-63546`, 1,808 transaction) và PostgreSQL `lsn 70936688 → 72250344`. Đuổi kịp hoàn
toàn **30.3 s** sau restart, trong khi việc ghi vẫn tiếp tục 28.4 s vào giai đoạn hồi phục.

**Mất: 0 dòng. Trùng: 110 event.** Đó là cơ chế chạy đúng như thiết kế, và một harness chấm trùng lặp
là thất bại sẽ báo false negative cho nó.

### What it changes (nó thay đổi gì)

Kiến trúc không đổi gì. Nó làm sắc nét một claim và tạo ra một nghĩa vụ.

1. **Claim.** "No data loss on connector restart" là đúng; "exactly-once" thì sai. Cửa sổ replay bị
   chặn bởi `offset.flush.interval.ms`, **không phải bởi độ dài lần sập**: sập 5 giây hay 5 phút đều
   phát lại khoảng 60 s công việc như nhau. Giảm chu kỳ thì cửa sổ thu hẹp, đổi lại phải commit
   offset thường xuyên hơn.
2. **Nghĩa vụ.** GR1 §4.2 đã nói Bronze nên *"deduplicate using the source offset as an idempotency
   key"*. A8 biến câu đó từ một ý hay trong thiết kế thành **một yêu cầu đã được đo**: khoảng 110
   event trùng mỗi lần sập là thứ Bronze sẽ thực sự nhận. M3 phải implement nó, và khoá là **toạ độ
   nguồn** (`(file, pos, row)` cho MySQL, `(lsn, txId)` cho PostgreSQL), không phải primary key nghiệp
   vụ.

### Correction (M3): cột "duplicates" ở trên không phải là số replay

Harness M2 đếm *số event trên mỗi key vượt quá cái đầu tiên*. Cách đếm đó cũng tính cả một dòng được
insert hợp lệ rồi update trong cửa sổ (một order đi từ `pending -> paid` là hai source event, không
phải một event giao hai lần), và bỏ qua hoàn toàn các bảng không có khoá tự sinh (`inventory`,
`products`). Vậy "110" là con số lẫn lộn replay với update bình thường trên bốn bảng, và thiếu hai
bảng khác.

M3 đổi metric thành đúng thứ claim cần: **cùng một source event được giao hơn một lần**, khớp theo
op, vị trí nguồn (`file/pos/row` hoặc `lsn/txId`, cộng `source.ts_ms`) và cả hai image. Lần chạy M3
tương đương (SIGKILL, 45 s, 25 ops/s, 2026-10-05) đo được **183 event bị replay trên sáu bảng**, 54
trong đó là insert bị replay; metric cũ trên cùng lần chạy đọc ra 138 trên bốn bảng có khoá. Tính
không-gap không bị ảnh hưởng (lại 0 missing), và mọi kết luận trên cũng vậy: cửa sổ replay là có thật
và Bronze phải hấp thụ nó. M3 sau đó **chứng minh Bronze làm được** (xem M3 trong BUILD-LOG): từng
insert trong 1,088 + 1,980 + 719 + 95 insert ở các dải đã khoanh có mặt trong Bronze đúng một lần, dù
Kafka nhận 1,107 + 2,010 + 724 + 95.

### Status

**Verified.** Tái hiện được theo yêu cầu bằng `python mosaic.py restart-safety`; mỗi lần chạy thêm
một dòng vào `experiments/results/restart_events.jsonl`. Từ M3, `python mosaic.py bronze-verify
exactly-once` theo cùng lần chạy đó tới tận Bronze.

---

## A9. §4.4's WAL-accumulation guard was inert as configured

*(Cơ chế chống phình WAL của §4.4 vô hiệu với cấu hình như viết)*

**Phát hiện mạnh nhất của M2**, và về cấu trúc là cùng một dạng lỗi với A6: một biện pháp GR1 nêu
trong văn xuôi, được cấu hình một cách thiện chí, nhưng âm thầm không làm gì.

### What GR1 says

§4.4 nhận diện đúng rủi ro vận hành riêng của PostgreSQL, và đây là chỗ duy nhất hai cơ chế CDC khác
nhau về *bản chất* chứ không chỉ về chi tiết:

> *"because a logical replication slot prevents PostgreSQL from removing WAL segments until
> Debezium has processed them, a prolonged Debezium outage causes WAL to accumulate on disk and
> can eventually exhaust available storage. This risk is mitigated … by emitting periodic
> heartbeat events that advance the confirmed-flush position even when the monitored tables
> receive no application writes."*

Tóm ý: replication slot ngăn PostgreSQL xoá WAL cho tới khi Debezium xử lý xong, nên Debezium sập
lâu thì WAL tích tụ và có thể làm đầy ổ. Rủi ro được giảm bằng heartbeat định kỳ, đẩy vị trí
confirmed-flush lên kể cả khi các bảng được theo dõi không có ai ghi.

Điều đó đúng, và bản implement đã cấu hình đúng như vậy:

```json
"heartbeat.interval.ms": "10000",
"heartbeat.action.query": "INSERT INTO public.cdc_heartbeat(ts) VALUES (now())"
```

### Why it did not work (vì sao không chạy)

`mosaic_pub` publish bốn bảng (`users`, `products`, `categories`, `inventory`) và **không có
`cdc_heartbeat`**. `pgoutput` chỉ giải mã bảng *đã publish*. Vì vậy câu `INSERT` heartbeat ghi ra WAL
mà Debezium không bao giờ nhận thành change event, không có LSN mới để xác nhận, và
`confirmed_flush_lsn` không nhúc nhích.

Mọi tín hiệu nhìn thấy được đều nói heartbeat khoẻ, và nó khoẻ thật: 503 dòng trong `cdc_heartbeat`,
503 message trên `__debezium-heartbeat.mosaic.postgres`. **Guard vẫn bắn nhưng không đạt được gì**,
không lỗi, không cảnh báo, không dòng log nào.

### Evidence

`python mosaic.py wal-guard --idle 130`, các bảng capture nhàn rỗi, connector **khoẻ** suốt quá
trình:

| Mẫu | `confirmed_flush_lsn` | `current_wal_lsn` | retained WAL |
|---|---|---|---|
| bắt đầu | `0/40D3EA8` | `0/4175C28` | 2,216,816 |
| +130 s | `0/40D3EA8` *(đứng yên)* | `0/4176308` *(đang tiến)* | 2,218,576 |

WAL tiến trong khi vị trí confirmed-flush đứng yên **chính là** tình trạng giữ WAL không giới hạn mà
§4.4 mô tả. Thứ được đặt ra để ngăn nó lại chính là thứ bị hỏng.

### The fix (bản sửa)

Thêm một bảng vào hai chỗ: publication và danh sách capture của connector.

```sql
ALTER PUBLICATION mosaic_pub ADD TABLE cdc_heartbeat;   -- và trong domains/ecommerce/ddl/postgres.sql
```
```json
"table.include.list": "public.users,public.products,public.categories,public.inventory,public.cdc_heartbeat"
```

Đo lại trên cùng cửa sổ nhàn rỗi 130 s:

| nhàn rỗi, connector sống | mức tiến `confirmed_flush_lsn` |
|---|---|
| bảng heartbeat ngoài publication | **0 byte** |
| bảng heartbeat được publish | **688,776 byte** |

Retained WAL giảm từ 704,880 xuống **4,544 byte**. `cdc-verify` sau đó cho thấy heartbeat chảy qua
CDC như một bảng capture bình thường (67 event `op=c` trên topic riêng của nó), nên bản sửa thấy được
từ đầu tới cuối chứ không chỉ trong thống kê slot.

### One honest limit, worth stating in the report (một giới hạn cần nói thẳng)

Câu `INSERT` heartbeat **do chính connector phát ra**. Kill worker là dừng người ghi duy nhất,
database trở nên nhàn rỗi thật, và không có WAL nào tích tụ. Guard bảo vệ trường hợp *tập bảng
capture nhàn rỗi bên cạnh một database bận rộn*, đúng là trường hợp đã đo ở trên. **Nó không bảo vệ
được gì khi chính consumer đang chết**, mà đó lại là kịch bản cụm từ *"a prolonged Debezium outage"*
của §4.4 gợi ra tự nhiên nhất. Trong kịch bản đó, giới hạn thật là `wal_keep_size` và dung lượng đĩa.

### Impact on GR1's claims

Phân tích rủi ro của §4.4 vẫn đứng vững và là một trong những đoạn tốt của báo cáo. Thứ không đứng
vững là giả định ngầm rằng chỉ cần cấu hình `heartbeat.action.query` là đủ: **heartbeat chỉ bảo vệ
WAL nếu connector nhìn thấy chính các lần ghi của nó.** Nên thêm câu đó vào §4.4.

Khi viết GR2, ghép phát hiện này với A6. Cả hai là biện pháp phía PostgreSQL mà GR1 nêu đúng ở mức ý
định nhưng âm thầm thất bại ở mức cấu hình; không cái nào phát hiện được bằng cách đọc, chỉ bằng cách
đo. Hai trường hợp độc lập biến nó thành một **pattern** chứ không còn là giai thoại, và pattern đó
là một câu trả lời đứng vững cho *"giai đoạn implement thực sự đóng góp gì?"*

### Status

**Verified, có cặp trước/sau.** Cả hai lần chạy được giữ trong
`experiments/results/restart_events.jsonl`. Lần fail chính là bằng chứng.

---

## A10. §4.2's "replay capability" expires with Kafka retention

*("Khả năng replay" của §4.2 hết hạn theo Kafka retention)*

**Phát hiện ở M3, hai lần.** Cùng dạng với A6 và A9: một thuộc tính báo cáo khẳng định mà không nêu
điều kiện tiên quyết của nó.

### What GR1 says

> **§4.2** — Bronze stores the unmodified envelope *"in order to preserve full auditability and
> replay capability"*. **Table 4.1** — Kafka is the *"durable streaming buffer that provides
> replay"*.

### What actually happens

Kafka chạy với mặc định `log.retention.hours=168` (7 ngày); không có gì trong Appendix B ghi đè giá
trị này. Retention được thực thi theo **giờ thực**, kể cả **ngay lúc một broker đã dừng khởi động
lại**, tức là trước khi bất kỳ consumer nào kịp đọc.

| Ngày | Tình huống | Offset sớm nhất còn giữ |
|---|---|---|
| 2026-09-12 | M1 snapshot ngày 2026-08-28; Bronze chưa tồn tại | `orders` 113,993 trên 125,464; `categories` **144 trên 144, tức topic rỗng** (categories không đổi sau khi seed nên không có event mới nào) |
| 2026-10-05 | stack dừng từ khoảng 2026-09-25 (Docker Desktop restart) | **cả 7 topic bảng: earliest == latest**. Checkpoint của Bronze trỏ vào offset không còn tồn tại và stream lỗi `OffsetOutOfRange` |

Một Bronze stream bootstrap từ topic ở cả hai thời điểm sẽ không bao giờ thấy được dòng nào chỉ tồn
tại trong phần đã bị xoá. Ở lần đầu, nó sẽ không thấy nổi một category nào.

### What we do instead

- **Khôi phục:** một Debezium **BLOCKING ad-hoc snapshot** qua Kafka signal channel
  (`python mosaic.py resnapshot`). Cách này không cần thêm quyền ở nguồn và không cần sửa publication.
  Một *incremental* snapshot thì cần một bảng signal, tức là cần quyền `INSERT` mà GR1 Appendix A cố ý
  không cấp cho `cdc`. Đo ngày 2026-10-05: cả 7 bảng phát lại **đúng** số dòng ở nguồn (114,709 event
  `op=r`, 40.4 s), và Bronze nạp đủ từng event (`n = distinct keys = source rows` mỗi bảng).
- **Phát hiện:** Bronze stream giữ `failOnDataLoss=true` của Spark, nên một khoảng hở dẫn tới
  container crash, không bị bỏ qua âm thầm. Cơ chế kiểm cặp của nó từ chối khởi động khi checkpoint đã
  có tiến độ mà bảng thì không còn. `--reset-checkpoint` *đổi tên* checkpoint thay vì xoá.

### Impact on GR1's claims

Replay từ Kafka bị giới hạn bởi retention; **khả năng audit nằm ở Bronze, không nằm ở Kafka**, và điều
đó chỉ đúng khi Bronze nạp liên tục. Nên thêm điều kiện tiên quyết này vào §4.2. Retention dài hơn
(hoặc `retention.ms=-1` trên các topic CDC) sẽ nới rộng cửa sổ; ở đây không làm vậy, vì quan điểm
thiết kế của báo cáo là Bronze, chứ không phải broker, mới là system of record.

### Status

**Verified hai lần; việc khôi phục đã verified.** Ledger: `experiments/results/snapshot_events.jsonl`.

---

## A11. MySQL `DATETIME` arrives 7 hours off; the harmonisation moves to Silver

*(MySQL `DATETIME` đến lệch 7 tiếng; việc chuẩn hoá chuyển sang Silver)*

**Phát hiện ở M3.** Là hệ quả của A1, chỉ lộ ra khi các dòng được ép kiểu.

### What GR1 says

> **§4.4** — *"the `TimestampConverter` transform harmonizes the differing timestamp
> representations produced by MySQL and PostgreSQL into a single ISO 8601 format."*

Câu này đọc như chuyện **hình thức**: một định dạng thay vì hai.

### What actually happens

Thực ra đó là chuyện **ngữ nghĩa**. MySQL `DATETIME` không có múi giờ. Dưới
`time.precision.mode=connect`, Debezium gửi nó dưới dạng epoch milliseconds của **giờ đồng hồ treo
tường được hiểu như thể là UTC**. MySQL ở đây chạy `time_zone=SYSTEM` = `+07` (container
`TZ=Asia/Ho_Chi_Minh`), nên giá trị nằm ở tương lai bảy tiếng so với thời điểm nó mô tả:

```
after.created_at  = 1791200479000      -> đọc như UTC: 2026-10-05 11:41:19
source.ts_ms      = 1791175279628      -> 2026-10-05 04:41:19.628 UTC   (thời điểm commit)
chênh lệch        = 25,199 s  ~ 7 h
```

PostgreSQL `TIMESTAMPTZ` đến dưới dạng một thời điểm ISO-8601 đúng (`...Z`). Nếu không chuẩn hoá thì
mọi phép so sánh thời gian giữa hai nguồn (một đơn hàng với người dùng của nó, một lần bán với một lần
hết hàng) đều lệch đúng bằng offset UTC của nguồn.

### What we do instead

A1 đã bỏ chuỗi SMT nên `TimestampConverter` không bao giờ chạy. Silver làm việc chuyển đổi: mọi
timestamp nguồn không có múi giờ được đọc theo `MOSAIC_SOURCE_TZ` và lưu thành
`timestamp(6) with time zone` (kiểu decode `wallclock_ms` trong
`pipelines/transform/macros/cdc_current_state.sql`).

### Evidence

`python -m tools.lakehouse.reconcile` so Silver với nguồn sống, timestamp tính bằng epoch
microseconds. Với múi giờ đúng: **0 dòng khác** ở cả 7 bảng. Negative control với
`MOSAIC_SOURCE_TZ=UTC`: **mọi** dòng MySQL đều khác (33,605 / 64,556 / 27,837) trong khi các bảng
PostgreSQL vẫn 0. Phép chuyển đổi là thứ duy nhất làm hai bên khớp nhau.

### Impact on GR1's claims

`TimestampConverter` của §4.4 là **load-bearing**, không phải hình thức. Nên ghi rõ trong §4.4 rằng
datetime của MySQL không có múi giờ và cần múi giờ của nguồn để thành một thời điểm thực, dù việc
chuyển đổi chạy ở đâu.

### Status

**Verified, có negative control.** Ledger: `experiments/results/lakehouse_events.jsonl`.

---

## A12. §4.3's `gold.orders` partitioning is too fine for the prototype

*(Phân vùng `gold.orders` của §4.3 quá mịn cho prototype)*

**Phát hiện ở M3.** Không phải mâu thuẫn, mà là một đặc tả được thiết kế cho quy mô khác, và báo cáo
không nói ra quy mô đó.

### What GR1 says

> **§4.3** — *"orders are partitioned by `PARTITIONED BY (days(created_at), bucket(16,
> region))` to support both time-series and regional analytics"*.

### What actually happens

Đã implement đúng như vậy (`ARRAY['day(created_at)', 'bucket(region, 16)']`). Seed trải dài 370 ngày
và có tới 15 giá trị region. Lần build đầu **fail**: `ICEBERG_TOO_MANY_OPEN_PARTITIONS -- Exceeded
limit of 100 open writers`. Nâng giới hạn (D14) thì build được, và bảng như sau:

| `gold.orders` | giá trị |
|---|---|
| số dòng | 36,678 |
| số phân vùng | **2,944** |
| data file | 2,968, trung bình **12.6 dòng mỗi file**, tổng 10.5 MB |

So với cùng dữ liệu trong `silver.orders` không phân vùng:

| thao tác | có phân vùng (GR1) | không phân vùng | tỉ lệ |
|---|---|---|---|
| `ANALYZE` (bước sau-ghi bắt buộc của §4.5) | 8.25 s | 0.25 s | **33x** |
| full scan `count(*), sum(total_amount)` | 1.28 s | 0.09 s | **14x** |
| MERGE incremental ~600–2,600 orders (dbt) | 19–30 s | 1.4–2.5 s (Silver) | ~10x |

Thêm hai quan sát. `bucket(16, region)` trên một cột chỉ có mười hay mười lăm giá trị thì thực chất là
phân vùng theo giá trị (*identity partitioning*) cộng thêm vài bước thừa: mỗi giá trị rơi vào bucket
riêng và vài bucket để trống. Và ở mục tiêu 10M dòng của chính GR1 (§10.1), cùng spec cho ra khoảng
3,400 dòng mỗi phân vùng: khá hơn, nhưng file vẫn nhỏ hơn hẳn một megabyte.

### Impact on GR1's claims

Spec thực thi được và tạm thời được giữ như viết. Báo cáo nên bổ sung quy mô mà spec giả định, và việc
chi phí `ANALYZE` tăng theo số phân vùng, nghĩa là overhead §4.5 đã cảnh báo bị nhân lên bởi lựa chọn
này. Phân vùng theo tháng (hoặc theo ngày nhưng bỏ bucket region) sẽ hợp với prototype hơn; **đây là
quyết định còn để ngỏ**. E2 (Top-N pushdown trên Gold) là nơi nên chốt, ở quy mô `full`.

*(Bổ sung 2026-10-13: một lần build incremental thay đổi 0 dòng vẫn tốn 45–61 s cho `gold.orders`,
và chính bảng này đẩy heap Trino lên 6+ GiB trước khi heap bị giới hạn. Xem STATUS finding 15.)*

### Status

**Measured.** Câu query federation ở GR1 Listing 4.2 chạy trên bảng này trong 1.61 s và trả đủ 100
dòng.

---

## A13. At prototype scale the planner skips the HNSW index, so §4.5's comparison is vacuous unless pinned

*(Ở quy mô prototype, planner bỏ qua index HNSW; phép so sánh của §4.5 vô nghĩa nếu không pin đường đi)*

### What GR1 says (GR1 nói gì)

§4.5, Track 2: *"its experiment design sweeps the M and ef_search parameters, plots recall at k against
latency, compares the HNSW index against exact brute-force search, and reports the 50th-, 95th-, and
99th-percentile latency together with the memory overhead of the index."* Appendix A tạo index
`USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)` và gợi ý query
`ORDER BY e.embedding <=> :q`.

### What actually happens (thực tế)

Index HNSW được tạo đúng như Appendix A. Nhưng **có index không có nghĩa là index được dùng**: PostgreSQL
chọn access path theo cost model, và với 946 vector (quy mô `small`) thì sequential scan + sort rẻ hơn:

```
-- không pin (mặc định)
Limit  (cost=230.56..230.59 rows=10)
  ->  Sort  (Sort Key: embedding <=> $0)
        ->  Seq Scan on product_embeddings  (rows=946)

-- SET enable_seqscan = off
Limit  (cost=416.94..430.92 rows=10)
  ->  Index Scan using product_embeddings_hnsw  (Order By: embedding <=> $0)
```

Lần query đầu tiên của M4 in ra `plan uses hnsw index: False`. Nghĩa là "kênh ANN" lúc đó **thực chất
là tìm chính xác**, và nếu chạy thí nghiệm Track 2 như viết thì:

- đường "HNSW" và đường "exact" là **cùng một plan**, nên recall luôn bằng 1.0 và latency trùng nhau;
- sweep `ef_search` cho ra một **đường cong phẳng**, vì tham số đó chỉ có tác dụng khi index được dùng.

Kết quả trông giống "HNSW hoàn hảo", nhưng thật ra không đo được gì về HNSW.

### What we do instead (ta làm gì thay thế)

Mỗi đường đi **pin access path** của mình (`pipelines/embeddings/ann.py`, `_set_path`):
HNSW = `enable_seqscan = off`; exact = `enable_indexscan = off`. Thêm vào đó, mỗi lần đo đều chạy
`EXPLAIN` trước và **fail** nếu một đường đi không dùng đúng plan nó tuyên bố. Kênh ANN được định nghĩa
là xấp xỉ (§3.4), nên pin nó vào HNSW đúng với định nghĩa; ở quy mô lớn planner tự chọn HNSW, và việc
pin trở thành vô hại.

### Evidence (bằng chứng)

Sau khi pin: exact p50 0.58–0.85 ms (sequential scan xác nhận bằng plan), HNSW `ef_search` 10/40/100
p50 0.34–0.71 ms (index scan xác nhận bằng plan), recall@10 = 1.0 trên 100 query. Recall 1.0 lúc này **có
nghĩa**: HNSW đã thật sự chạy và trùng đáp án chính xác, điều dự kiến ở 946 dòng.

### Impact on GR1's claims

Thiết kế thí nghiệm Track 2 vẫn đúng, nhưng thiếu một điều kiện tiên quyết: **phải pin access path và
kiểm plan**. Nên ghi điều này thành một dòng trong phần phương pháp của E3. Chưa kiểm ở quy mô `full`
(100k vector) xem planner có tự chọn HNSW không: **unverified**.

### Status

**Fixed and verified** (M4, 2026-10-16). Ledger: `experiments/results/embedding_events.jsonl`, event
`recall`, trường `plans_ok`.

---

# Part B — Substitutions: cùng cam kết, công cụ nhẹ hơn

Mỗi mục giữ đúng điều GR1 *hứa*, và chỉ đổi thứ *thực hiện* lời hứa đó. Trong báo cáo, trình bày nhất
quán mọi mục là **"substitution, not omission"** (thay thế, không phải bỏ sót), và nêu tên thành phần
gốc dùng ở quy mô production.

---

## B1. Apache Ranger → Open Policy Agent

**GR1:** Ranger thực thi phân cấp năm role của Table 4.6 và column masking lúc query của Table 4.5
(Layer 2), qua plugin Trino.

**Vấn đề:** một Ranger chạy được gồm *bốn* thành phần: Ranger Admin, một database quan hệ phía sau,
Solr để index audit, và plugin Trino. Trên target Compose một máy của Appendix B, nó tranh bộ nhớ với
Kafka, Spark, Trino, MinIO và hai database, và theo kinh nghiệm thường tốn nhiều tuần trước khi policy
đầu tiên được đánh giá.

**Ta làm gì:** OPA đứng sau access-control provider `opa` của Trino. **Năm role của Table 4.6
(`data_admin`, `data_engineer`, `data_analyst`, `ml_engineer`, `bi_user`) được viết nguyên văn bằng
Rego.** Column masking vẫn chạy lúc query và phụ thuộc role, đúng như Table 4.5 mô tả.

**Giữ được:** policy tập trung; kiểm soát truy cập ở mức catalog/schema/table/column; masking cột lúc
query; audit trail cho các quyết định.

**Được thêm, và đây là một lập luận thật cho buổi bảo vệ:** policy trở thành **văn bản thuần, quản lý
phiên bản được, unit test được**. `opa test policies/` chạy phân cấp role như một test suite trong CI.
Đó là bằng chứng mạnh hơn ảnh chụp màn hình giao diện admin. Bốn container còn một.

**Mất:** giao diện web soạn policy của Ranger (không phải yêu cầu của dự án), và tích hợp tag-sync với
catalog. Cái thứ hai quan trọng với N3, và *đó là lý do* mô hình tag do `platform/governance/` sở hữu
rồi giao cho OPA dưới dạng policy data. Chính điều đó làm N3 thành một đóng góp thay vì một bài tập cấu
hình.

Lập luận đầy đủ: [ADR 0001](adr/0001-opa-instead-of-ranger.md).

---

## B2. DataHub → OpenLineage + Marquez

**GR1 §3.6:** DataHub cung cấp catalog và đồ thị lineage mức cột, nhận lineage do Airflow và Spark phát
ra **theo định dạng OpenLineage**.

**Quan sát quyết định:** GR1 định nghĩa yêu cầu lineage theo **đường phát OpenLineage**, không theo
DataHub. DataHub chỉ là một *bên nhận* khả dĩ.

**Vấn đề:** một DataHub production gồm năm service (GMS, Elasticsearch, frontend, và các consumer
MAE/MCE) cộng các Kafka topic riêng. Chính comment trong Appendix B đã thừa nhận: *"Kept minimal here
for GR1."*

**Ta làm gì:** giữ đường phát **đúng như thiết kế**, và nhận nó bằng **Marquez**, bản implement tham
chiếu của OpenLineage: một service cộng một schema Postgres.

**Giữ được:** tự động bắt lineage từ Spark và dbt mà không phải sửa code từng pipeline; đồ thị lineage
duyệt được; metadata mức dataset.

**Được thêm:** mô hình dataset-version của Marquez ghi một facet cho mỗi lần chạy, và Freshness
Registry (C2/N1) đọc **trực tiếp** từ đó thay vì phải tự dựng lại. Lựa chọn nhẹ hơn này *ăn khớp hơn*
với thành phần mới của đề tài, chứ không chỉ rẻ hơn.

**Mất, cần nói thẳng:** bề mặt catalog phong phú hơn của DataHub (glossary, ownership, giao diện tìm
kiếm) và lineage **mức cột** của nó. dbt cho lineage mức cột ở chặng Silver/Gold, nhưng **chặng Bronze
vẫn chỉ ở mức bảng.** Ghi đây là giới hạn đã biết.

**Đảo ngược được:** vì phía phát không đổi, chuyển bên nhận về lại DataHub chỉ là đổi cấu hình, đúng
thuộc tính mở rộng mà GR1 §2.3 yêu cầu.

Lập luận đầy đủ: [ADR 0002](adr/0002-marquez-instead-of-datahub.md).

---

## B3. Spark for Silver/Gold + Great Expectations → dbt-trino

**GR1 Fig. 4.1 / §4.2:** tầng xử lý có ba engine (Spark Structured Streaming, dbt, và Great
Expectations), với Spark nắm toàn bộ đường ghi (append vào Bronze, merge/upsert vào Silver và Gold).

**Quan sát:** đó là hai công việc khác nhau.

- **Nạp Bronze thực sự là streaming**: tiêu thụ topic Debezium liên tục, chuẩn hoá envelope, khử trùng
  theo source offset, append.
- **Silver và Gold là các phép toán tập hợp theo batch** trên những bảng đã nằm trong Iceberg, và
  **Trino vốn đã ghi được Iceberg qua `MERGE INTO`.**

**Ta làm gì:**

| Tầng | Engine |
|---|---|
| Bronze | **Spark Structured Streaming**, không đổi. Streaming xứng đáng có mặt ở đây. |
| Silver, Gold | **dbt model biên dịch thành SQL Trino**, ghi Iceberg qua `MERGE INTO` |
| Quality gates | **khối `tests:` của dbt**, thay Great Expectations |

**Giữ được:** ngữ nghĩa medallion của §4.2 đúng y nguyên. Bronze giữ envelope chưa sửa; Silver được
khử trùng, ép kiểu, mask, kiểm tra; Gold được join, tổng hợp, phân vùng theo §4.3. **Spark vẫn ở trong
kiến trúc và trong báo cáo.**

**Được thêm:** Silver/Gold trở thành SQL khai báo thay vì job Spark mệnh lệnh: ít code hơn, và lineage
suy ra được miễn phí từ DAG. Không cần một runtime nặng thứ hai cho việc batch.

**Về Great Expectations:** *chức năng* của nó còn nguyên dưới dạng dbt test: unique trên khoá khử
trùng, not-null trên `_src_ts`, và **các phép kiểm tham chiếu chéo nguồn mà §4.3 nói không engine đơn
lẻ nào cưỡng chế được** (ví dụ `user_id` của một order so với bảng `users` bên PostgreSQL). Bộ từ vựng
expectation dư thừa và phần data-docs của GE bị mất; mọi phép kiểm §4.3 thực sự đặc tả đều diễn đạt
được.

**Ghi chú về `ANALYZE`:** GR1 §4.5 yêu cầu `ANALYZE` sau mỗi lần ghi batch để optimizer chi phí của
Trino đúng. Việc này trở thành **dbt post-hook** thay vì một task Airflow lên lịch riêng. Overhead vận
hành mà §4.5 cảnh báo vẫn có thật, nhưng giờ gắn vào chính lần ghi gây ra nó thay vì gắn vào một bộ hẹn
giờ.

Lập luận đầy đủ: [ADR 0003](adr/0003-dbt-trino-for-silver-gold.md).

**Status (M3): verified.** `MERGE INTO` trên Iceberg do Nessie quản lý, rủi ro kỹ thuật chính mà ADR
0003 mang theo, đã chạy bốn lần build incremental dưới các lần ghi đồng thời (insert cộng mọi kiểu
update load generator tạo ra). Sau đó Silver khớp nguồn sống từng dòng trên mọi cột được so, ở cả 7
bảng (`tools/lakehouse/reconcile.py`). Trino commit mỗi MERGE dưới dạng `overwrite` kèm position-delete
file (merge-on-read). 67 dbt test thay Great Expectations, gồm cả hai phép kiểm tham chiếu chéo nguồn
§4.3 nêu tên, và `ANALYZE` chạy như post-hook trên mọi model Gold.

---

## B4. Avro + Confluent Schema Registry → JsonConverter

Trình bày đầy đủ ở **A2**, vì đây trước hết là một finding, sau đó mới là một substitution.

---

# Part C — Additions: những gì GR1 không có

GR1 giao một bản thiết kế. Đây là những thứ đang được **xây** mà báo cáo chỉ mới đề xuất hoặc chưa hề
nhắc tới. **Đóng góp của đề tài nằm ở đây.**

---

## C1. A stated problem, and a four-rung abstraction ladder

**Khoảng trống:** GR1 mô tả kiến trúc rất kỹ nhưng **chưa bao giờ phát biểu một bài toán cụ thể** mà
nền tảng tồn tại để giải. Nó tạo động lực ở mức trừu tượng ("fragmentation carries a direct cost") và
hình thức hoá ở §5.3, nhưng không có một trường hợp cụ thể, đang hỏng, đo được.

Điều này quan trọng vì câu hỏi đầu tiên của giảng viên hướng dẫn là *"is the final product a platform
or a tool/API?"*, một câu hỏi không thể trả lời thuyết phục chỉ bằng một sơ đồ kiến trúc.

**Ta thêm:** [`docs/PROBLEM.md`](PROBLEM.md): nêu tên một câu query bị hỏng rồi leo từ đó lên một
nền tảng tổng quát qua bốn bậc, mỗi bậc có một artifact và một **phép thử máy móc**:

| Bậc | Câu hỏi | Artifact | Phép thử |
|---|---|---|---|
| **L0** | Cụ thể cái gì đang hỏng? | câu query hỏng | lỗi tái hiện được theo yêu cầu |
| **L1** | Mô hình hoá trường hợp hỏng thế nào? | `domains/ecommerce/` | lỗi *đo được* |
| **L2** | Phần nào không liên quan tới thương mại? | `platform/` | lỗi *được sửa* |
| **L3** | Sự trừu tượng là thật hay chỉ là lời nói? | `domains/jobmarket/` | domain thứ hai chạy, không sửa code |

**Lỗi L0, cụ thể:** *"Show the 10 products most relevant to shopper X, right now."* Ba thông tin, ba
hệ thống, ba mức tươi:

| Thông tin | Ở đâu | Độ cũ |
|---|---|---|
| Liên quan ngữ nghĩa | `product_embeddings` (pgvector) | **hàng giờ**: lịch refresh embedding |
| Sở thích mua, xếp hạng doanh thu | `iceberg.gold.*` | **vài phút**: độ trễ CDC + batch |
| Có mua được không? (tồn kho, giá) | MySQL/PostgreSQL sống | **vài giây** |

Gộp hai cái đầu bằng RRF thì: **một sản phẩm đã hết hàng năm phút trước vẫn đứng #1.** Điểm RRF chỉ
phụ thuộc vào vị trí,

```
RRF(d) = SUM_i  1 / (k + rank_i(d))
```

nên **không có số hạng nào cho độ cũ của dữ liệu đằng sau một bảng xếp hạng**, và hàm fusion không thể
phạt một kênh cũ vì đã tự tin sai.

**Đo bằng:** *stale-hit rate@10* (chính) và *nDCG@10 vs oracle* (phụ). Oracle là bảng xếp hạng lý
tưởng tính trực tiếp trên OLTP sống lúc query, nên tươi tuyệt đối theo định nghĩa. nDCG chống lại bản
sửa suy biến kiểu "chặn hết mọi thứ".

---

## C2. N1 — the Freshness Registry

**Trạng thái trong GR1:** không tồn tại. Nhưng **không có nó thì không tính được GR1 Eq. 5.1.**

Phương trình 5.1 cần `last_cdc(d)` và `last_refresh(d)`, tức là dữ liệu đằng sau một item cụ thể cũ
bao nhiêu. Hiện các mảnh có sẵn nhưng **chưa bao giờ được nối**: Iceberg có snapshot, Debezium có
offset, catalog có lineage. Không cái nào trả lời *"dữ liệu đằng sau **câu trả lời này** cũ bao
nhiêu?"*

**Ta xây:** một mặt phẳng watermark query được duy nhất, phủ bảng Iceberg, vector index và nguồn sống.
Schema đã deploy trong [`sql/platform.sql`](../sql/platform.sql) và query được từ Trino qua catalog
`platform`.

Các collector, mỗi cái cho một nguồn sự thật:

| Collector | Đọc từ | Cho ra |
|---|---|---|
| CDC | Kafka Connect offsets + `source.ts_ms` trong envelope | mỗi connector đang chậm bao xa |
| Table | Iceberg snapshot summary (`max(_src_ts)` lúc commit) | mỗi bảng đã materialise tươi tới đâu |
| Vector | `row_timestamp_column` khai báo của index | vector index tươi tới đâu |

**Một quyết định thiết kế đáng bảo vệ:** watermark theo từng entity **không được materialise**, vì
cách đó không scale quá vài triệu dòng. Thay vào đó mỗi dataset khai báo
`platform.datasets.row_timestamp_column`, là cột *đã* mang thời gian nguồn của từng dòng, và channel
query đọc nó trực tiếp. **Chỉ watermark mức dataset được lưu.**

**Cố ý xây trước N2**, vì không có nó thì fusion engine không đánh giá được.

*(Cập nhật M3: watermark của bảng do Trino ghi, tức Silver/Gold, phải đọc từ dữ liệu chứ không từ
snapshot summary, xem C8 và STATUS finding 12.)*

*(Cập nhật M4: `python mosaic.py register` đã đăng ký 14 dataset vào `platform.datasets` (Bronze, 7
Silver, 5 Gold, 1 vector), mỗi dataset được probe thật trước khi ghi, theo kiểu tất cả hoặc không gì cả.
Lần probe đầu cho thấy **bài toán nguồn nhàn rỗi** mà collector M5 phải giải: Bronze trễ 20 s vì heartbeat
CDC vẫn chảy qua nó, trong khi mọi bảng Silver/Gold "trễ" 11 ngày chỉ vì nguồn không có thay đổi nào.
Watermark theo event-time không phân biệt được "đứng yên" với "kẹt". Xem
[ADR 0006](adr/0006-m4-embedding-pipeline.md), M4-A3, M4-B4.)*

---

## C3. N2 — the freshness-aware fusion engine

**Trạng thái trong GR1:** §5.3 ghi rõ là *"a candidate research direction… not claimed as completed
work within GR1."* Nó đưa ra Eq. 5.1 (staleness) và Eq. 5.2 (freshness-weighted fusion) như một
**khung**, cố ý để trống dạng hàm decay `g(·)` và các trọng số α, β.

**Ta xây:** phần implement và phần đo lường.

| Policy | Hành vi |
|---|---|
| `rrf` | baseline của Cormack et al. (2009): chỉ dựa vào vị trí, không có số hạng freshness |
| `weighted` | trọng số cố định cho từng kênh trên điểm đã chuẩn hoá |
| `freshness_weighted` | **GR1 Eq. 5.2**: điểm của mỗi kênh được nhân với hệ số decay `g(·)` theo độ cũ của kênh đó. `g` vẫn pluggable (tuyến tính, mũ) vì *xác định dạng của nó chính là phần việc thí nghiệm*, chưa phải kết quả đã chốt |

Mọi kết quả đều mang **provenance** (nguồn gốc): kênh nào đóng góp, xếp hạng bao nhiêu trong từng kênh,
độ cũ quan sát được, hệ số decay đã áp. Provenance làm cho khác biệt giữa các policy *đọc được* trong
năm phút mà giám khảo dành cho một demo, và đó cũng là thứ thí nghiệm E4 đọc lại.

**Baseline thứ hai, chốt ở M4 (ADR 0006, M4-A2): "post-filter trên nguồn sống".** Câu phản biện tự
nhiên nhất với cả đề tài là: *"sao không lọc kết quả ANN theo tồn kho sống lúc query?"*. E4 sẽ trả lời
bằng số đo, không bằng lời. Post-filter là câu trả lời đúng cho **một domain biết rõ fact nào hay đổi**.
Nó cần tri thức domain trong runner (cột nào nghĩa là "còn bán được"), thêm một lượt gọi về OLTP mỗi
query, và một bộ lọc riêng cho từng loại fact nhanh. Freshness-weighted fusion claim một thứ khác: một
cơ chế **generic**, chỉ cần watermark. E4 phải cho thấy cái giá của sự generic (nDCG thấp hơn
post-filter bao nhiêu), chứ không giả vờ rằng không có cái giá đó.

**Kênh ANN đã có từ M4, và nó "sai" theo đúng cách L0 mô tả.** Index chỉ chứa thực thể **đủ điều kiện
lúc refresh** và lưu kèm ảnh chụp các fact nhanh (M4-A1). `embed-verify` đã tái hiện L0 end-to-end:
stockout kết quả #1 (sản phẩm 342, tồn kho sống 160 → 0), index vẫn trả nó ở #1 với `stock_qty=160`;
build lại Gold và refresh index thì nó biến mất.

---

## C4. N3 — tag governance that reaches the vector index

**GR1 mở ra khoảng trống này nhưng không đóng nó lại.**

§1.1 nêu động lực rõ ràng: *"personally identifiable information can leak through embeddings that are
not subject to the same masking and access-control policies as relational tables."* (PII có thể rò qua
embedding, vốn không chịu cùng chính sách masking và phân quyền như bảng quan hệ.)

Nhưng hai lớp masking ở §4.6 **đều dừng ở ranh giới quan hệ**: Layer 1 mask record Kafka, Layer 2 mask
cột Trino. **Không lớp nào chạm tới vector store.** Khoảng trống GR1 chỉ ra trong phần động lực vẫn mở
trong chính thiết kế của nó.

**Ta xây:**

- **Tag, không phải tên cột.** Một policy liệt kê tên cột phải viết lại cho từng domain, khiến
  governance phụ thuộc domain và phá claim L3. Policy được gắn vào tag (`sensitive`, `restricted`,
  `public`) đặt trên các cặp `(dataset, column)`.
- **Lan truyền.** Tag đi theo các cạnh `platform.column_lineage`, nên một tag khai báo một lần ở nguồn
  sống sót qua mọi tầng medallion mà không phải khai báo lại.
- **Chốt chặn.** Trước khi một embedding job đọc một cột text, nó hỏi cột đó có mang tag hạn chế không,
  trực tiếp hay do lan truyền. Nếu có, **job từ chối chạy.**

Điều đó biến *"PII không thể vào vector index"* từ một quy ước thành một thuộc tính được cưỡng chế.

*(Cập nhật M4: **chốt chặn đã tồn tại và đã được nối vào** job embedding duy nhất hiện có
(`platform/governance/guard.py`). Negative control đã chạy: gắn `sensitive` lên
`gold.product_catalog.description` thì refresh thoát với mã 2 (REFUSED) và index không bị đụng tới; gỡ
tag thì pass. **Lan truyền tag theo lineage chưa có (M8)**: hiện guard chỉ chặn được những gì được ghi
thẳng vào `column_tags`, và mỗi lần chạy nó in ra số tag đang biết để một kho tag rỗng không bị đọc nhầm
thành "đã an toàn". ADR 0006, M4-B3.)*

---

## C5. N4 — freshness-SLA query routing *(stretch)*

Không có trong GR1. Là hệ quả tự nhiên của N1: một request khai báo **freshness SLA chặt hơn**
watermark của lakehouse có thể được **định tuyến tới nguồn sống** thay vì Gold. Nó làm mặt phẳng
freshness trở nên *hành động được* chứ không chỉ quan sát được. Đây rõ ràng là mục stretch (làm nếu
còn thời gian).

---

## C6. A second domain as mechanical proof of genericity

**Trạng thái trong GR1:** không có. GR1 khẳng định MOSAIC là một platform nhưng không đưa ra cách nào
để kiểm.

**Ta thêm:** `domains/jobmarket/` (tin tuyển dụng và ứng viên), chạy trên **đúng các binary đó**. Thêm
nó chỉ cần thêm DDL nguồn và một `retrieval.yaml`, không gì khác.

Nó có cùng *hình dạng* và không dùng chung *từ* nào: mô tả công việc là kênh ngữ nghĩa chậm; "còn
tuyển", số lượng tuyển, điều chỉnh lương là các thông tin giao dịch nhanh. Lỗi là cùng một lỗi và dễ
hình dung như vậy: gợi ý một vị trí đã tuyển xong một giờ trước.

**Hai phép thử máy móc quyết định sự trừu tượng có thật hay không:**

```bash
python mosaic.py check-abstraction          # không có từ vựng domain trong platform/
python mosaic.py test-domain --domain jobmarket
```

Nếu cả hai pass, câu *"đây là platform hay tool?"* không còn là claim về ý định mà trở thành **một quan
sát về hành vi**. Đây là phần bổ sung có giá trị cao nhất so với GR1.

---

## C7. The `platform/` ÷ `domains/` split, enforced

Sự trừu tượng được làm thành thứ **có cấu trúc và grep được**, không chỉ được ghi trong tài liệu.
`platform/` là lõi chung; bất cứ thứ gì biết product là gì đều nằm dưới `domains/`.

Được cưỡng chế bởi một cổng CI quét `platform/` tìm 12 từ thuộc **cả hai** domain, vì lõi không được
biết về domain nào:

```
$ python mosaic.py check-abstraction
scanned 6 files under platform/ for 12 domain terms
PASS -- the generic core contains no domain vocabulary.
```

Tri thức domain đi vào lõi **chỉ dưới dạng dữ liệu**: một `retrieval.yaml` đã parse, một dòng trong
`platform.datasets`, một tag trong `platform.column_tags`. Định danh đến lúc runtime là những chuỗi mờ
(*opaque strings*): lõi xử lý chúng mà không cần hiểu nghĩa.

---

## C8. The `_src_ts` / `_ingest_ts` convention

**Không có trong GR1.** Mọi dòng Silver và Gold mang `_src_ts` (thời điểm commit ở nguồn) và
`_ingest_ts`, và mọi Iceberg commit ghi `max(_src_ts)` vào snapshot summary của nó.

**Vì sao là quy tắc cứng:** N1 **không thể bổ sung về sau** nếu một pipeline bỏ qua quy ước này. Không
có cách nào khôi phục, sau khi đã ghi, thời điểm commit nguồn của một dòng. Vì vậy nó được làm ngay ở
M3.

**Status (M3).** Bronze: mọi dòng mang `_src_ts` (`source.ts_ms` trong envelope) và `_ingest_ts` (một
hằng số mỗi micro-batch), và mọi commit đóng dấu `mosaic.max_src_ts[_ms]` (tổng thể và theo từng topic)
cùng `mosaic.ingest_ts_us` vào Iceberg snapshot summary. Cái sau nối mỗi dòng với snapshot đã publish
nó, và đó là cách đo KPI latency end-to-end. Silver và Gold: mọi dòng mang cả hai cột, định nghĩa là
của **thay đổi đóng góp mới nhất** (not-null được dbt test cưỡng chế). Nửa snapshot-summary thì
**không** áp được cho Silver/Gold: Trino không có cách ghi thuộc tính summary tuỳ biến (STATUS finding
12), nên N1 sẽ đọc watermark của các bảng do Trino ghi từ chính dữ liệu.

---

## C9. Seed and load generators as first-class deliverables

**GR1 §10.1** nhắc *"Faker + TPC-H patterns"* và mục tiêu 10M dòng trong đúng một dòng. Hai việc cần
nghĩ kỹ hơn thế:

**Corpus phải có ngữ nghĩa thật.** Mô tả được **ghép từ bộ từ vựng theo từng category**, không phải
text giả. Với lorem ipsum, embedding không mang cấu trúc gì, kênh ANN thành nhiễu, và cả lập luận về
freshness sụp đổ, vì **không có gì hợp lý để kênh cũ tự tin sai về nó.** Đã kiểm: *"waterproof hiking
backpack"* trả về dry bag, bếp cắm trại và gậy trekking, không có gì từ nhóm Kitchen.

Tên, mô tả và category lá được sinh **cùng nhau**, vì sinh độc lập từng tạo ra những item như
*"Minimalist Kettle"* được mô tả là nồi. Console hiển thị tên, còn xếp hạng dùng embedding của mô tả,
nên truy hồi đúng lại *trông như* sai.

**Tính tái lập là yêu cầu, không phải tiện ích.** Việc sinh là tất định với `(scale, seed, now)`, đều
ghi trong `manifest.json`. E3/E4 phải chạy lại trên một corpus giống hệt. Một phiên bản đầu đọc giờ hệ
thống ở cấp module, nên cùng seed cho ra timestamp khác nhau mỗi lần chạy. Lỗi này được bắt bởi một
phép so sánh từng byte, và phép so sánh đó giờ vẫn canh.

**Load generator** (`tools/loadgen/`, xây ở M1) tạo ra các lần ghi OLTP liên tục cộng **công tắc tiêm
lỗi hết hàng**. Nó là thứ *tạo ra* sự bất đối xứng freshness mà cả luận văn đo, nên nó là một
deliverable chứ không phải test fixture.

Ba điều nó phải làm đúng, và cả ba đều từng sai trước:

*Nhập kho được kích hoạt bởi mức thấp và có thời gian chờ.* Phiên bản đầu nhập cho bất kỳ sản phẩm nào
bán chạy, bất kể còn bao nhiêu. Vì nhu cầu chỉ lấy đi 1–3 đơn vị còn một lần nhập thêm 20–200, tồn kho
ở đầu catalogue phình không giới hạn (2,664 đơn vị so với seed 500), và **không gì có thể hết hàng một
cách tự nhiên**, mà đó lại là hành vi duy nhất corpus tồn tại để tạo ra. Phiên bản thứ hai nhập ngay
mọi thứ đang thấp, và xoá sạch phần cố ý để trống của seed trong một phút. Nhập kho thật thì phải chờ:
một sản phẩm hết hàng sẽ hết trong `RESTOCK_LEAD_SECONDS`, và **khoảng đó chính là cửa sổ mà một bảng
xếp hạng cũ tự tin sai.**

*Throughput được báo phải là tốc độ nguồn duy trì được, không phải tốc độ Python phát ra.* Các câu lệnh
được ghi vào một pipe, nên một generator không throttle sẽ báo bất cứ tốc độ nào bên gửi đạt được. Một
lần chạy báo 549 ops/s trong khi MySQL vẫn đang commit phần việc đó **152 giây sau**, và một lần trước
đó âm thầm mất 5,327 orders khi client bị kill lúc đang xả. Generator giờ đo số dòng đã commit trên một
connection riêng, throttle theo con số đó, và in riêng tốc độ *sent* và *sustained*. Đo trên máy này:
**khoảng 100 ops/s sustained**, bị giới hạn bởi fsync mỗi commit dưới Docker Desktop.

*Hiệu ứng được đọc lại, không bao giờ giả định.* Mỗi lần chạy kết thúc bằng việc đếm lại số dòng ở cả
hai engine và so với số đã gửi. Chính phép kiểm này đã bắt được cả hai lỗi trên; không có nó thì lần
chạy đã báo thành công hai lần.

*Không gì trong hỗn hợp tải chạm vào `products.name` hay `products.description`.* Đầu vào của kênh ngữ
nghĩa được cố ý giữ yên, để một stale hit chỉ có thể quy cho sự bất đối xứng freshness, chứ không phải
do text đã thay đổi dưới chân embedding.

---

# Part D — Operational changes (thay đổi vận hành)

Mang tính cơ học, không ảnh hưởng claim của kiến trúc, nhưng là lý do stack chạy được.

| # | Thay đổi | Lý do |
|---|---|---|
| **D1** | Task runner `mosaic.py` thay `make` | Máy không cài `make`. Chỉ dùng thư viện chuẩn Python nên chạy giống nhau trên Windows/macOS/Linux. Makefile uỷ quyền cho nó ở nơi có make. |
| **D2** | `up` đợi **healthy**, không chỉ **started** | `compose up -d` trả về ngay khi container *khởi động*. Kafka Connect nhận kết nối TCP từ rất lâu trước khi nhận connector. Mọi thứ phía sau phụ thuộc vào khác biệt đó. |
| **D3** | Compose profile: `core` / `gov` / `orch` / `console` | Stack đầy đủ của Appendix B không vừa thoải mái trên một máy. Profile mặc định là lát cắt dọc tối thiểu; governance, orchestration và giao diện demo là tuỳ chọn. |
| **D4** | Mọi port publish ra host được tham số hoá qua `docker/.env` | Cổng 9001 trên máy này bị một container `lakehouse-control-plane` không liên quan chiếm. Console MinIO chuyển sang 9101. **Port bên trong container không bao giờ đổi**, nên không có gì về kiến trúc bị ảnh hưởng. |
| **D5** | Metadata của platform nằm trong DB riêng `mosaic_platform`, cộng catalog Trino thứ 4 `platform` | Giữ trạng thái của N1 ngoài DB domain, vì DB domain là **nguồn CDC**: bảng platform đặt ở đó sẽ bị `mosaic_pub` capture và làm bẩn luồng thay đổi. Catalog thêm cho phép join một watermark với chính dữ liệu nó mô tả **trong một câu query Trino**. |
| **D6** | DDL domain và cấu hình connector đặt dưới `domains/<domain>/`, không ở `sql/` và `connectors/` cấp ngoài | Giữ câu *"thêm một domain chỉ chạm vào `domains/`"* **đúng theo nghĩa đen**, đó là thứ chứng minh L3 (C6) dựa vào. |
| **D7** | Đặt rõ `decimal.handling.mode=string`, `time.precision.mode=connect` | Mặc định của Debezium cho `DECIMAL` là `precise`, phát ra bytes mã hoá base64, không dùng được ở downstream nếu không xử lý đặc biệt. `string` giữ `DECIMAL(12,2)` chính xác và đọc được; Silver cast ngược lại. `connect` tránh các biểu diễn số nguyên microsecond mà Iceberg không ánh xạ trực tiếp được. GR1 không đặc tả. |
| **D8** | Không dùng `ExtractNewRecordState` ở connector | Hệ quả của A1: Bronze giữ envelope đầy đủ; việc làm phẳng chuyển sang Bronze→Silver. |
| **D9** | Bulk load MySQL chạy bằng `root`, không bằng `cdc` | `LOAD DATA INFILE` phía server cần **quyền global `FILE`**. `cdc` cố ý chỉ giữ các quyền replication tối thiểu của GR1 Appendix A. Cấp `FILE` cho tài khoản replication chỉ để một lần seed chạy được sẽ phá chính điều phụ lục đó cẩn thận giữ gìn. **Seed là một thao tác của người vận hành, nên dùng tài khoản người vận hành.** |
| **D10** | `REPLICA IDENTITY FULL` trên `users`, `products`, `inventory` | Bản sửa cho A6: không có nó thì PostgreSQL không phát before image. Chỉ áp cho ba bảng vì `FULL` làm WAL tăng theo độ rộng dòng. **Không áp hồi tố** cho volume đã tồn tại; với database đang chạy phải `ALTER` bằng tay. *(Dòng này thiếu trong bảng của bản gốc dù có trong Summary; đã bổ sung.)* |
| **D11** | Silver được **sinh tự động**, mỗi bảng capture một model, bởi `pipelines/transform/assemble.py`; Gold viết tay trong `domains/<domain>/gold/` rồi được chép vào | Việc của Silver (khử trùng, ép kiểu, latest-wins) chỉ phụ thuộc vào khoá, các cột và cách mã hoá của Debezium cho mỗi bảng, đều suy ra được từ cấu hình connector và catalog sống. Viết tay bảy model gần giống hệt nhau sẽ đưa từ vựng domain vào `pipelines/` và buộc domain thứ hai phải sửa nó. Sinh tự động thì **thêm domain vẫn chỉ chạm vào `domains/`** (C6). Một domain giờ là năm thứ: `ddl/`, `connectors/`, `gold/`, `retrieval.yaml`, `tags.yaml`. |
| **D12** | Silver = một **bảng trạng thái hiện tại không phân vùng** cho mỗi bảng capture, latest-wins theo vị trí nguồn, soft delete (`_is_deleted`) | GR1 §4.3 nói Silver *"partitioned by source system and date to simplify replay"*. Source system đã ngầm định trong bố cục mỗi bảng một Silver, và replay ở đây là việc của Bronze (A10). Phân vùng theo ngày trên một bảng được update tại chỗ sẽ khiến mỗi dòng được update phải chuyển phân vùng ở mỗi lần MERGE. **Có thể xem lại** nếu một consumer của Silver cần pruning. |
| **D13** | dbt chạy trong image riêng (`dbt-core 1.8.8`, `dbt-trino 1.8.3`), sau compose profile `transform`, qua `python mosaic.py dbt` | Python trên host (3.14) mới hơn mức dbt-core hỗ trợ, và một image đã pin khiến mọi lần build dùng cùng một dbt. Là job chạy một lần nên `up` không bao giờ đợi nó. |
| **D14** | Trino `iceberg.max-partitions-per-writer=5000` (mặc định 100) | Bắt buộc để chạy được partition spec `gold.orders` của GR1 §4.3 (A12). |
| **D15** | `tools/loadgen` lấy region từ `tools/seed/vocab.py` | Hai generator dùng hai bộ từ vựng region riêng (tên thành phố vs. `north`/`central`/…), nên mọi số tổng hợp theo region bị tách đôi, chỉ thấy được khi Gold group theo region. Đã sửa từ nay về sau; các dòng đã ghi giữ giá trị cũ. |
| **D16** | `product_embeddings` do **pipeline** tạo lại ở mỗi refresh (build bảng mới, swap nguyên tử), không còn nằm trong DDL nguồn. **Bỏ FK** `REFERENCES products(id)`. Thêm cột snapshot (`stock_qty`, `price`) và `embedded_at`. Thêm Gold `product_catalog` làm nguồn | GR1 Appendix A khai báo bảng trong DDL nguồn với FK tới `products`. (1) Một index *dẫn xuất* không bao giờ được phép chặn một lần ghi vào system of record: xoá một sản phẩm đang được index sẽ lỗi. (2) Index được *thiết kế* để mô tả sản phẩm như lúc refresh (M4-A1). (3) Publish nguyên tử cần build bảng mới rồi swap, nên định nghĩa bảng phải nằm ở pipeline. `gold.product_sales` không có mô tả hay tên category nên thêm `product_catalog`, đúng chữ *"Gold-layer product catalog"* của §4.1. Bảng vẫn **ngoài** `mosaic_pub` (đã kiểm). Xem [ADR 0006](adr/0006-m4-embedding-pipeline.md). |
| **D17** | Khôi phục checkpoint Bronze bị rách: cất `offsets/1890–1891` và `commits/1890–1891` (đều **0 byte**) sang `checkpoints/bronze.torn-2026-10-16/` | Docker Desktop dừng cứng ngày 2026-10-16. Khi bật lại, stream chết với `Failed to read log file .../offsets/1891. Incomplete log file`. Bốn file đã được rename vào chỗ nhưng nội dung chưa từng được flush xuống đĩa: checkpoint trên named volume **không bền khi VM bị dừng cứng**. Cặp nguyên vẹn gần nhất là 1889, nên Spark chạy lại từ batch 1890. Idempotency key của Bronze hấp thụ phần đã ghi: *"2 already in Bronze"*. Đây là một lần nữa chứng minh exactly-once, lần này đến từ sự cố thật chứ không phải test. Cất đi, không xoá, cùng tinh thần với `bronze.stale-2026-09-23`. |

### A debugging note worth keeping (D9)

MySQL báo thiếu quyền `FILE` như sau:

```
ERROR 1045 (28000): Access denied for user 'cdc'@'%' (using password: YES)
```

Đọc vào thì **y hệt** sai mật khẩu, nhưng không phải. Ba đường kết nối khác đều xác thực bình thường;
chỉ câu `LOAD DATA INFILE` lỗi. Tốn một vòng debug, đáng một chú thích nếu viết về bài học vận hành.

---

# Part E — What did **not** change (những gì KHÔNG đổi)

Khoanh vùng phần thay đổi là quan trọng: nó cho thấy thiết kế GR1 phần lớn vẫn đứng vững khi gặp thực
tế.

| Phạm vi | Trạng thái |
|---|---|
| **Kiến trúc lakehouse sáu tầng** (§4.1) | Không đổi |
| **Nguồn**: MySQL 8.0 (binlog) + PostgreSQL 15 (WAL), PostgreSQL kiêm hai vai trò với pgvector | Không đổi |
| **Mô hình dữ liệu** (Appendix A) | Không đổi, DDL dùng nguyên văn |
| **Điều kiện CDC của MySQL**: `binlog_format=ROW`, `binlog_row_image=FULL`, GTID | Không đổi, đã kiểm trên hệ thống sống |
| **Điều kiện CDC của PostgreSQL**: `wal_level=logical`, `mosaic_pub`, replication slot | Không đổi, đã kiểm trên hệ thống sống |
| **WAL-accumulation guard**: heartbeat + theo dõi `confirmed_flush_lsn` (§4.4) | Cơ chế không đổi; cấu hình phải sửa để nó có tác dụng (A9) |
| **Kafka 3.7 KRaft**, không ZooKeeper | Không đổi |
| **Iceberg trên MinIO**, catalog Nessie | Không đổi (sửa registry, A4) |
| **Ngữ nghĩa medallion Bronze/Silver/Gold** (§4.2) | Không đổi |
| **Chiến lược phân vùng** (§4.3) | Gold: implement như viết (nhưng xem A12, F3); Silver: xem D12 |
| **Trino 451** làm tầng federation, quy trình `ANALYZE` (§4.5) | Không đổi |
| **pgvector HNSW**, `m=16, ef_construction=64` (§3.4) | Không đổi |
| **Phân loại top-*k* hai nhánh** (§5.2) | Không đổi, được dùng làm cấu trúc thí nghiệm |
| **Năm KPI CDC** (Table 4.4) | Không đổi |
| **Phân cấp role Table 4.6** | Nội dung không đổi; engine cưỡng chế khác (B1) |
| **Mô hình masking hai lớp Table 4.5** | Bản chất không đổi; cơ chế khác (A1) |
| **Các thành phần hoãn lại**: MongoDB, Milvus, Prometheus/Grafana | Vẫn hoãn, theo quyết định phạm vi của chính GR1 |

---

# Part F — Defects inside the GR1 report itself (lỗi trong chính báo cáo GR1)

Đây là lỗi **trong văn bản**, độc lập với việc implement. Sửa chúng trong báo cáo trước khi nộp GR2,
vì giám khảo sẽ nhận ra.

## F1. Eight broken cross-references to a section that does not exist

*(Tám tham chiếu chéo trỏ vào một mục không tồn tại)*

Báo cáo **tám lần** nhắc tới một bản ghi quyết định phạm vi (*scope-decision record*), và mỗi lần đều
hiện ra là `??` theo nghĩa đen, vì lệnh `\ref` của LaTeX trỏ vào một label chưa bao giờ được định
nghĩa:

| Trang PDF | Ngữ cảnh |
|---|---|
| **p9** | *"…rationale behind it, is documented in full in Section **??**."* (§1.2) |
| **p13** | *"…the scope decisions recorded in Section **??** to defer components safely…"* (§2.3) |
| **p16** | *"…deferred to Graduation Research 2 as recorded in Section **??**."* (§3.4) |
| **p17** | *"…experiments are deferred to GR2 as recorded in Section **??**."* (§3.5) |
| **p33** | *"…as recorded in Section **??**, produces a more defensible research plan…"* (§6.1) |
| **p33** | *"…the GR1 minimum viable core described in Section **??**…"* (§6.2) |
| **p34** | *"…the components deferred to GR2 in Table **??**…"* (§6.2) |
| **p41** | *"…the complete GR1 minimum viable core described in Section **??**…"* (Appendix B) |

**Mục đó không có trong báo cáo.** Chương 4 chỉ có §4.1 tới §4.6, đã xác nhận từ mục lục trang 3–4 và
từ nội dung. Không có §4.7.

**Đây không phải lỗi do chuyển đổi.** Dấu `??` có ngay trong PDF đã biên dịch, tức đúng là thứ giám
khảo đọc trên trang giấy.

Đây không phải chuyện hình thức. Bản ghi đó được claim là một **deliverable cốt lõi**:

- **Abstract** liệt kê *"an explicit scope-decision record that separates the confirmed minimum viable
  core from components deferred to later phases"* như một đóng góp.
- **§1.2** hứa nó *"documented in full in Section ??"*.
- **§6.1** gọi nó là *"principal lesson"* của giai đoạn thiết kế.
- **Appendix B** tự mô tả là deploy *"the complete GR1 minimum viable core described in Section ??"*.

Vậy là báo cáo nhiều lần trích dẫn, như một trong ba deliverable chính, một mục mà nó không chứa.
**Ưu tiên sửa cao nhất.** Nội dung thì có sẵn: phần chia MVP/Extended nằm ở
`UDP_BaoCaoNghienCuu_v3_Enhanced.md` §1.3, và lý do hoãn rải rác ở §3.4, §3.5 và các comment trong
Appendix B. Cần viết nó thành một mục thật sự.

## F2. "Four data sources", but Table 4.2 lists three

*("Bốn nguồn dữ liệu", nhưng Table 4.2 chỉ có ba)*

**§4.3, PDF trang 22** viết *"Table 4.2 lists the **four** data sources that compose this domain"*.
Bảng ngay bên dưới (cũng trang 22) có **ba** dòng: MySQL, PostgreSQL, và Derived (pgvector).

Có lẽ là phần sót lại từ phạm vi cũ, khi MongoDB còn là một nguồn (ở
`UDP_BaoCaoNghienCuu_v3_Enhanced.md` §10.1 là bốn, có liệt kê cả MongoDB và Kafka producer). Sửa con
số, hoặc thêm lại dòng đó và đánh dấu là hoãn.

## F3. `users` is partitioned by a column it does not have

*(`users` được phân vùng theo một cột nó không có)*

**§4.3, PDF trang 23** viết *"users are partitioned by `bucket(32, user_id)`"*.

Nhưng **Appendix A.2** định nghĩa `users` với `id BIGSERIAL PRIMARY KEY`. **Bảng `users` không có cột
`user_id`.** `user_id` là foreign key *trên `orders`*, trỏ tới `users.id`.

Dạng đúng là `bucket(32, id)`, hoặc đổi tên cột trong DDL cho nhất quán.

**Đã giải quyết ở M3** mà không động tới DDL của Appendix A: `gold.users` đặt khoá thành `user_id`
(`u.id AS user_id`) và phân vùng `bucket(user_id, 32)`, đúng điều câu văn của §4.3 (và mọi phép join
từ `orders`) đã ngầm giả định. Văn bản báo cáo có thể giữ nguyên; chỉ cần một chú thích rằng Gold đổi
tên khoá.

## F4. Figure 4.1 shows deferred components as if in scope

*(Hình 4.1 vẽ các thành phần đã hoãn như thể nằm trong phạm vi)*

Hình 4.1 (PDF **trang 20**) vẽ tầng nguồn gồm **MySQL, PostgreSQL, MongoDB 6.0 và application logs**,
tầng lưu trữ có **Milvus 2.4** bên cạnh pgvector, và tầng governance có **observability
(Prometheus/Grafana)**.

Cả ba (MongoDB, Milvus, Prometheus/Grafana) đều **được hoãn rõ ràng** bởi §6.2 và bởi chính comment
trong Appendix B. Vì vậy hình vẽ đang claim quá phạm vi đã giao, và đoạn văn ở dòng 263 lặp lại việc
claim quá đó khi mô tả hình theo nghĩa đen.

**Gợi ý sửa:** vẽ lại với các thành phần hoãn theo kiểu khác (viền nét đứt, màu xám) kèm chú giải
"deferred to GR2/Thesis". Điều đó biến một điểm lệch thành minh chứng cho việc khoanh phạm vi có chủ
đích, đúng câu chuyện §6.1 muốn kể.

---

# Part G — How to present this in the GR2 report (cách trình bày trong báo cáo GR2)

Một gợi ý cách đóng khung, vì tư liệu ở đây thuận lợi một cách bất thường.

**1. Mở đầu bằng các finding, đừng chôn chúng.** Một giai đoạn implement tái tạo y hệt thiết kế chỉ
chứng minh được rằng thiết kế dễ làm. Một giai đoạn implement tìm ra **những chỗ cụ thể, có giải thích
kỹ thuật, nơi thiết kế không thể thực thi** thì chứng minh được năng lực kỹ thuật thật. Riêng A1, một
*mâu thuẫn logic* giữa hai yêu cầu cùng một bản sửa thoả cả hai, là kết quả đơn lẻ mạnh nhất cho tới
giờ.

**2. Trình bày A1 với hai cấu hình đặt cạnh nhau.** Chuỗi SMT của Appendix C và cấu hình
connector-native, xen giữa là phần giải thích về envelope lồng nhau. Trực quan, cụ thể, giải thích
trong chín mươi giây.

**3. Đóng khung Part B nhất quán là "substitution, not omission".** Với mỗi mục: *cam kết GR1 đưa ra
không đổi; engine cưỡng chế nhẹ hơn; đây là thứ production sẽ dùng.* Đừng bao giờ để nó đọc thành
"chúng tôi không chạy được Ranger".

**4. Nói rõ những gì đã mất.** Lineage mức cột ở chặng Bronze (B2). Schema evolution mức kiểu dữ liệu
(A2). Salt nằm trong file cấu hình (A1). **Tự nêu giới hạn của mình là thứ phân biệt một báo cáo kỹ
thuật với một bài quảng cáo**, và giám khảo đằng nào cũng sẽ tìm ra chúng.

**5. Giữ sự phân biệt "verified vs written" luôn hiển thị.** [`docs/STATUS.md`](STATUS.md) đã theo dõi
điều này. Báo một thứ là *đã implement nhưng chưa chạy* luôn tốt hơn ngầm ý rằng nó chạy được. *(Ví dụ
gốc ở mục này, "connector masking is implemented but not yet exercised", đã lỗi thời: masking đã
verified từ 2026-08-28. Nguyên tắc thì vẫn giữ nguyên.)*

**6. Sửa Part F trước khi nộp.** Đặc biệt F1: một báo cáo trích dẫn tám lần một mục không tồn tại, kể
cả từ abstract, sẽ làm giảm giá trị của một công trình vốn vững chắc.

---

---

# Appendix: on the two copies of the report (phụ lục: về hai bản sao của báo cáo)

`docs/` chứa cả `GR1_report.pdf` (deliverable thật, 49 trang) và `GR1_report.md` (bản chuyển đổi).
Phân tích trong tài liệu này được viết từ bản Markdown rồi **kiểm lại với PDF**. Kết quả:

**Bản chuyển đổi trung thực.** Độ dài văn bản sau chuẩn hoá khớp 98% (74,030 vs 75,618 ký tự). Mọi
trích dẫn quan trọng (chuỗi transform Appendix C, image tag Appendix B, các dòng Avro converter, dấu
`??`, số dòng Table 4.2, claim `bucket(32, user_id)`) đều xuất hiện giống hệt nhau ở cả hai bản.

**Các lỗi do chuyển đổi đã biết** (chỉ có trong `.md`, vô hại nhưng nên biết):

- Gạch nối khi xuống dòng được giữ khác đi, nên văn bản trích ra có những chữ dính như `opensource`,
  `freshnessunaware`, `datasovereignty`. Đây là hiệu ứng trích xuất, không phải lỗi của bản nào.
- Các bảng độ rộng cố định của PDF được chuyển thành bảng Markdown. Nội dung còn nguyên, nhưng căn
  chỉnh ô trong văn bản PDF thô (ví dụ Table 2.1) trông như các cột xen kẽ nhau.
- **Cả hai đường dẫn hình đều hỏng**: bản chuyển đổi trỏ tới
  `GR1_report_fixed_assets/figure-4-*.png`, trong khi file nằm trong `docs/`. Đã sửa 2026-08-25; hình
  giờ hiển thị được.

**Dùng PDF làm nguồn trích dẫn** khi viết GR2 (số trang, đánh số hình và bảng). Bản Markdown tiện cho
grep và diff, và đáng tin về nội dung.

---

## Where to find things (tìm thứ gì ở đâu)

| | |
|---|---|
| Lập luận đầy đủ cho mỗi substitution | [`docs/adr/`](adr/) |
| Bài toán và cái thang trừu tượng | [`docs/PROBLEM.md`](PROBLEM.md) |
| Cái gì đã xây và đã kiểm chứng lúc này | [`docs/STATUS.md`](STATUS.md) |
| Nhật ký xây dựng từng milestone | [`docs/BUILD-LOG.md`](BUILD-LOG.md) |
| Ngữ cảnh làm việc lâu dài + các gotcha môi trường | [`CLAUDE.md`](../CLAUDE.md) |
| Trạng thái sống trong một lệnh | `python mosaic.py doctor` |
