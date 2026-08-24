# ĐỒ ÁN TỐT NGHIỆP — GRADUATION RESEARCH 1 | V3.0

## BÁO CÁO NGHIÊN CỨU TỔNG QUAN

# Xây dựng Nền tảng Dữ liệu Thống nhất *(Unified Data Platform)*
## trên Môi trường Phân tán — Hỗ trợ Phân tích Đa nguồn & Tối ưu hóa Truy vấn Lai

| | |
|---|---|
| **Lĩnh vực** | Data Engineering, Distributed Systems & AI |
| **Phiên bản** | 3.0 — Báo cáo chính thức sau buổi meeting với instructor |
| **Tech Stack** | Debezium · Kafka · Spark · Iceberg · Trino · Milvus/pgvector |
| **Sources** | MySQL 8.0 · PostgreSQL 15 · MongoDB 6.0 · App logs |
| **Ngày** | 2025 |

---

## MỤC LỤC

1. [Tóm tắt điều hành (Executive Summary)](#1)
2. [Bối cảnh & Động lực nghiên cứu](#2)
3. [Định nghĩa sản phẩm — Trả lời câu hỏi của Instructor](#3)
4. [Kiến trúc tổng thể hệ thống](#4)
5. [Pipeline & Tích hợp — CDC từ nguồn dị hợp](#5)
6. [Storage & Quản lý Metadata](#6)
7. [Hiệu năng & Tối ưu hóa Truy vấn — Hai hướng Top-k](#7)
8. [Quản trị & Bảo mật Dữ liệu](#8)
9. [Đóng góp Nghiên cứu Mới — Freshness-aware Hybrid Top-k Fusion](#9)
10. [Kế hoạch Triển khai (GR1 → GR2 → Thesis)](#10)
11. [Câu hỏi cần làm rõ với Instructor](#11)
12. [Tài liệu tham khảo](#12)

---

## 1. TÓM TẮT ĐIỀU HÀNH {#1}

### 1.1 Định vị sản phẩm (một câu)

> **UDP là một open-source, self-hosted unified data platform phục vụ data engineering teams tại các tổ chức cần năng lực của Databricks/Snowflake nhưng không thể hoặc không muốn dùng managed cloud — giải quyết ba vấn đề còn tồn đọng: (1) thiếu reference architecture benchmark-ready hợp nhất CDC real-time từ nguồn dị hợp với lakehouse storage; (2) không có open-source platform nào quản trị thống nhất structured analytics lẫn vector search dưới cùng một RBAC và lineage model; và (3) chưa có giải pháp tối ưu cho hybrid top-k fusion khi kết hợp SQL result với ANN result có độ tươi dữ liệu khác nhau.**

### 1.2 Đóng góp nghiên cứu dự kiến

| Đóng góp | Loại | Giai đoạn |
|---|---|---|
| Reference architecture open-source có benchmark | Engineering | GR1 |
| So sánh định lượng MySQL binlog vs PostgreSQL WAL CDC | Experimental | GR1 |
| Phân loại và thực nghiệm hai hướng Top-k (SQL pushdown & ANN) | Experimental | GR1 |
| **Freshness-aware Hybrid Top-k Fusion** — novel research problem | Novel contribution | GR1 baseline → GR2 → Thesis |
| Benchmark AQP (approx_distinct, approx_percentile) trên realistic dataset | Experimental | GR1 extended |

### 1.3 Phạm vi GR1 (MVP vs Extended)

**MVP — Phải hoàn thành trong GR1:**
- Multi-source CDC pipeline (MySQL binlog + PostgreSQL WAL + MongoDB oplog) → Kafka
- Kafka Connect SMTs masking tại điểm ingestion
- Data Lakehouse: Bronze/Silver/Gold trên Apache Iceberg + MinIO
- Great Expectations quality gates tại Bronze→Silver
- Apache Trino federated query: Iceberg + MySQL + PostgreSQL + MongoDB
- Query optimization benchmark: Top-N pushdown, partition pruning, ANALYZE workflow
- Apache Ranger RBAC + column masking qua Trino plugin
- DataHub lineage graph (2–3 pipelines qua OpenLineage)
- Restart recovery demo cho Debezium connectors

**Extended — Thêm vào nếu còn thời gian:**
- Vector Store: Milvus + pgvector side-by-side comparison
- Semantic search demo (sentence-transformers embeddings)
- AQP experiments: approx_distinct vs exact, approx_percentile benchmarks
- Z-Ordering benchmark: before/after scanned bytes
- Full Airflow orchestration với quality gates
- Monitoring dashboard: Prometheus + Grafana

---

## 2. BỐI CẢNH & ĐỘNG LỰC NGHIÊN CỨU {#2}

### 2.1 Bức tranh dữ liệu trong tổ chức hiện đại

Các tổ chức hiện đại — từ startup đến doanh nghiệp — ngày càng phụ thuộc vào dữ liệu để đưa ra quyết định. Tuy nhiên, thực tế hạ tầng dữ liệu trong phần lớn tổ chức vẫn phân mảnh nghiêm trọng:

- **Dữ liệu giao dịch** nằm trong RDBMS (MySQL, PostgreSQL) với schema nghiêm ngặt, high-frequency writes
- **Dữ liệu catalog và semi-structured** nằm trong document stores (MongoDB) với schema linh hoạt
- **Sự kiện real-time** từ application logs, API calls, clickstream — volume cao, ephemeral
- **Dữ liệu ngữ nghĩa** cần embedding và similarity search cho các use case AI/ML

Mỗi hệ thống này có semantics, protocol, và performance characteristics khác nhau. Câu hỏi trung tâm mà đề tài đặt ra là: **làm thế nào để hợp nhất toàn bộ nguồn dữ liệu này vào một nền tảng thống nhất, hỗ trợ cả SQL analytics lẫn semantic retrieval, đồng thời đảm bảo governance và privacy xuyên suốt vòng đời dữ liệu?**

### 2.2 Bối cảnh học thuật

Kiến trúc Data Lakehouse được đề xuất như thế hệ kiến trúc dữ liệu tiếp theo, hợp nhất Data Lake và Data Warehouse trên nền open direct-access formats (Parquet) với ML/DS support và warehouse-grade performance. Đây là nền tảng học thuật cốt lõi của đề tài [Armbrust et al., CIDR 2021].

Trong khi các công trình học thuật về CDC [Kleppmann 2017], Lakehouse [Armbrust et al. 2021], ANN search [Malkov & Yashunin 2018], và AQP [Agarwal et al. 2013; Park et al. 2018] đã được nghiên cứu sâu một cách riêng lẻ, **chưa có công trình nào tích hợp chúng vào một hệ thống thống nhất với governance model xuyên suốt** và đặt ra bài toán **hybrid top-k fusion với asymmetric freshness** — đây là gap nghiên cứu mà đề tài hướng đến.

### 2.3 Bối cảnh thực tiễn tại Việt Nam và ASEAN

Một số đặc thù thực tiễn tại thị trường Việt Nam và ASEAN củng cố giá trị của đề tài:

- **Data sovereignty**: Các tổ chức tài chính, y tế, và chính phủ chịu yêu cầu lưu trữ dữ liệu nội địa, không thể đưa dữ liệu lên cloud nước ngoài
- **Chi phí managed cloud**: Databricks, Snowflake có chi phí đáng kể với nhiều SMEs
- **Heterogeneous legacy stack**: Nhiều tổ chức có hạ tầng hỗn hợp MySQL + MongoDB + logs chưa được tích hợp
- **Nhu cầu AI/ML ngày càng tăng**: Semantic search, recommendation, fraud detection — tất cả cần unified data foundation

### 2.4 Vấn đề nghiên cứu cụ thể

| Vấn đề | Hệ quả nếu không giải quyết |
|---|---|
| Không có reference architecture benchmark-ready cho multi-source CDC | Tổ chức tốn 3–6 tháng integration mà không có baseline |
| Thiếu governance thống nhất cho structured + vector data | PII rò rỉ qua vector embeddings; không có lineage xuyên suốt |
| Hybrid top-k fusion chưa tối ưu (staleness asymmetry) | Kết quả kết hợp không phản ánh đúng state của dữ liệu |
| CDC từ RDBMS dị hợp chưa được so sánh có benchmark | Không có căn cứ kỹ thuật để chọn MySQL vs PostgreSQL cho CDC |

---

## 3. ĐỊNH NGHĨA SẢN PHẨM {#3}

### 3.1 Trả lời câu hỏi 1: Platform hay Tool/API/DaaS?

**UDP là một Data Infrastructure Platform** — không phải tool đơn lẻ, không phải end-user application, không phải API/DaaS.

Phân loại trong data engineering landscape:

| Loại | Mô tả | Ví dụ |
|---|---|---|
| Data Application | End-user facing, người dùng business tương tác trực tiếp | Tableau, Power BI |
| Data Tool | Single-purpose utility, giải quyết một vấn đề cụ thể | Kafka (broker), dbt (transform) |
| API / DaaS | Exposes data/processing as a service | OpenAI API, weather API |
| **Data Platform** | **Infrastructure layer — data ops chạy trên đỉnh** | **Databricks, Snowflake, UDP** |

UDP đóng vai trò **infrastructure layer** nằm giữa source systems và consuming applications:

```
Source systems → [UDP Platform] → BI tools / ML pipelines / APIs
(MySQL, MongoDB)   (ingest, store,   (Superset, PyTorch,
                    govern, serve)    custom applications)
```

**Deliverable cụ thể của GR1**: Proof-of-concept platform + reference architecture documentation + benchmark report. Đây là cách trung thực nhất để mô tả output của một graduation research — không phải production system, nhưng là một working PoC có giá trị tái sử dụng và học thuật.

### 3.2 Trả lời câu hỏi 2: Khác gì so với hiện có? Giải quyết vấn đề gì?

**Landscape hiện tại:**

**Managed cloud platforms (Databricks, Snowflake, BigQuery):** Feature-complete nhưng có chi phí cao, vendor lock-in, và yêu cầu data rời khỏi on-premise. Vector search mới được thêm vào gần đây dưới dạng bolt-on feature, chưa được tích hợp sâu vào governance model.

**Hadoop ecosystem cũ (Cloudera, HDP):** On-premise, self-hosted nhưng dựa trên kiến trúc HDFS lỗi thời, không có real-time CDC, không có vector search, nặng và khó deploy.

**DIY open-source stack:** Linh hoạt nhưng không có reference architecture đầy đủ, phải tự integrate từng component — công việc tốn nhiều tháng mà không có baseline để so sánh.

**Ba gap cụ thể mà UDP điền vào:**

**Gap 1 — Không có open-source platform nào quản trị thống nhất SQL analytics + Vector search:**
Databricks gần đây có thêm Vector Search nhưng là proprietary và không open-source. Không có platform nào áp dụng cùng một RBAC model, data masking policy, và lineage tracking cho cả Iceberg tables lẫn Milvus/pgvector indexes.

**Gap 2 — CDC từ nguồn dị hợp như first-class citizen chưa có reference benchmark:**
Hầu hết tutorial architecture giả định batch load từ một nguồn đồng nhất. CDC đồng thời từ MySQL (binlog) + PostgreSQL (WAL) + MongoDB (oplog) với restart safety, schema evolution, và quality gates — chưa có tài liệu benchmark đầy đủ nào cho combination này.

**Gap 3 — Hybrid top-k fusion với asymmetric freshness chưa có giải pháp tối ưu:**
Khi kết hợp kết quả SQL (exact, freshness phụ thuộc CDC lag ~giây/phút) với ANN vector search (approximate, freshness phụ thuộc embedding refresh lag ~giờ), phương pháp Reciprocal Rank Fusion (RRF) đơn giản bỏ qua sự khác biệt về staleness và uncertainty bounds. Đây là novel research problem được formalize trong đề tài này (xem Chương 9).

### 3.3 Target users

**Primary:** Data engineering teams tại SMEs và tổ chức regulated industries (tài chính, y tế, chính phủ) cần:
- Enterprise data platform capabilities
- On-premise / self-hosted deployment
- Không có budget cho managed cloud services
- Heterogeneous existing stack (MySQL + MongoDB + logs)

**Secondary:** Researchers và giảng viên cần reproducible reference implementation với documented benchmarks để nghiên cứu và giảng dạy data engineering.

---

## 4. KIẾN TRÚC TỔNG THỂ HỆ THỐNG {#4}

### 4.1 Mô hình kiến trúc tham chiếu

Kiến trúc UDP được xây dựng theo mô hình **layered lakehouse architecture** — nhất quán với lakehouse reference thinking nơi ingestion, transformation, storage, serving, và governance được coi là first-class parts của kiến trúc [Databricks Lakehouse reference].

```
┌─────────────────────── SOURCE LAYER ──────────────────────────┐
│  [MySQL 8.0]    [PostgreSQL 15]   [MongoDB 6.0]   [App logs] │
│  (binlog CDC)   (WAL/pgoutput)    (oplog/CS)      (Kafka)     │
└──────┬──────────────┬───────────────────┬───────────┬─────────┘
       ↓              ↓                   ↓           ↓
┌─────────────────────── INGESTION LAYER ───────────────────────┐
│  [Debezium connectors]  →→→  [Apache Kafka cluster]           │
│  MySQL · PG · MongoDB        CDC topics · Schema Registry     │
│  snapshot + streaming        Connect SMTs — masking           │
└──────────────────────────────┬────────────────────────────────┘
                               ↓
┌─────────────────────── PROCESSING LAYER ──────────────────────┐
│  [Apache Spark]  [dbt]  [Great Expectations]                  │
│  Structured      SQL    data quality gates        ◄── Airflow │
│  Streaming       models Bronze→Gold                           │
└──────────────────────────────┬────────────────────────────────┘
                               ↓
┌─────────────────────── STORAGE LAYER ─────────────────────────┐
│  [Apache Iceberg on MinIO (S3)]   [Vector store]  ◄── Airflow │
│  ┌─Bronze─────────────────────┐   ┌─Milvus 2.4──┐            │
│  │ raw CDC events · history   │   │ HNSW·IVF-Flat│            │
│  ├─Silver─────────────────────┤ → ├─pgvector────┤            │
│  │ cleaned · masked · valid   │   │ PG native    │            │
│  ├─Gold───────────────────────┤   └─────────────┘            │
│  │ aggregated · Z-ordered     │                               │
│  └───────────────────────────┘                                │
└──────────────────────────────┬────────────────────────────────┘
                               ↓
┌─────────────────────── SERVING LAYER ─────────────────────────┐
│  [Apache Trino]  [BI tools]  [ML / AI pipelines]              │
│  federated SQL   Superset    semantic search                   │
└──────────────────────────────────────────────────────────────┘
┌──── GOVERNANCE (cross-cutting) ──────────────────────────────┐
│  [DataHub]           [Apache Ranger]      [Observability]    │
│  catalog · lineage   RBAC · col masking   Prometheus·Grafana  │
└──────────────────────────────────────────────────────────────┘
```

### 4.2 Quyết định kiến trúc — "Why these blocks"

| Component | Lý do chọn |
|---|---|
| **Apache Iceberg** | Open table format với metadata/snapshot model cho atomic commits, time-travel, và hidden partitioning. Catalog-agnostic — hoạt động tốt với Trino, Spark, Flink. Là chuẩn duy nhất được tất cả query engines chính hỗ trợ native [Iceberg spec] |
| **Debezium** | CDC platform mã nguồn mở hỗ trợ MySQL binlog, PostgreSQL WAL (pgoutput), và MongoDB change streams trong một framework thống nhất. Restart safety qua Kafka-stored offsets [Debezium docs] |
| **Apache Kafka** | Distributed streaming platform cung cấp durability buffer, fan-out consumers, replay capability, và decoupled scaling giữa sources và sinks [Kafka docs] |
| **Apache Trino** | Core value: query federation — truy vấn object storage, relational DBs, NoSQL trong một SQL engine với Top-N pushdown và dynamic filtering [Trino homepage] |
| **MinIO** | S3 API compatibility cho self-hosted lakehouse; full S3-compatible drop-in replacement [MinIO docs] |
| **Apache Ranger** | Centralized authorization framework với Trino plugin hỗ trợ column masking, row filtering, và audit logs [Trino Ranger docs] |
| **Kafka Connect SMTs** | In-pipeline masking tại điểm ingestion: MaskField, ReplaceField — irreversible, PII không bao giờ persist vào storage [Kafka Connect User Guide] |
| **OpenLineage + DataHub** | Lineage là foundation để understand how data is produced and used. DataHub support OpenLineage ingestion từ Airflow và Spark [OpenLineage; DataHub docs] |

### 4.3 Medallion Architecture — Chiến lược tổ chức dữ liệu

Dữ liệu trong Iceberg Lakehouse được tổ chức theo mô hình Medallion (Bronze → Silver → Gold), phản ánh mức độ xử lý và chất lượng tăng dần.

**Bronze Layer (Raw):** Dữ liệu gốc nguyên bản từ Debezium — toàn bộ CDC envelope với before/after state, operation type (c/u/d/r), source LSN/offset, và timestamp. Không có transformation nào được áp dụng. Mục đích: full auditability và replay capability.

**Silver Layer (Cleaned):** Dữ liệu đã qua deduplication, schema normalization, type casting, PII masking (kết hợp SMT tại Kafka và Spark UDF), và validation bởi Great Expectations Checkpoints. Schema được định nghĩa rõ ràng với Iceberg table definition.

**Gold Layer (Aggregated):** Dữ liệu đã được aggregate, join, và tổng hợp theo domain business logic bởi dbt models. Partitioning by time (days/months) và Z-Ordering được apply tối ưu tại layer này. Đây là layer mà Trino và BI tools chủ yếu serve.

**CDC → Iceberg write strategy (justified per docs):**
Spark Structured Streaming đọc Debezium topics từ Kafka → normalize CDC envelopes (extract after/before/op + metadata) → apply idempotency/dedup via source offsets → write append-only vào Bronze + merge/upsert vào Silver/Gold [Spark Kafka Integration; Iceberg Spark Quickstart].

### 4.4 Hybrid Storage Model — Structural và Vector

**Structured storage (Iceberg):** Primary source of truth cho analytics. Federated query qua Trino. Historical record với time-travel.

**Vector storage (Milvus/pgvector):** Complementary index cho semantic retrieval, **không thay thế Iceberg**. Được key bằng document/event IDs để join ngược lại structured tables. Hai lựa chọn được triển khai song song để so sánh:

- **Milvus:** Khi cần emphasize performance và explore multiple index families (HNSW, IVF-Flat, IVF-PQ). Purpose-built for scale [Milvus docs]
- **pgvector:** Khi cần simplicity và transactional integration — đặc biệt khi PostgreSQL vừa là CDC source vừa là vector store (dual-role architecture). SQL JOIN với structured metadata là native [pgvector GitHub]

**Lý do có hai lựa chọn:** Đây không phải redundancy — đây là thiết kế nghiên cứu. Chạy song song Milvus và pgvector cho phép benchmark thực nghiệm về performance, simplicity, và integration complexity, đóng góp vào phần "practical tradeoff analysis" của báo cáo.

---

## 5. PIPELINE & TÍCH HỢP — CDC TỪ NGUỒN DỊ HỢP {#5}

### 5.1 Change Data Capture — Tổng quan cơ chế

CDC là kỹ thuật phát hiện và ghi lại các thay đổi (INSERT, UPDATE, DELETE) xảy ra trong database nguồn để đồng bộ sang hệ thống đích một cách incremental và real-time, thay vì full-load batch định kỳ.

**Phân loại CDC:**

| Phương pháp | Cơ chế | Ưu điểm | Nhược điểm |
|---|---|---|---|
| **Log-based (chosen)** | Đọc transaction log (binlog, WAL, oplog) | Zero production impact, atomic, thứ tự đảm bảo | Cần quyền REPLICATION |
| Trigger-based | Database trigger ghi vào audit table | Đơn giản | Tăng write overhead, phức tạp schema |
| Polling-based | Query periodically theo updated_at | Đơn giản, không cần quyền đặc biệt | Latency cao, bỏ sót DELETE events |

Log-based CDC qua Debezium được chọn vì zero overhead trên production system và atomic capture của tất cả event types.

### 5.2 So sánh MySQL binlog vs PostgreSQL WAL — Research Contribution

Đây là **contribution học thuật** của đề tài: so sánh định lượng hai cơ chế CDC phổ biến nhất trong RDBMS ecosystem, từ đó đưa ra khuyến nghị có cơ sở thực nghiệm cho việc lựa chọn RDBMS khi yêu cầu CDC streaming.

**Bảng so sánh kỹ thuật:**

| Tiêu chí | MySQL 8.0 | PostgreSQL 15 |
|---|---|---|
| **Transaction log** | Binary Log (binlog) | Write-Ahead Log (WAL) |
| **Debezium mechanism** | Đọc binlog trực tiếp qua binlog protocol | Logical Replication Slot + pgoutput plugin |
| **Log format yêu cầu** | `binlog_format=ROW`, `binlog_row_image=FULL` | `wal_level=logical` |
| **Initial snapshot** | Full table scan → binlog streaming | Full table scan → WAL streaming qua replication slot |
| **Position tracking** | Binlog filename + position (LSN) | LSN — Log Sequence Number (hex: `0/1A2B3C4D`) |
| **DDL change capture** | Hỗ trợ tốt qua schema history topic | Giới hạn hơn — một số DDL không captured bởi pgoutput |
| **Restart safety** | Binlog offset trong Kafka Connect offsets topic | LSN trong replication slot; PG giữ WAL đến khi consumed |
| **GTID support** | Global Transaction ID — optional, giúp failover dễ hơn | N/A — dùng LSN làm global position |
| **WAL accumulation risk** | Thấp — binlog có `expire_logs_days` policy | **Cao** — replication slot giữ WAL đến khi consumer process |
| **Dual-role potential** | Không — chỉ là OLTP source | **Có** — PostgreSQL + pgvector = CDC source + vector store |

**⚠ Risk quan trọng — PostgreSQL WAL Accumulation:**
PostgreSQL replication slot ngăn WAL segments bị xóa cho đến khi consumer (Debezium) đã process. Nếu Debezium bị down trong thời gian dài → WAL tích lũy → PostgreSQL disk full → database crash.

**Mitigation:** Monitor `pg_replication_slots.confirmed_flush_lsn`, set `wal_keep_size`, configure alert khi lag vượt ngưỡng. MySQL không có vấn đề này — binlog có `expire_logs_days` policy.

**Debezium unified event envelope:**
Cả MySQL và PostgreSQL đều được normalize sang cùng một format, giúp downstream consumers không cần biết source system:

```json
{
  "op": "c|u|d|r",
  "before": { ...row_fields... },
  "after":  { ...row_fields... },
  "source": {
    "db": "ecommerce", "table": "orders", "ts_ms": 1699123456789,
    "file": "mysql-bin.000001", "pos": 1234,   // MySQL specific
    "lsn": "0/1A2B3C4D", "txId": 492           // PostgreSQL specific
  }
}
```

**KPIs để benchmark (thực nghiệm trong đề tài):**

| KPI | Cách đo |
|---|---|
| CDC end-to-end latency | Commit time → data available in Iceberg Bronze |
| Throughput | Events/sec qua pipeline dưới load |
| Restart recovery time | Stop Debezium → insert rows → restart → measure time to catch up |
| DDL handling | ALTER TABLE trên source → measure impact trên Debezium/downstream |
| WAL lag (PostgreSQL) | `pg_replication_slots.confirmed_flush_lsn` vs current LSN |

### 5.3 MongoDB CDC — Oplog và Change Streams

MongoDB sử dụng oplog (operations log) — a capped collection trong database `local`. Debezium MongoDB connector yêu cầu Replica Set mode để access change streams.

```javascript
// MongoDB Replica Set init (required cho Debezium)
rs.initiate()

// Debezium connector config
{
  "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
  "mongodb.connection.string": "mongodb://user:pass@host:27017/?replicaSet=rs0",
  "collection.include.list": "ecommerce.products,ecommerce.reviews"
}
```

Điểm khác biệt quan trọng: MongoDB CDC events có nested document structure, cần flatten trước khi land vào Iceberg Silver với schema cố định.

### 5.4 Kafka — Ingestion Backbone

Kafka đóng vai trò **shock absorber** giữa operational sources và lakehouse writing jobs:

- **Replay capability:** Consumer có thể reprocess từ bất kỳ offset nào trong retention window
- **Fan-out consumers:** Spark Streaming (write Iceberg) và Embedding Generator (write vector store) consume độc lập với offset riêng
- **Decoupled scaling:** Source và sink scale hoàn toàn độc lập
- **Schema Registry:** Avro serialization với compatibility checks — forward/backward compatible schema evolution

### 5.5 Kafka Connect SMTs — In-Pipeline Masking Layer 1

Single Message Transforms (SMTs) apply transformations ngay sau source connector, **trước khi data vào Kafka topic** — đây là first line of defense cho PII protection [Kafka Connect User Guide].

| SMT | Chức năng trong UDP |
|---|---|
| `MaskField` | Replace field values với null/constant. Dùng cho: email → null, phone → null |
| `ReplaceField` | Drop toàn bộ field nhạy cảm (vd: raw_password_hash) |
| `TimestampConverter` | Đồng nhất timestamp format từ MySQL (DATETIME) và PostgreSQL (TIMESTAMPTZ) sang ISO 8601 |
| `ValueToKey` | Promote một field thành Kafka message key để đảm bảo ordering per entity |

**Kết hợp SMT + Ranger = Belt-and-suspenders approach:** SMT đảm bảo raw PII không bao giờ persist vào bất kỳ storage nào. Ranger đảm bảo masking đúng role tại query time — hai lớp bảo vệ bổ sung, không thay thế nhau.

### 5.6 Debezium State Management & Restart Safety

Debezium store connector state trong Kafka:

- **Source offsets:** Binlog position (MySQL) hoặc LSN (PostgreSQL) → sau restart, Debezium resume chính xác từ đúng position
- **Schema history:** DDL changes (MySQL) → cần thiết để deserialize binlog events khi schema đã thay đổi

**Deliverable có thể demo:** Stop Debezium → insert rows vào MySQL/PostgreSQL → restart → verify không miss events. Đây là concrete, demonstrable test case cho giảng viên [Debezium storage docs].

### 5.7 Apache Airflow — Orchestration Engine

Airflow là open-source platform để author, schedule, và monitor workflows-as-code (DAGs) [Airflow docs]. Trong UDP, Airflow không nằm trong data flow — nó là **control plane** điều phối:

- **Batch ETL:** Silver → Gold transformation jobs theo schedule, với SLA tracking
- **Iceberg maintenance:** Schedule `rewrite_data_files` (compaction) và `expire_snapshots` vào off-peak hours hàng ngày
- **Quality promotion gates:** `GreatExpectationsOperator` phải pass trước khi data được promote
- **Embedding refresh:** Trigger embedding generation khi upstream Gold tables thay đổi
- **OpenLineage emission:** Không cần sửa DAG code — config Airflow OpenLineage provider → tự động emit lineage events vào DataHub [Airflow OpenLineage docs]

---

## 6. STORAGE & QUẢN LÝ METADATA {#6}

### 6.1 Apache Iceberg — Technical Deep Dive

Iceberg là open table format với metadata/snapshot model cho atomic commits và consistent reads. Time-travel được thực hiện bằng cách query previous snapshots [Iceberg spec].

**Table state model:**
- Mỗi write tạo **metadata file** mới thay thế cái cũ qua atomic swap → ACID semantics
- **Snapshots** represent table states tại points in time → foundation của time-travel
- **Manifest files** track data files với column-level statistics (min/max, null counts) → scan planning

**Multi-layer "indexing" trong Iceberg** (lưu ý: đây là metadata-based, KHÔNG phải B-tree index như OLTP):

| Layer | Cơ chế | AWS best-practices framing |
|---|---|---|
| **Partition Pruning** | Hidden partition transforms (day/month, bucket, truncate). Engine skip toàn bộ partitions không match WHERE clause | "Partitions as primary indexing layer" |
| **Column Statistics** | Manifest files lưu min/max/null per column per file. Engine skip files không thể contain qualifying rows | "Column stats as secondary indexing layer" |
| **Bloom Filters** | `write.parquet.bloom-filter-enabled.column.<col>` — probabilistic structure cho high-cardinality equality filters | Tertiary — point lookups |
| **Z-Ordering / Sort Order** | Cluster data theo multi-column space-filling curve → better file-level statistics → reduced scanned bytes | Layout optimization |

**Partitioning strategy cho E-Commerce domain:**
- Gold orders: `PARTITIONED BY (days(created_at), bucket(16, region))` — time-series analytics + regional queries
- Gold users: `PARTITIONED BY (bucket(32, user_id))` — user-specific queries
- Silver (cleaned): partition by source và date để dễ replay

### 6.2 MinIO — Object Storage Layer

MinIO đóng vai trò S3-compatible object storage backend. Toàn bộ Iceberg metadata + data files, embedding vectors, và audit logs đều stored trên MinIO.

**Bucket structure:**
```
udp-lakehouse/
  bronze/           # Raw CDC data
  silver/           # Cleaned, masked data
  gold/             # Aggregated analytics data
udp-vectors/        # Embedding vectors (pgvector backup)
udp-metadata/       # Iceberg catalog metadata (REST Catalog / Nessie)
udp-audit/          # Query audit logs
```

**Security:** SSE-S3 (keys managed by KMS) cho encryption at rest. TLS cho in-transit encryption giữa Spark/Trino và MinIO.

### 6.3 Iceberg Catalog — Project Nessie

UDP dùng Project Nessie làm Iceberg REST Catalog. Nessie cung cấp **git-like semantics** cho data:
- **Branching:** Tạo feature branch để test schema changes mà không ảnh hưởng production
- **Merging:** Merge changes sau khi validation passes
- **Tags:** Tag snapshots cho reproducibility

### 6.4 DataHub — Metadata Platform

DataHub là data discovery application trên extensible metadata platform, originated tại LinkedIn [DataHub docs]. Trong UDP:

- **Data Catalog:** Inventory tất cả datasets, schemas, owners, tags
- **Lineage Graph:** Visual graph từ MySQL/PostgreSQL/MongoDB → Iceberg Bronze → Silver → Gold → Embedding indexes. Column-level lineage
- **Data Quality tracking:** Import Great Expectations assertion results để track quality score theo thời gian
- **OpenLineage ingestion:** Nhận events từ Airflow và Spark để build lineage automatically [DataHub OpenLineage docs]

**Lineage demo deliverable:** 3 pipelines emit OpenLineage → DataHub hiển thị graph:
1. `MySQL → Debezium → Kafka → Spark → Iceberg.Bronze.orders`
2. `Iceberg.Bronze.orders → dbt → Iceberg.Silver.orders`
3. `Iceberg.Silver.products → Embedding Job → Milvus.product_embeddings`

---

## 7. HIỆU NĂNG & TỐI ƯU HÓA TRUY VẤN — HAI HƯỚNG TOP-K {#7}

### 7.1 Apache Trino — Federated Query Engine

Trino là distributed SQL query engine với core value proposition là query federation: truy vấn object storage, RDBMS, và NoSQL trong một SQL engine [Trino homepage].

**Connectors trong UDP:**

| Connector | Source | Caveat cần acknowledge |
|---|---|---|
| Iceberg | Primary analytics (MinIO) | Full pushdown support |
| MySQL | Live operational queries | Hạn chế pushdown so với Iceberg |
| PostgreSQL | Live dimension lookups | Tương tự MySQL |
| MongoDB | Semi-structured queries | MongoDB connector có hạn chế SQL compatibility |
| Kafka | Near-real-time diagnostic views | **CAVEAT:** Rows có thể disappear khi segments drop [Trino Kafka docs] — đây là lý do Iceberg là historical store thực sự |

**Federated query example:**
```sql
SELECT o.order_id, u.name, p.description, o.total_amount
FROM iceberg.gold.orders o
JOIN postgresql.prod.users u ON o.user_id = u.id
JOIN mongodb.catalog.products p ON o.product_id = p._id
WHERE o.created_at >= DATE '2024-01-01'
  AND o.status = 'delivered'
ORDER BY o.total_amount DESC
LIMIT 100
```

Trino tối ưu hóa cross-source joins bằng dynamic filtering: filter values từ dimension table được dynamically passed xuống Iceberg scan, giảm scanned bytes.

### 7.2 Trino Query Optimization — Technical Details

**Cost-Based Optimizer (CBO) & Statistics:**
Trino compute plan node costs dựa trên table statistics — visible trong EXPLAIN outputs [Trino cost-in-explain docs].

**Critical caveat:** Khi Spark write vào Iceberg tables, Trino stats cần được refresh thủ công qua `ANALYZE`. Nếu stats stale → CBO đưa ra quyết định sai (wrong join order, wrong probe side) → performance degradation [Trino table statistics docs].

**Experiment design cho báo cáo:**
1. Baseline queries WITHOUT statistics → record query plan (EXPLAIN) + scanned bytes + wall time
2. Run `ANALYZE table` → trigger statistics collection
3. Compare: join order changes, CPU time, scanned bytes trong EXPLAIN ANALYZE
4. Document improvement với concrete numbers

**Design caveats cần acknowledge trong báo cáo:**

| Caveat | Lý do acknowledge |
|---|---|
| Federated query ≠ silver bullet cho performance | Pushdown support khác nhau per connector. Iceberg tốt nhất, MongoDB kém nhất |
| Stats freshness | ANALYZE cần thiết sau Spark writes — operational overhead cần document |
| WAL accumulation (PostgreSQL) | Replication slot giữ WAL → disk risk nếu Debezium lag |
| Spill/fault tolerance | Trino spill-to-disk là legacy; fault-tolerant execution cần exchange manager [Trino spill docs] |

Các caveats này KHÔNG làm yếu đề tài — ngược lại, chúng demonstrate engineering maturity và justify tại sao scope bao gồm optimization experiments.

### 7.3 Hai Hướng Top-k — Phân loại học thuật rõ ràng

Đây là điểm quan trọng nhất trong research framing. "Top-k" trong đề tài này chia thành HAI hướng nghiên cứu hoàn toàn khác nhau:

---

#### Track 1: SQL Top-N Pushdown trong Distributed Query Engine

**Bài toán:** Khi query có `ORDER BY ... LIMIT k` trên federated sources, làm thế nào để minimize I/O và network transfer?

**Nền tảng lý thuyết:** Threshold Algorithm (Fagin, Lotem, Naor) — formalize cách tìm top-k objects từ multiple sorted lists với monotone aggregation function, minimize total accesses [Fagin et al., PODS 2003; arXiv:cs/0204046].

Ứng dụng trong federated context: mỗi Trino connector là một "sorted list". Threshold Algorithm minimize round-trips để find global top-k bằng cách:
1. Read từ tất cả sources theo thứ tự giảm dần (sorted access)
2. Maintain running threshold (minimum score của k candidates hiện tại)
3. Dừng khi threshold condition được thỏa mãn — không cần scan toàn bộ data

**Trino implementation:** `applyTopN` method trong Connector API — khi query có `ORDER BY + LIMIT`, Trino push down Top-N operation xuống từng connector [Trino connector developer guide]. Connector trả về chỉ k rows thay vì full result set.

**Experiment design:**
- So sánh EXPLAIN plans: với/không có `applyTopN` pushdown
- Measure: scanned bytes, CPU time, wall time per query
- Datasets: 10M+ rows để thấy sự khác biệt có ý nghĩa thống kê
- Isolate variable: chỉ thay đổi `applyTopN` support, giữ nguyên data và query

---

#### Track 2: Approximate Nearest Neighbor (ANN) top-k trong Vector Store

**Bài toán:** Cho một query vector q và k, tìm k nearest neighbors trong tập N vectors (N có thể đến tỷ) với acceptable recall và minimal latency.

**Nền tảng lý thuyết:** HNSW — Hierarchical Navigable Small World graphs [Malkov & Yashunin, arXiv:1603.09320, 2018]. Cơ chế: hierarchical proximity graph với layers, mỗi layer có sparser connections. Search bắt đầu từ top layer (coarse navigation), xuống dần bottom layer (local refinement). Approximate: không đảm bảo exact k-NN, nhưng recall@k thường > 95% với latency < 1ms ở scale hàng triệu vectors.

**Key parameters:**
- `M`: connections per node — higher M → better accuracy, higher memory
- `ef_construction`: build-time search expansion — higher → better quality index, longer build
- `ef_search`: query-time expansion — higher → better recall, higher latency

**Experiment design:**
- Sweep M và ef_search trên Milvus: plot recall@k vs latency curve
- Compare: HNSW vs IVF-Flat vs exact brute-force search
- Measure: recall@10, recall@100, latency p50/p95/p99
- Document: memory overhead per index type

---

### 7.4 Approximate Query Processing (AQP)

Đối với tập dữ liệu lớn (hàng tỷ records), approximate result trả về trong 0.1 giây đôi khi tốt hơn exact result trả về trong 30 giây.

**Nền tảng học thuật:**

| System | Đóng góp |
|---|---|
| BlinkDB [Agarwal et al., 2013] | Sampling-based AQP với bounded error/response-time tradeoffs [arXiv:1203.5485] |
| VerdictDB [Park et al., 2018] | Platform-independent AQP middleware approach [arXiv:1804.00770] |
| Apache DataSketches | HyperLogLog, Count-Min Sketch, Quantile Sketches, Frequent Items [datasketches.apache.org] |

**Trino built-in AQP (practical lever):**
- `approx_distinct(column)` — HyperLogLog, ~1-2% error, vài KB memory
- `approx_percentile(column, 0.95)` — t-Digest, chính xác tại tail percentiles
- `WITH ERROR 5% AT CONFIDENCE 95%` — BlinkDB-style query syntax (Trino extension)

**Experiments trong đề tài:**
```sql
-- Exact vs approximate COUNT DISTINCT
SELECT COUNT(DISTINCT user_id) FROM gold.clickstream;          -- 30s, exact
SELECT APPROX_DISTINCT(user_id) FROM gold.clickstream;          -- 0.2s, ~1-2% error

-- Exact vs approximate percentile
SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY order_amount)
FROM gold.orders;                                                -- exact
SELECT APPROX_PERCENTILE(order_amount, 0.95) FROM gold.orders;  -- approximate
```

Report format: Bar charts với speedup factor và error % cho từng AQP technique.

### 7.5 Iceberg Performance Optimization — Benchmarks

**Z-Ordering experiment:**
```sql
-- Trước Z-Ordering: full partition scan
EXPLAIN ANALYZE SELECT * FROM gold.orders
WHERE user_id = 12345 AND product_id = 'P001';
-- Scanned: 5.2 GB

-- Apply Z-Ordering
CALL system.rewrite_data_files(
  table => 'gold.orders',
  strategy => 'sort',
  sort_order => 'zorder(user_id, product_id, created_at)'
);

-- Sau Z-Ordering: data skipping hiệu quả hơn
EXPLAIN ANALYZE SELECT * FROM gold.orders
WHERE user_id = 12345 AND product_id = 'P001';
-- Scanned: 0.4 GB (tốt hơn 13x)
```

**Bloom Filter experiment:**
```sql
-- Enable Bloom Filter cho high-cardinality equality filter
ALTER TABLE gold.orders
SET TBLPROPERTIES ('write.parquet.bloom-filter-enabled.column.order_id' = 'true');

-- Point lookup benefit
SELECT * FROM gold.orders WHERE order_id = 'ORD-99999';
-- With BF: skip 99.9% files without reading data
```

---

## 8. QUẢN TRỊ & BẢO MẬT DỮ LIỆU {#8}

### 8.1 Data Masking — Hai lớp bảo vệ

ISO định nghĩa data masking là creating a functional substitute cho PII để data stays usable while preventing unauthorized identification [ISO information security].

| Layer | Mechanism | Timing | PII fate |
|---|---|---|---|
| **Layer 1: Kafka Connect SMTs** | MaskField, ReplaceField | Tại ingestion point, TRƯỚC Kafka topic | **Irreversible** — PII không bao giờ persist |
| **Layer 2: Trino + Ranger** | Column Masking policy | Tại query time, dynamic, role-based | Raw exists trong Bronze; masked version served |

**PII masking strategy:**

| Loại PII | Layer 1 (SMT) | Layer 2 (Ranger) |
|---|---|---|
| Email address | Drop hoặc null | Partial: `u***@domain.com` |
| Số điện thoại | Null | `***-***-7890` |
| Họ tên | Deterministic hash (pseudonym) | Pseudonym |
| Số CCCD/CMND | **Null — không bao giờ lưu** | N/A |
| Credit card | Null | 4 số cuối only (PCI-DSS) |
| IP Address | Giữ nguyên hoặc truncate | `/24 subnet` |

### 8.2 Apache Ranger — Centralized Authorization

Apache Ranger là centralized authorization framework, supporting RBAC/ABAC/tag-based control và centralized auditing [Apache Ranger docs]. Trino Ranger plugin hỗ trợ:
- Fine-grained authorization: catalog → schema → table → column level
- Column masking policies (dynamic, role-based, không cần modify query)
- Row filtering (WHERE clause auto-inject)
- Centralized audit logs cho compliance reporting

**Role hierarchy trong UDP:**

| Role | Access |
|---|---|
| `data_admin` | Full access tất cả layers. Raw PII visible. DBA + Platform team only |
| `data_engineer` | Read/write Silver, Gold. Read Bronze (masked). Cannot see raw PII |
| `data_analyst` | Read-only Gold. Ranger auto-apply column masking. Cannot see raw rows |
| `ml_engineer` | Read Gold + Vector Store. Anonymized data only |
| `bi_user` | Read-only aggregated views trong Gold. No row-level access |

### 8.3 Transport & Storage Encryption

- **In-transit:** Kafka SSL/TLS + SASL [Kafka security docs]; MinIO HTTPS; Trino TLS
- **At-rest:** MinIO SSE-S3 (keys managed by KMS) [MinIO security docs]
- **PostgreSQL connection:** SSL mode=require cho Debezium

### 8.4 Query Audit & GDPR Compliance

- **Mọi Trino query** được log (user, query text, tables accessed, timestamp, execution time) vào Iceberg audit table — searchable, analyzable, immutable
- **GDPR Right to Erasure:** Iceberg row-level deletes (Merge-on-Read / positional delete files) + metadata tracking. Không cần rewrite toàn bộ Parquet files
- **Data Retention Policy:** Bronze: 90 ngày. Silver: 2 năm. Gold: 5+ năm theo business requirement

---

## 9. ĐÓNG GÓP NGHIÊN CỨU MỚI — FRESHNESS-AWARE HYBRID TOP-K FUSION {#9}

### 9.1 Bài toán được formalize

**Bối cảnh:** Hệ thống UDP serve kết quả từ hai nguồn:
- **SQL results (Trino):** Exact match queries trên Iceberg Gold layer. Freshness phụ thuộc CDC lag từ OLTP → Iceberg (~giây đến phút). Score space: absolute business metrics (revenue, count, rating).
- **ANN results (Milvus/pgvector):** Approximate top-k similarity search trên embedding indexes. Freshness phụ thuộc embedding refresh lag (~giờ). Score space: cosine similarity [0,1].

**Ví dụ use case:** Query "tìm top-10 sản phẩm phù hợp với người dùng X" cần kết hợp:
- SQL rank (dựa trên purchase history, recency, price range từ Iceberg Gold)
- Semantic rank (dựa trên embedding similarity từ Milvus)

**Bài toán chưa được giải quyết tốt:**
Reciprocal Rank Fusion (RRF) — phương pháp phổ biến nhất — đơn giản combine ranks bằng công thức:

```
RRF(d) = Σ 1 / (k + rank_i(d))
```

RRF **bỏ qua**:
1. **Asymmetric staleness:** SQL data có thể mới hơn 100 lần so với embeddings. Một sản phẩm vừa hết hàng 5 phút trước vẫn xuất hiện trong ANN results với cao ranking, nhưng SQL query đã biết nó hết hàng
2. **Heterogeneous score spaces:** SQL score (absolute) vs cosine similarity (normalized [0,1]) — không thể weighted-sum trực tiếp một cách có ý nghĩa
3. **Recall uncertainty:** ANN result là approximate — một item có thể absent khỏi top-k ANN kết quả không có nghĩa là nó thực sự không relevant; chỉ có nghĩa là không được tìm thấy

### 9.2 Research Question chính thức

> **RQ: Trong môi trường Unified Data Platform nơi structured analytics và vector similarity search có độ tươi dữ liệu không đồng đều (asymmetric freshness), làm thế nào thiết kế một fusion function f(SQL_topk, ANN_topk, Δt_sql, Δt_emb) để maximize relevance + freshness của kết quả kết hợp, so với baseline RRF không-aware-freshness?**

### 9.3 Freshness-aware Fusion — Proposed Framework

**Staleness score:** Mỗi data point có staleness s = time_since_last_update / acceptable_freshness_window

```
staleness_sql(d) = (now - last_cdc_event(d)) / T_sql_sla
staleness_emb(d) = (now - last_embedding_refresh(d)) / T_emb_sla
```

**Freshness-weighted fusion:**
```
fusion_score(d) = α × sql_score(d) × f(staleness_sql(d))
                + β × ann_score(d) × f(staleness_emb(d))
```

Trong đó `f(staleness)` là decay function (linear hoặc exponential) giảm confidence khi data stale.

**Research trajectory:**

| Giai đoạn | Nội dung | Deliverable |
|---|---|---|
| **GR1** | Survey fusion methods (RRF, Borda Count, weighted combination, learned fusion). Formalize problem. Baseline experiment với RRF vs simple weighted combination | Problem formalization + baseline results |
| **GR2** | Implement staleness scoring từ CDC lag metadata. Benchmark freshness-weighted fusion vs RRF trên synthetic scenarios với controlled staleness | Experimental comparison với metrics |
| **Thesis** | Learned fusion weights từ implicit feedback. Uncertainty bounds dựa trên ANN recall guarantee. Formal proof of optimality conditions | Full research paper |

### 9.4 Tại sao đây là novel contribution

Kiểm tra literature:
- **RRF [Cormack et al., 2009]:** Classic fusion method, widely used nhưng không aware freshness
- **Learned sparse retrieval [SPLADE, etc.]:** Tập trung vào relevance, không về multi-source fusion với heterogeneous freshness
- **Temporal ranking [Li & Croft, 2003]:** Document retrieval có decay, nhưng không về multi-modality fusion
- **Databricks/Snowflake vector search:** Bolt-on features, không có unified freshness model

**Gap:** Chưa có formalization của bài toán hybrid top-k fusion với asymmetric data freshness trong context của a unified data platform combining structured analytics và vector search. Đây là gap thực sự.

---

## 10. KẾ HOẠCH TRIỂN KHAI {#10}

### 10.1 Domain và Dataset

**Domain: E-Commerce Platform** — thực tế, quen thuộc, bao phủ đầy đủ data types:

| Source | System | Nội dung |
|---|---|---|
| MySQL | Transactions | Orders, Payments, Order items |
| PostgreSQL | Master data | Users, Products, Categories, Inventory |
| MongoDB | Catalog | Product catalog (flexible attributes), Reviews |
| Kafka producers | Events | Clickstream, API logs, Error logs |
| Derived (pgvector) | Vector index | Product description embeddings (384-dim) |

**PostgreSQL dual-role:** Vừa là CDC source (master data) vừa là vector store (pgvector extension) — một trong những điểm thú vị nhất của đề tài về architectural efficiency.

**Dataset scale:** Minimum 10 triệu rows để benchmark optimization techniques có ý nghĩa. Sử dụng data generator (Faker + TPC-H patterns) để tạo synthetic data với realistic distribution.

### 10.2 Phân chia giai đoạn (20 tuần)

| Giai đoạn | Tuần | Nội dung | Deliverables |
|---|---|---|---|
| **Phase 1: Lý thuyết** | 1–3 | Lakehouse paper; CDC mechanisms; Top-k: Fagin TA + HNSW; AQP: BlinkDB + VerdictDB; Data Governance standards | Literature review, ADRs, Technology comparison matrix |
| **Phase 2: Thiết kế** | 4–5 | Architecture diagram; Schema design (MySQL, PostgreSQL, MongoDB); Security model; Slides giảng viên | System design doc, ER diagrams, Architecture slides |
| **Phase 3: CDC Pipeline** | 6–8 | MySQL + PostgreSQL Debezium connectors; Kafka + Schema Registry; SMTs masking; Restart recovery demo | End-to-end CDC pipeline, restart demo |
| **Phase 4: MongoDB + Processing** | 9–10 | Debezium MongoDB connector; Spark Structured Streaming Bronze landing; Great Expectations gates | Multi-source landing vào Bronze Iceberg |
| **Phase 5: Silver + Gold ETL** | 11–12 | Bronze→Silver transforms; dbt models Silver→Gold; Partitioning strategy; Airflow DAGs | Medallion layers, quality reports |
| **Phase 6: Trino + Optimization** | 13–15 | Trino + connectors; Federated queries; ANALYZE workflow; Benchmark: partition pruning, Z-Ordering, Top-N pushdown; AQP experiments | Performance benchmark report |
| **Phase 7: Vector + Governance** | 16–17 | Milvus + pgvector setup; Embedding pipeline; DataHub lineage; Ranger RBAC; Hybrid fusion baseline | Semantic search demo, lineage graph, RBAC policies |
| **Phase 8: Báo cáo + Demo** | 18–20 | Viết báo cáo cuối kỳ; Demo prep; Peer review | Final report, Demo presentation |

### 10.3 Rủi ro và biện pháp

| Rủi ro | Biện pháp |
|---|---|
| PostgreSQL WAL accumulation nếu Debezium lag | Monitor `pg_replication_slots.confirmed_flush_lsn`; alert khi lag > threshold; `wal_keep_size` backup |
| Milvus setup complexity (etcd + MinIO + Pulsar) | Fallback sang pgvector (đã có PostgreSQL); chạy song song để so sánh |
| Resource hạn chế (laptop cá nhân) | Docker Compose single-node trước; K3s khi cần; GCP/AWS free tier cho burst |
| DataHub setup tốn thời gian | Fallback sang OpenMetadata (lighter) hoặc Apache Atlas |
| Phạm vi quá rộng | Prioritize MVP (CDC + Iceberg + Trino + RBAC); Vector + AQP là Extended |
| Thiếu dataset thực tế | Faker + TPC-H generator; minimum 10M rows cho meaningful benchmarks |

---

## 11. CÂU HỎI CẦN LÀM RÕ VỚI INSTRUCTOR {#11}

### 11.1 Về phạm vi (Scope)

1. Việc thêm PostgreSQL vào cạnh MySQL để nghiên cứu **so sánh MySQL binlog vs PostgreSQL WAL** có được coi là đóng góp học thuật đủ giá trị không? Hay nên giữ một RDBMS để tập trung sâu hơn?

2. **Novel contribution (Freshness-aware Hybrid Top-k Fusion):** Đây là hướng nghiên cứu phù hợp với mức GR1 → GR2 → Thesis không? Hay nên scope xuống chỉ survey + baseline experiment trong GR1?

3. pgvector (PostgreSQL extension) làm vector store có được coi là "hybrid storage" đúng nghĩa không, hay cần dedicated vector DB (Milvus) để đủ tiêu chuẩn?

4. **4 trụ cột (Pipeline, Storage, Performance, Governance) hay 2–3 trụ cột?** Có nên deep-dive vào ít thành phần hơn nhưng nghiên cứu kỹ hơn không?

### 11.2 Về đánh giá

1. Tiêu chí đánh giá chính: Độ đầy đủ của kiến trúc, benchmark performance, hay chất lượng nghiên cứu lý thuyết và novel contribution?

2. **MySQL vs PostgreSQL CDC comparison** — có phải là novelty đủ, hay cần thêm gì?

3. Demo format: live demo, video recording, hay báo cáo viết là đủ?

4. Có cần paper/publication output từ GR1 không?

### 11.3 Về công nghệ

1. Apache Ranger vs OPA vs Trino native security — có preference không?

2. DataHub vs Apache Atlas vs OpenMetadata cho metadata management?

3. Cần cloud deployment (AWS/GCP) hay local K3s/Minikube là đủ?

4. Có yêu cầu cụ thể về CDC tool? (Debezium vs Maxwell vs Eventuate)

---

## 12. TÀI LIỆU THAM KHẢO {#12}

### 12.1 Tài liệu học thuật chính

| Ref | Citation | Relevance |
|---|---|---|
| [1] | Armbrust et al., "Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics," CIDR 2021 | Nền tảng lý thuyết cho toàn bộ kiến trúc Lakehouse |
| [2] | Malkov & Yashunin, "Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs," arXiv:1603.09320, 2018 | Foundation cho ANN vector retrieval (Track 2 Top-k) |
| [3] | Fagin, Lotem & Naor, "Optimal Aggregation Algorithms for Middleware," PODS 2003; arXiv:cs/0204046 | Foundation cho Threshold Algorithm (Track 1 Top-k federated) |
| [4] | Agarwal et al., "BlinkDB: Queries with Bounded Errors and Bounded Response Times on Very Large Data," EuroSys 2013; arXiv:1203.5485 | AQP với bounded error/response-time tradeoffs |
| [5] | Park et al., "VerdictDB: Universalizing Approximate Query Processing," SIGMOD 2018; arXiv:1804.00770 | Platform-independent AQP middleware |
| [6] | Kleppmann, "Designing Data-Intensive Applications," O'Reilly, 2017 | CDC mechanisms, transaction logs, streaming systems |
| [7] | Cormack, Clarke & Buettcher, "Reciprocal Rank Fusion outperforms Condorcet and individual Rank Learning Methods," SIGIR 2009 | Baseline fusion method (RRF) để compare với novel contribution |

### 12.2 Documentation chính

| Ref | Source |
|---|---|
| [8] | Debezium MySQL/PostgreSQL/MongoDB connectors documentation |
| [9] | Apache Iceberg spec và AWS Iceberg best-practices |
| [10] | Trino connector developer guide, pushdown docs, statistics docs |
| [11] | Milvus HNSW documentation |
| [12] | Trino HyperLogLog/AQP functions documentation |
| [13] | Trino Ranger access control documentation |
| [14] | Kafka Connect User Guide (SMTs) |
| [15] | DataHub OpenLineage integration docs |
| [16] | Airflow OpenLineage provider documentation |
| [17] | Apache Ranger documentation |
| [18] | pgvector GitHub repository |

---

*Tài liệu v3.0 — Cập nhật sau buổi meeting với instructor, bổ sung product definition rõ ràng và novel research contribution.*

*Mọi nội dung có thể được điều chỉnh dựa trên phản hồi của giảng viên hướng dẫn.*

**Unified Data Platform — Graduation Research 1 © 2025**
