# E1 — So sánh CDC: MySQL binlog vs PostgreSQL WAL (GR1 Table 4.4)

*Sinh bởi `python mosaic.py e1 --phase report` lúc 2026-11-17T08:01:45Z. Mọi số ở đây đọc từ entry mới nhất của mỗi phase trong `experiments/results/e1_cdc_events.jsonl` (gitignored); file này là bản ghi vào repo. Giao thức: [ADR 0007](../../docs/adr/0007-e1-cdc-comparison.md).*

Cùng một workload (`tools/loadgen`), cùng đồng hồ, cùng stack; mỗi KPI đo trên cả hai nguồn trong cùng một phiên. Quy mô `small`. Ledger giữ cả lần thất bại.

## Table 4.4 với số thật

| Indicator | MySQL 8.0 (binlog) | PostgreSQL 15 (WAL / pgoutput) | Ghi chú |
|---|---|---|---|
| End-to-end latency, commit -> Bronze, 20 ops/s x 120 s | p50 **7.38 s**, p95 12.45, p99 14.44, max 15.56 (5,015 events) | p50 **7.96 s**, p95 12.83, p99 14.54, max 15.15 (2,767 events) | capture (commit -> Kafka) p50 0.26 / 0.51 s; phần còn lại là trigger 10 s của Bronze |
| Throughput @ 25 ops/s requested (sustained 24.99 ops/s) | **53.02 ev/s**, p95 11.87 s | **29.83 ev/s**, p95 12.50 s | rows +4,334 / +141; 2251 ops sent in 90 s, committed in 90 s |
| Throughput @ 50 ops/s requested (sustained 29.70 ops/s) | **64.10 ev/s**, p95 11.88 s | **35.51 ev/s**, p95 12.73 s | rows +8,840 / +260; 4501 ops sent in 90 s, committed in 152 s |
| Throughput @ 100 ops/s requested (sustained 41.00 ops/s) | **88.50 ev/s**, p95 13.07 s | **48.40 ev/s**, p95 12.69 s | rows +17,339 / +449; 8861 ops sent in 90 s, committed in 216 s |
| Restart recovery (SIGKILL worker 45 s under load, writes continue) | topics flat **42.12 s** after restart | topics flat **42.12 s** after restart | connectors RUNNING after 20.13 s; tải còn ghi 40.15 s sau restart, topic phẳng 1.97 s sau lần ghi cuối (hai connector phẳng ở cùng một lần poll: catch-up bị chặn bởi lúc tải dừng, không bởi cơ chế); 1440 ops during outage; gaps 0, replayed 193 (at-least-once, A8); committed offset passed the kill point after 86.97 s |
| DDL handling (ADD COLUMN under load, UPDATE one row, DROP + full-refresh) | ALTER 0.27 s; probe on Kafka after **3.72 s**; connector RUNNING:RUNNING; Bronze 1.00 s; Silver column after incremental build: yes; gone after DROP + incremental: yes; restored: yes | ALTER 0.28 s; probe on Kafka after **3.72 s**; connector RUNNING:RUNNING; Bronze 1.40 s; Silver column after incremental build: yes; gone after DROP + incremental: yes; restored: yes | MySQL schema topics got {"_schema_history.mosaic.mysql": 1, "mosaic.mysql": 1} new message(s); pgoutput emits no DDL event, the connector picks the column up from the next Relation message. dbt incremental (ADD) 21.70 s, incremental (DROP) 14.20 s, full-refresh 22.40 s. Lần chạy đầu fail ở Silver (A15, ADR 0007 E1-B6) |
| WAL lag (PostgreSQL only) | n/a (binlog retention by expire_logs_days) | under load: flush lag 209,640 B; retained WAL grew 373,752 B during the 45 s outage; after recovery lag 168 B, retained 1,295,864 B | idle 150 s with worker alive: confirmed_flush advanced 55,568 B (heartbeat 10000 ms, verdict pass); wal_keep_size bounds retention at 512 MB |

## Đọc thế nào

- *Latency*: `_src_ts` (commit ở nguồn) → `committed_at` của Iceberg snapshot chứa dòng đó. Snapshot read (`op=r`) bị loại. *Capture* = commit → Kafka record; phần còn lại là trigger của Spark.
- *Throughput*: số event mỗi connector đưa lên Kafka, chia cho cửa sổ commit (tới lúc topic ngừng tăng). `sustained` là số ops thật sự commit chia cho thời gian commit, không phải tốc độ gửi (CLAUDE.md). Một op không phải một event: `new_order` = 3 insert MySQL + 1 update PostgreSQL. Latency p95 gần như không đổi qua các bậc, nên **nút cổ chai là tốc độ commit của nguồn, không phải CDC**.
- *Restart recovery*: thời điểm topic của từng connector ngừng tăng, trừ thời điểm worker sống lại (poll 2 s). Tải vẫn ghi trong 20 s đầu sau restart (GR1: catch-up *while writes continue*), nên con số gồm cả thời gian tải còn chạy; phần thuần đuổi kịp là "phẳng sau lần ghi cuối".
- *DDL*: cột `e1_ddl_probe` (nullable) thêm vào một bảng có key tự sinh của mỗi nguồn, dưới tải; một dòng được UPDATE qua cột đó; probe được tìm trong after-image trên Kafka, trong `envelope` của Bronze, và trong Silver sau một `dbt build` incremental. Sau đó DROP ở nguồn, build incremental lần nữa (cột phải tự mất, `on_schema_change: sync_all_columns`), rồi `--full-refresh` để hoàn nguyên. Lần chạy đầu fail ở Silver: xem ADR 0007 E1-B6 và CHANGES-FROM-GR1 A15.
- *WAL lag*: `pg_replication_slots` lấy mẫu mỗi 5 s trong lần chạy restart (dưới tải); `wal-guard` chạy riêng khi các bảng được capture đứng yên (A9).
