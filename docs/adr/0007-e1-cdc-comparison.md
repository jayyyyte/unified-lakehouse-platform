# ADR 0007 — E1: thí nghiệm so sánh CDC (GR1 Table 4.4) chạy thế nào

*(Các quyết định về giao thức đo của E1: quy mô, cách đo từng KPI, cách thử DDL, và cái gì được coi là "xong")*

**Status:** Accepted · **Date:** 2026-11-17 · **Relates to:** GR1 §4.4 (Table 4.3, 4.4), §5.1
(Contribution 1); STATUS.md M7; CHANGES-FROM-GR1 A8, A9, A10, A14; ADR 0004, 0005

> Theo quy ước từ M4 (ADR 0006): các câu hỏi định hướng được hỏi và chốt **trước** khi viết code, mỗi
> quyết định có mã `E1-xx` để code comment và báo cáo tra ngược. Phiên 2026-11-17.

---

## Context (bối cảnh)

GR1 §4.4 thiết kế một benchmark so sánh MySQL binlog và PostgreSQL WAL làm nguồn CDC, với năm KPI ở
Table 4.4, và nói rõ chúng "sẽ được đo khi pipeline được implement ở GR2". Đến cuối M4, bốn trong năm
KPI đã được đo **rải rác** ở M2–M3 (latency ở `bronze-verify latency`, recovery ở `restart-safety`, WAL
ở `wal-guard`, tốc độ commit ở loadgen), nhưng:

- chưa có lần chạy nào đo **cả năm KPI trên cả hai nguồn trong cùng một phiên, cùng workload**, là điều
  kiện GR1 §5.1 đặt ra ("under identical conditions");
- `restart-safety` chỉ cho **một** số recovery cho cả worker, không quy được cho từng connector;
- KPI **DDL handling** chưa từng được thử;
- không có bảng kết quả nào ở dạng Table 4.4 để đưa vào báo cáo GR2.

E1 là việc đóng khoảng trống đó. Nó không đo gì mới về mặt cơ chế; nó **gom** các phép đo đã có thành
một giao thức, thêm phép đo còn thiếu, và sinh ra bảng kết quả.

---

## A. Hướng đi

### E1-A1. Chạy E1 ở quy mô `small`, ngay bây giờ, không đợi `full`

**Quyết định.** E1 chạy trên corpus `small` hiện có (~37k orders). Không nạp `full` trước.

**Vì sao.** Năm KPI của Table 4.4 đo **cơ chế** capture (độ trễ, tốc độ, phục hồi, DDL, WAL), không đo
khối lượng dữ liệu nằm sẵn trong bảng. Cái quyết định các con số là tốc độ ghi của workload và cấu
hình connector, cả hai không đổi theo quy mô corpus. `full` (ước tính 40 phút nạp, chưa từng chạy trên
máy này, và chưa đo giới hạn bộ nhớ Trino ở quy mô đó) là điều kiện của E2/E3, không phải của E1.

**Phương án đã loại.** Nạp `full` trước: chặn E1 sau một bước tốn kém mà không đổi kết quả; nếu cần,
chạy lại E1 ở `full` sau khi E2/E3 đã dùng corpus đó, và so hai bảng.

### E1-A2. E1 là một runner riêng, tái dùng các công cụ đo đã có làm thư viện

**Quyết định.** `experiments/e1_cdc.py`, gọi qua `python mosaic.py e1 [--phase ...]`. Nó gọi thẳng
`cmd_restart` / `cmd_wal_guard` của `tools/cdc/restart.py` và dùng `LATENCY_SQL` + `wait_landed` của
`tools/bronze/verify.py`; không chép lại logic đo. Mỗi phase ghi một entry vào ledger riêng
`experiments/results/e1_cdc_events.jsonl`; phase `report` sinh `experiments/results/e1_cdc_report.md`.

**Vì sao.** Các công cụ M2–M3 đã được kiểm chứng (và đã từng bắt lỗi harness, BUILD-LOG 4.6). Viết lại
là mời thêm lỗi. Ledger `.jsonl` bị gitignore, nên **file `.md` sinh ra là artefact vào repo**, và là
thứ báo cáo GR2 trích.

**Hệ quả.** Các entry cũ trong `restart_events` / `bronze_events` (M2–M3) vẫn là bằng chứng lịch sử,
nhưng **không** được dùng làm kết quả E1: E1 chỉ báo số của lần chạy E1.

---

## B. Từng KPI

### E1-B1. Latency: đo lại ở 20 ops/s × 120 s, cùng định nghĩa với M3

**Quyết định.** `_src_ts` (commit ở nguồn) → `committed_at` của Iceberg snapshot chứa dòng; loại
`op=r`; tách *capture* (commit → Kafka record) và *landing* (Kafka → snapshot). Đúng như
`bronze-verify latency`, để số E1 so sánh trực tiếp được với M3 (p50 7.76 / 7.99 s).

### E1-B2. Throughput: bậc thang 25 / 50 / 100 ops/s, mỗi bậc 90 s, đếm lại từ nguồn

**Quyết định.** Ba bậc. Mỗi bậc ghi: ops đã gửi, **cửa sổ commit** (tới khi pipe client đã xả hết),
`sustained = ops / cửa sổ commit`, số dòng mới ở từng bảng (đọc lại `MAX(key)` ở nguồn), số event
mỗi connector đưa lên Kafka (offset topic, trừ topic heartbeat) chia cho cửa sổ, và percentile latency
Bronze ở bậc đó.

**Vì sao.** Một con số throughput giấu mất điểm bão hoà. Loadgen gửi vào pipe, MySQL commit ~100 ops/s
trên máy này (CLAUDE.md), nên "tốc độ gửi" không phải "tốc độ commit"; cửa sổ commit kết thúc ở
`close()` của session, không phải lúc ngừng gửi. Con số events/s **không** bằng ops/s: một `new_order`
là 3 insert MySQL + 1 update PostgreSQL, một `advance_status` là 1 update; tỉ lệ event/op là đặc tính
của workload, được ghi trong ledger để người đọc tự quy đổi.

### E1-B3. Restart recovery: cùng phép SIGKILL của M2, nhưng quy thời gian đuổi kịp cho **từng connector**

**Quyết định.** Sửa `tools/cdc/restart.py`: trong vòng lặp drain, theo dõi offset của các topic **theo
connector**; thời gian đuổi kịp của một connector = lúc topic của nó ngừng tăng, trừ lúc worker sống
lại. Ghi `recovery_by_connector` vào ledger bên cạnh `recovery_seconds` cũ (không đổi nghĩa số cũ).

**Vì sao.** GR1 so sánh hai cơ chế; "worker đuổi kịp sau 30 s" không nói cơ chế nào chậm hơn. Cả hai
connector chạy trong cùng một worker, nên phép kill là chung, nhưng catch-up thì tách được vì topic
tách.

### E1-B4. DDL handling: ADD COLUMN nullable dưới tải, UPDATE một dòng, rồi DROP + `--full-refresh` Silver

**Quyết định.** Trên **một bảng có key tự sinh của mỗi nguồn** (chọn tự động từ connector config, là
bảng đầu tiên trong `table.include.list` có auto-increment / sequence), dưới tải 20 ops/s:

1. `ALTER TABLE t ADD COLUMN e1_ddl_probe VARCHAR(32) NULL` (đo thời gian lệnh);
2. `UPDATE t SET e1_ddl_probe = 'e1-<engine>' WHERE key = MAX(key)`;
3. đợi event mang giá trị đó trong `after` trên topic của bảng (đo giây), ghi trạng thái connector;
4. MySQL: đếm message mới trên `_schema_history.mosaic.mysql` và topic schema-change `mosaic.mysql`,
   trích câu DDL Debezium ghi lại; PostgreSQL: không có topic tương đương (pgoutput không phát DDL);
5. đợi dòng đó xuất hiện trong Bronze (`json_extract_scalar(envelope, '$.after.e1_ddl_probe')`), xác nhận
   stream vẫn active;
6. `dbt build --select silver__<t>` (incremental): cột mới có xuất hiện trong Silver không, giá trị
   probe có đúng không;
7. hoàn nguyên: `DROP COLUMN` ở nguồn, `dbt build --full-refresh --select` hai model đó, kiểm cột đã
   mất và `count(*)` Silver = nguồn.

**Vì sao chọn cách này.**
- Bronze giữ envelope nguyên văn (ADR 0005, `schemas.enable=false`), nên DDL **không thể** làm hỏng
  Bronze; điều cần đo là nó lan tới đâu và bao lâu.
- Silver được **sinh từ catalog sống** ở mỗi lần dbt (`assemble.py`), và dbt đặt
  `on_schema_change: append_new_columns`. Vậy cột mới sẽ tự vào Silver ở build kế tiếp, nhưng
  **`append_new_columns` không bao giờ bỏ cột**: sau DROP ở nguồn, Silver vẫn giữ cột (toàn NULL) cho
  tới khi `--full-refresh`. Đó là "downstream impact" GR1 muốn thấy, và là lý do bước 7 bắt buộc.
- Nullable, không default: biến thể `NOT NULL DEFAULT` không phát lại các dòng cũ qua CDC (Debezium
  không re-emit), nên không có gì thêm để đo mà lại khó hoàn nguyên hơn.
- Chỉ build Silver, không build Gold: Gold đọc Silver qua các cột khai báo, và sau bước 7 Silver y hệt
  trước. Gold không bị chạm.

**Phương án đã loại.** Giữ cột (không hoàn nguyên): để schema nguồn lệch với DDL trong repo và Silver
mang một cột lạ. Bỏ KPI: phải khai "chưa đo" trong báo cáo cho một KPI GR1 tự đặt ra.

**Rủi ro đã nhận.** Nếu `assemble.py` không thấy cột mới (cache metadata của Trino connector), đó là
một finding, không phải lỗi hại: ledger ghi `silver_has_column_after_incremental=false` và phase báo
FAIL thay vì giả vờ pass.

### E1-B5. WAL lag: tái dùng `wal-guard` (nguồn nhàn rỗi) + chuỗi slot của lần chạy restart (dưới tải)

**Quyết định.** Hai góc nhìn, vì KPI này chỉ có nghĩa khi có cả hai: (a) `wal-guard --idle 150`, nguồn
đứng yên, worker sống rồi chết, chứng minh heartbeat là thứ đẩy `confirmed_flush_lsn` (A9); (b) từ lần
chạy restart của E1: flush lag dưới tải trước kill, WAL giữ lại tăng bao nhiêu trong 45 s outage, lag
sau phục hồi. MySQL: cột "n/a", vì binlog retention theo `expire_logs_days`, độc lập với consumer
(GR1 Table 4.3).

### E1-B6. *(chốt sau lần chạy đầu)* Silver phải chịu được DDL ở nguồn theo **cả hai chiều**, không cần `--full-refresh`

**Điều lần chạy đầu (2026-11-17 07:48 UTC) cho thấy.** Debezium ở cả hai nguồn stream qua `ADD COLUMN`
trong < 5 s, Bronze nhận trong ~1 s, nhưng `dbt build` incremental của hai model Silver **vỡ**:
`Column 't.e1_ddl_probe' cannot be resolved`. Nguyên nhân nằm ở thứ tự của dbt, không ở Debezium:
`assemble.py` đọc catalog sống nên model mới đã liệt kê cột mới; macro `cdc_current_state` tự join với
bảng Silver hiện có (`{{ this }} t`) bằng chính danh sách cột đó; nhưng dbt chỉ áp `on_schema_change`
lên bảng đích **sau khi** đã build xong bảng tạm từ SQL của model. Bảng tạm không bao giờ build được, nên
`append_new_columns` không có cơ hội chạy. Hệ quả thực tế trước bản sửa: **mọi `ALTER TABLE ADD COLUMN`
ở nguồn làm Silver ngừng cập nhật cho tới khi có người chạy `--full-refresh`**. Ledger giữ entry
`fail` này (`e1_cdc_events.jsonl`, kind `e1-ddl`, 07:48 UTC).

**Quyết định.**
1. Nhánh self-join của macro chỉ lấy từ `t` những cột **bảng đích đang có**
   (`adapter.get_columns_in_relation(this)`); cột mới thì `cast(null as <type>)`; cột đã bỏ ở nguồn thì
   không còn được chọn. Macro vẫn domain-free: nó chỉ so hai danh sách tên.
2. `on_schema_change: sync_all_columns` thay cho `append_new_columns`, để cột bị DROP ở nguồn cũng rời
   khỏi Silver. Với `append_new_columns`, bảng đích giữ cột cũ còn bảng tạm thì không, và câu MERGE của
   dbt (liệt kê cột theo bảng đích) sẽ vỡ ở chiều ngược lại.
3. Phase DDL đo **cả chiều DROP** bằng một build incremental trước khi `--full-refresh` (bước hoàn
   nguyên vẫn giữ, để kết quả hoàn nguyên không phụ thuộc vào bản sửa).

**Kết quả sau sửa (07:51 UTC):** ADD → incremental MERGE 474 / 39 dòng, cột xuất hiện với đúng giá trị
probe ở cả hai Silver; DROP → incremental MERGE 0 dòng, cột **tự mất**; `--full-refresh` sau đó tạo lại
49,556 / 7,660 dòng, bằng nguồn. Hai entry fail/pass nằm cạnh nhau trong ledger, như cặp của `wal-guard`
ở A9.

**Vì sao đây là finding cho báo cáo.** GR1 Table 4.4 đặt KPI "DDL handling" ở tầng CDC (binlog có
schema-history, pgoutput "more limited"). Phép đo cho thấy tầng CDC **không phải** nơi DDL gây hại ở
stack này: cả hai connector đều qua được như nhau (MySQL còn ghi câu DDL vào `_schema_history` và
topic schema-change; PostgreSQL thì không có dấu vết nào ngoài after-image). Nơi DDL gây hại là tầng
**transform**, và chỉ vì cách dbt xếp thứ tự "build tạm rồi mới sửa schema". Xem CHANGES-FROM-GR1 A15.

---

## C. Định nghĩa "xong"

- `python mosaic.py e1` chạy hết sáu phase trên stack đang sống, mỗi phase ghi một entry, kể cả khi
  fail (ledger giữ cả thất bại, như mọi ledger khác).
- `experiments/results/e1_cdc_report.md` có đủ năm dòng của Table 4.4 với số thật cho cả hai nguồn,
  được commit.
- Schema nguồn và Silver **y nguyên** sau phase DDL (kiểm bằng máy, không bằng mắt).
- STATUS.md: M7 chuyển sang "E1 done, verified"; BUILD-LOG mục 7.11; CHANGES-FROM-GR1 bổ sung nếu phase
  DDL lộ ra điều GR1 không nói.

## Kết quả (2026-11-17)

Mọi phase pass trên cả hai nguồn; bảng đầy đủ ở `experiments/results/e1_cdc_report.md`, nhật ký ở BUILD-LOG 7.11.

| KPI | MySQL | PostgreSQL |
|---|---|---|
| Latency p50 / p99 (20 ops/s) | 7.38 / 14.44 s | 7.96 / 14.54 s |
| Kafka ev/s ở bậc 100 ops/s (sustained 41.0) | 88.5 | 48.4 |
| Restart: phẳng sau restart | 42.1 s | 42.1 s |
| DDL: cột mới trên Kafka | 3.72 s | 3.72 s |
| WAL | n/a | idle + worker sống: flush tiến 55,568 B; outage 45 s dưới tải: giữ thêm 373,752 B |

Bốn điều rút ra cho báo cáo GR2:

1. **Hai cơ chế ngang nhau ở mọi KPI đo được trên máy này**, trừ capture p50 (0.26 s binlog so với 0.51 s pgoutput).
   Phần lớn độ trễ là trigger 10 s của Bronze, không phải CDC.
2. **Throughput bị chặn ở nguồn**, không ở CDC: p95 không đổi khi tải tăng (finding 24).
3. **Restart recovery không tách được theo connector khi tải còn ghi**: hai connector phẳng ở cùng một lần poll,
   vì catch-up kết thúc khi tải dừng. Phần "thuần đuổi kịp" là 1.97 s sau lần ghi cuối. Muốn so hai cơ chế ở
   KPI này phải dừng tải ngay lúc restart, tức là đổi định nghĩa của GR1; chưa làm.
4. **DDL hỏng ở tầng transform, không ở tầng CDC** (E1-B6, A15).
