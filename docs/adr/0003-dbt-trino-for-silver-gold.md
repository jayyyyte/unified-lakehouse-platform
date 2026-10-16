# ADR 0003 — dbt-trino for Silver and Gold; Spark retained for Bronze

*(Dùng dbt-trino cho Silver và Gold; giữ Spark cho Bronze)*

**Status:** Accepted · **Date:** 2026-08-24 · **Relates to:** GR1 §4.1, §4.2, Figure 4.1

## Context (bối cảnh)

Figure 4.1 đặt **ba engine** ở tầng xử lý: Spark Structured Streaming, dbt, và Great Expectations.
GR1 §4.2 giao cho Spark **toàn bộ đường ghi**: append vào Bronze, rồi merge/upsert vào Silver và Gold.

Thực ra đó là **hai công việc khác nhau**:
- **Nạp Bronze** là streaming thật sự: tiêu thụ liên tục các topic Debezium, chuẩn hoá envelope, khử
  trùng theo source offset, append.
- **Silver và Gold** là các phép toán tập hợp **theo batch** trên những bảng đã nằm trong Iceberg, và
  Trino vốn đã ghi được Iceberg qua `MERGE INTO`.

## Decision (quyết định)

- **Bronze**: Spark Structured Streaming, như thiết kế. Streaming xứng đáng có mặt ở đây.
- **Silver và Gold**: các **dbt model** biên dịch thành SQL Trino, ghi Iceberg qua `MERGE INTO`.
- **Quality gates** (cổng chất lượng): khối `tests:` của dbt, thay Great Expectations.

## Consequences (hệ quả)

**Giữ được.** Ngữ nghĩa medallion của GR1 §4.2 đúng y nguyên: Bronze giữ envelope chưa sửa; Silver được
khử trùng, ép kiểu, mask, kiểm tra; Gold được join, tổng hợp và phân vùng theo §4.3. Spark vẫn ở trong
kiến trúc và trong báo cáo.

**Được thêm.**
- Silver và Gold thành **SQL khai báo** thay vì job Spark mệnh lệnh: ít code hơn, và lineage suy ra
  được miễn phí từ DAG của dbt.
- Không cần một runtime nặng thứ hai cho việc batch.
- Great Expectations biến mất khỏi danh sách phụ thuộc nhưng **chức năng của nó còn nguyên**: unique
  trên khoá khử trùng, not-null trên `_src_ts`, và **các phép kiểm tham chiếu chéo nguồn** mà GR1 §4.3
  nói không engine đơn lẻ nào cưỡng chế được (ví dụ `user_id` của một order bên MySQL phải tồn tại trong
  bảng `users` bên PostgreSQL). Tất cả viết được thành dbt test.

**Mất.** Bộ từ vựng expectation phong phú hơn và phần **data-docs** của Great Expectations. dbt test
phủ đủ các phép kiểm §4.3 thực sự đặc tả; phần dư ra vốn không được dùng.

**Ghi chú.** Lệnh `ANALYZE` sau mỗi lần ghi batch (GR1 §4.5, để optimizer của Trino có thống kê đúng)
trở thành một **dbt post-hook** thay vì một task Airflow lên lịch riêng. Overhead vận hành §4.5 cảnh báo
vẫn có thật, nhưng giờ nó gắn vào **chính lần ghi gây ra nó** thay vì gắn vào một bộ hẹn giờ.

> **Trạng thái implement (M3, 2026-10-05): verified.** Rủi ro kỹ thuật chính của ADR này là `MERGE
> INTO` trên Iceberg do Nessie quản lý. Sau 4 lần build incremental dưới tải ghi đồng thời, Silver khớp
> nguồn sống từng dòng ở cả 7 bảng. 78/78 dbt node pass (67 test), gồm cả hai phép kiểm chéo nguồn của
> §4.3. Chi tiết: BUILD-LOG mục 5, CHANGES-FROM-GR1 B3.
