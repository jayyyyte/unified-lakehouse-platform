# ADR 0002 — OpenLineage + Marquez in place of DataHub

*(Dùng OpenLineage + Marquez thay cho DataHub)*

**Status:** Accepted · **Date:** 2026-08-24 · **Relates to:** GR1 §3.6, Table 4.1

## Context (bối cảnh)

GR1 chọn **DataHub** làm data catalog và đồ thị **lineage mức cột** (cột nào được sinh ra từ cột nào,
qua những bước nào). DataHub nhận lineage do Airflow và Spark phát ra theo định dạng **OpenLineage**
(§3.6).

Một DataHub production gồm **năm service**: GMS (metadata service), Elasticsearch, frontend, và hai
consumer MAE/MCE, cộng thêm các Kafka topic riêng. Chính comment trong Appendix B đã thừa nhận: *"Kept
minimal here for GR1."*

**Quan sát quyết định:** yêu cầu lineage của GR1 được định nghĩa theo **đường phát OpenLineage** (phía
pipeline phát sự kiện lineage), **không** theo DataHub. DataHub chỉ là **một bên nhận** khả dĩ.

## Decision (quyết định)

Giữ đường phát OpenLineage **đúng như thiết kế**, và nhận nó bằng **Marquez**, bản implement tham chiếu
(*reference implementation*) của OpenLineage: một service cộng một schema Postgres.

## Consequences (hệ quả)

**Giữ được.** Tự động bắt lineage từ Spark và dbt mà không phải sửa code từng pipeline; đồ thị lineage
duyệt được; metadata mức dataset. Đó là phần cốt lõi của yêu cầu governance (ii) trong GR1.

**Được thêm.** Mô hình **dataset-version** của Marquez ghi một *facet* (gói metadata) cho mỗi lần chạy.
Freshness Registry (N1) có thể đọc thẳng từ đó, không phải tự dựng lại. Lựa chọn nhẹ hơn này **ăn khớp
hơn** với thành phần mới của đề tài, chứ không chỉ rẻ hơn.

**Mất.**
- Bề mặt catalog phong phú hơn của DataHub: glossary, ownership, giao diện tìm kiếm.
- **Lineage mức cột** của DataHub. Đây là mất mát thật duy nhất: dbt cho lineage mức cột ở chặng
  Silver/Gold, nhưng **chặng Bronze vẫn chỉ ở mức bảng**. Ghi lại như một giới hạn đã biết thay vì che
  đi.

**Cách trình bày.** Thay thế, không phải bỏ sót. Vì phía phát không đổi, chuyển bên nhận về lại DataHub
sau này chỉ là **đổi cấu hình**, đúng thuộc tính mở rộng mà GR1 §2.3 yêu cầu.

> **Trạng thái implement (2026-10-16):** `marquez` nằm trong profile `gov`, **chưa từng được bật**.
