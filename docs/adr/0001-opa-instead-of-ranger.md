# ADR 0001 — Open Policy Agent in place of Apache Ranger

*(Dùng Open Policy Agent thay cho Apache Ranger)*

**Status:** Accepted · **Date:** 2026-08-24 · **Relates to:** GR1 §4.6, Table 4.5, Table 4.6

> *ADR (Architecture Decision Record) = tài liệu ngắn ghi lại một quyết định kiến trúc: bối cảnh, quyết
> định, hệ quả.*

## Context (bối cảnh)

GR1 Appendix B chọn **Apache Ranger** làm engine phân quyền (*authorization*), có hai nhiệm vụ:
- cưỡng chế **phân cấp năm role** ở Table 4.6;
- **column masking lúc query** ở Table 4.5 (masking Layer 2: che cột tuỳ theo role của người đang
  query).

Một Ranger chạy được cần **bốn thành phần**: service Ranger Admin, một database quan hệ phía sau, Solr
để index audit log, và plugin Trino gắn vào coordinator. Trên target **Docker Compose một máy** của
Appendix B, chừng đó tranh bộ nhớ với Kafka, Spark, Trino, MinIO và hai database. Theo kinh nghiệm
chung, tích hợp Ranger thường tốn **nhiều tuần** trước khi policy đầu tiên được đánh giá.

## Decision (quyết định)

Cưỡng chế phân quyền bằng **Open Policy Agent (OPA)**, đứng sau access-control provider `opa` có sẵn
của Trino. Khi Trino nhận một query, nó hỏi OPA "user này có được làm việc này không", và OPA trả lời
dựa trên policy.

**Mô hình policy không đổi.** Năm role của Table 4.6 (`data_admin`, `data_engineer`, `data_analyst`,
`ml_engineer`, `bi_user`) được viết **nguyên văn** bằng **Rego**, ngôn ngữ policy của OPA. Column
masking vẫn chạy lúc query và phụ thuộc role, đúng như Table 4.5 mô tả.

## Consequences (hệ quả)

**Giữ được.** Mọi cam kết GR1 đưa ra: policy tập trung; kiểm soát truy cập theo role ở mức catalog /
schema / table / column; column masking lúc query; và audit trail cho các quyết định.

**Được thêm.** Policy trở thành một artifact **văn bản thuần, quản lý bằng git, unit test được**. Lệnh
`opa test` chạy phân cấp role như một test suite trong CI. Khi bảo vệ, đó là bằng chứng tốt hơn hẳn ảnh
chụp màn hình một giao diện admin. Và chỉ cần **một container thay vì bốn**.

**Mất.**
- Giao diện web để soạn policy của Ranger: không phải yêu cầu của dự án.
- Tích hợp **tag-sync** của Ranger với catalog (tự đồng bộ tag từ catalog sang policy). Cái này quan
  trọng với N3 (lan truyền tag). Vì vậy mô hình tag được giao cho `platform/governance/` sở hữu, rồi
  truyền sang OPA dưới dạng **policy data**. Đó cũng là lý do N3 là một **đóng góp** chứ không phải một
  bài tập cấu hình.

**Cách trình bày (Framing).** Đây là một **thay thế, không phải bỏ sót** (*substitution, not
omission*). Ranger vẫn là lựa chọn đúng ở quy mô production với một đội platform riêng, và báo cáo nên
nói vậy. Giám khảo so repo này với GR1 Appendix B sẽ nhận ra khác biệt; câu trả lời trung thực đã nằm
sẵn trên giấy trước khi họ hỏi.

> **Trạng thái implement (2026-10-16):** profile `gov` (chứa `opa`) **chưa từng được bật**. Đây là
> quyết định đã chốt, chưa phải thứ đã chạy. Xem STATUS.md.
