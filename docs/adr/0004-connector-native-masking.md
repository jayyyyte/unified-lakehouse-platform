# ADR 0004 — Connector-native masking instead of Kafka Connect SMTs

*(Masking ngay trong connector thay vì dùng SMT của Kafka Connect)*

**Status:** Accepted · **Date:** 2026-08-24 · **Relates to:** GR1 §4.2, §4.4, §4.6, Table 4.5, Appendix C

> ADR này ghi lại một **mâu thuẫn bên trong chính thiết kế GR1**, chỉ lộ ra khi thiết kế được
> implement. Nên viết nó thành một **finding** trong báo cáo GR2, đừng chôn ở đây.

## The contradiction (mâu thuẫn)

GR1 đặt ra hai yêu cầu mà các SMT có sẵn của Kafka Connect **không thể thoả cùng lúc**.

**§4.2: Bronze phải giữ envelope.** *"The Bronze tier stores the unmodified Debezium change envelope,
including the before/after row state, the operation type, the source offset or log sequence number, and
the event timestamp, with no transformation applied, in order to preserve full auditability and replay
capability."*

Tức là Bronze lưu **nguyên vẹn** phong bì sự kiện Debezium (dòng trước/sau, loại thao tác, offset/LSN,
timestamp), **không biến đổi gì**.

**§4.6 / Table 4.5: masking phải không đảo ngược được và chạy trước Kafka.** Layer 1 áp `MaskField` /
`ReplaceField` *"at ingestion, before the Kafka topic"*, để PII thô *"never persists to any storage
tier."*

**Vấn đề nằm ở đây.** `MaskField$Value` và `ReplaceField$Value` chỉ chạm được các field **ở cấp ngoài
cùng** của record. Trong một change event của Debezium, các cột nghiệp vụ nằm **lồng bên trong**
`before` và `after`, nên hai SMT này không với tới. Đó chính là lý do chuỗi `transforms` của Appendix C
chạy `unwrap` (`ExtractNewRecordState`) **trước tiên**:

```
"transforms": "unwrap,maskPII,dropSensitive,ts"
```

Nhưng `unwrap` làm phẳng record, chỉ giữ ảnh `after` và **vứt bỏ `before`**. Vậy Appendix C, như đã
viết, sinh ra một Bronze **không thể** chứa envelope nguyên vẹn mà §4.2 yêu cầu. **Áp dụng chuỗi SMT và
tôn trọng §4.2 loại trừ lẫn nhau.**

Còn một vấn đề thứ hai, khó thấy hơn. Kể cả nếu bằng cách nào đó envelope được giữ lại song song với
chuỗi SMT, `MaskField` vẫn chỉ mask ảnh `after`. **Ảnh `before` của một UPDATE sẽ mang giá trị cũ chưa
mask đi thẳng vào Bronze.** Thiết kế SMT làm rò đúng thứ PII nó được đặt ra để chặn, ở **mọi lần
update** một cột cần mask.

## Decision (quyết định)

Chuyển masking Layer 1 từ SMT của Kafka Connect **vào chính connector Debezium**, và **bỏ hẳn `unwrap`**
khỏi connector.

| Việc cần làm | Appendix C (SMT) | Ở đây (connector-native) |
|---|---|---|
| Che một giá trị | `MaskField$Value` | `column.mask.hash.SHA-256.with.salt.<salt>` |
| Loại một cột | `ReplaceField$Value` | `column.exclude.list` |
| Chuẩn hoá timestamp | `TimestampConverter$Value` | `time.precision.mode=connect`, rồi cast ở Silver |
| Làm phẳng | ở connector | ở bước Bronze → Silver |

Masking native chạy **trước khi envelope được dựng**: connector đọc log, mask cột, rồi mới xây event. Vì
vậy nó phủ cả `before` lẫn `after`, và để envelope nguyên vẹn.

## Consequences (hệ quả)

**Cả hai yêu cầu của GR1 giờ cùng đúng.** Bronze nhận envelope đầy đủ (§4.2); PII thô không bao giờ
chạm tới một Kafka topic, càng không tới kho lưu trữ (§4.6). Mô hình hai lớp của Table 4.5 không đổi về
bản chất: Layer 1 vẫn không đảo ngược được và vẫn chạy trước Kafka; chỉ cơ chế dịch chuyển.

**Layer 1 trở nên mạnh hơn hẳn.** Ảnh before cũng được mask, nên lỗ rò khi update ở trên được bịt.

**Băm thay vì xoá trắng.** `MaskField` với giá trị thay thế rỗng **phá huỷ** giá trị. SHA-256 có salt
cũng không đảo ngược được, nhưng giữ cột **join được và đếm được**, nên Silver/Gold vẫn trả lời được
câu "có bao nhiêu người mua khác nhau" mà không bao giờ thấy một địa chỉ email nào. Các field GR1 nói
**không bao giờ được lưu**, là `users.national_id` và `payments.raw_card_number`, thì bị **loại hẳn**
chứ không băm, vì một hash có salt vẫn là lưu trữ.

**Chi phí.** Salt là một bí mật triển khai. Trong PoC này nó nằm trong file cấu hình connector, điều
không chấp nhận được ở production. Ghi nhận như một giới hạn; cách sửa (một Connect config provider đọc
từ secret store) được nêu tên nhưng chưa implement.

**Chuẩn hoá timestamp chuyển xuống downstream.** `TimestampConverter` là SMT thứ ba và cũng mắc đúng
giới hạn "chỉ chạm field cấp ngoài". Mục tiêu của GR1 §4.4 (một biểu diễn ISO-8601 thống nhất cho cả
MySQL và PostgreSQL) được thực hiện ở bước Bronze→Silver, nơi việc chuẩn hoá kiểu dữ liệu vốn đã diễn
ra. *(Ở M3 hoá ra đây không chỉ là chuyện định dạng: MySQL `DATETIME` lệch 7 tiếng. Xem
CHANGES-FROM-GR1 §A11.)*

## Verification (2026-08-28) — what was actually observed

*(Kiểm chứng: những gì thực sự quan sát được)*

Quyết định ở trên được đưa ra từ tài liệu. Giờ nó đã được **chạy thật**. `python mosaic.py cdc-verify`
rút kỳ vọng **từ chính cấu hình connector** và lấy mẫu ở **đuôi** mỗi topic, nên nó kiểm các change
event sống chứ không phải initial snapshot (snapshot toàn `before: null`, kiểm before-image trên đó sẽ
pass một cách vô nghĩa).

Trên 300 message lấy mẫu mỗi topic, sau một lần `python mosaic.py loadgen`:

| Khẳng định | Bằng chứng |
|---|---|
| Envelope nguyên vẹn | mọi message có `op` / `before` / `after` / `source`; không có `ExtractNewRecordState` |
| `after` đã mask | kiểm 754 giá trị đã mask trên `orders`, 674 trên `users`, tất cả là hex digest |
| **`before` đã mask** | **502 before image trên cả hai nguồn, tất cả đã mask** |
| Cột bị loại vắng mặt | `payments.raw_card_number` và `users.national_id` vắng mặt trong 637 image: không phải null, mà không có key |

Trường hợp before-image, toàn bộ lập luận của ADR này, trên một `UPDATE` PII thật:

```json
{"op": "u",
 "before": {"id": 6427, "full_name": "Dang Khanh",
            "email": "4cd0afef…0c618c", "phone": "252b376c…8a45157", "region": "north"},
 "after":  {"id": 6427, "full_name": "Dang Khanh",
            "email": "4cd0afef…0c618c", "phone": "67118265…4033879", "region": "south"}}
```

Số điện thoại đã đổi, nên hai hash khác nhau, và **cả hai** đều là hash. Email không đổi nên hai hash
giống nhau, chứng tỏ cột vẫn join được. Dưới chuỗi SMT của Appendix C, phía `before` của record này sẽ
mang số điện thoại cũ ở dạng rõ. `national_id` không có mặt ở image nào.

## Two things the running system revealed that the documentation did not

*(Hai điều hệ thống đang chạy cho thấy mà tài liệu không nói)*

**1. Hash bị cắt theo độ rộng cột nguồn.** `orders.buyer_phone VARCHAR(32)` nhận 32 ký tự hex, tức nửa
digest SHA-256, trong khi `orders.buyer_email VARCHAR(255)` nhận đủ 64. Không có log nào. Giá trị vẫn
là digest một chiều có salt, nên cam kết của §4.6 vẫn đúng, nhưng **độ mạnh của mask phụ thuộc vào DDL
nguồn**, và một cột PII hẹp sẽ âm thầm cho ra một mask yếu. `tools/cdc/verify.py` đọc độ rộng cột từ
`information_schema` và kỳ vọng `min(64, width)`, báo ra việc bị cắt thay vì chấp nhận. Chi tiết:
CHANGES-FROM-GR1 §A7.

**2. Trên PostgreSQL ban đầu không có before image nào để mask.** Với replica identity mặc định,
PostgreSQL chỉ ghi primary key của dòng cũ, nên Debezium phát `before: null` ở mọi UPDATE. Điều đó làm
claim của ADR này không kiểm được trên nguồn đó, và yêu cầu envelope của GR1 §4.2 không thể thoả ở đó.
Sửa bằng `REPLICA IDENTITY FULL` trên `users`, `products` và `inventory` trong
`domains/ecommerce/ddl/postgres.sql`. Chi tiết: CHANGES-FROM-GR1 §A6.

Cả hai đáng nêu trong báo cáo GR2. Điều thứ nhất cho thấy một cam kết masking có thể bị **làm yếu bởi
một quyết định schema ở nơi khác**. Điều thứ hai cho thấy một yêu cầu thiết kế có thể **không thoả được
ở nguồn này nhưng thoả được ở nguồn kia** mà không nguồn nào cảnh báo.

## Reference retained (giữ tham chiếu)

Cấu hình SMT gốc của Appendix C được giữ nguyên văn trong báo cáo GR1. Repo này không tái tạo nó, vì nó
không chạy như đặc tả. Trong báo cáo GR2 nên đặt **hai cấu hình cạnh nhau** làm ví dụ minh hoạ cho
finding này.
