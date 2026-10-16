# ADR 0005 — JSON converter instead of Confluent Avro + Schema Registry

*(Dùng JSON converter thay cho Confluent Avro + Schema Registry)*

**Status:** Accepted · **Date:** 2026-08-25 · **Relates to:** GR1 Appendix B, Appendix C, Fig. 4.1, Table 4.4

## Context (bối cảnh)

*Converter* là thành phần quyết định Kafka Connect tuần tự hoá (serialize) mỗi message thành bytes như
thế nào trước khi ghi vào Kafka.

GR1 Appendix B cấu hình Connect worker dùng `io.confluent.connect.avro.AvroConverter` kết nối với một
**Confluent Schema Registry** (service lưu schema Avro), và Appendix C lặp lại cấu hình đó cho từng
connector.

Ở lần chạy thật đầu tiên, Connect worker **chết ngay khi khởi động**, trước khi phục vụ được request
nào:

```
ERROR  Stopping due to error   [org.apache.kafka.connect.cli.AbstractConnectCli]
org.apache.kafka.common.config.ConfigException: Invalid value
io.confluent.connect.avro.AvroConverter for configuration key.converter:
Class io.confluent.connect.avro.AvroConverter could not be found.
```

Image `debezium/connect` **không đóng gói** Avro converter của Confluent. Converter này theo giấy phép
Confluent và được phát hành riêng. Vậy **Appendix B không thể đã từng được chạy như đã viết**.

## Decision (quyết định)

Dùng `org.apache.kafka.connect.json.JsonConverter` với `schemas.enable=false` cho cả key lẫn value.

Các phương án khác đã cân nhắc:

| Phương án | Vì sao không |
|---|---|
| Tự build image kèm jar Avro của Confluent | Khoảng 8 jar phụ thuộc bắc cầu phải pin bằng tay, dễ vỡ khi đổi version. Tệ hơn: wire format của Confluent gắn một byte "magic" và schema id ở đầu mỗi message, mà `from_avro` của Spark bản OSS **không tích hợp Schema Registry**, nên Bronze job sẽ phải dùng ABRiS hoặc tự giải mã. |
| Converter Apicurio (`ENABLE_APICURIO_CONVERTERS=true`) | Có sẵn trong image nên không phải tải về, nhưng đổi Confluent Schema Registry thành **Apicurio Registry**, một service Appendix B không hề nêu, và vẫn để nguyên gánh nặng wire format Avro cho Spark. |

## Consequences (hệ quả)

**Giữ được.** Envelope Debezium (`op`, `before`, `after`, `source`) được giữ đầy đủ, đúng điều GR1 §4.2
yêu cầu ở Bronze. `schemas.enable=false` chỉ bỏ *khối schema* Connect đính kèm mỗi message (mô tả kiểu
từng field), **không** bỏ envelope.

**Có thể nói là tốt hơn cho Bronze.** §4.2 biện minh cho Bronze bằng khả năng audit và replay. JSON tự
mô tả đọc được bằng bất kỳ công cụ nào mà không cần tra registry. Một payload Avro nhị phân có schema
nằm ở service khác là bằng chứng audit yếu hơn, vì mất service đó là không đọc được. Phía downstream,
Spark chỉ cần parse JSON thường thay vì cần một bộ giải mã biết Schema Registry.

**Mất.** Mã hoá nhị phân gọn, và việc registry cưỡng chế tương thích schema. Ở quy mô của dự án (một
node, 10M dòng) cả hai đều không thiết yếu.

**Mất một phần: độ trung thực khi schema thay đổi.** Với `schemas.enable=false`, việc **thêm hoặc bỏ
một cột** vẫn quan sát được qua thay đổi tập field trong payload, nên KPI DDL-handling của GR1 Table
4.4 vẫn đo được cho trường hợp thông thường. Nhưng **đổi kiểu dữ liệu** (ví dụ `INT` → `BIGINT`) mà
không đổi tập field thì **không quan sát được**. Ghi nhận như một giới hạn; nếu thí nghiệm DDL cần,
bật `value.converter.schemas.enable` thành `true` sẽ khôi phục metadata kiểu, đổi lại message to hơn
khoảng một bậc độ lớn (~10 lần).

**Schema Registry vẫn deploy nhưng nằm ngoài đường dữ liệu.** Nó vẫn ở trong stack và trong Fig. 4.1,
để quay lại Avro chỉ là đổi cấu hình chứ không phải deploy lại. Hiện nó **không được dùng**, và báo cáo
nên nói rõ như vậy thay vì ngầm ý nó là load-bearing.

**Đảo ngược được.** Converter là một dòng trong config worker và hai dòng mỗi connector.
