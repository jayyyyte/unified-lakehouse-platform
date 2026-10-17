# Advisor Briefing — MOSAIC (GR2)

**Kịch bản: nói gì, theo thứ tự nào, demo gì, và hỏi gì.**

Soạn 2026-09-08 (sau M2), bổ sung phần M3 sau 2026-10-05 · Covers M0 → M3

> Mọi con số trong kịch bản này là **số đo thật**, không phải ước lượng. Mỗi số đều tái hiện được từ
> `experiments/results/` hoặc bằng cách chạy lại lệnh được ghi kèm. Nếu bị hỏi *"làm sao em biết?"*,
> câu trả lời luôn là **một lệnh**, và hãy nói ra lệnh đó.

---

## 0. Before the meeting (trước buổi gặp, 10 phút)

```bash
python mosaic.py up          # bật stack VÀ đợi tới khi healthy
python mosaic.py doctor      # kỳ vọng: all checks passed
python mosaic.py cdc-verify  # chứng minh masking + envelope; kỳ vọng mọi topic khớp
```

Nếu `doctor` không sạch thì **đừng demo**. Trình bày từ tài liệu này và các ledger. Một demo trực tiếp
bị hỏng gây hại nhiều hơn một demo được kể lại.

---

## 1. Opening — one paragraph (mở đầu, 30 giây)

> "The design phase produced an architecture. This phase is producing *evidence*. Four
> milestones are closed and verified — the stack, the sources with live CDC, restart safety, and the
> medallion layer on Iceberg. The most useful output so far is not that the design works; it is the
> places where the GR1 design **cannot** work as written, each found by building the thing and
> measuring it. That is what I want to walk through."

*Ý chính:* Giai đoạn thiết kế tạo ra một kiến trúc; giai đoạn này tạo ra **bằng chứng**. Bốn milestone
đã đóng và đã kiểm chứng. Sản phẩm hữu ích nhất không phải là "thiết kế chạy được", mà là **những chỗ
thiết kế GR1 *không thể* chạy như đã viết**, mỗi chỗ đều tìm ra bằng cách xây và đo.

*(Bản soạn ngày 09-08 nói "three milestones"; đã cập nhật thành bốn vì M3 đóng ngày 10-05.)*

---

## 2. Where the project stands (dự án đang ở đâu, 1 phút)

| | Milestone | Trạng thái |
|---|---|---|
| M0 | Stack, problem spec, skeleton | done, verified |
| M1 | Sources, seed corpus, load generator, live CDC | done, verified |
| M2 | CDC restart safety + WAL guard | done, verified |
| **M3** | **Medallion on Iceberg: Spark Bronze, dbt-trino Silver/Gold** | **done, verified 2026-10-05 (mới so với lần briefing M2)** |
| **M4** | **Embedding pipeline (kênh chậm) + đăng ký dataset** | **done, verified 2026-10-16: L0 tái hiện end-to-end; quyết định ở ADR 0006** |
| M5–M8 | Freshness registry, fusion engine, experiments, governance | chưa bắt đầu |

Một câu về quy mô: mọi thứ chạy ở mức `small` (~86k dòng seed, ~140k sau khi load generator ghi thêm).
Lần chạy 10M dòng cho E2 được **cố ý hoãn** cho tới khi hình dạng pipeline không còn thay đổi, vì mỗi
lần chạy tốn khoảng 40 phút.

---

## 3. The headline result (kết quả chính, 3 phút): *mở đầu bằng phần này*

**Hai phát hiện từ M2. Cả hai đều là lỗi im lặng: không lỗi, không cảnh báo, không dòng log nào. Cả
hai chỉ được tìm ra nhờ xây phép đo.**

### Finding A8: "restart safety" là at-least-once, không phải exactly-once

GR1 Table 4.3 nói restart safety đạt được vì offset được lưu trong offsets topic của Kafka Connect.
Đúng, nhưng offset được flush **theo đồng hồ** (`offset.flush.interval.ms = 60000`), không theo từng
record. Vì vậy một lần sập sẽ phát lại tới một phút công việc từ một offset cũ.

> **Đo được:** SIGKILL connector 45 s dưới tải. **Không** mất dòng nào trên 4 bảng và 3,711 dòng
> nguồn. **110 duplicate event** được phát lại. Đuổi kịp hoàn toàn **30.3 s** sau restart. *(Đính chính
> ở M3: "110" là đếm event theo key, nên lẫn cả các update hợp lệ; đếm đúng theo danh tính sự kiện thì
> một lần chạy tương đương có 183 replay. Kết luận không đổi.)*

Điểm cần nhấn mạnh: cửa sổ replay bị giới hạn bởi **chu kỳ flush**, không phải bởi **độ dài lần sập**.
Sập 5 giây hay 5 phút đều phát lại khoảng 60 s công việc như nhau. Đây cũng là lý do câu *"deduplicate
on the source offset"* của GR1 §4.2 là **bắt buộc** cho M3 chứ không phải trang trí.

### Finding A9: WAL-bloat guard của GR1 §4.4 vô hiệu

GR1 §4.4 nhận diện đúng rủi ro riêng của PostgreSQL: một logical replication slot giữ WAL cho tới khi
consumer xác nhận, nên một consumer bị kẹt khiến WAL phình không giới hạn. GR1 nêu cả biện pháp: một
heartbeat đẩy `confirmed_flush_lsn` lên kể cả khi các bảng capture đang nhàn rỗi. Biện pháp đó đã được
cấu hình, **nhưng nó không làm gì cả.**

Heartbeat vẫn bắn hoàn hảo: 503 dòng được ghi, 503 message trên topic heartbeat. Nhưng
`heartbeat.action.query` ghi vào một bảng **không nằm trong publication**, mà `pgoutput` chỉ giải mã
bảng đã publish. Vì vậy WAL của heartbeat không bao giờ thành change event, Debezium không có LSN mới
để xác nhận, và vị trí flush đứng yên trong khi WAL tăng.

> **Đo được, cùng cửa sổ nhàn rỗi 130 s, connector khoẻ:**
>
> | | mức tiến `confirmed_flush_lsn` |
> |---|---|
> | bảng heartbeat ngoài publication | **0 byte** |
> | bảng heartbeat được publish *(bản sửa)* | **688,776 byte** |
>
> Retained WAL giảm từ 704,880 xuống 4,544 byte.

**Cách đóng khung quan trọng nhất:** đây là cùng *dạng* với một phát hiện trước đó. PostgreSQL không
phát before image với replica identity mặc định, nên yêu cầu envelope của GR1 §4.2 âm thầm thất bại ở
một trong hai nguồn. Cả hai đều là biện pháp GR1 nêu trong văn xuôi nhưng vô hiệu trong thực tế cho tới
khi có thứ gì đó đo chúng. Pattern đó, *"báo cáo đúng về rủi ro nhưng sai khi cho rằng biện pháp tự
động có tác dụng"*, là **đóng góp mạnh nhất** của giai đoạn implement cho tới giờ.

> Câu nói gợi ý: *"The report is right about the risk and wrong about the fix being automatic."*

---

### M3 in one breath (M3 trong một hơi, nếu buổi gặp diễn ra sau 2026-10-05)

- **Bronze là exactly-once trên một nguồn at-least-once.** Một lần SIGKILL Connect đưa 1,107 insert
  event lên Kafka cho 1,088 order; Bronze chứa đúng 1,088. Một lần SIGKILL Spark rơi *đúng vào khe*
  giữa lúc Iceberg commit và lúc checkpoint commit; batch chạy lại ghi trùng 0 trên 673 dòng.
- **KPI đầu tiên của Table 4.4 đã đo được:** từ commit ở nguồn tới Iceberg Bronze p50 7.8 s (MySQL) /
  8.0 s (PostgreSQL). Bản thân Debezium dưới 1 giây; phần còn lại là trigger 10 s của Spark.
- **dbt-trino `MERGE` trên Nessie Iceberg chạy được.** Sau bốn lần build incremental dưới tải, Silver
  khớp nguồn sống từng dòng ở cả 7 bảng (và negative control với múi giờ sai làm mọi dòng MySQL khác
  đi, nên phép kiểm "có răng").
- **Ba phát hiện mới** (CHANGES-FROM-GR1 A10–A12): Kafka retention âm thầm xoá lịch sử CDC hai lần;
  MySQL `DATETIME` đến lệch 7 h (`TimestampConverter` của GR1 hoá ra thiết yếu); phân vùng
  `gold.orders` của §4.3 cho ra file 12.6 dòng ở quy mô này, `ANALYZE` chậm 33 lần.

---

## 4. Live demo (demo trực tiếp, 5 phút): tuỳ chọn, chọn một

**Phương án A: WAL guard** (khuyến nghị; là kết quả mới nhất của M2 và trực quan).

```bash
python mosaic.py wal-guard --idle 130
```

Chạy khoảng 6 phút, quá dài cho buổi gặp. **Hãy cho xem output đã ghi lại** ở `docs/BUILD-LOG.md`
§4.4, nơi có cặp fail/pass đặt cạnh nhau. Nói: *"this is recorded because it takes six minutes; the
command is one line and you can re-run it."* (Phần này được ghi lại vì mất sáu phút; lệnh chỉ một dòng
và thầy/cô có thể chạy lại.)

**Phương án B: restart safety** (hợp hơn nếu giảng viên muốn thấy thứ gì đó *xảy ra*).

```bash
python mosaic.py restart-safety --rate 25 --warmup 20 --stall 45 --drain 20
```

Chạy khoảng 4 phút. Vừa chạy vừa kể:
1. warm-up: CDC đang stream bình thường;
2. **SIGKILL** connector; việc ghi vẫn tiếp tục đổ vào một pipeline đã chết;
3. restart; connector sống lại sau khoảng 8 s;
4. phần chứng minh: mọi auto-increment id được ghi trong lúc sập phải xuất hiện trên topic của nó.

Sau đó chỉ vào bảng kết quả: `missing` bằng 0 ở mọi dòng, còn `dup` thì không.

**Phương án C: không demo.** Cho xem `python mosaic.py doctor` (10 giây, tất cả xanh) rồi nói theo mục 3.
Đây là lựa chọn an toàn.

---

## 5. What is next (việc tiếp theo, 1 phút)

*(Cập nhật 2026-10-16: M4 đã xong. Index chỉ chứa sản phẩm còn hàng lúc refresh kèm ảnh chụp tồn
kho, nên khi một sản phẩm hết hàng sau refresh, index vẫn trả nó "còn hàng": L0 giờ là một lệnh,
`python mosaic.py embed-verify`. Câu nên chuẩn bị trả lời: "sao không lọc theo tồn kho sống lúc
query?". Câu trả lời đã chốt là đưa post-filter vào E4 làm baseline thứ hai, xem ADR 0006 M4-A2.)*

**M5: N1 Freshness Registry.** Câu hỏi cần bàn trước: khi nguồn đứng yên, watermark nên là
`max(_src_ts)` (báo "cũ 11 ngày" cho một hệ khoẻ) hay mốc heartbeat đã xác nhận (STATUS finding 20).

Hai quyết định nên đem ra bàn:
- giữ partition spec `gold.orders` của GR1 §4.3 hay chuyển sang theo tháng (A12);
- có nên cho các topic CDC retention dài hơn không (A10).

---

## 6. If asked: what is *not* verified (nếu bị hỏi: cái gì CHƯA kiểm chứng)

Trả lời thẳng. Đừng nói giảm đi.

- Bản sửa publication được áp **bằng tay lên database đang chạy**; phần sửa trong DDL chỉ có hiệu lực
  với volume mới.
- Chưa có Iceberg maintenance (compaction, snapshot expiry) và chưa có lịch chạy dbt: Gold chỉ tươi
  bằng lần chạy `python mosaic.py dbt` thủ công gần nhất.
- Các profile `gov`, `orch`, `console` chưa từng được bật.
- Mọi thứ ở quy mô `small`.

---

## 7. Questions to ask the advisor (câu hỏi để hỏi giảng viên)

Mang theo các câu này. Một buổi gặp mà sinh viên chỉ báo cáo là một buổi lãng phí.

1. **Về cách trình bày các finding.** Bốn cấu hình GR1 không thể thực thi như đã viết (Avro converter,
   chuỗi SMT masking, replica identity, heartbeat publication), cộng hai image tag không tồn tại, và từ
   M3 thêm A10–A12. Nên trình bày chúng thành một **chương kết quả riêng** trong GR2, hay rải trong
   chương implement khi chúng xuất hiện? *Ý em: một chương riêng, vì chúng là đóng góp chính của giai
   đoạn này và chôn chúng đi là đánh giá thấp công việc.*

2. **Về phạm vi.** M4–M8 còn nhiều: freshness registry, fusion engine, bốn thí nghiệm, governance,
   domain thứ hai. Nếu thiếu thời gian, GR2 nào tốt hơn: **đủ cả cái thang nhưng nông** (cả bốn bậc,
   domain thứ hai chứng minh tính tổng quát), hay **L0–L2 thật sâu** (kết quả fusion được đo kỹ, tính
   tổng quát được lập luận chứ không được chứng minh)?

3. **Về thí nghiệm.** GR1 §10.1 muốn 10M+ dòng để thí nghiệm Top-N pushdown có ý nghĩa. Với tốc độ
   nạp đo được, mỗi lần sinh lại corpus mất khoảng 40 phút. Một lần chạy 10M ở cuối có chấp nhận được
   không, hay phép so sánh cần nhiều lần lặp?

4. **Về claim tính mới.** *"Freshness-aware rank fusion, with staleness as an explicit term in the
   scoring function"* (fusion xếp hạng có tính tới độ tươi, với độ cũ là một số hạng tường minh trong
   hàm chấm điểm) có đứng vững làm đóng góp của luận văn không, khi RRF là baseline phổ biến khắp nơi?
   Có tài liệu gần đây nào em nên đối chiếu không?

---

## 8. One-line summary to leave them with (một câu tổng kết)

> "Four milestones closed, three of GR1's five CDC KPIs now measured, the medallion layer proven
> exactly-once end to end, and a growing list of places where the design as written could not
> have been run — each found by measuring rather than reading. Next is the slow channel: the
> embedding pipeline."

*Ý chính:* Bốn milestone đã đóng, ba trong năm KPI CDC của GR1 đã đo, tầng medallion được chứng minh
exactly-once từ đầu tới cuối, và một danh sách ngày càng dài những chỗ thiết kế như đã viết không thể
chạy, mỗi chỗ đều tìm ra bằng **đo** chứ không phải bằng **đọc**. Tiếp theo là kênh chậm: embedding
pipeline.

---

### Appendix — the numbers, for reference during the meeting

*(Phụ lục: các con số để tra trong buổi gặp)*

| Phép đo | Giá trị | Nguồn |
|---|---|---|
| Service healthy | 10/10 | `mosaic.py doctor` |
| Corpus seed | 86,314 dòng trên 7 bảng | `manifest.json` |
| Tốc độ ghi OLTP duy trì được | ~100 ops/s | M1, bị giới hạn bởi fsync mỗi commit |
| Chu kỳ flush offset | 60,000 ms | config worker đang chạy |
| Số dòng mất qua một lần SIGKILL 45 s | **0** (trên 3,711) | `restart_events.jsonl` |
| Duplicate event được phát lại | 110 (M2; đếm đúng ở M3: 183) | như trên |
| Connector `RUNNING` sau restart | 8.3 s | như trên |
| Đuổi kịp hoàn toàn sau restart | 30.3 s | như trên |
| Số GTID MySQL phát lại khi hồi phục | 1,808 (`1-61738` → `1-63546`) | như trên |
| Mức tiến flush, guard hỏng → đã sửa | 0 → 688,776 byte | như trên |
| Retained WAL, guard hỏng → đã sửa | 704,880 → 4,544 byte | như trên |
| Latency commit → Bronze, p50 (M3) | 7.76 s MySQL / 7.99 s PostgreSQL | `bronze_events.jsonl` |
| Silver vs nguồn sống (M3) | 0 missing / 0 extra / 0 differing, 7 bảng | `lakehouse_events.jsonl` |
