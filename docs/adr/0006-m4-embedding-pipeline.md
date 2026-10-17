# ADR 0006 — M4: embedding pipeline (kênh chậm) và việc đăng ký dataset

*(Các quyết định thiết kế của M4: kênh "chậm" là gì, đọc từ đâu, ghi ra sao, và cái gì được coi là "xong")*

**Status:** Accepted · **Date:** 2026-10-16 · **Relates to:** GR1 §3.4, §4.1, §5.3 (Eq. 5.1, 5.2),
Appendix A (`product_embeddings`); PROBLEM.md L0–L3; CHANGES-FROM-GR1 C2–C4, C9

> **Vì sao ADR này dài hơn các ADR khác.** ADR 0001–0005 mỗi cái ghi *một* phép thay thế. ADR này ghi
> **một loạt quyết định** được hỏi và chốt *trước khi* viết dòng code nào của M4 (phiên 2026-10-16). Mỗi
> quyết định có mã `M4-xx` trùng với mã câu hỏi lúc đó, để sau này đọc code comment (`M4-A1`, `M4-C2`…)
> là tra ngược về đúng lý do. Nhiều quyết định ở đây **định nghĩa lại** ý nghĩa của "kênh chậm" trong
> PROBLEM.md, nên nếu sau này có ai muốn đổi, phải đọc phần *Context* trước.

---

## Context (bối cảnh): câu hỏi thật sự của M4

STATUS.md giao M4 việc "embed text sản phẩm vào `product_embeddings`". Đọc theo nghĩa đen thì đó là việc
kỹ thuật thuần tuý: gọi model, ghi vector. Nhưng khi đặt cạnh L0 thì lộ ra một vấn đề **về nghĩa**:

- L0 nói: *kênh ngữ nghĩa cũ 6 tiếng nên không biết sản phẩm đã hết hàng, và vẫn xếp nó #1*.
- Nhưng embedding chỉ mã hoá **ý nghĩa của mô tả**. Nó không mã hoá tồn kho.
- Và load generator **cố ý không bao giờ sửa** `products.name` / `description` (CHANGES-FROM-GR1 C9),
  để mọi stale hit chỉ có thể quy cho bất đối xứng độ tươi.

Hệ quả: nếu M4 chỉ "embed mô tả", thì một embedding cũ 6 tiếng **giống hệt** một embedding mới. Kênh
ANN sẽ không bao giờ *sai*, chỉ *già*. Hàm `g(s_emb)` của Eq. 5.2 sẽ phạt một kênh không hề sai, và thí
nghiệm E4 sẽ đo một hiệu ứng **không tồn tại**. Đây là rủi ro lớn nhất của M4, và nó không nằm ở kỹ thuật.

Các quyết định dưới đây là câu trả lời cho rủi ro đó, cộng với những lựa chọn kỹ thuật đi kèm.

---

## A. Hướng đi

### M4-A1. Kênh ANN "cũ" là cũ về **tập ứng viên và các fact chụp lại lúc refresh**

**Quyết định.** Index chỉ chứa các sản phẩm **đủ điều kiện tại thời điểm refresh** (ở domain
ecommerce: `stock_qty > 0`). Bên cạnh mỗi vector, index lưu **ảnh chụp** (*snapshot*) của các fact thay
đổi nhanh tại thời điểm đó (`stock_qty`, `price`).

**Vì sao.** Đây là cách duy nhất làm cho "index cũ" có nghĩa là "index **sai**":

- Lúc refresh, sản phẩm P còn 25 cái → P vào index, kèm `stock_qty=25`.
- Năm phút sau P hết hàng ở nguồn sống. Index không biết, vẫn trả P, và vẫn "tin" rằng còn 25.
- Đó chính xác là **stale hit** mà L0 mô tả. Nó giờ là một sự kiện quan sát được (`embed-verify`
  kiểm chính điều này), không còn là lời kể.

Nó cũng khớp với hệ thống thật: một semantic index tính theo batch là một *bức ảnh* của catalogue tại
một thời điểm, gồm cả câu hỏi "cái gì đáng được index". Hai điều kiện then chốt:

1. **Điều kiện đủ (`eligible`) và các cột snapshot là do domain khai báo** trong `retrieval.yaml`, không
   phải do platform biết. Platform chỉ chèn predicate vào `WHERE` và chép các cột được nêu tên.
2. **Text đem embed vẫn là text chậm** (M4-C3). Snapshot nằm *cạnh* vector, không nằm *trong* nó.

**Phương án đã loại.** Embed mọi sản phẩm, không có snapshot: index không bao giờ sai, mất toàn bộ L0
(lý do ở phần Context).

### M4-A2. "Lọc theo tồn kho sống lúc query" được đưa vào E4 làm **baseline thứ hai**

**Câu hỏi phản biện mà giám khảo gần như chắc chắn sẽ hỏi:** *"Sao không lọc kết quả ANN theo tồn kho
sống lúc query? Lỗi biến mất mà không cần fusion gì cả."*

**Quyết định.** Không né câu hỏi này. Đưa **post-filter trên nguồn sống** vào E4 thành baseline thứ
hai, bên cạnh RRF. Lập luận sẽ được bảo vệ bằng số đo chứ không bằng lời:

| | RRF | Post-filter sống | Freshness-weighted fusion (đề xuất) |
|---|---|---|---|
| Cần biết cột nào nghĩa là "còn bán được" | không | **có**: tri thức domain đi vào runner | không: chỉ cần watermark |
| Chi phí mỗi query | 2 kênh | 2 kênh + **một lượt gọi về OLTP** | 2 kênh + đọc watermark (đã có sẵn) |
| Xử lý fact nhanh *khác* tồn kho (giá, bị gỡ, người bán bị khoá) | không | mỗi loại một bộ lọc riêng | tự động, vì cùng là "dữ liệu cũ" |
| Có tổng quát sang domain thứ hai (L3) | có | phải viết lại bộ lọc cho `jobmarket` | có |

Post-filter là **câu trả lời đúng cho một domain biết rõ fact nào hay đổi**. Đề tài claim một thứ khác:
một cơ chế **generic** không cần biết điều đó. E4 phải cho thấy cái giá của sự generic là bao nhiêu
(nDCG thấp hơn post-filter bao nhiêu), chứ không giả vờ rằng không có cái giá đó.

**Ảnh hưởng tới M4:** không có code; đây là cam kết cho M6/M7. Ghi vào CHANGES-FROM-GR1 C3.

### M4-A3. Đọc từ **Gold**, và `refreshed_at` = **watermark của dữ liệu đã đọc**

**Quyết định.**
- Nguồn của index là `iceberg.gold.product_catalog` (model Gold mới, xem M4-B2a), đúng câu GR1 §4.1:
  *"pgvector serves semantic-similarity queries against the embeddings generated from the Gold-layer
  product catalog"*.
- Mỗi lần refresh, **mọi dòng** được đóng cùng một `refreshed_at` = `max(_src_ts)` của bảng Gold **tại
  Iceberg snapshot mà lần đọc đã pin vào**. Giờ chạy job thì lưu riêng ở `embedded_at`, chỉ để chẩn đoán.

**Vì sao không đóng dấu bằng giờ chạy job.** Gold chỉ tươi bằng lần `dbt` gần nhất. Nếu Gold build lúc
10:00 rồi 13:00 mới embed, mà đóng dấu `refreshed_at = 13:00`, thì index **tự nhận là tươi** trong khi
nó mô tả thế giới lúc 10:00. Đó chính là kiểu che giấu độ cũ mà N1 sinh ra để phơi bày. Với cách đóng
dấu theo watermark, độ cũ của kênh ANN = (độ cũ của Gold lúc đọc) + (thời gian từ lúc đó tới nay). Hai
thành phần này **tách được**, vì `embedded_at − refreshed_at` chính là độ cũ của Gold lúc embed (ledger
ghi lại là `age_on_landing_s`).

**Vì sao pin snapshot.** Lần đọc lấy snapshot hiện tại của bảng rồi đọc `FOR VERSION AS OF <id>`, trong
**một câu lệnh** trả về cả dữ liệu lẫn watermark (`max(_src_ts) OVER ()`). Nếu dbt commit giữa chừng, dữ
liệu và watermark vẫn đến từ cùng một phiên bản. Snapshot id được ghi vào ledger, nên `embed-verify`
đếm lại được *đúng* tập dữ liệu mà lần refresh đã thấy.

**Hệ quả đã thấy ngay lần chạy đầu (bài toán nguồn nhàn rỗi).** Nguồn không có thay đổi nào từ
2026-10-05, nên `max(_src_ts)` của Gold đứng ở 2026-10-05, và index build ngày 2026-10-16 **cũ 11 ngày
ngay lúc ra đời**. Xét theo dữ liệu thì điều đó đúng: không có sự kiện nào mới hơn để biết. Nhưng nó
cũng nói lên một điều N1 (M5) phải giải: **watermark theo event-time không phân biệt được "nguồn đứng
yên" với "pipeline bị kẹt"**. Debezium heartbeat (A9) là nguyên liệu để phân biệt: nó chứng minh connector
đã đọc tới "bây giờ" dù không có thay đổi dữ liệu. Ghi lại như một **việc của M5**, không sửa ở M4.

**Phương án đã loại.**
- Đọc thẳng Silver hoặc PostgreSQL: lệch spec GR1, và đọc OLTP trực tiếp làm mờ ranh giới "kênh chậm đọc
  từ lakehouse".
- `refreshed_at` = giờ chạy job: lý do ở trên.

---

## B. Phạm vi M4

### M4-B1. M4 = embedding pipeline + đăng ký dataset; Track 1 (Top-N pushdown) để E2

Phần federation của M4 đã xong từ M0 (Listing 4.2 chạy được, 1.61 s). Thí nghiệm Top-N pushdown cần quy
mô `full`, nên thuộc E2 ở M7.

### M4-B2. Runner **domain-free**, được gate kiểm bằng máy

**Quyết định.** `pipelines/embeddings/` không biết domain nào. Domain khai báo trong
`domains/<d>/retrieval.yaml`: dataset nguồn, khoá entity, các cột text, predicate `eligible`, các cột
snapshot, bảng đích, model, tham số HNSW, SLA. Schema của file được validate bởi
`platform/spec/retrieval_spec.py`, viết **ngay từ M4** thay vì đợi M6.

**Gate được mở rộng** (`mosaic.py`, `GATED_DIRS`) để quét cả `pipelines/embeddings/`, vì một quyết định
không có gì kiểm thì sẽ mòn dần. Hai chi tiết:

- **`ORDER BY` được miễn trừ.** Từ cấm `order` khớp cả từ khoá SQL, mà mọi truy vấn xếp hạng đều có
  `ORDER BY`. Miễn trừ đúng cụm `order\s+by`, không miễn trừ gì khác: `"orders"` vẫn bị bắt (đã chạy
  negative control).
- Gate **bắt được một lỗi thật** ngay lần chạy đầu: comment *"cosine distance and inner **product**
  agree"*. Đã đổi thành "dot similarity".

`pipelines/transform/` **không** được đưa vào gate: các Silver model sinh tự động ở đó mang tên bảng là
chuyện hợp lệ, và `assemble.py` mới là thứ giữ cho project dbt domain-free.

**M4-B2a. Model Gold mới `product_catalog`.** `gold.product_sales` không có `description` hay tên category.
Thay vì làm phình nó, M4 thêm `domains/ecommerce/gold/product_catalog.sql`: một dòng mỗi sản phẩm, gồm
text (tên, mô tả, tên category lá) và hai fact nhanh (`stock_qty`, `price`). `product_sales` là nguyên
liệu của *kênh SQL*; `product_catalog` là nguyên liệu của *kênh ANN*. Hai bảng có hai lý do tồn tại khác
nhau.

### M4-B3. Chốt chặn N3: **gắn vào ngay, propagation để M8**

**Quyết định.** `platform/governance/guard.py` kiểm, trước mỗi lần đọc text: có cột nào trong các cột
text mang tag `sensitive` trong `platform.column_tags` không. Có thì **từ chối** refresh (exit 2, ghi vào
ledger là `refused_by_guard`) và index đang chạy không bị đụng tới.

Cái **chưa** có là **lan truyền tag theo lineage** (source → Bronze → Silver → Gold): việc của M8. Hiện
`column_tags` chỉ chứa những gì được ghi thẳng vào nó. Để không ai đọc nhầm "0 cột bị chặn" thành "đã
an toàn", mỗi lần chạy guard in ra **tổng số tag đang biết**, kèm ghi chú khi kho tag rỗng.

**Kiểm chứng có "răng":** `embed-verify` gắn tạm tag `sensitive` lên `product_catalog.description`, chạy
refresh, đòi phải bị từ chối và index không đổi, rồi gỡ tag và đòi guard pass.

### M4-B4. Đăng ký dataset ở M4; danh sách do domain khai báo

**Quyết định.** `python mosaic.py register` nạp vào `platform.datasets`:
- dataset dùng chung của platform (`platform/freshness/datasets.yaml`: Bronze, `domain = NULL`),
- dataset của domain (mục `datasets:` trong `retrieval.yaml`: 7 Silver + 5 Gold),
- index vector, **tự suy ra** từ mục `embeddings:` (kind `vector`, row timestamp `refreshed_at`).

**Probe trước khi ghi, tất cả hoặc không gì cả.** Mỗi dataset được query thật (`count(*)`,
`max(row_timestamp_column)`). Bảng không tồn tại, hoặc cột thời gian khai báo sai, thì **không đăng ký
gì cả**. Dataset của domain không còn được khai báo thì bị xoá, để `platform.datasets` luôn phản chiếu
đúng spec. Lệnh in luôn watermark hiện tại của từng dataset: đó là **bản xem trước** những gì collector
của M5 sẽ duy trì, chứ **không** ghi vào `platform.watermarks`.

**Vì sao khai báo thay vì tự suy ra Silver từ connector.** Mỗi dataset cần một **SLA**, mà SLA là phán
đoán của domain, không suy ra được. Khai báo tường minh dễ đọc hơn trong báo cáo, và bước probe bắt được
mọi chỗ spec lệch khỏi thực tế.

---

## C. Quyết định kỹ thuật

### M4-C1. Không có scheduler; chu kỳ refresh là **biến thí nghiệm**

`python mosaic.py embed refresh` là lệnh one-shot. Không có cron, không có Airflow (profile `orch` chưa
từng bật). Lý do: **chu kỳ refresh *chính là* độ cũ của kênh chậm**. Nếu nó nằm trong một dòng cron, nó
thành một hằng số bị giấu. E4 phải *điều khiển* nó: refresh tại thời điểm chọn, rồi đo.

SLA khai báo trong spec giữ **tỉ lệ** mà PROBLEM.md mô tả, ở thang thời gian nén mà demo và E4 chạy được:

| Dataset | SLA | Ý nghĩa ở thang thật |
|---|---|---|
| Bronze | 60 s | "vài giây" (p99 đo được ở M3: ~13 s) |
| Silver, Gold | 300 s | "vài phút" |
| Index vector | **1800 s** | "hàng giờ" |

Ban đầu đề xuất ví dụ là 10 phút cho index. Con số được chọn là 30 phút, vì 10 phút sẽ chỉ gấp đôi SLA
của Gold (5 phút), làm mất câu chuyện "kênh vector chậm hơn hẳn". 30 phút giữ tỉ lệ 6 lần. SLA là **khai
báo**, không phải lịch chạy: không có gì tự refresh khi hết SLA.

### M4-C2. Full rebuild, publish nguyên tử, một `refreshed_at` cho cả index

Mỗi lần refresh:
1. tạo bảng mới `<table>__build` (không index), `COPY` toàn bộ vào;
2. build HNSW trên bảng mới (`maintenance_work_mem` 512 MB, tính cho quy mô `full`);
3. trong **cùng transaction**: `DROP` bảng cũ, `RENAME` bảng mới, đổi tên index; rồi `COMMIT`.

DDL trong PostgreSQL có transaction, nên người đọc thấy index cũ cho tới lúc commit, và index mới ngay
sau đó, **không bao giờ** thấy một index build dở. `lock_timeout = 30s` để một query treo không làm job
kẹt vô hạn.

**Vì sao không incremental.** Incremental phải theo dõi cái gì đổi, mà đúng tinh thần PROBLEM.md thì
refresh là **một quyết định ngân sách theo lô**. Full rebuild cũng là thứ duy nhất cho được một
`refreshed_at` đồng nhất, tức là mức dataset trùng với mức entity, đúng với thiết kế của `platform.datasets`
(per-entity freshness không được materialise).

**Vì sao build bảng mới thay vì `DELETE` + `INSERT`.** Chèn từng dòng vào một bảng đã có HNSW nghĩa là
cập nhật đồ thị từng dòng một, chậm hơn nhiều so với build một lần trên dữ liệu đầy đủ khi lên 100k dòng.

### M4-C3. Text = `name + description + category_name`; không bao giờ embed giá hay tồn kho

Nối bằng `". "`, bỏ qua phần rỗng. Giá và tồn kho là fact nhanh: đưa vào text thì embedding sẽ "cũ theo
text", trộn hai nguồn độ cũ làm một, và phá điều kiện C9 (stale hit chỉ quy được cho bất đối xứng độ
tươi). Chúng nằm ở cột snapshot (M4-A1).

### M4-C4. Chạy trong container; model bake sẵn, pin revision, chạy offline

- Image `mosaic/jobs:1` (`docker/jobs/`): Python 3.11, torch **CPU-only** (wheel PyPI mặc định kéo theo
  ~3 GB thư viện CUDA vô dụng ở đây), sentence-transformers 3.0.1, psycopg 3.
- Model `sentence-transformers/all-MiniLM-L6-v2` được tải **lúc build** ở revision
  `1110a243fdf4706b3f48f1d95db1a4f5529b4d41`, chỉ lấy các file sentence-transformers cần (repo còn chứa
  ONNX, OpenVINO, TF, Rust nặng gấp ~10 lần). Lúc chạy, `HF_HUB_OFFLINE=1`.
- Mỗi lần refresh ghi `model_revision` vào ledger. E3/E4 so sánh các lần chạy trên **cùng một model**
  hoặc không so sánh.

**Một cái bẫy đặt tên đáng ghi lại.** `platform/` trùng tên với module `platform` của thư viện chuẩn
Python, mà torch và transformers đều import. Trong container, nó được mount thành `mosaic_platform`
(`/opt/mosaic/lib/mosaic_platform`), không bao giờ thành `platform`.

**Không có foreign key tới `products`.** GR1 Appendix A khai báo `product_id ... REFERENCES products(id)`.
M4 bỏ FK này vì hai lý do: (1) một index *dẫn xuất* không bao giờ được phép chặn một lần ghi vào system
of record (xoá một sản phẩm đang được index sẽ lỗi); (2) index được *thiết kế* để mô tả sản phẩm như lúc
refresh, kể cả sản phẩm đã đổi sau đó. Định nghĩa bảng chuyển từ DDL nguồn sang pipeline. Ghi vào
CHANGES-FROM-GR1.

### M4-C5. HNSW giữ `m=16, ef_construction=64` của GR1; sweep để E3

Ở quy mô `small` (1,000 sản phẩm, 946 đủ điều kiện), tìm chính xác đã rẻ và recall của HNSW gần 1.0 *theo
cấu tạo*. Kiểm tra recall ở M4 chỉ chứng minh **index được dùng thật và trả đúng**: mỗi đường đi (exact,
HNSW) đều được `EXPLAIN` trước, vì một đường "HNSW" mà lặng lẽ rơi về sequential scan sẽ cho recall 1.0
hoàn hảo và vô nghĩa. Đánh đổi recall/latency là việc của E3 ở quy mô `full` (100k sản phẩm).

---

## D. Việc tồn đọng và định nghĩa "xong"

### M4-D1. Commit M3 trước khi bắt đầu M4

Toàn bộ code M3 (Bronze stream, image Spark/dbt, Gold models, giới hạn bộ nhớ Trino) chưa từng được
commit. Đã commit thành `c24134a` trước khi viết dòng M4 đầu tiên, để M4 có một mốc sạch để so diff.

### M4-D2. Ba open question để sau M4

Partition của `gold.orders` (A12), Kafka retention (A10), quy mô `full`: để sau M4. Riêng retention nên
chốt sớm, vì máy đã hai lần mất lịch sử CDC do tắt máy lâu.

### M4-D3. Tiêu chí "M4 done, verified"

`python mosaic.py embed-verify` phải pass cả sáu kiểm tra, mỗi kiểm tra đều **có khả năng fail**:

| # | Kiểm tra | Fail khi |
|---|---|---|
| 1 | **counts** | số dòng index ≠ ledger ≠ số dòng đủ điều kiện đếm lại ở *đúng* Iceberg snapshot đã pin; `refreshed_at` không đồng nhất hoặc ≠ `max(_src_ts)` của snapshot đó |
| 2 | **semantics** | một query thử (ví dụ "waterproof hiking backpack") có < 7/10 kết quả trong đúng category gốc |
| 3 | **recall** | mean recall@10 ở `ef_search=40` < 0.95, hoặc một đường đi không dùng đúng plan nó tuyên bố |
| 4 | **guard** | gắn tag `sensitive` mà refresh vẫn chạy, hoặc index bị đổi |
| 5 | **no drift** | trước khi kéo công tắc, index đã mâu thuẫn với nguồn sống |
| 6 | **L0** | stockout kết quả tốt nhất mà index không còn trả nó như "còn hàng" |

`--with-refresh` thêm nửa còn lại: đưa thay đổi qua Bronze → Silver → Gold, refresh index, và đòi stale
hit **biến mất**. Điều đó chứng minh độ cũ chính là chu kỳ refresh, không phải lỗi gì khác.

Cộng thêm: `check-abstraction` pass trên phạm vi mới, `register` đăng ký đủ dataset, và mọi lần chạy để
lại dòng trong `experiments/results/embedding_events.jsonl`.

---

## Consequences (hệ quả)

**Được.**
- L0 lần đầu tiên tái hiện được **từ đầu tới cuối** trên stack thật, bằng một lệnh.
- Kênh chậm mang theo đủ nguyên liệu cho Eq. 5.1: `refreshed_at` (= `last_refresh(d)`), SLA, và ảnh chụp
  của thứ nó tin. N2 không cần hỏi thêm ai.
- `retrieval.yaml` + `platform/spec` ra đời sớm. M6 chỉ cần thêm mục `channels` và `fusion`.
- Bằng chứng L3 mạnh hơn: runner được gate kiểm, không chỉ `platform/`.

**Mất / phải trả.**
- Image ~1 GB và một lần build vài phút.
- Index là một bảng **được tạo lại** mỗi lần refresh: OID đổi, quyền cấp trên bảng (nếu sau này có) phải
  cấp lại sau swap. Hiện chưa có quyền nào như vậy.
- `eligible` là SQL thô do domain viết, được tin ở cùng mức với Gold model của domain đó.

**Để ngỏ cho M5.** Bài toán nguồn nhàn rỗi (M4-A3): watermark event-time không phân biệt được "đứng yên"
với "kẹt". Collector của N1 cần kết hợp heartbeat của CDC.

**Đảo ngược được.** Mọi tri thức domain nằm trong `retrieval.yaml` và một Gold model; runner không biết
gì về chúng.
