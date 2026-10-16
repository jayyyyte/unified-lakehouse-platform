# Changes from the GR1 Report

**A complete record of how the implementation differs from `GR1_report.md`, and why.**

Last updated: 2026-10-05 · Covers milestones M0–M3 (M3 complete)

> **Source verified.** Every claim here was checked against **`GR1_report.pdf`** (49 pages),
> not only against the Markdown conversion. Page numbers below refer to the PDF.
> The conversion `GR1_report.md` was independently confirmed faithful — 98% text-length
> match, and each quoted configuration matches the PDF byte for byte. See
> *Appendix: on the two copies of the report* at the end.

---

## How to read this document

GR1 was a **design study**. This repository is the **implementation** of that design.
Whenever an implementation meets reality, three kinds of difference appear, and this
document keeps them strictly separate because they mean very different things in a defence:

| Part | Kind of change | What it means |
|---|---|---|
| **A** | **Findings** — GR1 says something that *cannot be executed as written* | Not a mistake you should hide. This is the **product** of an implementation phase, and it is the most valuable material in this document. |
| **B** | **Substitutions** — a component replaced by a lighter equivalent | A deliberate engineering trade. The *guarantee* is unchanged; the tool differs. |
| **C** | **Additions** — things GR1 does not contain at all | Where the novelty lives. GR1 proposed; this builds. |
| **D** | **Operational changes** — structure, tooling, conventions | Mechanical. No effect on the architecture's claims. |
| **E** | **Unchanged** — what GR1 specified and we kept exactly | Bounds the delta so a reader knows the design largely survived. |
| **F** | **Defects inside GR1 itself** — broken references, contradictions | Fix these in the report before submitting GR2. |

Every entry follows the same shape: what GR1 says → what we do → why → evidence →
what it does to GR1's claims. Where a change has an Architecture Decision Record, the ADR
holds the full argument and is linked.

**One sentence to take away:** the architecture of GR1 survived implementation almost
entirely intact. What did not survive were four specific *configurations* in Appendices B
and C — and discovering that is exactly what GR2 is for.

---

## Summary of every change

| # | Change | Type | ADR |
|---|---|---|---|
| A1 | Appendix C's SMT masking chain cannot satisfy §4.2 and §4.6 at the same time | Finding | [0004](adr/0004-connector-native-masking.md) |
| A2 | Appendix B's Avro converter is not on the Connect image's classpath — worker dies at startup | Finding | [0005](adr/0005-json-converter.md) |
| A3 | Image tag `debezium/connect:2.7` does not exist | Finding | — |
| A4 | Image tag `projectnessie/nessie:0.90.4` is not on Docker Hub | Finding | — |
| A5 | The Nessie image cannot run an HTTP healthcheck (no curl/wget) | Finding | — |
| A6 | PostgreSQL emits **no before image** under the default replica identity, so §4.2's envelope is unsatisfiable for that source as Appendix A stands | Finding | [0004](adr/0004-connector-native-masking.md) |
| A7 | Debezium **silently truncates the mask hash to the source column's width** — a narrow column yields a partial digest | Finding | [0004](adr/0004-connector-native-masking.md) |
| A8 | **Restart safety is at-least-once with a replay window**, not exactly-once — Connect flushes offsets on a timer, which Table 4.3's one-line summary does not say | Finding | — |
| A9 | **§4.4's WAL-bloat guard was inert** — the heartbeat wrote to a table outside the publication, so `pgoutput` never decoded it and `confirmed_flush_lsn` never moved | Finding | — |
| A10 | **§4.2's "replay capability" expires with Kafka retention** — twice, Bronze could not be bootstrapped from the topics; after a 10-day stop every table topic was empty | Finding | — |
| A11 | **MySQL `DATETIME` arrives 7 hours off** — Debezium encodes the zone-less wall clock as if it were UTC; the harmonisation §4.4 gave to a `TimestampConverter` SMT has to happen in Silver | Finding | (see A1) |
| A12 | **§4.3's `gold.orders` partitioning is too fine for the prototype** — 2,944 partitions of 12.6 rows on average; the build fails Trino's default writer cap; `ANALYZE` 33x and a full scan 14x slower than the same data unpartitioned | Finding | — |
| B1 | Apache Ranger → Open Policy Agent | Substitution | [0001](adr/0001-opa-instead-of-ranger.md) |
| B2 | DataHub → OpenLineage + Marquez | Substitution | [0002](adr/0002-marquez-instead-of-datahub.md) |
| B3 | Spark (Silver/Gold) + Great Expectations → dbt-trino | Substitution | [0003](adr/0003-dbt-trino-for-silver-gold.md) |
| B4 | Avro + Confluent Schema Registry → JsonConverter | Substitution | [0005](adr/0005-json-converter.md) |
| C1 | A stated problem and a four-rung abstraction ladder (`PROBLEM.md`) | Addition | — |
| C2 | **N1** Freshness Registry — a queryable watermark plane | Addition | — |
| C3 | **N2** Freshness-aware fusion engine — implements §5.3 | Addition | — |
| C4 | **N3** Tag governance that reaches the vector index | Addition | — |
| C5 | **N4** Freshness-SLA query routing (stretch) | Addition | — |
| C6 | A second domain (`jobmarket`) as mechanical proof of genericity | Addition | — |
| C7 | `platform/` ÷ `domains/` split, enforced by a CI gate | Addition | — |
| C8 | `_src_ts` / `_ingest_ts` watermark convention | Addition | — |
| C9 | Seed generator and load generator as first-class deliverables | Addition | — |
| D1 | `mosaic.py` task runner (`make` is unavailable) | Operational | — |
| D2 | `up` waits for *healthy*, not merely *started* | Operational | — |
| D3 | Compose profiles so the default footprint fits one machine | Operational | — |
| D4 | All published host ports parameterised | Operational | — |
| D5 | Platform metadata in its own database + a 4th Trino catalog | Operational | — |
| D6 | Domain DDL and connectors moved under `domains/<domain>/` | Operational | — |
| D7 | Explicit `decimal.handling.mode` and `time.precision.mode` | Operational | — |
| D8 | No `ExtractNewRecordState` at the connector | Operational | (see A1) |
| D9 | MySQL bulk load runs as `root`, not `cdc` | Operational | — |
| D10 | `REPLICA IDENTITY FULL` on `users`, `products`, `inventory` | Operational | (see A6) |
| D11 | Silver is **generated** per captured table; Gold lives in `domains/<domain>/gold/` | Operational | (see C6) |
| D12 | Silver = one unpartitioned current-state table per captured table, soft deletes | Operational | — |
| D13 | dbt runs in its own container (`mosaic/dbt:1.8`), behind the `transform` profile | Operational | [0003](adr/0003-dbt-trino-for-silver-gold.md) |
| D14 | Trino `iceberg.max-partitions-per-writer` raised 100 → 5000 | Operational | (see A12) |
| D15 | Load generator draws regions from the seed's vocabulary | Operational | — |

---

# Part A — Findings: GR1 specifications that cannot be executed as written

These are the four (plus one) places where following GR1 literally produces a system that
does not start or does not do what the report says it does. **Each was found by running the
thing, not by reading it.** Present these prominently in the GR2 report.

---

## A1. The SMT masking chain contradicts the Bronze envelope requirement

**This is the most important finding in this document.** It is a genuine internal
contradiction in GR1, not a configuration slip.

### What GR1 says

Two requirements, in two different sections:

> **§4.2** — *"The Bronze tier stores the unmodified Debezium change envelope, including
> the before/after row state, the operation type, the source offset or log sequence number,
> and the event timestamp, **with no transformation applied**, in order to preserve full
> auditability and replay capability."*

> **§4.6 / Table 4.5** — Layer-1 masking applies `MaskField` / `ReplaceField`
> *"at ingestion, before the Kafka topic"*, so that raw PII *"never persists to any storage
> tier."*

And Appendix C implements the second with this transform chain:

```json
"transforms": "unwrap,maskPII,dropSensitive,ts",
"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
"transforms.maskPII.type": "org.apache.kafka.connect.transforms.MaskField$Value",
"transforms.maskPII.fields": "buyer_email,buyer_phone"
```

### Why it cannot work

A Debezium change event is **nested**. The business columns do not sit at the top level;
they live inside `before` and `after`:

```json
{
  "op": "u",
  "before": { "buyer_email": "old@example.com", "total_amount": "100.00" },
  "after":  { "buyer_email": "new@example.com", "total_amount": "150.00" },
  "source": { "file": "mysql-bin.000001", "pos": 1234, "ts_ms": 1699123456789 }
}
```

Kafka Connect's `MaskField$Value` and `ReplaceField$Value` address **top-level fields
only**. They cannot reach `after.buyer_email`. That is precisely why Appendix C puts
`unwrap` **first** in the chain — `ExtractNewRecordState` flattens the event down to just
the `after` image so that `MaskField` has something it can address.

But flattening **discards `before`, `op`, and the envelope structure**. So:

> Running Appendix C's chain gives you a Bronze tier that **cannot** contain the unmodified
> envelope §4.2 requires. Satisfying §4.6 as written makes §4.2 impossible.

### The second, quieter problem

Even setting the contradiction aside, the SMT approach **leaks the PII it exists to stop**.
`MaskField` runs after `unwrap`, so it only ever sees the `after` image. If the envelope
were somehow preserved, **the `before` image of every UPDATE would carry the unmasked
previous value straight into Bronze.** Change a user's email, and the old address is written
to permanent storage in the clear.

### What we do instead

Move masking from Kafka Connect SMTs **into the Debezium connector itself**, and drop
`unwrap` entirely.

| Concern | Appendix C (SMT) | Here (connector-native) |
|---|---|---|
| Mask a value | `MaskField$Value` | `column.mask.hash.SHA-256.with.salt.<salt>` |
| Drop a column | `ReplaceField$Value` | `column.exclude.list` |
| Harmonise timestamps | `TimestampConverter$Value` | `time.precision.mode=connect`, cast in Silver |
| Flattening | at the connector | in the Bronze→Silver step |

Actual configuration now in [`domains/ecommerce/connectors/mysql-cdc.json`](../domains/ecommerce/connectors/mysql-cdc.json):

```json
"column.mask.hash.SHA-256.with.salt.mosaicPiiSalt":
    "ecommerce_tx.orders.buyer_email,ecommerce_tx.orders.buyer_phone",
"column.exclude.list": "ecommerce_tx.payments.raw_card_number"
```

### Why this fixes both problems at once

Connector-native masking runs **before the change envelope is constructed**. The connector
reads the binlog/WAL, masks the column, and *then* builds the event. Consequences:

1. **Both `before` and `after` are masked** — the update leak is closed.
2. **The envelope is fully preserved** — §4.2 is satisfiable.
3. Masking is still irreversible and still happens before any Kafka topic — §4.6 and
   Table 4.5's Layer-1 semantics are unchanged.

### One deliberate improvement

GR1's `MaskField` with an empty replacement **destroys** the value. We use a **salted
SHA-256 hash** instead, which is equally irreversible but keeps the column joinable and
countable — Silver/Gold can still answer *"how many distinct buyers ordered this week"*
without ever seeing an address.

Fields GR1 says must **never persist at all** — `users.national_id`,
`payments.raw_card_number` — are **excluded outright** rather than hashed, because a salted
hash is still persistence.

### Impact on GR1's claims

Table 4.5's two-layer model is **unchanged in substance**; only the mechanism moved. The
guarantee in the report ("irreversible, never persisted") is now actually true, which it
was not before.

**Known limitation:** the salt sits in the connector config, which would be unacceptable in
production. The fix (a Kafka Connect config provider backed by a secret store) is named, not
implemented. Say so in the report.

### Status

**Written, not yet exercised.** The connector configs are valid JSON but have not been
registered against a live Debezium 2.7.3. Expect the first run to surface issues here.

Full argument: [ADR 0004](adr/0004-connector-native-masking.md).

---

## A2. The Avro converter is not on the Connect image's classpath

### What GR1 says

Appendix B configures the Connect worker, and Appendix C repeats it per connector:

```yaml
KEY_CONVERTER: io.confluent.connect.avro.AvroConverter
VALUE_CONVERTER: io.confluent.connect.avro.AvroConverter
CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
```

### What actually happens

The worker **dies during startup**, before serving a single request:

```
ERROR  Stopping due to error   [org.apache.kafka.connect.cli.AbstractConnectCli]
org.apache.kafka.common.config.ConfigException: Invalid value
io.confluent.connect.avro.AvroConverter for configuration key.converter:
Class io.confluent.connect.avro.AvroConverter could not be found.
```

The `debezium/connect` image does **not** bundle Confluent's Avro converter — it is
Confluent-licensed and ships separately. There is no way for Appendix B to have been run as
written.

### What we do instead

`org.apache.kafka.connect.json.JsonConverter` with `schemas.enable=false` on key and value.

Two alternatives were weighed and rejected:

| Option | Why not |
|---|---|
| Custom image bundling Confluent's Avro jars | ~8 transitive jars pinned by hand, brittle across versions. Worse: Confluent's wire format prefixes a magic byte + schema id, and open-source Spark's `from_avro` has **no Schema Registry integration** — the Bronze job would need ABRiS or manual deserialisation. |
| Apicurio converters (`ENABLE_APICURIO_CONVERTERS=true`) | Ships inside the image, so no download problem — but swaps Confluent Schema Registry for **Apicurio Registry**, a service Appendix B does not name, and leaves the same Avro wire-format burden on Spark. |

### What is kept, and what is lost

**Kept.** The Debezium envelope — `op`, `before`, `after`, `source` — is fully preserved.
`schemas.enable=false` drops Connect's per-message *schema block*, **not** the envelope. §4.2
is unaffected.

**Arguably improved for Bronze.** §4.2 justifies Bronze by auditability and replay.
Self-describing JSON can be read by any tool with no registry lookup; a binary Avro payload
whose schema lives in a separate service is a *weaker* audit artifact if that service is
lost.

**Lost.** Compact binary encoding, and registry-enforced schema compatibility checking. At
one node and 10M rows, neither is load-bearing.

**Partially lost — be honest about this one.** With `schemas.enable=false`, an **added or
dropped column** is still visible as a change in the payload's field set, so GR1 Table 4.4's
DDL-handling KPI stays measurable for the common case. But a **type change** (say `INT` →
`BIGINT`) that does not alter the field set is **not** observable. If the DDL experiment
needs type fidelity, flipping `value.converter.schemas.enable` to `true` restores it, at
roughly 10× the message size.

**Schema Registry stays deployed but is off the data path.** It remains in the stack so that
returning to Avro is a config change, not a redeployment. **Do not describe it as
load-bearing in the report** — it currently carries no traffic.

**Reversibility:** one line in the worker config, two per connector.

Full argument: [ADR 0005](adr/0005-json-converter.md).

---

## A3. `debezium/connect:2.7` does not exist

**GR1 Appendix B:** `image: debezium/connect:2.7`

**What happens:**
```
failed to resolve reference "docker.io/debezium/connect:2.7":
docker.io/debezium/connect:2.7: not found
```

Debezium publishes **patch-level tags only** — `2.7.0.Final`, `2.7.1.Final`, `2.7.2.Final`,
`2.7.3.Final`. There is no rolling `2.7` tag.

**Fix:** `debezium/connect:2.7.3.Final` — the newest 2.7 patch, so GR1's stated major.minor
version is honoured exactly.

---

## A4. `projectnessie/nessie:0.90.4` is not on Docker Hub

**GR1 Appendix B:** `image: projectnessie/nessie:0.90.4`

**What happens:** the tag does not resolve. Enumerating Docker Hub shows
`projectnessie/nessie` **stopped receiving updates at 0.76.6** — the project moved its
container publishing to GitHub Container Registry.

**Fix:** `ghcr.io/projectnessie/nessie:0.90.4`. Verified present by enumerating all 350 tags
on GHCR. **The version GR1 specifies is correct — only the registry is wrong.**

---

## A5. The Nessie image cannot run an HTTP healthcheck

**Not stated in GR1**, but it follows directly from Appendix B's stack and is worth
recording because it produces a *silent* failure.

Nessie is built on a minimal base image that ships **neither `curl` nor `wget`**. A Compose
healthcheck like `curl -fsS http://localhost:19120/api/v2/config` therefore fails **forever**
— not because Nessie is unhealthy, but because the probe binary does not exist.

**Why that matters:** the container is marked permanently unhealthy, and every service with
`depends_on: nessie: {condition: service_healthy}` — **Trino and Spark, the two most
important consumers** — never starts. The stack appears to hang with no useful error.

**Fix:** no healthcheck on Nessie; dependents use `condition: service_started`; readiness is
probed **from the host**, where curl exists. Verified:

```
$ curl -fsS http://localhost:19120/api/v2/config
{ "defaultBranch": "main", "maxSupportedApiVersion": 2, "specVersion": "2.1.0", ... }
```

---

## A6. PostgreSQL sends no before image, so §4.2's envelope cannot hold for that source

**GR1 §4.2** requires Bronze to store *"the unmodified Debezium change envelope, including the
before/after row state."* **GR1 Appendix A.2** gives the PostgreSQL DDL, and it never sets a
replica identity — so every table keeps PostgreSQL's default.

**What that default means:** for an `UPDATE`, PostgreSQL writes only the *primary key* of the
old row into the WAL. There is nothing else for logical decoding to hand over, so Debezium
emits:

```json
{"op": "u", "before": null, "after": { ... }}
```

**`before` is null on every PostgreSQL update.** Observed directly: `mosaic.postgres.public.products`
and `.inventory` each returned 300 events with `op=u` and **0 before images**, while
`mosaic.mysql.ecommerce_tx.orders` — same platform, same connector family — returned 79 update
events with 79 before images. MySQL's `binlog_row_image=FULL` (which Appendix B *does* set)
has no PostgreSQL counterpart in Appendix A.

**Why it matters twice over:**

1. §4.2's envelope requirement is simply unsatisfiable for the PostgreSQL half of the system.
   Bronze would store a permanently half-empty envelope and nobody would notice, because a
   null `before` looks like a valid absence rather than a missing capability.
2. **ADR 0004's central claim becomes unprovable on that source.** The argument against SMT
   masking is that it leaks the *before* image of PII updates. On a source that emits no
   before image, neither approach can be distinguished — the evidence has to come from MySQL,
   or the replica identity has to change.

**Fix:** `ALTER TABLE ... REPLICA IDENTITY FULL` on the three tables whose previous state is
actually read — `users` (PII, so the masking claim is testable), `inventory` (the fast-moving
fact the L0 failure turns on) and `products` (price history). Now in
`domains/ecommerce/ddl/postgres.sql`.

**Cost, stated honestly:** `FULL` writes the entire old row into the WAL on every UPDATE and
DELETE, so WAL volume grows with row width. That is why it is applied to three named tables
rather than to the whole schema.

**Evidence after the fix** — the same lever, end to end, `python mosaic.py stockout`:

```json
{"op": "u",
 "before": {"product_id": 29, "stock_qty": 192, "warehouse": "SIN-01"},
 "after":  {"product_id": 29, "stock_qty": 0,   "warehouse": "SIN-01"},
 "source_ts_ms": 1787889547921}
```

That is the L0 event with its previous state intact and a source commit timestamp — exactly
what N1 will read as `last_cdc(d)`.

---

## A7. Connector-native masking truncates the hash to the source column's width

**Not stated in GR1** — Appendix C uses SMTs, and this behaviour belongs to the connector-native
mechanism that replaced them (ADR 0004). It is recorded because it is silent, and because the
obvious verification ("is it a SHA-256 hash?") reports a **false failure** if you do not know it.

Two columns, one masking rule (`column.mask.hash.SHA-256.with.salt.mosaicPiiSalt`):

| Source column | Declared type | Hash emitted |
|---|---|---|
| `orders.buyer_email` | `VARCHAR(255)` | `43883ab2…08082` — **64 hex chars**, the full digest |
| `orders.buyer_phone` | `VARCHAR(32)` | `1e3ebf29…2736c` — **32 hex chars**, half the digest |
| `users.phone` (PostgreSQL) | `TEXT` | 64 hex chars, full digest |

Debezium shortens the hash to the declared width of the column it replaces. No warning is
logged; the value still *looks* like a hash.

**What it does and does not mean.** The value remains a one-way salted digest, so no plaintext
reaches Kafka and Table 4.5's guarantee holds. But the property is now width-dependent: a
`VARCHAR(10)` PII column would yield a 40-bit digest, and for a low-entropy, fixed-format value
such as a national phone number, that is within brute-force reach of anyone holding the salt.
**The strength of the mask is a property of the source DDL, not of the masking configuration** —
which is exactly the kind of coupling a reviewer should be told about rather than left to find.

**How it is handled here:** `tools/cdc/verify.py` reads `character_maximum_length` from
`information_schema` at verification time and expects `min(64, width)` hex characters, so a
truncated mask is *reported* rather than silently accepted:

```
NOTE: buyer_phone is narrower than a SHA-256 digest, so the mask is truncated
      to 32 hex chars (128 bits)
```

`buyer_phone` is left at `VARCHAR(32)` deliberately: Appendix A's schema is kept faithful, 128
bits is not a practical weakness, and the finding is more useful to the report than the fix
would be. Widening a masked column to 64 characters is the remedy where full-digest strength
is required.

---

## A8. "Restart safety" is at-least-once with a replay window, not exactly-once

**Measured in M2.** The correction is one of degree rather than of kind — but the degree is
exactly what a downstream layer has to be built for.

### What GR1 says

> **Table 4.3** — Restart safety: *"Offset stored in the Kafka Connect offsets topic"*
> (MySQL) / *"LSN stored in the replication slot"* (PostgreSQL).

> **Table 4.1** — Debezium is chosen partly for *"restart safety achieved through Kafka-stored
> source offsets"*.

Both true. Neither says **when** that offset is written.

### What actually happens

It is written on a **timer**:

```
offset.flush.interval.ms = 60000        # read from the live worker; the Connect default
```

Not per record, and not on crash — a SIGKILL has no opportunity to flush anything. So the
offset in `_connect_offsets` trails the last produced record by up to a minute, and on restart
Debezium resumes from that stale position and **re-reads everything committed since**.

### Evidence

`python mosaic.py restart-safety --rate 25 --warmup 20 --stall 45 --drain 20`, with the worker
stopped by **SIGKILL** — not `docker stop`, which flushes offsets on the way out and therefore
proves materially less:

| Captured table | id range written during the outage | rows | missing | duplicates |
|---|---|---|---|---|
| `ecommerce_tx.orders` | 27000..28054 | 1,055 | **0** | 36 |
| `ecommerce_tx.order_items` | 52728..54617 | 1,890 | **0** | 48 |
| `ecommerce_tx.payments` | 23612..24282 | 671 | **0** | 20 |
| `public.users` | 5614..5708 | 95 | **0** | 6 |

Committed offsets did resume and advance: `mysql-bin.000007:1238339 → :2399951`
(GTID `…:1-61738 → …:1-63546`, 1,808 transactions) and PostgreSQL `lsn 70936688 → 72250344`.
Fully caught up **30.3 s** after restart, with writes continuing 28.4 s into the recovery.

**Rows lost: zero. Duplicate events: 110.** That is the mechanism working exactly as designed,
and a harness that scored duplicates as failure would report a false negative on it.

### What it changes

Nothing in the architecture. It sharpens one claim and creates one obligation.

1. **The claim.** "No data loss on connector restart" is correct; "exactly-once" would not be.
   The replay window is bounded by `offset.flush.interval.ms`, **not by the length of the
   outage** — a 5-second crash and a 5-minute crash replay the same ~60 s of work. Lowering the
   interval shrinks the window at the cost of more frequent offset commits.
2. **The obligation.** GR1 §4.2 already says Bronze should *"deduplicate using the source offset
   as an idempotency key."* A8 turns that from a design nicety into a measured requirement:
   **~110 duplicate events per crash is what Bronze will actually receive.** M3 must implement
   it, and the key is the source coordinates — `(file, pos, row)` for MySQL, `(lsn, txId)` for
   PostgreSQL — not the business primary key.

### Correction (M3): the "duplicates" column above is not a count of replays

M2's harness counted *events per key beyond the first*. That also counts a row legitimately
inserted and then updated inside the window -- an order advancing `pending -> paid` is two
source events, not one event twice -- and it ignored tables without a generated key
(`inventory`, `products`) entirely. So "110" mixes replays with ordinary updates on four
tables and omits two others.

M3 changed the metric to what the claim needs: **the same source event delivered more than
once**, matched on op, source position (`file/pos/row` or `lsn/txId`, plus `source.ts_ms`) and
both images. The comparable M3 run (SIGKILL, 45 s, 25 ops/s, 2026-10-05) measured **183
replayed events across six tables**, 54 of them replayed inserts; the old metric on the same run
read 138 over the four keyed tables. Gap-freedom was unaffected (0 missing, again), and so is
every conclusion above -- the replay window is real and Bronze must absorb it. M3 then
**proved it does** (see the M3 section of BUILD-LOG): every one of the 1,088 + 1,980 + 719 + 95
inserts in the bracketed ranges is in Bronze exactly once, although Kafka received 1,107 +
2,010 + 724 + 95 of them.

### Status

**Verified.** Reproducible on demand with `python mosaic.py restart-safety`; every run appends
to `experiments/results/restart_events.jsonl`. Since M3, `python mosaic.py bronze-verify
exactly-once` carries the same run through to Bronze.

---

## A9. §4.4's WAL-accumulation guard was inert as configured

**The strongest finding of M2**, and structurally the same failure as A6: a mitigation GR1
names in prose, configured in good faith, silently doing nothing.

### What GR1 says

§4.4 identifies the PostgreSQL-specific operational risk correctly — it is the one place where
the two CDC mechanisms differ in kind rather than in detail:

> *"because a logical replication slot prevents PostgreSQL from removing WAL segments until
> Debezium has processed them, a prolonged Debezium outage causes WAL to accumulate on disk and
> can eventually exhaust available storage. This risk is mitigated … by emitting periodic
> heartbeat events that advance the confirmed-flush position even when the monitored tables
> receive no application writes."*

That is right, and the implementation configured exactly it:

```json
"heartbeat.interval.ms": "10000",
"heartbeat.action.query": "INSERT INTO public.cdc_heartbeat(ts) VALUES (now())"
```

### Why it did not work

`mosaic_pub` published four tables — `users`, `products`, `categories`, `inventory` — and
**not `cdc_heartbeat`**. `pgoutput` decodes only *published* tables. So the heartbeat's
`INSERT` wrote WAL that Debezium never received as a change event, gained no new LSN to
confirm, and `confirmed_flush_lsn` did not move.

Every visible signal said the heartbeat was healthy, and it was: 503 rows in `cdc_heartbeat`,
503 messages on `__debezium-heartbeat.mosaic.postgres`. **The guard was firing and achieving
nothing** — no error, no warning, no log line.

### Evidence

`python mosaic.py wal-guard --idle 130`, captured tables idle, connector **healthy** throughout:

| Sample | `confirmed_flush_lsn` | `current_wal_lsn` | retained WAL |
|---|---|---|---|
| start | `0/40D3EA8` | `0/4175C28` | 2,216,816 |
| +130 s | `0/40D3EA8` *(frozen)* | `0/4176308` *(advancing)* | 2,218,576 |

WAL advancing while the confirmed-flush position does not **is** the unbounded-retention
condition §4.4 describes. The guard meant to prevent it was the thing that was broken.

### The fix

One table, in two places — the publication and the connector's capture list:

```sql
ALTER PUBLICATION mosaic_pub ADD TABLE cdc_heartbeat;   -- and in domains/ecommerce/ddl/postgres.sql
```
```json
"table.include.list": "public.users,public.products,public.categories,public.inventory,public.cdc_heartbeat"
```

Re-measured over the same 130 s idle window:

| idle, connector alive | `confirmed_flush_lsn` advance |
|---|---|
| heartbeat table outside the publication | **0 bytes** |
| heartbeat table published | **688,776 bytes** |

Retained WAL fell from 704,880 to **4,544 bytes**. `cdc-verify` then showed the heartbeat
flowing through CDC as an ordinary captured table (67 `op=c` events on its own topic), so the
fix is visible end-to-end and not only in the slot statistics.

### One honest limit, worth stating in the report

The heartbeat's `INSERT` is issued **by the connector**. Killing the worker therefore stops the
only writer, the database goes genuinely idle, and no WAL accumulates. The guard protects *an
idle capture set beside a busy database* — the case measured above. **It cannot protect anything
while the consumer itself is down**, which is the scenario §4.4's phrase *"a prolonged Debezium
outage"* most naturally suggests. There, the real bounds are `wal_keep_size` and disk.

### Impact on GR1's claims

§4.4's analysis of the risk survives intact and is one of the report's better passages. What
does not survive is the implicit assumption that configuring `heartbeat.action.query` is
sufficient: **the heartbeat only guards WAL if the connector can see its own writes.** Add that
sentence to §4.4.

Pair this with A6 when writing GR2. Both are PostgreSQL-side mitigations that GR1 states
correctly at the level of intent and that fail silently at the level of configuration; neither
is discoverable by reading, only by measuring. Two independent instances make it a pattern
rather than an anecdote — and that pattern is a defensible answer to *"what did the
implementation phase actually contribute?"*

### Status

**Verified, with a before/after pair.** Both runs are kept in
`experiments/results/restart_events.jsonl`. The failing one is the evidence.

---

## A10. §4.2's "replay capability" expires with Kafka retention

**Found in M3, twice.** The same shape as A6 and A9: a property the report states without its
precondition.

### What GR1 says

> **§4.2** — Bronze stores the unmodified envelope *"in order to preserve full auditability and
> replay capability"*. **Table 4.1** — Kafka is the *"durable streaming buffer that provides
> replay"*.

### What actually happens

Kafka runs its default `log.retention.hours=168`; nothing in Appendix B overrides it. Retention
is enforced on wall-clock time, including **the moment a stopped broker starts again** -- before
any consumer has had a chance to read.

| Date | Situation | Earliest retained offsets |
|---|---|---|
| 2026-09-12 | M1 snapshotted on 2026-08-28; Bronze did not exist yet | `orders` 113,993 of 125,464; `categories` **144 of 144 -- an empty topic** (categories never change after seeding) |
| 2026-10-05 | stack stopped since ~2026-09-25 (Docker Desktop restart) | **every one of the 7 table topics: earliest == latest**. Bronze's own checkpoint pointed at offsets that no longer existed and the stream failed with `OffsetOutOfRange` |

A Bronze stream bootstrapped from the topics at either point could never have seen a row that
existed only in the deleted range -- in the first case, not one category.

### What we do instead

- **Recovery:** a Debezium **BLOCKING ad-hoc snapshot** over a Kafka signal channel
  (`python mosaic.py resnapshot`). It needs no source grants and no publication change -- an
  incremental snapshot would need a signalling table, i.e. an `INSERT` grant GR1 Appendix A
  deliberately withholds from `cdc`. Measured 2026-10-05: all 7 tables re-emitted **exactly**
  their source row counts (114,709 `op=r` events, 40.4 s), and Bronze landed every one
  (`n = distinct keys = source rows` per table).
- **Detection:** the Bronze stream keeps Spark's `failOnDataLoss=true`, so a gap is a crashed
  container, not a silent skip. Its pairing guard refuses to start when the checkpoint has
  progress but the table is gone. `--reset-checkpoint` *renames* the checkpoint rather than
  deleting it.

### Impact on GR1's claims

Replay from Kafka is bounded by retention; **auditability lives in Bronze, not in Kafka** --
true only once Bronze is landing continuously. Add the precondition to §4.2. A longer retention
(or `retention.ms=-1` on the CDC topics) would widen the window; it is not done here, because
the report's design point is that Bronze, not the broker, is the system of record.

### Status

**Verified twice; recovery verified.** Ledger: `experiments/results/snapshot_events.jsonl`.

---

## A11. MySQL `DATETIME` arrives 7 hours off; the harmonisation moves to Silver

**Found in M3.** A consequence of A1 that only becomes visible once rows are typed.

### What GR1 says

> **§4.4** — *"the `TimestampConverter` transform harmonizes the differing timestamp
> representations produced by MySQL and PostgreSQL into a single ISO 8601 format."*

The sentence reads as cosmetic -- one format instead of two.

### What actually happens

It is semantic. MySQL `DATETIME` has no zone. Under `time.precision.mode=connect` Debezium sends
it as epoch milliseconds of the **wall clock interpreted as UTC**. MySQL here runs
`time_zone=SYSTEM` = `+07` (container `TZ=Asia/Ho_Chi_Minh`), so the value is seven hours in
the future of the instant it describes:

```
after.created_at  = 1791200479000      -> read as UTC: 2026-10-05 11:41:19
source.ts_ms      = 1791175279628      -> 2026-10-05 04:41:19.628 UTC   (the commit)
difference        = 25,199 s  ~ 7 h
```

PostgreSQL `TIMESTAMPTZ` arrives as a correct ISO-8601 instant (`...Z`). Unharmonised, every
cross-source comparison of event times -- an order against its user, a sale against a stock-out
-- is off by the source's UTC offset.

### What we do instead

A1 removed the SMT chain, so the `TimestampConverter` never runs. Silver does the conversion:
every zone-less source timestamp is read in `MOSAIC_SOURCE_TZ` and stored as
`timestamp(6) with time zone` (`cdc_decode` kind `wallclock_ms` in
`pipelines/transform/macros/cdc_current_state.sql`).

### Evidence

`python -m tools.lakehouse.reconcile` compares Silver with the live sources, timestamps as epoch
microseconds. With the correct zone: **0 differing rows** in all 7 tables. Negative control with
`MOSAIC_SOURCE_TZ=UTC`: **every** MySQL row differs (33,605 / 64,556 / 27,837) while the
PostgreSQL tables stay at 0 -- the conversion is the only thing that makes the two agree.

### Impact on GR1's claims

§4.4's `TimestampConverter` is load-bearing, not cosmetic. State in §4.4 that MySQL datetimes
are zone-less and need the source's zone to become instants, wherever that conversion runs.

### Status

**Verified, with a negative control.** Ledger: `experiments/results/lakehouse_events.jsonl`.

---

## A12. §4.3's `gold.orders` partitioning is too fine for the prototype

**Found in M3.** Not a contradiction -- a specification sized for a different scale, which the
report does not say.

### What GR1 says

> **§4.3** — *"orders are partitioned by `PARTITIONED BY (days(created_at), bucket(16,
> region))` to support both time-series and regional analytics"*.

### What actually happens

Implemented exactly (`ARRAY['day(created_at)', 'bucket(region, 16)']`). The seed spans 370 days
and up to 15 region values. The first full build **failed**: `ICEBERG_TOO_MANY_OPEN_PARTITIONS
-- Exceeded limit of 100 open writers`. With the cap raised (D14) it builds, and the table is:

| `gold.orders` | value |
|---|---|
| rows | 36,678 |
| partitions | **2,944** |
| data files | 2,968, **12.6 rows each** on average, 10.5 MB in total |

Measured against the same rows in the unpartitioned `silver.orders`:

| operation | partitioned (GR1) | unpartitioned | ratio |
|---|---|---|---|
| `ANALYZE` (§4.5's required post-write step) | 8.25 s | 0.25 s | **33x** |
| full scan `count(*), sum(total_amount)` | 1.28 s | 0.09 s | **14x** |
| incremental MERGE of ~600-2,600 orders (dbt) | 19-30 s | 1.4-2.5 s (Silver) | ~10x |

Two further observations. `bucket(16, region)` over a column with ten or fifteen values is
identity partitioning with extra steps: each value lands in its own bucket and several buckets
stay empty. And at GR1's own 10M-row target (§10.1) the same spec gives roughly 3,400 rows per
partition -- better, but still files of well under a megabyte.

### Impact on GR1's claims

The spec is executable, and is kept as written for now. What the report should add is the scale
it assumes, and that `ANALYZE` cost grows with partition count -- the overhead §4.5 already
flags is multiplied by this choice. A month-level time partition (or day without the region
bucket) would suit the prototype; **that is a design decision left open** -- E2 (Top-N pushdown
over Gold) is where it should be settled, at `full` scale.

### Status

**Measured.** GR1 Listing 4.2's federated query runs against this table in 1.61 s and returns
its 100 rows.

---

# Part B — Substitutions: same guarantee, lighter tool

Each of these keeps what GR1 *promises* and changes what *delivers* it. Frame every one in
the report as **"substitution, not omission"**, and name the production-scale original.

---

## B1. Apache Ranger → Open Policy Agent

**GR1:** Ranger enforces the five-role hierarchy of Table 4.6 and the query-time column
masking of Table 4.5 (Layer 2), via its Trino plugin.

**Problem:** a working Ranger is *four* moving parts — Ranger Admin, a backing relational
database, Solr for audit indexing, and the Trino plugin. On the single-node Compose target
of Appendix B it competes for memory with Kafka, Spark, Trino, MinIO and two databases, and
historically costs multiple weeks before the first policy evaluates.

**What we do:** OPA behind Trino's `opa` access-control provider. **Table 4.6's five roles —
`data_admin`, `data_engineer`, `data_analyst`, `ml_engineer`, `bi_user` — are expressed
verbatim as Rego.** Column masking stays query-time and role-dependent exactly as Table 4.5
describes.

**Kept:** centralised policy; catalog/schema/table/column-level access control; query-time
column masking; an audit trail of decisions.

**Gained — and this is a real argument for the defence:** policy becomes **plain text,
version-controlled, and unit-testable**. `opa test policies/` runs the role hierarchy as a
test suite in CI. That is stronger evidence than screenshots of an admin UI. Four containers
become one.

**Lost:** Ranger's policy-authoring web UI (not a project requirement), and its tag-sync
integration with a catalog. The second matters for N3, which is *why* the tag model is owned
by `platform/governance/` and handed to OPA as policy data — and that is what makes N3 a
contribution rather than a configuration exercise.

Full argument: [ADR 0001](adr/0001-opa-instead-of-ranger.md).

---

## B2. DataHub → OpenLineage + Marquez

**GR1 §3.6:** DataHub provides the catalog and column-level lineage graph, ingesting lineage
emitted by Airflow and Spark **in OpenLineage format**.

**The decisive observation:** GR1 defines its lineage requirement in terms of the
**OpenLineage emission path**, not in terms of DataHub. DataHub is one possible *receiver*.

**Problem:** a production DataHub is five services — GMS, Elasticsearch, the frontend, and
the MAE/MCE consumers — plus its own Kafka topics. Appendix B's own comment concedes it:
*"Kept minimal here for GR1."*

**What we do:** keep the emission path **exactly as designed**, and receive it with
**Marquez**, the OpenLineage reference implementation — one service plus a Postgres schema.

**Kept:** automatic lineage capture from Spark and dbt with no per-pipeline code changes; a
navigable lineage graph; dataset-level metadata.

**Gained:** Marquez's dataset-version model records a facet per run, which the Freshness
Registry (C2/N1) consumes **directly** rather than reconstructing. The lighter choice is
*better aligned* with the novel component, not merely cheaper.

**Lost — state this honestly:** DataHub's richer catalog surface (glossary, ownership,
search UI) and its **column-level** lineage. dbt gives column-level lineage for the
Silver/Gold hop, but **the Bronze hop stays table-level.** Record it as a known limitation.

**Reversibility:** because the emission side is unchanged, swapping the receiver back to
DataHub is a configuration change — which is exactly the extensibility property GR1 §2.3
asks for.

Full argument: [ADR 0002](adr/0002-marquez-instead-of-datahub.md).

---

## B3. Spark for Silver/Gold + Great Expectations → dbt-trino

**GR1 Fig. 4.1 / §4.2:** three engines in the processing layer — Spark Structured Streaming,
dbt, and Great Expectations — with Spark owning the whole write path (Bronze append plus
merge/upsert into Silver and Gold).

**The observation:** those are two different jobs.

- **Bronze landing is genuinely streaming** — consume Debezium topics continuously,
  normalise the envelope, deduplicate on the source offset, append.
- **Silver and Gold are batch set-operations** over tables that already live in Iceberg, and
  **Trino can already write Iceberg via `MERGE INTO`.**

**What we do:**

| Tier | Engine |
|---|---|
| Bronze | **Spark Structured Streaming** — unchanged. Streaming earns its place here. |
| Silver, Gold | **dbt models compiled to Trino SQL**, writing Iceberg via `MERGE INTO` |
| Quality gates | **dbt `tests:` blocks**, replacing Great Expectations |

**Kept:** the medallion semantics of §4.2 exactly — Bronze holds the unmodified envelope;
Silver is deduplicated, typed, masked, validated; Gold is joined, aggregated, partitioned per
§4.3. **Spark stays in the architecture and in the report.**

**Gained:** Silver/Gold become declarative SQL instead of imperative Spark jobs — less code,
and lineage derivable from the DAG for free. No second heavy runtime for batch work.

**On Great Expectations:** its *function* survives as dbt tests — uniqueness on the dedup
key, not-null on `_src_ts`, and the **cross-source referential checks §4.3 says no single
engine can enforce** (an order's `user_id` against the PostgreSQL `users` table). GE's
surplus expectation vocabulary and data-docs output are lost; the checks §4.3 actually
specifies are all expressible.

**Note on `ANALYZE`:** GR1 §4.5 requires an `ANALYZE` after each batch write to keep Trino's
cost-based optimiser correct. That becomes a **dbt post-hook** rather than a separately
scheduled Airflow task — the operational overhead §4.5 flags is still real, but it is now
attached to the write that causes it instead of to a timer.

Full argument: [ADR 0003](adr/0003-dbt-trino-for-silver-gold.md).

**Status (M3): verified.** `MERGE INTO` on Nessie-catalogued Iceberg -- the main technical
risk ADR 0003 carried -- ran four incremental builds against concurrent writes (inserts plus
every update kind the load generator makes), after which Silver equalled the live sources row
for row on every compared column, in all 7 tables (`tools/lakehouse/reconcile.py`). Trino
commits each MERGE as an `overwrite` with position-delete files (merge-on-read). 67 dbt tests
replace Great Expectations, including both cross-source referential checks §4.3 names, and
`ANALYZE` runs as a post-hook on every Gold model.

---

## B4. Avro + Confluent Schema Registry → JsonConverter

Covered in full under **A2**, because it is a finding first and a substitution second.

---

# Part C — Additions: what GR1 does not contain

GR1 delivered a design. These are the things being **built** that the report either only
proposed or never mentioned. **This is where the contribution lives.**

---

## C1. A stated problem, and a four-rung abstraction ladder

**The gap:** GR1 describes the *architecture* thoroughly but **never states the single
concrete problem the platform exists to solve.** It motivates in the abstract ("fragmentation
carries a direct cost") and formalises in §5.3, but there is no worked, failing, measurable
case.

This matters because the advisor's very first question was *"is the final product a platform
or a tool/API?"* — a question you cannot answer convincingly from an architecture diagram.

**What we add:** [`docs/PROBLEM.md`](PROBLEM.md), which names one failing query and climbs
from it to a generic platform in four rungs, each with an artifact and a **mechanical test**:

| Rung | Question | Artifact | Test |
|---|---|---|---|
| **L0** | What concretely breaks today? | the failing query | failure reproducible on demand |
| **L1** | What is the model of the broken case? | `domains/ecommerce/` | failure is *measurable* |
| **L2** | What of that is not about commerce? | `platform/` | failure is *fixed* |
| **L3** | Is the abstraction real or asserted? | `domains/jobmarket/` | second domain runs, zero code change |

**The L0 failure, concretely:** *"Show the 10 products most relevant to shopper X, right
now."* Three facts, three systems, three freshness levels:

| Fact | Where | Staleness |
|---|---|---|
| Semantic relevance | `product_embeddings` (pgvector) | **hours** — embedding refresh schedule |
| Purchase affinity, revenue rank | `iceberg.gold.*` | **minutes** — CDC + batch lag |
| Is it purchasable? (stock, price) | MySQL/PostgreSQL live | **seconds** |

Fuse the first two with RRF and: **a product that sold out five minutes ago still ranks #1.**
RRF's score depends only on rank position —

```
RRF(d) = SUM_i  1 / (k + rank_i(d))
```

— so there is **no term for how old the data behind a ranking is**, and the fusion function
cannot penalise a stale channel for being confidently wrong.

**Measured by:** *stale-hit rate@10* (primary) and *nDCG@10 vs an oracle* (secondary). The
oracle is the ideal ranking computed directly against live OLTP at query time — fresh by
construction. nDCG guards against a degenerate "suppress everything" fix.

---

## C2. N1 — the Freshness Registry

**GR1 status:** does not exist. But **GR1 Eq. 5.1 cannot be evaluated without it.**

Equation 5.1 needs `last_cdc(d)` and `last_refresh(d)` — how old the data behind a specific
item is. Today the pieces exist and are **never joined**: Iceberg exposes snapshots, Debezium
exposes offsets, a catalog exposes lineage. Nothing answers *"how old is the data behind
**this** answer?"*

**What we build:** one queryable watermark plane spanning Iceberg tables, the vector index,
and live sources — schema already deployed in
[`sql/platform.sql`](../sql/platform.sql) and queryable from Trino as the `platform` catalog.

Collectors, one per source of truth:

| Collector | Reads | Produces |
|---|---|---|
| CDC | Kafka Connect offsets + envelope `source.ts_ms` | how far behind each connector is |
| Table | Iceberg snapshot summary (`max(_src_ts)` at commit) | how fresh each materialised relation is |
| Vector | the index's declared `row_timestamp_column` | how fresh the embedding index is |

**A design decision worth defending:** per-entity watermarks are **not materialised** — that
does not scale past a few million rows. Each dataset instead declares
`platform.datasets.row_timestamp_column`, the column that *already* carries its per-row
source time, and the channel query reads it inline. **Only dataset-level watermarks are
stored.**

**Built before N2 deliberately**, because the fusion engine is unevaluable without it.

---

## C3. N2 — the freshness-aware fusion engine

**GR1 status:** §5.3 is explicitly *"a candidate research direction… not claimed as completed
work within GR1."* It gives Eq. 5.1 (staleness) and Eq. 5.2 (freshness-weighted fusion) as a
**framework**, with the decay `g(·)` and the weights α, β deliberately unspecified.

**What we build:** the implementation and the measurement.

| Policy | Behaviour |
|---|---|
| `rrf` | the Cormack et al. (2009) baseline — rank position only, no freshness term |
| `weighted` | fixed per-channel weights over normalised scores |
| `freshness_weighted` | **GR1 Eq. 5.2** — per-channel score scaled by a decay `g(·)` of that channel's staleness. `g` stays pluggable (linear, exponential) because *determining its form is the experimental work*, not a settled result. |

Every result carries **provenance**: which channels contributed, rank within each, staleness
observed, decay applied. That provenance is what makes the difference between policies
legible in the five minutes an examiner gives a demo — and it is what experiment E4 reads
back.

---

## C4. N3 — tag governance that reaches the vector index

**GR1 opens this gap and does not close it.**

§1.1 names the motivation explicitly: *"personally identifiable information can leak through
embeddings that are not subject to the same masking and access-control policies as relational
tables."*

But §4.6's two masking layers **both stop at the relational boundary** — Layer 1 masks Kafka
records, Layer 2 masks Trino columns. **Neither touches the vector store.** The gap GR1
identifies in its own motivation remains open in its own design.

**What we build:**

- **Tags, not column names.** A policy listing column names must be rewritten per domain,
  which would make governance domain-specific and break the L3 claim. Policy is keyed on
  tags (`sensitive`, `restricted`, `public`) attached to `(dataset, column)` pairs.
- **Propagation.** Tags travel along `platform.column_lineage` edges, so a tag declared once
  at the source survives every medallion hop without redeclaration.
- **The guard.** Before an embedding job reads a text column, it asks whether that column
  carries a restricting tag — directly or by propagation. If so, **the job refuses.**

That turns *"PII cannot reach the vector index"* from a convention into an enforced property.

---

## C5. N4 — freshness-SLA query routing *(stretch)*

Not in GR1. A natural consequence of N1: a request declaring a **tighter freshness SLA** than
the lake watermark can be **routed to the live source** instead of Gold. Makes the freshness
plane *actionable* rather than merely observable. Explicitly a stretch item.

---

## C6. A second domain as mechanical proof of genericity

**GR1 status:** absent. GR1 asserts MOSAIC is a platform; it offers no way to check.

**What we add:** `domains/jobmarket/` — postings and candidates — running on the **identical
binaries**. Adding it means adding source DDL and one `retrieval.yaml`. Nothing else.

It has the same *shape* and none of the same *words*: a job description is the slow semantic
channel; "still open", headcount, salary revision are the fast transactional facts. The
failure is the same failure and just as intuitive — recommending a role filled an hour ago.

**Two mechanical tests decide whether the abstraction is real:**

```bash
python mosaic.py check-abstraction          # no domain vocabulary in platform/
python mosaic.py test-domain --domain jobmarket
```

If both pass, *"is this a platform or a tool?"* stops being a claim about intent and becomes
**an observation about behaviour**. That is the single highest-value addition relative to
GR1.

---

## C7. The `platform/` ÷ `domains/` split, enforced

The abstraction is made **structural and greppable**, not just documented. `platform/` is the
generic core; anything that knows what a product is lives under `domains/`.

Enforced by a CI gate that scans `platform/` for 12 domain terms across **both** domains — the
core must not know about either:

```
$ python mosaic.py check-abstraction
scanned 6 files under platform/ for 12 domain terms
PASS -- the generic core contains no domain vocabulary.
```

Domain knowledge reaches the core **only as data**: a parsed `retrieval.yaml`, a row in
`platform.datasets`, a tag in `platform.column_tags`. Identifiers arriving at runtime are
opaque strings.

---

## C8. The `_src_ts` / `_ingest_ts` convention

**Not in GR1.** Every Silver and Gold row carries `_src_ts` (source commit time) and
`_ingest_ts`, and every Iceberg commit records `max(_src_ts)` in its snapshot summary.

**Why it is a hard rule:** N1 is **impossible to retrofit** if a pipeline skips it. There is
no way to recover, after the fact, what the source-commit time of an already-written row was.
This lands in M3 for that reason.

**Status (M3).** Bronze: every row carries `_src_ts` (envelope `source.ts_ms`) and `_ingest_ts`
(one literal per micro-batch), and every commit stamps `mosaic.max_src_ts[_ms]` -- overall and
per topic -- plus `mosaic.ingest_ts_us` into the Iceberg snapshot summary; the last joins each
row to the snapshot that published it, which is how the end-to-end latency KPI is measured.
Silver and Gold: every row carries both columns, defined as those of the **newest contributing
change** (not-null enforced by dbt tests). The snapshot-summary half **cannot** hold for
Silver/Gold: Trino has no way to write custom summary properties (STATUS finding 12), so N1
will read Trino-written tables' watermarks from the data instead.

---

## C9. Seed and load generators as first-class deliverables

**GR1 §10.1** mentions *"Faker + TPC-H patterns"* and a 10M-row target in one line. Two
things needed more thought than that:

**The corpus must be semantically real.** Descriptions are **compositional over a
category-scoped vocabulary**, not fake text. With lorem ipsum, embeddings carry no structure,
the ANN channel becomes noise, and the whole freshness argument collapses because **there is
nothing sensible for the stale channel to be confidently wrong about.** Verified: *"waterproof
hiking backpack"* retrieves dry bags, camp stoves and trekking poles — nothing from Kitchen.

Name, description and leaf category are generated **together**, because drawing them
independently produced items like *"Minimalist Kettle"* described as a stock pot — the console
shows the name while ranking uses the description embedding, so correct retrieval would have
*looked* broken.

**Reproducibility is a requirement, not a nicety.** Generation is deterministic given
`(scale, seed, now)`, all recorded in `manifest.json`. E3/E4 must re-run against an identical
corpus. An early version read the wall clock at module level, so the same seed produced
different timestamps every run — caught by a byte-comparison check that now guards it.

**The load generator** (`tools/loadgen/`, built in M1) drives continuous OLTP writes plus the
**stock-out injection lever**. It is what *manufactures* the freshness asymmetry the entire
thesis measures, so it is a deliverable, not a test fixture.

Three things it had to get right, each of which was wrong first:

*Replenishment is triggered by a low level and has a lead time.* The first version topped up
whichever product was popular, regardless of level. Since demand removes 1-3 units and a
replenishment adds 20-200, stock on the head of the catalogue inflated without bound — 2,664
units against a 500-unit seed — and **nothing could ever sell out organically**, which is the
one behaviour the corpus exists to produce. The second version replenished anything low
*immediately*, and erased the seed's deliberately-empty slice within a minute. Real
replenishment waits: a product that sells out stays out for `RESTOCK_LEAD_SECONDS`, and **that
interval is precisely the window in which a stale ranking is confidently wrong.**

*The reported throughput has to be a rate the source sustained, not a rate Python emitted.*
Statements are written into a pipe, so an unthrottled generator reports whatever the sender
managed. One run reported 549 ops/s while MySQL was still committing that work **152 seconds
later**, and an earlier run silently lost 5,327 orders when the client was killed mid-drain.
The generator now measures committed rows on a separate connection, throttles against that,
and prints *sent* and *sustained* rates separately. Measured on this machine: **~100 ops/s
sustained**, bounded by per-commit fsync under Docker Desktop.

*Effects are read back, never assumed.* Every run ends by re-counting rows in both engines and
comparing against what it sent. That check is what caught both defects above; without it the
run reported success twice.

*Nothing in the mix touches `products.name` or `products.description`.* The semantic channel's
inputs are held still on purpose, so a stale hit can be attributed to the freshness asymmetry
alone rather than to the text having changed underneath the embedding.

---

# Part D — Operational changes

Mechanical. No effect on the architecture's claims, but they are why the stack runs.

| # | Change | Reason |
|---|---|---|
| **D1** | `mosaic.py` task runner instead of `make` | `make` is not installed on this machine. Standard-library Python only, so it works identically on Windows/macOS/Linux. A Makefile delegates to it where make exists. |
| **D2** | `up` waits for **healthy**, not merely **started** | `compose up -d` returns as soon as containers *start*. Kafka Connect accepts TCP connections long before it will accept a connector. Everything downstream depends on the difference. |
| **D3** | Compose profiles — `core` / `gov` / `orch` / `console` | Appendix B's full stack does not comfortably fit one machine. The default profile is the vertical slice; governance, orchestration and the demo UI are opt-in. |
| **D4** | All published host ports parameterised via `docker/.env` | Port 9001 on this machine is held by an unrelated `lakehouse-control-plane` container. MinIO's console moves to 9101. **Container-internal ports never change**, so nothing architectural is affected. |
| **D5** | Platform metadata in its own `mosaic_platform` database, plus a 4th Trino catalog `platform` | Keeps N1's state out of the domain database — which is a **CDC source**, so platform tables there would be captured by `mosaic_pub` and pollute the change stream. The extra catalog lets a watermark be joined against the data it describes **inside one Trino query**. |
| **D6** | Domain DDL and connector configs under `domains/<domain>/`, not top-level `sql/` and `connectors/` | Keeps *"adding a domain touches only `domains/`"* **literally true** — which is what the L3 proof (C6) rests on. |
| **D7** | Explicit `decimal.handling.mode=string`, `time.precision.mode=connect` | Debezium's default for `DECIMAL` is `precise`, which emits base64-encoded bytes — unusable downstream without special handling. `string` keeps `DECIMAL(12,2)` exact and readable; Silver casts back. `connect` avoids microsecond-int representations Iceberg cannot map directly. Not specified in GR1. |
| **D8** | No `ExtractNewRecordState` at the connector | Consequence of A1 — Bronze keeps the full envelope; flattening moves to Bronze→Silver. |
| **D9** | MySQL bulk load runs as `root`, not `cdc` | Server-side `LOAD DATA INFILE` needs the **global `FILE` privilege**. `cdc` deliberately holds only GR1 Appendix A's least-privilege replication grants. Granting `FILE` to the replication account to make a one-off seed work would undermine the very thing that appendix is careful about. **Seeding is an operator action, so it uses the operator account.** |
| **D11** | Silver is **generated**, one model per captured table, by `pipelines/transform/assemble.py`; Gold is hand-written in `domains/<domain>/gold/` and copied in | Silver's job (dedup, type, latest-wins) depends only on a table's key, columns and Debezium's encodings -- all derivable from the connector config and the live catalog. Hand-written, seven near-identical models would put domain vocabulary into `pipelines/` and make the second domain edit it. Generated, **adding a domain still touches only `domains/`** (C6). A domain is now five things: `ddl/`, `connectors/`, `gold/`, `retrieval.yaml`, `tags.yaml`. |
| **D12** | Silver = one **unpartitioned current-state table** per captured table, latest-wins by source position, soft deletes (`_is_deleted`) | GR1 §4.3 says Silver is *"partitioned by source system and date to simplify replay"*. Source system is implicit in a per-table layout, and replay is Bronze's job here (A10). A date partition on a table updated in place moves every updated row between partitions on each MERGE. **Open to revisit** if a Silver consumer needs pruning. |
| **D13** | dbt runs in its own image (`dbt-core 1.8.8`, `dbt-trino 1.8.3`), behind compose profile `transform`, via `python mosaic.py dbt` | The host interpreter (3.14) is newer than dbt-core supports, and a pinned image makes every build resolve the same dbt. A one-shot job, so `up` never waits on it. |
| **D14** | Trino `iceberg.max-partitions-per-writer=5000` (default 100) | Required to run GR1 §4.3's `gold.orders` partition spec at all (A12). |
| **D15** | `tools/loadgen` draws regions from `tools/seed/vocab.py` | The two generators had separate region vocabularies (cities vs. `north`/`central`/...), so every per-region aggregate split in two -- visible only once Gold grouped by region. Fixed going forward; rows already written keep their values. |

### A debugging note worth keeping (D9)

MySQL reports a missing `FILE` privilege as:

```
ERROR 1045 (28000): Access denied for user 'cdc'@'%' (using password: YES)
```

That reads **exactly** like a wrong password. It is not. Three separate connection paths
authenticated fine; only the `LOAD DATA INFILE` statement failed. Cost one debugging cycle —
worth a footnote if you write about operational lessons.

---

# Part E — What did **not** change

Bounding the delta matters: it shows the GR1 design largely survived contact with reality.

| Area | Status |
|---|---|
| **Six-layer layered lakehouse architecture** (§4.1) | Unchanged |
| **Sources**: MySQL 8.0 (binlog) + PostgreSQL 15 (WAL), PostgreSQL dual-role with pgvector | Unchanged |
| **Data model** (Appendix A) | Unchanged, DDL used verbatim |
| **MySQL CDC prerequisites** — `binlog_format=ROW`, `binlog_row_image=FULL`, GTID | Unchanged, verified live |
| **PostgreSQL CDC prerequisites** — `wal_level=logical`, `mosaic_pub`, replication slot | Unchanged, verified live |
| **WAL-accumulation guard** — heartbeat + `confirmed_flush_lsn` monitoring (§4.4) | Unchanged |
| **Kafka 3.7 KRaft**, no ZooKeeper | Unchanged |
| **Iceberg on MinIO**, Nessie catalog | Unchanged (registry corrected, A4) |
| **Medallion Bronze/Silver/Gold semantics** (§4.2) | Unchanged |
| **Partitioning strategy** (§4.3) | Gold: implemented as written (but see A12, F3); Silver: see D12 |
| **Trino 451** as federation layer, `ANALYZE` workflow (§4.5) | Unchanged |
| **pgvector HNSW**, `m=16, ef_construction=64` (§3.4) | Unchanged |
| **Two-track top-*k* taxonomy** (§5.2) | Unchanged — adopted as the experiment structure |
| **Five CDC KPIs** (Table 4.4) | Unchanged |
| **Table 4.6 role hierarchy** | Unchanged in content; enforcement engine differs (B1) |
| **Table 4.5 two-layer masking model** | Unchanged in substance; mechanism differs (A1) |
| **Deferred components** — MongoDB, Milvus, Prometheus/Grafana | Still deferred, per GR1's own scope decision |

---

# Part F — Defects inside the GR1 report itself

These are problems **in the document**, independent of implementation. Fix them in the report
before submitting GR2 — an examiner will notice.

## F1. Eight broken cross-references to a section that does not exist

The report refers **eight times** to a scope-decision record and renders each one as a
literal `??`, because the LaTeX `
ef` points at a label that was never defined:

| PDF page | Context |
|---|---|
| **p9** | *"…rationale behind it, is documented in full in Section **??**."* (§1.2) |
| **p13** | *"…the scope decisions recorded in Section **??** to defer components safely…"* (§2.3) |
| **p16** | *"…deferred to Graduation Research 2 as recorded in Section **??**."* (§3.4) |
| **p17** | *"…experiments are deferred to GR2 as recorded in Section **??**."* (§3.5) |
| **p33** | *"…as recorded in Section **??**, produces a more defensible research plan…"* (§6.1) |
| **p33** | *"…the GR1 minimum viable core described in Section **??**…"* (§6.2) |
| **p34** | *"…the components deferred to GR2 in Table **??**…"* (§6.2) |
| **p41** | *"…the complete GR1 minimum viable core described in Section **??**…"* (Appendix B) |

**The section is not in the report.** Chapter 4 contains §4.1 through §4.6 only — confirmed
from the table of contents on p3–p4 and from the body. There is no §4.7.

**This is not a conversion artifact.** The `??` markers are present in the compiled PDF
itself, so this is exactly what an examiner reads on the page.

This is not cosmetic. That record is claimed as a **core deliverable**:

- The **abstract** lists *"an explicit scope-decision record that separates the confirmed
  minimum viable core from components deferred to later phases"* as a contribution.
- **§1.2** promises it is *"documented in full in Section ??"*.
- **§6.1** calls it the *"principal lesson"* of the design phase.
- **Appendix B** describes itself as deploying *"the complete GR1 minimum viable core
  described in Section ??"*.

So the report repeatedly cites, as one of its three main deliverables, a section it does not
contain. **Highest-priority fix.** The content exists — the MVP/Extended split is in
`UDP_BaoCaoNghienCuu_v3_Enhanced.md` §1.3, and the deferral rationale is scattered through
§3.4, §3.5 and Appendix B's comments. It needs to be written up as an actual section.

## F2. "Four data sources", but Table 4.2 lists three

**§4.3, PDF p22** says *"Table 4.2 lists the **four** data sources that compose this
domain"*. The table immediately below it (also p22) has **three** rows: MySQL, PostgreSQL,
and Derived (pgvector).

Probably a leftover from the earlier scope, where MongoDB was a source (it is four in
`UDP_BaoCaoNghienCuu_v3_Enhanced.md` §10.1, which lists MongoDB and Kafka producers too).
Fix the number, or restore the row and mark it deferred.

## F3. `users` is partitioned by a column it does not have

**§4.3, PDF p23** says *"users are partitioned by `bucket(32, user_id)`"*.

But **Appendix A.2** defines `users` with `id BIGSERIAL PRIMARY KEY` — **there is no
`user_id` column on `users`.** `user_id` is the foreign key *on `orders`*, pointing at
`users.id`.

Correct form is `bucket(32, id)`, or rename the column in the DDL for consistency. This will
surface for real in M3 when the Gold partitioning is actually applied.

**Resolved in M3** without touching Appendix A's DDL: `gold.users` exposes the key as
`user_id` (`u.id AS user_id`) and is partitioned `bucket(user_id, 32)`, which is what §4.3's
sentence -- and every join from `orders` -- already assumes. The report text can then stay;
only a footnote that Gold renames the key is needed.

## F4. Figure 4.1 shows deferred components as if in scope

Figure 4.1 (PDF **p20**) shows a source layer of **MySQL, PostgreSQL, MongoDB 6.0, and application logs**, and
the storage layer shows **Milvus 2.4** alongside pgvector. The governance layer shows
**observability (Prometheus/Grafana)**.

All three — MongoDB, Milvus, Prometheus/Grafana — are **explicitly deferred**, by §6.2 and by
Appendix B's own comment. The figure therefore over-claims the delivered scope, and the prose
at line 263 repeats the over-claim by describing the figure literally.

**Suggested fix:** redraw with deferred components in a distinct visual treatment (dashed
outline, greyed) and a legend saying "deferred to GR2/Thesis". That turns a discrepancy into
a demonstration of deliberate scoping — which is the story §6.1 wants to tell anyway.

---

# Part G — How to present this in the GR2 report

A suggested framing, since the material is unusually favourable.

**1. Lead with the findings, do not bury them.** An implementation phase that reproduces a
design exactly has demonstrated only that the design was easy. An implementation phase that
finds **four specific, technically-explained places where the design cannot be executed** has
demonstrated genuine engineering. A1 in particular — a *logical contradiction* between two
requirements, with a fix that satisfies both — is the strongest single result so far.

**2. Show A1 with both configs side by side.** The Appendix C SMT chain and the
connector-native config, with the nested-envelope explanation between them. It is visual,
concrete, and takes ninety seconds to explain.

**3. Frame Part B consistently as "substitution, not omission".** For each: *the guarantee
GR1 claims is unchanged; the enforcement engine is lighter; here is what production would
use.* Never let it read as "we could not get Ranger working".

**4. Be explicit about what you lost.** Column-level lineage on the Bronze hop (B2). Type-level
schema evolution (A2). The salt in the config file (A1). **Naming your own limitations is what
separates an engineering report from a sales pitch**, and an examiner will find them anyway.

**5. Keep the "verified vs written" distinction visible.** [`docs/STATUS.md`](STATUS.md)
already tracks it. Reporting that connector masking is *implemented but not yet exercised* is
strictly better than implying it works.

**6. Fix Part F before submission.** F1 especially — a report that cites a missing section
eight times, including from its abstract, undercuts otherwise solid work.

---

---

# Appendix: on the two copies of the report

`docs/` holds both `GR1_report.pdf` (the real deliverable, 49 pages) and `GR1_report.md`
(a conversion). The analysis in this document was written from the Markdown and then
**re-verified against the PDF**. Result:

**The conversion is faithful.** Normalised text length matches at 98% (74,030 vs 75,618
characters). Every load-bearing quotation — the Appendix C transform chain, the Appendix B
image tags, the Avro converter lines, the `??` markers, the Table 4.2 row count, the
`bucket(32, user_id)` claim — appears identically in both.

**Known conversion artifacts** (present in the `.md` only, harmless but worth knowing):

- Hyphenation across line breaks is preserved differently, so text extraction shows joins
  like `opensource`, `freshnessunaware`, `datasovereignty`. These are extraction effects,
  not errors in either document.
- The PDF's fixed-width tables were reflowed into Markdown tables. Content survives, but
  cell alignment in the raw PDF text (e.g. Table 2.1) reads as interleaved columns.
- **Both figure paths were broken** — the conversion pointed at
  `GR1_report_fixed_assets/figure-4-*.png`, while the files sit in `docs/`. Fixed
  2026-08-25; the images now render.

**Use the PDF as the citable source** when writing GR2 (page numbers, figure and table
numbering). The Markdown is convenient for grep and diff, and is trustworthy for content.

---

## Where to find things

| | |
|---|---|
| Full argument for each substitution | [`docs/adr/`](adr/) |
| The problem and abstraction ladder | [`docs/PROBLEM.md`](PROBLEM.md) |
| What is built and verified right now | [`docs/STATUS.md`](STATUS.md) |
| Durable working context + environment gotchas | [`CLAUDE.md`](../CLAUDE.md) |
| Live state in one command | `python mosaic.py doctor` |
