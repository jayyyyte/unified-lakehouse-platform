# ADR 0004 — Connector-native masking instead of Kafka Connect SMTs

**Status:** Accepted · **Date:** 2026-08-24 · **Relates to:** GR1 §4.2, §4.4, §4.6, Table 4.5, Appendix C

> This ADR records a **contradiction inside the GR1 design** that only surfaces when the design
> is implemented. It is worth writing up in the GR2 report as a finding, not buried here.

## The contradiction

GR1 makes two requirements that stock Kafka Connect SMTs cannot satisfy simultaneously.

**§4.2 — Bronze must keep the envelope.** *"The Bronze tier stores the unmodified Debezium
change envelope, including the before/after row state, the operation type, the source offset or
log sequence number, and the event timestamp, with no transformation applied, in order to
preserve full auditability and replay capability."*

**§4.6 / Table 4.5 — masking must be irreversible and happen before Kafka.** Layer 1 applies
`MaskField` / `ReplaceField` *"at ingestion, before the Kafka topic"*, so that raw PII
*"never persists to any storage tier."*

`MaskField$Value` and `ReplaceField$Value` address **top-level** record fields. In a Debezium
change event the business columns are nested under `before` and `after`, so neither SMT can
reach them. That is precisely why Appendix C's `transforms` chain runs
`unwrap` (`ExtractNewRecordState`) **first**:

```
"transforms": "unwrap,maskPII,dropSensitive,ts"
```

But `unwrap` flattens the record to the `after` image and discards `before`. So Appendix C, as
written, produces a Bronze tier that **cannot** contain the unmodified envelope §4.2 requires.
Applying the SMT chain and honouring §4.2 are mutually exclusive.

There is a second, quieter problem. Even if the envelope were somehow preserved alongside the
SMT chain, `MaskField` would only ever mask the `after` image — **the before-image of an UPDATE
would carry the unmasked previous value straight into Bronze.** The SMT design leaks exactly the
PII it is there to stop, on every update to a masked column.

## Decision

Move Layer-1 masking from Kafka Connect SMTs into the **Debezium connector itself**, and remove
`unwrap` from the connector entirely.

| Concern | Appendix C (SMT) | Here (connector-native) |
|---|---|---|
| Mask a value | `MaskField$Value` | `column.mask.hash.SHA-256.with.salt.<salt>` |
| Drop a column | `ReplaceField$Value` | `column.exclude.list` |
| Harmonise timestamps | `TimestampConverter$Value` | `time.precision.mode=connect`, then cast in Silver |
| Flattening | at the connector | in the Bronze-to-Silver step |

Connector-native masking runs **before the envelope is constructed**, so it covers `before` and
`after` alike and leaves the envelope intact.

## Consequences

**Both GR1 requirements now hold at once.** Bronze receives the full envelope (§4.2); raw PII
never reaches a Kafka topic, let alone storage (§4.6). Table 4.5's two-layer model is unchanged
in substance — Layer 1 is still irreversible and still pre-Kafka; only the mechanism moved.

**Layer 1 got strictly stronger.** Before-images are masked too, closing the update leak above.

**Hashing rather than nulling.** `MaskField` with an empty replacement destroys the value.
A salted SHA-256 is equally irreversible but keeps the column joinable and countable, so
Silver/Gold can still answer "how many distinct buyers" without ever seeing an address. Fields
GR1 says must never persist at all — `users.national_id`, `payments.raw_card_number` — are
excluded outright rather than hashed, because a salted hash is still persistence.

**Cost.** The salt is a deployment secret. In this PoC it sits in the connector config, which
would be unacceptable in production; noted as a limitation, with the fix (Connect config
provider backed by a secret store) named rather than implemented.

**Timestamp harmonisation moves downstream.** `TimestampConverter` was the third SMT and has the
same nesting limitation. GR1 §4.4's goal — one ISO-8601 representation across MySQL and
PostgreSQL — is met in the Bronze-to-Silver transform instead, which is where type normalisation
already happens.

## Verification (2026-08-28) — what was actually observed

The decision above was made from the documentation. It has now been run. `python mosaic.py
cdc-verify` derives its expectations from the connector config itself and samples the *tail* of
each topic, so it inspects live change events rather than the initial snapshot.

Against 300 sampled messages per topic, after `python mosaic.py loadgen`:

| Claim | Evidence |
|---|---|
| Envelope intact | every message carries `op` / `before` / `after` / `source`; no `ExtractNewRecordState` |
| `after` masked | 754 masked values checked on `orders`, 674 on `users` — all hex digests |
| **`before` masked** | **502 before images inspected across both sources, all masked** |
| Excluded columns absent | `payments.raw_card_number` and `users.national_id` absent from 637 images — not null, absent |

The before-image case, which is the entire argument of this ADR, on a PII `UPDATE`:

```json
{"op": "u",
 "before": {"id": 6427, "full_name": "Dang Khanh",
            "email": "4cd0afef…0c618c", "phone": "252b376c…8a45157", "region": "north"},
 "after":  {"id": 6427, "full_name": "Dang Khanh",
            "email": "4cd0afef…0c618c", "phone": "67118265…4033879", "region": "south"}}
```

The phone changed, so the two hashes differ — and **both** are hashed. Under Appendix C's SMT
chain the `before` side of this record would have carried the previous phone number in
plaintext. `national_id` appears in neither image.

## Two things the running system revealed that the documentation did not

**1. The hash is truncated to the source column's width.** `orders.buyer_phone VARCHAR(32)`
receives 32 hex characters — half a SHA-256 digest — while `orders.buyer_email VARCHAR(255)`
receives all 64. Nothing is logged. The value is still a one-way salted digest, so §4.6's
guarantee holds, but **the strength of the mask depends on the source DDL**, and a narrow PII
column would silently produce a weak one. `tools/cdc/verify.py` reads the column width from
`information_schema` and expects `min(64, width)`, reporting truncation rather than accepting
it. Full detail: CHANGES-FROM-GR1 §A7.

**2. On PostgreSQL there was no before image to mask at all.** Under the default replica
identity, PostgreSQL logs only the primary key of the old row, so Debezium emits
`before: null` on every UPDATE — making this ADR's claim untestable on that source and GR1
§4.2's envelope requirement unsatisfiable there. Fixed by `REPLICA IDENTITY FULL` on `users`,
`products` and `inventory` in `domains/ecommerce/ddl/postgres.sql`. Full detail:
CHANGES-FROM-GR1 §A6.

Both are worth stating in the GR2 report: the first shows that a masking guarantee can be
weakened by a schema decision made elsewhere, and the second that a design requirement can be
unsatisfiable on one source and satisfiable on another with no warning from either.

## Reference retained

Appendix C's original SMT configuration is kept verbatim in the GR1 report. This repository does
not reproduce it, because it does not work as specified; the two configs should be shown
side by side in the GR2 report as the worked example of the finding.
