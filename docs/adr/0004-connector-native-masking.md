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

## Reference retained

Appendix C's original SMT configuration is kept verbatim in the GR1 report. This repository does
not reproduce it, because it does not work as specified; the two configs should be shown
side by side in the GR2 report as the worked example of the finding.
