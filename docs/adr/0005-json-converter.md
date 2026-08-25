# ADR 0005 — JSON converter instead of Confluent Avro + Schema Registry

**Status:** Accepted · **Date:** 2026-08-25 · **Relates to:** GR1 Appendix B, Appendix C, Fig. 4.1, Table 4.4

## Context

GR1 Appendix B configures the Kafka Connect worker with
`io.confluent.connect.avro.AvroConverter` against a Confluent Schema Registry, and
Appendix C repeats it per connector.

On the first real run the Connect worker **died at startup**, before serving any request:

```
ERROR  Stopping due to error   [org.apache.kafka.connect.cli.AbstractConnectCli]
org.apache.kafka.common.config.ConfigException: Invalid value
io.confluent.connect.avro.AvroConverter for configuration key.converter:
Class io.confluent.connect.avro.AvroConverter could not be found.
```

The `debezium/connect` image does not bundle Confluent's Avro converter — it is
Confluent-licensed and ships separately. Appendix B cannot have been executed as written.

## Decision

Use `org.apache.kafka.connect.json.JsonConverter` with `schemas.enable=false` on both key
and value.

Alternatives weighed:

| Option | Why not |
|---|---|
| Custom image with Confluent Avro jars | ~8 transitive jars pinned by hand, brittle across versions. Worse, Confluent's wire format prefixes a magic byte and schema id, and OSS Spark's `from_avro` has no Schema Registry integration — the Bronze job would need ABRiS or manual deserialisation. |
| Apicurio converters (`ENABLE_APICURIO_CONVERTERS=true`) | Ships inside the image, so no download problem, but swaps Confluent Schema Registry for Apicurio Registry — a service Appendix B does not name — and leaves the same Avro wire-format burden on Spark. |

## Consequences

**Kept.** The Debezium change envelope — `op`, `before`, `after`, `source` — is fully
preserved, which is what GR1 §4.2 requires of Bronze. `schemas.enable=false` drops Connect's
per-message *schema block*, not the envelope.

**Arguably improved for Bronze.** §4.2 justifies Bronze by auditability and replay.
Self-describing JSON can be read by any tool without a registry lookup; a binary Avro payload
whose schema lives in a separate service is a weaker audit artifact if that service is lost.
Downstream, Spark parses plain JSON instead of needing a Schema-Registry-aware deserialiser.

**Lost.** Compact binary encoding, and registry-enforced compatibility checking. At this
project's scale — a single node, 10M rows — neither is load-bearing.

**Partially lost: schema-evolution fidelity.** With `schemas.enable=false` an added or
dropped column is still observable as a change in the payload's field set, so GR1 Table 4.4's
DDL-handling KPI stays measurable for the common case. A *type* change (say `INT` to `BIGINT`)
that does not alter the field set is **not** observable. Recorded as a limitation; if the DDL
experiment needs it, flipping `value.converter.schemas.enable` to `true` restores type
metadata at roughly an order of magnitude in message size.

**Schema Registry stays deployed but is off the data path.** It remains in the stack and in
Fig. 4.1 so that returning to Avro is a config change rather than a redeployment. It is
currently unused, and the report should say so rather than implying it is load-bearing.

**Reversible.** The converter is one line in the worker config and two per connector.
