# Advisor Briefing — MOSAIC (GR2)

**A script for the supervision meeting: what to say, in what order, what to show live, and
what to ask.**

Prepared 2026-09-08 · Covers M0 → M2 · Student: Chu Tuan Linh

> Numbers in this script are real measurements, not estimates. Every one is reproducible from
> `experiments/results/` or by re-running the command shown. If asked "how do you know?",
> the answer is always a command — say it.

---

## 0. Before the meeting (10 minutes)

```bash
python mosaic.py up          # brings the stack up AND waits for healthy
python mosaic.py doctor      # expect: all checks passed
python mosaic.py cdc-verify  # masking + envelope proof; expect all topics matching
```

If `doctor` is not clean, **do not demo** — brief from this document and the ledgers instead.
A failed live demo costs more than a described one.

---

## 1. Opening — one paragraph (30 seconds)

> "The design phase produced an architecture. This phase is producing *evidence*. Three
> milestones are closed and verified — the stack, the sources with live CDC, and now restart
> safety. The most useful output so far is not that the design works; it is the four places
> where the GR1 design **cannot** work as written, each found by building the thing and
> measuring it. That is what I want to walk through."

---

## 2. Where the project stands (1 minute)

| | Milestone | State |
|---|---|---|
| M0 | Stack, problem spec, skeleton | done, verified |
| M1 | Sources, seed corpus, load generator, live CDC | done, verified |
| M2 | CDC restart safety + WAL guard | done, verified |
| **M3** | **Medallion on Iceberg — Spark Bronze, dbt-trino Silver/Gold** | **done, verified 2026-10-05 (new since M2 briefing)** |
| M4–M8 | Federation, freshness registry, fusion engine, experiments, governance | not started |

One sentence on scale: everything runs at `small` (~86k seeded rows, ~140k after load-generator growth). The 10M-row run for E2
is deliberately deferred until the pipeline shape stops changing, because it costs ~40 minutes
per run.

---

## 3. The headline result (3 minutes) — *lead with this*

**Two findings from M2. Both are silent failures: no error, no warning, no log line. Both
were found only by building the measurement.**

### Finding A8 — "restart safety" is at-least-once, not exactly-once

GR1 Table 4.3 says restart safety is achieved because the offset is stored in the Kafka
Connect offsets topic. True — but the offset is flushed **on a timer**
(`offset.flush.interval.ms = 60000`), not per record. So a crash replays up to a minute of
work from a stale offset.

> **Measured:** SIGKILL the connector for 45 s under load. **Zero** rows lost across 4 tables
> and 3,711 source rows. **110 duplicate events** replayed. Fully caught up **30.3 s** after
> restart. *(M3 correction: "110" counted events per key, which includes legitimate updates;
> the identity-matched replay count for a comparable run is 183. The conclusion is unchanged.)*

The point to make: the replay window is bounded by the *flush interval*, not by the length of
the outage. A 5-second crash and a 5-minute crash replay the same ~60 s of work. This is also
why GR1 §4.2's "deduplicate on the source offset" is load-bearing for M3 rather than
decorative.

### Finding A9 — GR1 §4.4's WAL-bloat guard was inert

GR1 §4.4 correctly identifies the PostgreSQL-specific risk: a logical replication slot retains
WAL until the consumer confirms it, so a stalled consumer grows WAL without bound. It names the
mitigation — a heartbeat that advances `confirmed_flush_lsn` even when the captured tables are
idle. That mitigation was configured. **It was doing nothing.**

The heartbeat was firing perfectly: 503 rows written, 503 messages on the heartbeat topic. But
`heartbeat.action.query` wrote to a table that was **not in the publication**, and `pgoutput`
decodes only published tables. So the heartbeat's WAL never became a change event, Debezium
never gained a new LSN to confirm, and the flush position stood still while WAL grew.

> **Measured, over the same 130 s idle window with a healthy connector:**
>
> | | `confirmed_flush_lsn` advance |
> |---|---|
> | heartbeat table outside the publication | **0 bytes** |
> | heartbeat table published *(the fix)* | **688,776 bytes** |
>
> Retained WAL fell from 704,880 to 4,544 bytes.

**The framing that matters:** this is the same *shape* as an earlier finding — PostgreSQL emits
no before-image under the default replica identity, so GR1 §4.2's envelope requirement silently
failed on one of the two sources. Both are mitigations GR1 names in prose that are ineffective
in practice until something measures them. That pattern — *"the report is right about the risk
and wrong about the fix being automatic"* — is the strongest contribution of the implementation
phase so far.

---

### M3 in one breath (if the meeting is after 2026-10-05)

- **Bronze is exactly-once over an at-least-once source.** A Connect SIGKILL put 1,107 insert
  events on Kafka for 1,088 orders; Bronze holds 1,088. A Spark SIGKILL landed *between* an
  Iceberg commit and its checkpoint commit; the re-run batch wrote 0 of 673 rows twice.
- **First Table 4.4 KPI unblocked:** source commit -> Iceberg Bronze p50 7.8 s (MySQL) /
  8.0 s (PostgreSQL); Debezium itself is sub-second, the 10 s trigger is the rest.
- **dbt-trino `MERGE` on Nessie Iceberg works** -- after four incremental builds under load,
  Silver equals the live sources row for row in all 7 tables (and a wrong-zone negative
  control makes every MySQL row differ, so the check has teeth).
- **Three new findings** (CHANGES-FROM-GR1 A10-A12): Kafka retention silently erased the CDC
  history twice; MySQL `DATETIME` arrives 7 h off (GR1's TimestampConverter was load-bearing);
  §4.3's `gold.orders` partitioning yields 12.6-row files here, `ANALYZE` 33x slower.

---

## 4. Live demo (5 minutes) — optional, pick one

**Option A — the WAL guard (recommended; it is the newest result and it is visual).**

```bash
python mosaic.py wal-guard --idle 130
```

Runs ~6 minutes. Too long for a live meeting — **show the recorded output instead** from
`docs/BUILD-LOG.md` §3.4, which has the fail/pass pair side by side. Say: "this is recorded
because it takes six minutes; the command is one line and you can re-run it."

**Option B — restart safety (better if they want to see something happen).**

```bash
python mosaic.py restart-safety --rate 25 --warmup 20 --stall 45 --drain 20
```

Runs ~4 minutes. Narrate while it runs:
1. warm-up — CDC streaming normally
2. **SIGKILL** the connector; writes keep going into a dead pipeline
3. restart; connectors back in ~8 s
4. the proof — every auto-increment id written during the outage must appear on its topic

Then point at the table: `missing` is 0 in every row, `dup` is not.

**Option C — no demo.** Show `python mosaic.py doctor` (10 seconds, all green) and talk from
section 3. This is the safe choice.

---

## 5. What is next (1 minute)

*(Written before M3; M3 is now done -- see the box in section 3.)*

**M4 -- the embedding pipeline**, then dataset registration for **M5 (N1 Freshness Registry)**.
The embedding batch interval *is* the slow channel's staleness, so it gets chosen and recorded
deliberately. Two decisions worth raising: keep GR1 §4.3's `gold.orders` partition spec or
move to month-level (A12), and whether CDC topics should keep a longer retention (A10).

---

## 6. If asked: what is *not* verified

Answer these directly. Do not soften them.

- The publication fix is applied to the **running** database by hand; the DDL edit only takes
  effect on a fresh volume.
- No Iceberg maintenance (compaction, snapshot expiry) and no dbt schedule yet: Gold is as
  fresh as the last manual `python mosaic.py dbt`.
- The `gov`, `orch`, `console` profiles have never been started.
- Everything is at `small` scale.

---

## 7. Questions to ask the advisor

Bring these; a supervision meeting where the student only reports is a wasted one.

1. **On framing the findings.** Four GR1 configurations cannot be executed as written (Avro
   converter, SMT masking chain, replica identity, heartbeat publication) plus two image tags
   that do not resolve. Should these be presented as a **dedicated results chapter** in GR2, or
   distributed through the implementation chapter as they arise? *My preference: a dedicated
   chapter — they are the phase's main contribution and burying them undersells the work.*

2. **On scope.** M3–M8 is a lot: medallion, freshness registry, fusion engine, four
   experiments, governance, second domain. If time runs short, which is the better GR2:
   the **full ladder shallowly** (all four rungs, second domain proving genericity), or
   **L0–L2 deeply** (the fusion result measured properly, genericity argued rather than
   demonstrated)?

3. **On the experiments.** GR1 §10.1 wants 10M+ rows for the Top-N pushdown experiment to be
   meaningful. At the measured load rate that is ~40 minutes per corpus regeneration. Is a
   single 10M run at the end acceptable, or does the comparison need repeated trials?

4. **On the novelty claim.** Is "freshness-aware rank fusion, with staleness as an explicit
   term in the scoring function" defensible as the thesis contribution, given RRF's ubiquity as
   the baseline? Anything in the recent literature I should be checking against?

---

## 8. One-line summary to leave them with

> "Four milestones closed, three of GR1's five CDC KPIs now measured, the medallion layer proven
> exactly-once end to end, and a growing list of places where the design as written could not
> have been run — each found by measuring rather than reading. Next is the slow channel: the
> embedding pipeline."

---

### Appendix — the numbers, for reference during the meeting

| Measurement | Value | Source |
|---|---|---|
| Services healthy | 10/10 | `mosaic.py doctor` |
| Seeded corpus | 86,314 rows across 7 tables | `manifest.json` |
| Sustained OLTP write rate | ~100 ops/s | M1, bounded by per-commit fsync |
| Offset flush interval | 60,000 ms | live worker config |
| Rows lost across a 45 s SIGKILL | **0** (of 3,711) | `restart_events.jsonl` |
| Duplicate events replayed | 110 | same |
| Connectors `RUNNING` after restart | 8.3 s | same |
| Fully caught up after restart | 30.3 s | same |
| MySQL GTIDs replayed during recovery | 1,808 (`1-61738` → `1-63546`) | same |
| Flush advance, guard broken → fixed | 0 → 688,776 bytes | same |
| Retained WAL, guard broken → fixed | 704,880 → 4,544 bytes | same |
