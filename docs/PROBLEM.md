# MOSAIC — Problem Statement and Abstraction Ladder

> This document is the missing first chapter of the GR1 design. `GR1_report.md` describes the
> *architecture* thoroughly, but never states the single concrete problem the platform exists
> to solve. Everything built in this repository hangs off the problem stated here.

The structure is a four-rung ladder. Each rung has a written artifact and a mechanical test.
We start from one failing query in one domain, and climb until nothing in the platform knows
what domain it is running.

| Rung | Question it answers | Artifact | Test |
|---|---|---|---|
| **L0** | What concretely breaks today? | this section | the failure is reproducible on demand |
| **L1** | What is the model of the broken case? | `domains/ecommerce/` | the failure is *measurable* |
| **L2** | What of that is not about commerce? | `platform/` | the failure is *fixed* |
| **L3** | Is the abstraction real or asserted? | `domains/jobmarket/` | second domain runs, zero code change |

---

## L0 — One concrete failure

The query:

> **"Show the 10 products most relevant to shopper X, right now."**

Answering it well requires three facts that live in three different systems, at three
materially different freshness levels:

| Fact needed | Where it lives | Typical staleness | Why |
|---|---|---|---|
| Semantic relevance to the query | `product_embeddings` (pgvector, 384-d) | **hours** | bounded by the embedding refresh schedule |
| Purchase affinity, revenue rank, recency | `iceberg.gold.*` | **minutes** | bounded by CDC lag + batch cadence |
| Is it actually purchasable? (stock, price, still listed) | MySQL / PostgreSQL, live | **seconds** | the source of truth itself |

The conventional answer is: run the SQL ranking, run the vector ranking, fuse the two with
Reciprocal Rank Fusion. RRF is the standard baseline for exactly this shape of problem
(Cormack, Clarke & Buettcher, SIGIR 2009).

### The defect

**RRF treats every input ranking as equally current.** Its score depends only on rank position:

```
RRF(d) = SUM_i  1 / (k + rank_i(d))
```

There is no term anywhere in that expression for *when the ranking was computed* or *how old
the data behind it is*. So the concrete failure is:

> A product that sold out five minutes ago still ranks **#1**.

The semantic channel put it there. The semantic channel is six hours old. It does not know the
product is gone, and — this is the part that matters — **the fusion function has no way to
penalise it for not knowing.** Meanwhile the transactional channel, fresh to the second, already
reflects the stock-out and is being outvoted by a stale opinion carrying equal weight.

This generalises past stock-outs: a price that changed, a listing that was withdrawn, a seller
that was suspended. Any fact that moves faster than the slowest channel in the fusion.

### Why this is not simply "refresh the embeddings faster"

Re-embedding a catalogue is expensive and gets more expensive as the catalogue grows; the
refresh interval is a budget decision, not an oversight. The asymmetry is **structural**, not a
misconfiguration. Every real deployment that combines a batch-computed semantic index with a
streaming transactional store has it. The question is not how to eliminate the asymmetry — it
is how to **make the fusion function aware of it**.

### How the failure is measured

Two metrics, both computed against an **oracle**: at query time *t*, the ideal ranking computed
directly against the live OLTP databases using the same scoring logic. The oracle is fresh by
construction, which is what makes it usable as ground truth.

- **stale-hit rate@10** *(primary)* — the fraction of returned items whose position rests on
  data the live source already contradicts. This is the metric that names the bug.
- **nDCG@10 vs oracle** *(secondary)* — guards against a degenerate fix. A policy that
  suppresses everything scores a perfect stale-hit rate and a terrible nDCG; both must move in
  the right direction for a result to mean anything.

---

## L1 — Model the concrete case

Formalise the e-commerce instance so the failure stops being an anecdote:

- **Entities and sources** — GR1 Appendix A, unchanged. MySQL 8.0 holds transactions
  (`orders`, `order_items`, `payments`, captured via binlog); PostgreSQL 15 holds master data
  (`users`, `products`, `categories`, `inventory`, captured via WAL) *and* doubles as the
  vector store through pgvector.
- **Two ranking channels** — a SQL channel over `iceberg.gold.*`, and an ANN channel over
  `product_embeddings`.
- **Two freshness signals** — `last_cdc(d)` and `last_refresh(d)`, per GR1 Eq. 5.1.
- **The fusion function** — GR1 Eq. 5.2, with the decay `g(.)` left pluggable, because
  determining its right form *is* the experimental work.

The L1 deliverable is a pipeline that makes the query answerable **and** the failure
reproducible on demand — a switch that stocks out the current #1 result and lets you re-run.

---

## L2 — Abstract it

Nothing above is about commerce. Strip the vocabulary and the invariant is:

> A **retrievable entity** is ranked by *N* **channels**. Each channel is materialised by some
> pipeline, and every pipeline has a measurable **watermark**. Correct fusion is therefore a
> function of scores **and** watermarks — not scores alone.

Three domain-neutral artifacts fall out of that sentence. They are the platform.

### 1. Freshness Registry — `platform/freshness/`

One queryable watermark plane spanning every dataset in the system — Iceberg tables, the vector
index, live sources — at both dataset and per-entity granularity.

The parts already exist and are never joined: Iceberg exposes snapshots, Debezium exposes
offsets, a catalog exposes lineage. None of them answers *"how old is the data behind this
particular answer?"* Without that answer GR1 Eq. 5.1 cannot be evaluated at all — which is why
this is built before the fusion engine, not after.

### 2. Declarative retrieval spec — `platform/spec/`, `domains/*/retrieval.yaml`

A domain binds an entity to its channels — engine, query template, registry key, freshness SLA —
plus a fusion policy. The runner is domain-free; only the YAML carries domain knowledge.

### 3. Tag-driven governance — `platform/governance/`

PII policy keyed on catalog **tags**, not column names, propagating source to Bronze to Silver
to Gold to embedding. A column tagged sensitive cannot become embedding input.

GR1 §1.1 names PII leaking through embeddings as a motivating gap; GR1 §4.6's two masking layers
both stop at the relational boundary. This rung closes the platform's own open gap.

---

## L3 — Prove the abstraction rather than assert it

A second, unrelated domain — `jobmarket`, postings and candidates — runs on the identical
binaries. Adding it means adding source DDL and one `retrieval.yaml`. Nothing else.

It has the same shape and none of the same words: a job description is the slow semantic
channel; "still open", headcount, salary revision are the fast transactional facts. The failure
is the same failure, and just as intuitive — recommending a role that was filled an hour ago.

Two mechanical tests decide whether the abstraction is real:

```bash
# 1. No domain vocabulary may appear in the generic core.
python mosaic.py check-abstraction

# 2. The second domain works against the same running stack.
python mosaic.py test-domain --domain jobmarket
```

If both pass, the answer to the advisor's *"is this a platform or a tool?"* stops being a claim
about intent and becomes an observation about behaviour. **That is the point of the ladder.**

---

## What this buys, restated plainly

GR1 identified three gaps. The ladder maps onto them:

| GR1 gap | Rung that addresses it |
|---|---|
| (i) no benchmark-ready reference architecture for multi-source CDC | L1 — the pipeline, plus experiments E1-E3 |
| (ii) no unified governance across structured **and** vector data | L2 — tag propagation reaching the embeddings |
| (iii) no freshness-aware hybrid top-*k* fusion | L0/L1 — the failure and its measurement; L2 — the fix |

And it adds one the report does not claim: **evidence that the result is a platform**, in the
form of a second domain running on it unmodified.
