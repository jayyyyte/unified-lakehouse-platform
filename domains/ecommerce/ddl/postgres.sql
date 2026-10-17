-- =======================================================================
-- MOSAIC :: PostgreSQL 15 source schema (E-Commerce MASTER data + VECTORS)
-- Domain: ecommerce.  Source of GR1 Appendix A.2 -- kept faithful to the report.
-- Dual role:
--   (1) CDC source (master data) captured via WAL / pgoutput logical decoding.
--   (2) Vector store for semantic search via the pgvector extension.
-- ========================================================================
CREATE EXTENSION IF NOT EXISTS vector;   -- pgvector

-- ---- categories ----
CREATE TABLE IF NOT EXISTS categories (
    id          BIGSERIAL   PRIMARY KEY,
    name        TEXT        NOT NULL,
    parent_id   BIGINT      NULL REFERENCES categories(id),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---- users ----
CREATE TABLE IF NOT EXISTS users (
    id          BIGSERIAL   PRIMARY KEY,
    full_name   TEXT        NOT NULL,
    email       TEXT        NULL,        -- PII: masked at SMT layer
    phone       TEXT        NULL,        -- PII: masked at SMT layer
    national_id TEXT        NULL,        -- PII: NEVER persisted (nulled at SMT layer)
    region      TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---- products ----
CREATE TABLE IF NOT EXISTS products (
    id          BIGSERIAL     PRIMARY KEY,
    sku         TEXT          NOT NULL UNIQUE,
    name        TEXT          NOT NULL,
    description TEXT          NULL,      -- source text for embeddings
    category_id BIGINT        NOT NULL REFERENCES categories(id),
    price       NUMERIC(12,2) NOT NULL,
    updated_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- ---- inventory ----
-- stock_qty is the fast-moving fact that the L0 failure turns on: the load generator
-- drives it, and `make stockout` forces it to zero for a chosen product.
CREATE TABLE IF NOT EXISTS inventory (
    product_id  BIGINT      PRIMARY KEY REFERENCES products(id),
    stock_qty   INT         NOT NULL DEFAULT 0,
    warehouse   TEXT        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---- product_embeddings: created by the embedding pipeline, not here (M4) ----
-- GR1 Appendix A defined this table in the source DDL as (product_id PK REFERENCES
-- products(id), embedding vector(384), refreshed_at) with an HNSW index. M4 moves its
-- definition into the pipeline (pipelines/embeddings, driven by retrieval.yaml), which
-- builds a fresh table beside the live one on every refresh and swaps it in atomically:
--
--   product_id   BIGINT PRIMARY KEY      -- no foreign key, see below
--   embedding    vector(384) NOT NULL
--   stock_qty, price                     -- snapshot of the fast facts at refresh time
--   refreshed_at TIMESTAMPTZ NOT NULL    -- watermark of the Gold data read (N1 signal)
--   embedded_at  TIMESTAMPTZ NOT NULL    -- wall clock of the job
--   + HNSW (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)
--
-- The foreign key to products is dropped on purpose: a derived index must never be able
-- to block a write to the system of record (a DELETE of an indexed product would fail),
-- and the index is *meant* to describe products as they were at refresh time.
-- It stays out of mosaic_pub: it is derived data, not a CDC source.
-- Rationale for every column: docs/adr/0006-m4-embedding-pipeline.md.

-- ---- CDC heartbeat sink (advances confirmed_flush_lsn; guards WAL bloat) ----
-- This table MUST be part of mosaic_pub below, and it was not until M2 measured the
-- guard and found it inert. The connector's heartbeat.action.query INSERTs here every
-- 10 s, which writes WAL -- but pgoutput only decodes changes to *published* tables, so
-- while this table sat outside the publication Debezium never received a change event
-- for it, never gained a new LSN to confirm, and confirmed_flush_lsn did not move.
-- Measured over 130 s of idle with the connector healthy: current_wal_lsn advanced
-- 0/4175C28 -> 0/4176308 while confirmed_flush_lsn stayed at 0/40D3EA8. That is the
-- exact unbounded-retention condition GR1 section 4.4 says this heartbeat prevents.
-- The heartbeat only guards WAL if the connector can *see* its writes.
CREATE TABLE IF NOT EXISTS cdc_heartbeat (ts TIMESTAMPTZ NOT NULL);

-- ---- before images (REPLICA IDENTITY) ----
-- PostgreSQL's default replica identity puts only the primary key in the WAL for an
-- UPDATE, so Debezium emits `before: null` and there is no before image at all. Two
-- things in this project depend on there being one:
--   * GR1 section 4.2 requires Bronze to store the unmodified change envelope, and an
--     envelope whose `before` is always null is not that envelope.
--   * ADR 0004's whole argument is that connector-native masking covers the before
--     image where an SMT chain does not -- unprovable on a source that has none.
-- FULL logs the entire old row, which costs WAL volume. That cost is accepted here for
-- the three tables whose previous state is actually read: PII (users), the fast-moving
-- fact the L0 failure turns on (inventory), and price history (products).
ALTER TABLE users     REPLICA IDENTITY FULL;
ALTER TABLE products  REPLICA IDENTITY FULL;
ALTER TABLE inventory REPLICA IDENTITY FULL;

-- ---- CDC replication user & logical publication ----
-- Debezium requires REPLICATION and a publication for pgoutput.
ALTER USER cdc WITH REPLICATION;
DROP PUBLICATION IF EXISTS mosaic_pub;
CREATE PUBLICATION mosaic_pub
    FOR TABLE users, products, categories, inventory, cdc_heartbeat;
