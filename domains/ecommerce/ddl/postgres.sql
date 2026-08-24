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

-- ---- product_embeddings (pgvector; NOT a CDC source, populated by pipeline) ----
-- 384-dim vectors from a sentence-transformers model (all-MiniLM-L6-v2).
-- refreshed_at is the freshness signal consumed by the Freshness Registry (N1)
-- and by last_refresh(d) in GR1 Eq. 5.1.
CREATE TABLE IF NOT EXISTS product_embeddings (
    product_id   BIGINT      PRIMARY KEY REFERENCES products(id),
    embedding    vector(384) NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- HNSW index for approximate top-k nearest-neighbour (cosine distance).
CREATE INDEX IF NOT EXISTS idx_prod_emb_hnsw
    ON product_embeddings USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- ---- CDC heartbeat sink (advances confirmed_flush_lsn; guards WAL bloat) ----
CREATE TABLE IF NOT EXISTS cdc_heartbeat (ts TIMESTAMPTZ NOT NULL);

-- ---- CDC replication user & logical publication ----
-- Debezium requires REPLICATION and a publication for pgoutput.
ALTER USER cdc WITH REPLICATION;
DROP PUBLICATION IF EXISTS mosaic_pub;
CREATE PUBLICATION mosaic_pub FOR TABLE users, products, categories, inventory;
