-- =======================================================================
-- MOSAIC :: platform metadata store  (N1 Freshness Registry + N3 tag model)
--
-- DOMAIN-FREE BY CONTRACT. No table, column, or seeded value in this file may
-- name a business entity. Dataset identifiers are *data*, not schema.
--
-- Lives in its own database so that it is never captured by mosaic_pub and never
-- mixed with domain data, while still sharing the PostgreSQL container.
-- =======================================================================
CREATE DATABASE mosaic_platform;
\connect mosaic_platform

CREATE SCHEMA IF NOT EXISTS platform;

-- ---------------------------------------------------------------------
-- datasets: every relation the platform can rank from or report freshness for.
--
-- row_timestamp_column is the key design decision of N1. Rather than materialising
-- a watermark per entity (which does not scale past a few million rows), each
-- dataset declares the column that already carries its per-row source time. Dataset
-- watermarks are stored; per-entity freshness -- last_cdc(d) / last_refresh(d) in
-- GR1 Eq. 5.1 -- is read from that column by the channel query itself.
-- ---------------------------------------------------------------------
CREATE TABLE platform.datasets (
    dataset_id            TEXT PRIMARY KEY,        -- e.g. 'iceberg.gold.<relation>'
    engine                TEXT NOT NULL,           -- trino | postgres | mysql
    relation              TEXT NOT NULL,           -- fully-qualified relation name
    kind                  TEXT NOT NULL,           -- source | bronze | silver | gold | vector
    row_timestamp_column  TEXT NULL,               -- per-row source time, if any
    freshness_sla_seconds INTEGER NULL,            -- T_sla in GR1 Eq. 5.1
    domain                TEXT NULL,               -- opaque label; never interpreted
    registered_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT datasets_kind_ck
        CHECK (kind IN ('source','bronze','silver','gold','vector'))
);

-- ---------------------------------------------------------------------
-- watermarks: current freshness per dataset. One row per dataset, upserted by the
-- collectors in platform/freshness/.
--
--   watermark_ts    -- max source-commit time materialised so far
--   observed_at     -- when the collector last looked
--   lag_seconds     -- observed_at - watermark_ts, denormalised for cheap reads
--   source_position -- binlog file:pos, LSN, Iceberg snapshot id, ... (opaque)
-- ---------------------------------------------------------------------
CREATE TABLE platform.watermarks (
    dataset_id      TEXT PRIMARY KEY REFERENCES platform.datasets(dataset_id) ON DELETE CASCADE,
    watermark_ts    TIMESTAMPTZ NOT NULL,
    observed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    lag_seconds     DOUBLE PRECISION NOT NULL,
    source_position TEXT NULL,
    collector       TEXT NOT NULL,
    detail          JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Append-only history: what the console charts and what E1/E4 read back.
CREATE TABLE platform.watermark_history (
    id              BIGSERIAL PRIMARY KEY,
    dataset_id      TEXT NOT NULL,
    watermark_ts    TIMESTAMPTZ NOT NULL,
    observed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    lag_seconds     DOUBLE PRECISION NOT NULL,
    source_position TEXT NULL
);
CREATE INDEX idx_wm_hist_dataset_time
    ON platform.watermark_history (dataset_id, observed_at DESC);

-- ---------------------------------------------------------------------
-- N3 governance: tags are attached to (dataset, column) pairs and propagate along
-- edges. An edge says "this downstream column is derived from that upstream one",
-- which is what lets a `sensitive` tag survive every medallion hop and reach the
-- embedding-input guard.
-- ---------------------------------------------------------------------
CREATE TABLE platform.column_tags (
    dataset_id  TEXT NOT NULL,
    column_name TEXT NOT NULL,
    tag         TEXT NOT NULL,
    origin      TEXT NOT NULL DEFAULT 'declared',   -- declared | propagated
    PRIMARY KEY (dataset_id, column_name, tag)
);

CREATE TABLE platform.column_lineage (
    src_dataset_id TEXT NOT NULL,
    src_column     TEXT NOT NULL,
    dst_dataset_id TEXT NOT NULL,
    dst_column     TEXT NOT NULL,
    PRIMARY KEY (src_dataset_id, src_column, dst_dataset_id, dst_column)
);

-- ---------------------------------------------------------------------
-- Query audit (GR1 section 4.6). Immutable by convention; no UPDATE/DELETE grant.
-- ---------------------------------------------------------------------
CREATE TABLE platform.query_audit (
    id            BIGSERIAL PRIMARY KEY,
    query_ts      TIMESTAMPTZ NOT NULL DEFAULT now(),
    principal     TEXT NOT NULL,
    role_name     TEXT NULL,
    action        TEXT NOT NULL,
    datasets      TEXT[] NOT NULL DEFAULT '{}',
    fusion_policy TEXT NULL,
    duration_ms   INTEGER NULL,
    detail        JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX idx_audit_ts ON platform.query_audit (query_ts DESC);
