"""Register a domain's datasets in platform.datasets -- the hand-off from M4 to N1 (M5).

    python -m mosaic_platform.freshness.register --domain <domain>

The list comes from the domain's spec (plus the platform's own shared datasets); this
module only loads it as data. Before anything is written, every dataset is *probed*:
the relation must exist and its declared row-timestamp column must be readable, so a
spec that has drifted from what the pipelines actually build fails here, loudly, instead
of producing a registry that names tables nobody materialises.

All-or-nothing: if one probe fails, nothing is registered. Datasets of this domain that
the spec no longer declares are removed, so platform.datasets mirrors the spec exactly.

The probe also prints each dataset's current watermark (max of its row timestamp). That
is a preview of what the N1 collectors will maintain, not a substitute for them -- it
writes nothing to platform.watermarks.
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

from mosaic_platform.runtime import db
from mosaic_platform.spec import retrieval_spec as specs

DOMAINS_DIR = Path("/opt/mosaic/domains")
PLATFORM_DATASETS = Path(__file__).with_name("datasets.yaml")


def probe(ds: specs.Dataset) -> tuple[int, dt.datetime | None]:
    ts = ds.row_timestamp_column
    select = f"SELECT count(*), {f'max({ts})' if ts else 'NULL'} FROM "
    if ds.engine == "trino":
        _, rows = db.trino(select + ds.relation)
        n, wm = rows[0]
    else:
        with db.pg(ds.database) as conn, conn.cursor() as cur:
            cur.execute(select + ds.table)
            n, wm = cur.fetchone()
    # psycopg hands back the session's zone (+07 here); everything printed is UTC.
    wm = db.parse_ts(wm)
    return int(n), wm.astimezone(dt.timezone.utc) if wm else None


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="register", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--domain", required=True)
    ap.add_argument("--dry-run", action="store_true", help="probe only; write nothing")
    a = ap.parse_args(argv)

    spec = specs.load(DOMAINS_DIR, a.domain)
    datasets = specs.load_platform_datasets(PLATFORM_DATASETS) + list(spec.datasets)

    now = dt.datetime.now(dt.timezone.utc)
    width = max(len(d.id) for d in datasets)
    print(f"probing {len(datasets)} dataset(s) for domain '{a.domain}' "
          f"(+ platform-shared)\n")
    print(f"  {'dataset'.ljust(width)}  {'kind':<7} {'rows':>9}  {'watermark (UTC)':<26} "
          f"{'lag':>10}  sla")
    failed = 0
    for d in datasets:
        try:
            n, wm = probe(d)
        except Exception as e:   # noqa: BLE001 -- report every failure, then stop
            failed += 1
            print(f"  {d.id.ljust(width)}  {d.kind:<7} FAILED: {str(e).splitlines()[0][:110]}")
            continue
        lag = f"{(now - wm).total_seconds():.0f}s" if wm else "-"
        print(f"  {d.id.ljust(width)}  {d.kind:<7} {n:>9}  "
              f"{wm.strftime('%Y-%m-%d %H:%M:%S') if wm else '-':<26} {lag:>10}  "
              f"{d.freshness_sla_seconds or '-'}")

    if failed:
        print(f"\nFAIL -- {failed} dataset(s) could not be probed; nothing registered.")
        return 1
    if a.dry_run:
        print("\ndry run -- nothing registered.")
        return 0

    with db.pg(db.PLATFORM_DB) as conn, conn.cursor() as cur:
        for d in datasets:
            cur.execute(
                "INSERT INTO platform.datasets (dataset_id, engine, relation, kind, "
                "row_timestamp_column, freshness_sla_seconds, domain) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (dataset_id) DO UPDATE SET engine = EXCLUDED.engine, "
                "relation = EXCLUDED.relation, kind = EXCLUDED.kind, "
                "row_timestamp_column = EXCLUDED.row_timestamp_column, "
                "freshness_sla_seconds = EXCLUDED.freshness_sla_seconds, "
                "domain = EXCLUDED.domain",
                (d.id, d.engine, d.relation, d.kind, d.row_timestamp_column,
                 d.freshness_sla_seconds, d.domain))
        declared = [d.id for d in spec.datasets]
        cur.execute("DELETE FROM platform.datasets WHERE domain = %s AND NOT dataset_id = ANY(%s) "
                    "RETURNING dataset_id", (a.domain, declared))
        removed = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT kind, count(*) FROM platform.datasets "
                    "WHERE domain = %s OR domain IS NULL GROUP BY kind ORDER BY kind", (a.domain,))
        summary = ", ".join(f"{k} {c}" for k, c in cur.fetchall())
    print(f"\nregistered {len(datasets)} dataset(s): {summary}"
          + (f"; removed no-longer-declared: {', '.join(removed)}" if removed else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
