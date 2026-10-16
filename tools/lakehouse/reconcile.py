"""Silver against the live sources, row by row: the end-to-end proof for the medallion path.

    python -m tools.lakehouse.reconcile [--domain ecommerce]

Every dbt test can pass while Silver is wrong -- uniqueness and not-null say nothing about
whether the *values* are the source's. This compares them. For each generated Silver table
it full-outer-joins the live source table (read through Trino's federation catalogs) with
the Silver rows that are not soft-deleted, on the key, and compares a fingerprint of every
column the two can share:

  missing   in the source, not in Silver   -> a change never made it through
  extra     in Silver, not in the source   -> a delete was lost, or a key invented
  differ    in both, values disagree       -> ordering, typing or merge is wrong

Exact only at quiescence: stop writing, let Bronze drain, run `python mosaic.py dbt`, then
this. Under load, in-flight rows show up as differences by construction.

What is compared, and how it is normalised so the two sides are comparable at all:

  * masked columns are skipped -- Silver holds the hash, the source the plaintext, and the
    masking is GR1 section 4.6's layer 1, proven separately by `cdc-verify`;
  * timestamps are compared as epoch microseconds; a zone-less source timestamp is read in
    the source time zone first, which is exactly the conversion Silver claims to make, so
    a wrong zone shows up here as every row differing;
  * text is right-trimmed on both sides (CHAR padding is not a value).

The column/key derivation is assemble.py's own, imported rather than re-implemented, so
this checks the Silver that was generated, not a second opinion of what it should be.
"""
from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from mosaic import host_port  # noqa: E402
from tools.trino import query  # noqa: E402

LEDGER = ROOT / "experiments" / "results" / "lakehouse_events.jsonl"
SOURCE_TZ = os.environ.get("MOSAIC_SOURCE_TZ", "Asia/Ho_Chi_Minh")   # == compose's dbt env


def _assemble():
    """assemble.py, pointed at this checkout and the host-published Trino port."""
    os.environ["MOSAIC_DOMAINS_DIR"] = str(ROOT / "domains")
    os.environ["MOSAIC_TRINO_HOST"] = "localhost"
    os.environ["MOSAIC_TRINO_PORT"] = str(host_port("TRINO_PORT"))
    sys.path.insert(0, str(ROOT / "pipelines" / "transform"))
    return importlib.import_module("assemble")


def _source_types(cap) -> dict[str, str]:
    return {n: t for n, t in query(
        f"SELECT column_name, data_type FROM {cap.catalog}.information_schema.columns "
        f"WHERE table_schema = '{cap.schema}' AND table_name = '{cap.relation}'")}


def _fingerprint(exprs: list[str]) -> str:
    return "concat_ws('|', " + ", ".join(
        f"coalesce({e}, '<null>')" for e in exprs) + ")"


def _norm(column: str, source_type: str, side: str) -> str:
    """One column as comparable text. `side` is 'src' or 'sil'."""
    c = f'{side}."{column}"'
    t = source_type.lower()
    if t.startswith("timestamp"):
        if side == "src" and "with time zone" not in t:
            c = f"with_timezone({c}, '{SOURCE_TZ}')"
        return f"cast(cast(round(to_unixtime({c}) * 1000000) as bigint) as varchar)"
    if t.startswith(("varchar", "char")):
        return f"rtrim(cast({c} as varchar))"
    return f"cast({c} as varchar)"


def reconcile(asm, cap) -> dict:
    types = _source_types(cap)
    compared = [c["name"] for c in cap.columns
                if not asm._matches(cap.masked, f"{cap.table}.{c['name']}")]
    skipped = [c["name"] for c in cap.columns if c["name"] not in compared]
    key = cap.key
    on = " AND ".join(f"s.k_{i} = t.k_{i}" for i in range(len(key)))
    src_keys = ", ".join(f'src."{k}" AS k_{i}' for i, k in enumerate(key))
    sil_keys = ", ".join(f'sil."{k}" AS k_{i}' for i, k in enumerate(key))
    fp_src = _fingerprint([_norm(c, types[c], "src") for c in compared])
    fp_sil = _fingerprint([_norm(c, types[c], "sil") for c in compared])
    sql = f"""
        WITH s AS (SELECT {src_keys}, {fp_src} AS fp
                   FROM {cap.catalog}.{cap.schema}.{cap.relation} src),
             t AS (SELECT {sil_keys}, {fp_sil} AS fp
                   FROM iceberg.silver.{cap.relation} sil WHERE NOT sil._is_deleted)
        SELECT count_if(t.k_0 IS NULL), count_if(s.k_0 IS NULL),
               count_if(s.k_0 IS NOT NULL AND t.k_0 IS NOT NULL AND s.fp <> t.fp),
               count(s.k_0), count(t.k_0),
               slice(array_agg(CAST(s.k_0 AS varchar)) FILTER (
                   WHERE s.k_0 IS NOT NULL AND t.k_0 IS NOT NULL AND s.fp <> t.fp), 1, 3)
        FROM s FULL OUTER JOIN t ON {on}"""
    missing, extra, differ, n_src, n_sil, sample = query(sql)[0]
    return {"table": cap.table, "key": key, "compared": compared, "skipped_masked": skipped,
            "source_rows": n_src, "silver_rows": n_sil, "missing": missing,
            "extra": extra, "differ": differ, "differ_sample": sample or []}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--domain", default=None, help="restrict to one domain")
    a = p.parse_args()

    asm = _assemble()
    caps = [c for c in asm.captured_tables() if a.domain in (None, c.domain)]
    results, ok = [], True
    print(f"reconcile: Silver vs the live sources, {len(caps)} table(s)\n")
    print(f"  {'table':<26} {'source':>8} {'silver':>8} {'missing':>8} {'extra':>6} "
          f"{'differ':>7}  compared")
    for cap in caps:
        reason = asm.resolve(cap)
        if reason:
            print(f"  {cap.table:<26} skipped: {reason}")
            continue
        r = reconcile(asm, cap)
        results.append(r)
        exact = r["missing"] == r["extra"] == r["differ"] == 0
        ok &= exact
        note = f"{len(r['compared'])} cols" + (
            f", masked skipped: {','.join(r['skipped_masked'])}" if r["skipped_masked"] else "")
        print(f"  {r['table']:<26} {r['source_rows']:>8} {r['silver_rows']:>8} "
              f"{r['missing']:>8} {r['extra']:>6} {r['differ']:>7}  {note}"
              + ("" if exact else f"   e.g. keys {r['differ_sample']}"))
    print()
    print("  PASS -- Silver equals the live sources, row for row, on every compared column."
          if ok else "  MISMATCH -- exact only at quiescence; if writes were running, rerun idle.")
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    with LEDGER.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"kind": "reconcile", "verdict": "pass" if ok else "mismatch",
                             "tables": results,
                             "recorded_at": datetime.now(timezone.utc).isoformat()}) + "\n")
    print(f"\n  recorded to {LEDGER.relative_to(ROOT).as_posix()}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
