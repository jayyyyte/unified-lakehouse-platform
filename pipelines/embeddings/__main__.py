"""The slow channel's index pipeline (M4). Runs in the `jobs` image:

    python -m embeddings refresh --domain <d> [--index <id>] [--guard-only]
    python -m embeddings status  --domain <d> [--index <id>]
    python -m embeddings query   --domain <d> --text "..." [--k 10] [--exact] [--json]
    python -m embeddings recall  --domain <d> [--queries 100] [--k 10] [--ef 40 --ef 100]

Normally reached through `python mosaic.py embed ...`. One-shot by design: there is no
scheduler. The refresh cadence *is* the slow channel's staleness, so it is an
experimental variable that E4 drives, not a cron line (ADR 0006, M4-C1).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from mosaic_platform.spec import retrieval_spec as specs

DOMAINS_DIR = Path("/opt/mosaic/domains")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="embeddings", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p):
        p.add_argument("--domain", required=True)
        p.add_argument("--index", default=None, help="index id; optional if the domain has one")
        return p

    r = common(sub.add_parser("refresh", help="full rebuild, swapped in atomically"))
    r.add_argument("--guard-only", action="store_true",
                   help="run the N3 input guard and stop (exit 2 if it refuses)")
    common(sub.add_parser("status", help="rows, refreshed_at, age against the SLA"))
    q = common(sub.add_parser("query", help="top-k by cosine similarity"))
    q.add_argument("--text", required=True)
    q.add_argument("--k", type=int, default=10)
    q.add_argument("--ef-search", type=int, default=40)
    q.add_argument("--exact", action="store_true", help="sequential scan instead of HNSW")
    q.add_argument("--json", action="store_true", help="also emit one machine-readable line")
    rc = common(sub.add_parser("recall", help="HNSW vs exact recall@k and latency"))
    rc.add_argument("--queries", type=int, default=100)
    rc.add_argument("--k", type=int, default=10)
    rc.add_argument("--ef", type=int, action="append", help="ef_search value; repeatable")
    rc.add_argument("--seed", default="m4")
    a = ap.parse_args(argv)

    spec = specs.load(DOMAINS_DIR, a.domain)
    ix = spec.index(a.index)

    if a.cmd == "refresh":
        from .index import refresh
        return refresh(spec, ix, guard_only=a.guard_only)
    if a.cmd == "status":
        from .index import status
        return status(spec, ix)
    if a.cmd == "query":
        from .ann import query
        return query(spec, ix, a.text, a.k, a.ef_search, a.exact, a.json)
    from .ann import recall
    return recall(spec, ix, a.queries, a.k, a.ef or [40], a.seed)


if __name__ == "__main__":
    sys.exit(main())
