"""Load generator CLI: continuous OLTP writes, and the stock-out lever.

    python -m tools.loadgen.run start --rate 10 --duration 300
    python -m tools.loadgen.run stockout --top 5
    python -m tools.loadgen.run restock --last
    python -m tools.loadgen.run events

`start` manufactures the freshness asymmetry; `stockout` is the switch docs/PROBLEM.md
promises -- "the failure is reproducible on demand". They are separate commands on
purpose: the lever has to be pullable *while* load is running, from another shell,
because the interesting measurement is what a stale channel keeps returning in the
seconds after a stock-out that the live source already knows about.

Every stock-out -- pulled or organic -- is appended to
experiments/results/stockout_events.jsonl, with both the source commit time and the
host observation time. E1/E3 read that ledger to know when the clock started.

Verification discipline: the run counts the statements it sent, then re-reads row
counts from both engines and reports the delta it actually caused. A load generator
that reports its intentions rather than its effects is worse than none.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tools.dbsession import MySqlSession, PostgresSession, SessionDied  # noqa: E402
from tools.loadgen.workload import Workload  # noqa: E402

LEDGER = ROOT / "experiments" / "results" / "stockout_events.jsonl"
PROGRESS_EVERY = 5.0     # seconds between progress lines and organic-stockout sweeps


# ------------------------------------------------------------------- ledger
def _append(event: dict) -> None:
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    event.setdefault("recorded_at", datetime.now(timezone.utc).isoformat())
    with LEDGER.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event) + "\n")


def _read_ledger() -> list[dict]:
    if not LEDGER.exists():
        return []
    out = []
    for line in LEDGER.read_text(encoding="utf-8").splitlines():
        if line.strip():
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


# ---------------------------------------------------------------- selection
def _top_selling(my: MySqlSession, n: int) -> list[int]:
    """The products the affinity ranking would put at the top.

    Stocking out a random product proves little: nothing was going to rank it
    anyway. The demonstration only bites when the item that disappears is one the
    stale channel is confident about, so the default selection is by realised
    demand.
    """
    rows = my.query_rows(
        "SELECT product_id, SUM(quantity) AS units FROM order_items "
        f"GROUP BY product_id ORDER BY units DESC LIMIT {n}"
    )
    return [int(r[0]) for r in rows if r and r[0]]


def _describe(pg: PostgresSession, ids: list[int]) -> dict[int, tuple[str, str, int]]:
    if not ids:
        return {}
    in_list = ",".join(str(i) for i in ids)
    rows = pg.query_rows(
        "SELECT p.id, p.sku, p.name, COALESCE(i.stock_qty,0) FROM products p "
        f"LEFT JOIN inventory i ON i.product_id = p.id WHERE p.id IN ({in_list})"
    )
    return {int(r[0]): (r[1], r[2], int(r[3])) for r in rows if len(r) >= 4}


# ------------------------------------------------------------------ command: start
def cmd_start(a: argparse.Namespace) -> int:
    rng = random.Random(a.seed) if a.seed is not None else random.Random()
    my, pg = MySqlSession().open(), PostgresSession().open()
    wl = Workload(my, pg, rng)

    try:
        wl.prime()
    except Exception as e:
        my.close(), pg.close()
        print(f"cannot start: {e}")
        return 1

    before = _row_counts(my, pg)
    watch_from = pg.scalar("SELECT now()")
    print(f"load generator: rate={a.rate}/s  duration={a.duration}s  "
          f"seed={a.seed if a.seed is not None else 'random'}")
    print(f"  catalogue: {wl.max_product:,} products, {wl.max_user:,} users, "
          f"{len(wl.open_orders)} advanceable orders")
    print("  ctrl-c stops cleanly and still reports.\n")

    t0 = time.perf_counter()
    deadline = t0 + a.duration if a.duration else None
    interval = 1.0 / a.rate if a.rate > 0 else 0.0
    next_op = t0
    next_report = t0 + PROGRESS_EVERY
    n = 0
    last_lag_at = 0          # ops count when the commit lag was last measured
    lag = 0
    stopped = ""

    try:
        while deadline is None or time.perf_counter() < deadline:
            now = time.perf_counter()
            if now < next_op:
                time.sleep(min(next_op - now, 0.25))
                continue
            wl.step()
            n += 1
            next_op += interval
            # If we have fallen behind the schedule by more than a second, stop
            # pretending we are keeping up -- reset the clock and report the real
            # achieved rate at the end rather than accumulating phantom debt.
            if next_op < time.perf_counter() - 1.0:
                next_op = time.perf_counter()

            # Backpressure. Statements sit in a pipe, so without this the send rate
            # is bounded only by Python and the reported throughput becomes a
            # fiction -- one run "achieved" 549/s while MySQL was still committing
            # it 152 seconds later. The lag probe costs a round trip, so it runs
            # only every --max-lag operations: that bounds the overshoot to roughly
            # one max-lag window while keeping the probe off the hot path.
            due_by_ops = n - last_lag_at >= a.max_lag
            due_by_time = time.perf_counter() >= next_report
            if due_by_ops or due_by_time:
                lag = _commit_lag(my, before["orders"], wl.counters.orders)
                last_lag_at = n
                if lag > a.max_lag:
                    print(f"      throttling: {lag:,} orders uncommitted "
                          f"(--max-lag {a.max_lag:,})")
                    while lag > a.max_lag // 2 and (deadline is None
                                                    or time.perf_counter() < deadline):
                        time.sleep(0.5)
                        lag = _commit_lag(my, before["orders"], wl.counters.orders)
                    next_op = time.perf_counter()

            if due_by_time:
                elapsed = time.perf_counter() - t0
                print(f"  t+{elapsed:6.0f}s  ops={n:<7,} sent={n / elapsed:5.1f}/s  "
                      f"orders={wl.counters.orders:,} items={wl.counters.order_items:,} "
                      f"inv={wl.counters.inventory_updates:,} "
                      f"status={wl.counters.status_updates:,}  lag={lag:,}")
                watch_from = _sweep_organic(pg, watch_from)
                next_report = time.perf_counter() + PROGRESS_EVERY
    except KeyboardInterrupt:
        stopped = "interrupted"
    except SessionDied as e:
        print(f"\nFAILED: {e}")
        stopped = "session died"

    elapsed = max(time.perf_counter() - t0, 1e-6)
    # Closing waits for the clients to finish executing whatever is still in the
    # pipe. At high send rates that backlog is real work and can take a while;
    # cutting it short is how a run loses writes it believes it made.
    drain0 = time.perf_counter()
    my_out, pg_out = my.close(), pg.close()
    drain = time.perf_counter() - drain0
    if drain > 2.0:
        print(f"\n  drained client backlog in {drain:.1f}s after the send window closed")

    _sweep_organic(pg, watch_from)
    after = _row_counts(my, pg)

    # Two rates, because they differ and only one of them is honest. `sent` is how
    # fast statements left this process; `sustained` includes the time the engines
    # needed to finish committing them, and is the number to quote.
    print(f"\nran {elapsed:.1f}s, {n:,} operations, {n / elapsed:.1f}/s sent, "
          f"{n / (elapsed + drain):.1f}/s sustained"
          f"{' (' + stopped + ')' if stopped else ''}")
    print("  mix: " + ", ".join(f"{k}={v:,}" for k, v in sorted(wl.counters.ops.items())))

    # Effects, read back from the engines rather than trusted from the counters.
    print("\nrow-count delta observed at the source:")
    bad = 0
    expected = {"orders": wl.counters.orders, "order_items": wl.counters.order_items,
                "payments": wl.counters.payments, "users": wl.counters.users}
    for table, want in expected.items():
        got = after[table] - before[table]
        ok = got == want
        bad += not ok
        print(f"  {table:<12} +{got:<8,} sent {want:<8,} {'ok' if ok else 'MISMATCH'}")
    if bad or stopped == "session died":
        print(f"\nclient output (mysql):\n{my_out[-1200:] or '  (none)'}")
        print(f"client output (postgres):\n{pg_out[-1200:] or '  (none)'}")
    if bad:
        print(f"\n{bad} table(s) did not receive what was sent -- do not trust this run.")
        return 1
    return 0 if stopped != "session died" else 1


def _commit_lag(my: MySqlSession, baseline: int, sent_orders: int) -> int:
    """Orders sent minus orders the engine has actually committed.

    Read on a separate connection, so it sees committed state rather than what the
    writer session believes it has queued.
    """
    committed = int(my.scalar("SELECT count(*) FROM orders")) - baseline
    return max(sent_orders - committed, 0)


def _row_counts(my: MySqlSession, pg: PostgresSession) -> dict[str, int]:
    return {
        "orders": int(my.scalar("SELECT count(*) FROM orders")),
        "order_items": int(my.scalar("SELECT count(*) FROM order_items")),
        "payments": int(my.scalar("SELECT count(*) FROM payments")),
        "users": int(pg.scalar("SELECT count(*) FROM users")),
    }


_organic_seen: set[int] = set()


def _sweep_organic(pg: PostgresSession, since: str) -> str:
    """Record products that sold out on their own since the last sweep.

    Organic stock-outs matter as much as pulled ones: they are the case a real
    deployment hits without anybody pulling a lever, and they arrive at times
    nobody chose, which is what makes them honest test cases.

    Deduplicated within a run: a further order against an already-empty product
    still moves updated_at (the decrement floors at zero), and that is not a new
    stock-out event.
    """
    now = pg.scalar("SELECT now()")
    rows = pg.query_rows(
        "SELECT product_id, updated_at FROM inventory "
        f"WHERE stock_qty = 0 AND updated_at > '{since}' AND updated_at <= '{now}'"
    )
    fresh = 0
    for r in rows:
        if len(r) >= 2 and int(r[0]) not in _organic_seen:
            _organic_seen.add(int(r[0]))
            _append({"mode": "organic", "product_id": int(r[0]),
                     "source_updated_at": r[1], "previous_qty": None})
            fresh += 1
    if fresh:
        print(f"  [ledger] {fresh} product(s) sold out organically")
    return now


# --------------------------------------------------------------- command: stockout
def cmd_stockout(a: argparse.Namespace) -> int:
    """Force stock to zero. The L0 switch.

    Only `inventory` is touched. products.name, description and category are left
    exactly as they were, which is the whole point: the semantic channel has no way
    to notice, so it keeps ranking the item, and the gap between the two channels'
    watermarks becomes measurable rather than rhetorical.
    """
    my, pg = MySqlSession().open(), PostgresSession().open()
    try:
        if a.product_id:
            ids = list(a.product_id)
        elif a.random:
            ids = [int(r[0]) for r in pg.query_rows(
                "SELECT product_id FROM inventory WHERE stock_qty > 0 "
                f"ORDER BY random() LIMIT {a.random}")]
        else:
            ids = _top_selling(my, a.top)
        if not ids:
            print("nothing selected.")
            return 1

        info = _describe(pg, ids)
        print(f"stocking out {len(ids)} product(s):")
        for pid in ids:
            sku, name, qty = info.get(pid, ("?", "?", 0))
            row = pg.query_rows(
                f"UPDATE inventory SET stock_qty = 0, updated_at = now() "
                f"WHERE product_id = {pid} RETURNING product_id, updated_at"
            )
            if not row:
                print(f"  product {pid}: no inventory row -- skipped")
                continue
            _append({"mode": "manual", "product_id": pid, "sku": sku, "name": name,
                     "previous_qty": qty, "source_updated_at": row[0][1],
                     "selection": "explicit" if a.product_id else
                                  ("random" if a.random else "top-selling")})
            print(f"  {pid:>7}  {sku:<14} stock {qty:>5} -> 0   {name[:44]}")
        print(f"\nledger: {LEDGER.relative_to(ROOT).as_posix()}")
        print("the live source now contradicts every ranking that still returns these.")
        return 0
    finally:
        my.close(), pg.close()


# ---------------------------------------------------------------- command: restock
def cmd_restock(a: argparse.Namespace) -> int:
    """Undo a stock-out so an experiment can be re-run from a known state."""
    pg = PostgresSession().open()
    try:
        if a.product_id:
            ids = list(a.product_id)
        elif a.last:
            ids = [e["product_id"] for e in _read_ledger() if e.get("mode") == "manual"]
            ids = ids[-a.last:]
        else:
            print("specify --product-id or --last N")
            return 1
        for pid in dict.fromkeys(ids):
            pg.execute(f"UPDATE inventory SET stock_qty = {a.qty}, updated_at = now() "
                       f"WHERE product_id = {pid}")
            _append({"mode": "restock", "product_id": pid, "stock_qty": a.qty})
            print(f"  {pid:>7}  stock -> {a.qty}")
        return 0
    finally:
        pg.close()


# ------------------------------------------------------------------ command: events
def cmd_events(a: argparse.Namespace) -> int:
    events = _read_ledger()
    if not events:
        print(f"no events yet ({LEDGER.relative_to(ROOT).as_posix()})")
        return 0
    for e in events[-a.tail:]:
        print(f"  {e.get('recorded_at', '?')[:19]}  {e.get('mode', '?'):<8} "
              f"product={e.get('product_id')}  {e.get('name', '')[:40]}")
    print(f"\n{len(events)} event(s) total")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(prog="loadgen", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    st = sub.add_parser("start", help="continuous OLTP write load")
    st.add_argument("--rate", type=float, default=10.0, help="operations per second")
    st.add_argument("--duration", type=int, default=0, help="seconds; 0 = until ctrl-c")
    st.add_argument("--seed", type=int, default=None, help="pin the RNG for a repeatable mix")
    st.add_argument("--max-lag", type=int, default=2000,
                    help="pause sending when this many orders are still uncommitted, so the "
                         "reported rate is one the source really sustained. Each probe is a "
                         "round trip, so a small window costs throughput: 500 measured 48/s "
                         "sustained where 2000 measured 98/s on the same machine.")
    st.set_defaults(func=cmd_start)

    so = sub.add_parser("stockout", help="force products out of stock (the L0 switch)")
    so.add_argument("--product-id", type=int, action="append", help="repeatable")
    so.add_argument("--top", type=int, default=1, help="N best-selling products")
    so.add_argument("--random", type=int, default=0, help="N random in-stock products")
    so.set_defaults(func=cmd_stockout)

    rs = sub.add_parser("restock", help="undo a stock-out")
    rs.add_argument("--product-id", type=int, action="append")
    rs.add_argument("--last", type=int, default=0, help="the last N manual stock-outs")
    rs.add_argument("--qty", type=int, default=100)
    rs.set_defaults(func=cmd_restock)

    ev = sub.add_parser("events", help="show the stock-out ledger")
    ev.add_argument("--tail", type=int, default=20)
    ev.set_defaults(func=cmd_events)

    a = p.parse_args()
    return a.func(a)


if __name__ == "__main__":
    raise SystemExit(main())
