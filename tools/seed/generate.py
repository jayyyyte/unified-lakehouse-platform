"""Generate the e-commerce corpus as CSV files ready for bulk load.

Two-step by design: generate to CSV, then bulk-load (tools/seed/load.py). At the
target scale of GR1 section 10.1 -- 10M+ rows so that the Top-N pushdown experiment
(E2) is meaningful -- row-by-row INSERTs are hours of work, while `LOAD DATA INFILE`
and `COPY` are minutes. Separating the steps also means a corpus can be regenerated
deterministically without touching a database, which experiments E3/E4 rely on.

PII columns are populated with realistic-looking values on purpose. Masking cannot
be *demonstrated* against empty columns: the point of GR1 Table 4.5 is that a real
value exists at the source and is provably absent downstream.

    python -m tools.seed.generate --scale small --out .seed/
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from tools.seed import vocab  # noqa: E402

# products, users, orders. order_items and payments derive from orders.
SCALES = {
    "small":  {"products": 1_000,   "users": 5_000,   "orders": 20_000},
    "medium": {"products": 20_000,  "users": 50_000,  "orders": 500_000},
    "full":   {"products": 100_000, "users": 200_000, "orders": 2_000_000},
}

ORDER_STATUS = ["pending", "paid", "shipped", "delivered", "cancelled"]
ORDER_STATUS_W = [5, 15, 20, 55, 5]
PAY_METHOD = ["card", "ewallet", "cod", "bank_transfer"]
PAY_METHOD_W = [40, 30, 25, 5]
PAY_STATUS = ["authorized", "captured", "failed", "refunded"]
PAY_STATUS_W = [10, 80, 6, 4]

HISTORY_DAYS = 365


def _ts(rng, now: datetime, days_back_max: int = HISTORY_DAYS) -> datetime:
    return now - timedelta(seconds=rng.randint(0, days_back_max * 86_400))


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _writer(out: Path, name: str, header: list[str]):
    fh = (out / f"{name}.csv").open("w", newline="", encoding="utf-8")
    w = csv.writer(fh, quoting=csv.QUOTE_MINIMAL)
    w.writerow(header)
    return fh, w


def generate(scale: str, out: Path, seed: int = 20260824,
             now: datetime | None = None) -> dict[str, int]:
    """Write the corpus as CSV. Deterministic given (scale, seed, now).

    `now` is an explicit parameter rather than a module-level clock read: with a
    wall-clock anchor the same seed produces different timestamps on every run, so
    the corpus would not be reproducible and E3/E4 could not be re-run against an
    identical dataset. The anchor actually used is recorded in manifest.json.
    """
    cfg = SCALES[scale]
    rng = random.Random(seed)
    now = (now or datetime.now(timezone.utc)).replace(microsecond=0)
    out.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}

    # ---------------------------------------------------------- categories
    # Two levels: one parent per vocabulary category, then subcategories under it.
    cat_names = list(vocab.CATEGORIES)
    fh, w = _writer(out, "categories", ["id", "name", "parent_id", "updated_at"])
    cat_id = 0
    leaf_by_category: dict[str, list[tuple[int, str]]] = {}
    for name in cat_names:
        cat_id += 1
        parent = cat_id
        w.writerow([parent, name, "", _iso(_ts(rng, now))])
        leaves = []
        for t in vocab.CATEGORIES[name]["types"][:5]:
            cat_id += 1
            w.writerow([cat_id, t.title(), parent, _iso(_ts(rng, now))])
            leaves.append((cat_id, t))
        leaf_by_category[name] = leaves
    fh.close()
    counts["categories"] = cat_id

    # --------------------------------------------------------------- users
    fh, w = _writer(out, "users", ["id", "full_name", "email", "phone",
                                   "national_id", "region", "created_at", "updated_at"])
    for uid in range(1, cfg["users"] + 1):
        name = vocab.compose_person(rng)
        slug = name.lower().replace(" ", ".")
        created = _ts(rng, now)
        w.writerow([
            uid, name,
            f"{slug}{rng.randint(1, 9999)}@example.com",      # PII -> hashed by Debezium
            f"+849{rng.randint(10_000_000, 99_999_999)}",     # PII -> hashed by Debezium
            f"{rng.randint(100_000_000_000, 999_999_999_999)}",  # PII -> excluded outright
            rng.choice(vocab.REGIONS),
            _iso(created), _iso(created + timedelta(seconds=rng.randint(0, 86_400))),
        ])
    fh.close()
    counts["users"] = cfg["users"]

    # ------------------------------------------------------------ products
    fh, w = _writer(out, "products", ["id", "sku", "name", "description",
                                      "category_id", "price", "updated_at"])
    fh_inv, w_inv = _writer(out, "inventory",
                            ["product_id", "stock_qty", "warehouse", "updated_at"])
    product_prices: list[float] = [0.0]  # 1-indexed
    for pid in range(1, cfg["products"] + 1):
        category = rng.choice(cat_names)
        leaf_id, ptype = rng.choice(leaf_by_category[category])
        name, description = vocab.compose_product(rng, category, ptype)
        price = round(rng.uniform(50_000, 8_000_000), 2)
        product_prices.append(price)
        w.writerow([
            pid, f"SKU-{pid:08d}", name, description, leaf_id,
            f"{price:.2f}", _iso(_ts(rng, now)),
        ])
        # A deliberate slice starts at zero stock, so the stale-hit failure is
        # observable immediately after seeding without waiting for the load generator.
        stock = 0 if rng.random() < 0.03 else rng.randint(1, 500)
        w_inv.writerow([pid, stock, rng.choice(vocab.WAREHOUSES), _iso(_ts(rng, now, 30))])
    fh.close()
    fh_inv.close()
    counts["products"] = cfg["products"]
    counts["inventory"] = cfg["products"]

    # -------------------------------------------- orders / items / payments
    fh_o, w_o = _writer(out, "orders",
                        ["order_id", "user_id", "status", "total_amount", "currency",
                         "region", "buyer_email", "buyer_phone", "created_at", "updated_at"])
    fh_i, w_i = _writer(out, "order_items",
                        ["order_item_id", "order_id", "product_id", "quantity",
                         "unit_price", "created_at"])
    fh_p, w_p = _writer(out, "payments",
                        ["payment_id", "order_id", "method", "amount",
                         "raw_card_number", "status", "created_at"])

    item_id = 0
    pay_id = 0
    n_products = cfg["products"]
    for oid in range(1, cfg["orders"] + 1):
        uid = rng.randint(1, cfg["users"])
        created = _ts(rng, now)
        status = rng.choices(ORDER_STATUS, weights=ORDER_STATUS_W, k=1)[0]

        # Zipf-ish product popularity: a small head of products carries most of the
        # order volume, which is what makes an affinity ranking meaningful at all.
        n_items = rng.choices([1, 2, 3, 4, 5], weights=[45, 27, 15, 8, 5], k=1)[0]
        total = 0.0
        for _ in range(n_items):
            if rng.random() < 0.7:
                product_id = rng.randint(1, max(1, n_products // 10))   # head
            else:
                product_id = rng.randint(1, n_products)                 # tail
            qty = rng.choices([1, 2, 3], weights=[70, 22, 8], k=1)[0]
            unit = product_prices[product_id]
            total += unit * qty
            item_id += 1
            w_i.writerow([item_id, oid, product_id, qty, f"{unit:.2f}", _iso(created)])

        w_o.writerow([
            oid, uid, status, f"{total:.2f}", "VND",
            rng.choice(vocab.REGIONS),
            f"buyer{uid}@example.com",                     # PII -> hashed by Debezium
            f"+849{rng.randint(10_000_000, 99_999_999)}",  # PII -> hashed by Debezium
            _iso(created), _iso(created + timedelta(seconds=rng.randint(0, 172_800))),
        ])

        if status != "pending":
            pay_id += 1
            w_p.writerow([
                pay_id, oid,
                rng.choices(PAY_METHOD, weights=PAY_METHOD_W, k=1)[0],
                f"{total:.2f}",
                f"4{rng.randint(10**14, 10**15 - 1)}",     # PII -> excluded outright
                rng.choices(PAY_STATUS, weights=PAY_STATUS_W, k=1)[0],
                _iso(created + timedelta(seconds=rng.randint(1, 3_600))),
            ])

    fh_o.close()
    fh_i.close()
    fh_p.close()
    counts["orders"] = cfg["orders"]
    counts["order_items"] = item_id
    counts["payments"] = pay_id
    return counts


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--scale", choices=list(SCALES), default="small")
    p.add_argument("--out", type=Path, default=Path(".seed"))
    p.add_argument("--seed", type=int, default=20260824,
                   help="RNG seed")
    p.add_argument("--now", default=None, metavar="ISO8601",
                   help="clock anchor for generated timestamps (default: current UTC). "
                        "Together with --seed this makes the corpus byte-identical "
                        "across runs; both are recorded in manifest.json.")
    a = p.parse_args()

    now = (datetime.fromisoformat(a.now) if a.now
           else datetime.now(timezone.utc)).replace(microsecond=0)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    print(f"generating '{a.scale}' corpus into {a.out}/ (seed={a.seed}, now={now.isoformat()})")
    counts = generate(a.scale, a.out, a.seed, now)

    # Reproducibility record. E3/E4 re-run against an identical dataset by reading
    # seed and now back out of here rather than hoping the defaults were unchanged.
    (a.out / "manifest.json").write_text(json.dumps({
        "scale": a.scale,
        "seed": a.seed,
        "now": now.isoformat(),
        "history_days": HISTORY_DAYS,
        "counts": counts,
    }, indent=2), encoding="utf-8")

    total = sum(counts.values())
    width = max(len(k) for k in counts)
    for k in ["categories", "users", "products", "inventory",
              "orders", "order_items", "payments"]:
        print(f"  {k.ljust(width)}  {counts[k]:>12,}")
    print(f"  {'TOTAL'.ljust(width)}  {total:>12,} rows")
    if total < 10_000_000:
        print(f"\n  note: GR1 section 10.1 targets 10M+ rows for a meaningful E2 "
              f"benchmark.\n        --scale full reaches it; this run is for iteration speed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
