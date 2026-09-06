"""The OLTP write mix, and the events it is designed to manufacture.

This is not a benchmark harness. Its job is to produce, continuously, the exact
condition docs/PROBLEM.md (L0) is about: *facts that move faster than the slowest
channel in the fusion*. Every operation below exists because some downstream
measurement needs it.

    new_order        MySQL INSERT x3 in one transaction, plus a PostgreSQL
                     inventory decrement. Advances the transactional watermark and
                     is what eventually drives stock to zero on its own.
    advance_status   MySQL UPDATE. The only op that produces a *before* image, so
                     it is what proves ADR 0004's masking covers the before image
                     and not just `after` (an SMT chain masks only `after`).
    restock          PostgreSQL UPDATE. Keeps the catalogue from draining to a
                     uniformly out-of-stock state, which would make the stale-hit
                     metric degenerate.
    price_change     PostgreSQL UPDATE of a fact the embedding does not carry --
                     a second, non-stock instance of the same staleness failure.
    new_user         PostgreSQL INSERT with populated PII, so masking is exercised
                     on live traffic and not only on the seeded snapshot.
    user_update      PostgreSQL UPDATE of a PII column. The PostgreSQL counterpart
                     of advance_status: it is the only op that produces a before
                     image containing PII on that source, and therefore the only
                     way to show ADR 0004's masking reaches it. Requires REPLICA
                     IDENTITY FULL -- see domains/ecommerce/ddl/postgres.sql.

Note what is deliberately absent: nothing here touches products.name or
products.description. The semantic channel's inputs are held still on purpose. If
the load generator edited descriptions, the embeddings would be stale for a second
reason and the experiment could no longer attribute a stale hit to the freshness
asymmetry alone.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field

from tools.dbsession import MySqlSession, PostgresSession

# Weights of the write mix. Order-heavy on purpose: transactional volume is what
# moves the CDC watermark, and a realistic catalogue changes far more slowly than
# it sells.
MIX = [
    ("new_order",      57),
    ("advance_status", 20),
    ("price_change",    8),
    ("restock",         7),
    ("new_user",        5),
    ("user_update",     3),
]

STATUS_NEXT = {
    "pending": "paid",
    "paid": "shipped",
    "shipped": "delivered",
}
PAY_METHOD = ["card", "ewallet", "cod", "bank_transfer"]
PAY_STATUS = ["authorized", "captured", "captured", "captured", "failed"]
REGIONS = ["north", "central", "south", "highlands", "delta"]
RESTOCK_THRESHOLD = 25    # a replenishment is triggered by a low level, not by popularity
RESTOCK_LEAD_SECONDS = 300  # ...and not instantly: see op_restock
FIRST = ["An", "Binh", "Chi", "Dung", "Giang", "Ha", "Hieu", "Khanh", "Lan",
         "Minh", "Nam", "Oanh", "Phuong", "Quang", "Thao", "Tuan", "Vy"]
LAST = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Vu", "Dang", "Bui", "Do", "Ho"]


def _q(s: str) -> str:
    """Single-quote a literal for SQL. Values here are generated, never user input."""
    return "'" + s.replace("\\", "\\\\").replace("'", "''") + "'"


@dataclass
class Counters:
    ops: dict[str, int] = field(default_factory=dict)
    orders: int = 0
    order_items: int = 0
    payments: int = 0
    users: int = 0
    inventory_updates: int = 0
    price_updates: int = 0
    status_updates: int = 0
    user_updates: int = 0

    def bump(self, op: str) -> None:
        self.ops[op] = self.ops.get(op, 0) + 1


class Workload:
    """Stateless-ish generator of write bursts against both sources.

    Holds only the id ranges it needs to pick plausible foreign keys. Those are
    read once at start-up and grown locally, so the hot path never reads back from
    a database.
    """

    def __init__(self, mysql: MySqlSession, postgres: PostgresSession,
                 rng: random.Random) -> None:
        self.my = mysql
        self.pg = postgres
        self.rng = rng
        self.counters = Counters()
        self.max_product = 0
        self.max_user = 0
        self.prices: dict[int, float] = {}
        self.open_orders: list[tuple[int, str]] = []

    # ------------------------------------------------------------- start-up
    def prime(self) -> None:
        """Read id ranges, the price map, and a pool of advanceable orders."""
        self.max_user = int(self.pg.scalar("SELECT COALESCE(max(id),0) FROM users"))
        # The price map is read once and then maintained locally, because the price
        # an order records has to be the price the catalogue held at commit time --
        # and the catalogue is in PostgreSQL while the order is in MySQL, so the
        # INSERT cannot read it. op_price_change writes an explicit new price and
        # updates this map in the same step, which keeps the two in step exactly.
        self.prices = {int(pid): float(price)
                       for pid, price in self.pg.query_rows("SELECT id, price FROM products")}
        self.max_product = max(self.prices) if self.prices else 0
        if not (self.max_product and self.max_user):
            raise RuntimeError(
                "products/users are empty -- seed first:\n"
                "  python mosaic.py generate && python -m tools.seed.load --dir .seed"
            )
        self._refill_open_orders()

    def _refill_open_orders(self) -> None:
        rows = self.my.query_rows(
            "SELECT order_id, status FROM orders "
            "WHERE status IN ('pending','paid','shipped') "
            "ORDER BY order_id DESC LIMIT 500"
        )
        self.open_orders = [(int(r[0]), r[1]) for r in rows if len(r) >= 2]
        self.rng.shuffle(self.open_orders)

    # --------------------------------------------------------------- picking
    def _product(self) -> int:
        """Zipf-ish: a head of the catalogue carries most of the volume.

        Matches tools/seed/generate.py, so live traffic reinforces the same
        popularity distribution the seeded history already has -- an affinity
        ranking computed over both is then coherent.
        """
        head = max(1, self.max_product // 10)
        return (self.rng.randint(1, head) if self.rng.random() < 0.7
                else self.rng.randint(1, self.max_product))

    # ------------------------------------------------------------ operations
    def step(self) -> str:
        op = self.rng.choices([n for n, _ in MIX], weights=[w for _, w in MIX], k=1)[0]
        getattr(self, f"op_{op}")()
        self.counters.bump(op)
        return op

    def op_new_order(self) -> None:
        uid = self.rng.randint(1, self.max_user)
        region = self.rng.choice(REGIONS)
        n_items = self.rng.choices([1, 2, 3, 4], weights=[50, 28, 15, 7], k=1)[0]
        picks = [(self._product(), self.rng.choices([1, 2, 3], weights=[70, 22, 8], k=1)[0])
                 for _ in range(n_items)]

        item_rows = ",".join(
            f"(@oid,{pid},{qty},{self.prices.get(pid, 100_000.0):.2f})"
            for pid, qty in picks
        )
        total = sum(self.prices.get(pid, 100_000.0) * qty for pid, qty in picks)
        status = self.rng.choices(["pending", "paid"], weights=[35, 65], k=1)[0]

        sql = (
            "START TRANSACTION;"
            f"INSERT INTO orders (user_id,status,total_amount,currency,region,"
            f"buyer_email,buyer_phone) VALUES ({uid},{_q(status)},{total:.2f},'VND',"
            f"{_q(region)},{_q(f'buyer{uid}@example.com')},"
            f"{_q('+849' + str(self.rng.randint(10_000_000, 99_999_999)))});"
            "SET @oid = LAST_INSERT_ID();"
            f"INSERT INTO order_items (order_id,product_id,quantity,unit_price) "
            f"VALUES {item_rows};"
        )
        if status != "pending":
            sql += (f"INSERT INTO payments (order_id,method,amount,raw_card_number,status) "
                    f"VALUES (@oid,{_q(self.rng.choice(PAY_METHOD))},{total:.2f},"
                    f"{_q('4' + str(self.rng.randint(10**14, 10**15 - 1)))},"
                    f"{_q(self.rng.choice(PAY_STATUS))});")
            self.counters.payments += 1
        sql += "COMMIT;"
        self.my.execute(sql)
        self.counters.orders += 1
        self.counters.order_items += n_items

        # The stock side of the same purchase. Split across engines because the
        # sources really are split -- this is the cross-source consistency window
        # the platform has to reason about, not an accident of the generator.
        for pid, qty in picks:
            self.pg.execute(
                f"UPDATE inventory SET stock_qty = GREATEST(stock_qty - {qty}, 0), "
                f"updated_at = now() WHERE product_id = {pid}"
            )
            self.counters.inventory_updates += 1

    def op_advance_status(self) -> None:
        """The UPDATE path. Produces the before image ADR 0004 turns on."""
        if not self.open_orders:
            self._refill_open_orders()
            if not self.open_orders:
                return self.op_new_order()
        order_id, status = self.open_orders.pop()
        nxt = STATUS_NEXT.get(status)
        if nxt is None:
            return
        self.my.execute(
            f"UPDATE orders SET status = {_q(nxt)} WHERE order_id = {order_id}"
        )
        self.counters.status_updates += 1
        if nxt in STATUS_NEXT:
            self.open_orders.insert(0, (order_id, nxt))

    def op_restock(self) -> None:
        """Replenish something that is actually low.

        An earlier version topped up a *popular* product regardless of its level.
        Because demand removes 1-3 units and a replenishment adds 20-200, head
        stock inflated without bound (2,664 units on a 500-unit seed) and nothing
        could ever sell out organically -- the one behaviour the whole corpus
        exists to produce. Real replenishment is triggered by a low level, so this
        one is too, and stock stays in a range where demand can empty it.

        The lead time matters as much as the threshold. Replenishing the instant a
        product hits zero would erase the very window this project measures -- and
        it erased the seed corpus's deliberately empty slice within a minute of the
        first run. A real order takes time to arrive, so a product that sells out
        stays out for at least RESTOCK_LEAD_SECONDS. That interval is the window in
        which a stale ranking is confidently wrong.
        """
        qty = self.rng.randint(20, 120)
        self.pg.execute(
            f"UPDATE inventory SET stock_qty = stock_qty + {qty}, updated_at = now() "
            f"WHERE product_id = (SELECT product_id FROM inventory "
            f"WHERE stock_qty < {RESTOCK_THRESHOLD} "
            f"AND updated_at < now() - interval '{RESTOCK_LEAD_SECONDS} seconds' "
            f"ORDER BY random() LIMIT 1)"
        )
        self.counters.inventory_updates += 1

    def op_price_change(self) -> None:
        # +/- 15%, and updated_at moves. products.updated_at is what an incremental
        # embedding refresh would key off, so this is also the op that makes the
        # embedding pipeline's lag observable later (M4).
        pid = self._product()
        new_price = round(self.prices.get(pid, 100_000.0) * (1 + self.rng.uniform(-0.15, 0.15)), 2)
        self.pg.execute(
            f"UPDATE products SET price = {new_price:.2f}, updated_at = now() WHERE id = {pid}"
        )
        self.prices[pid] = new_price
        self.counters.price_updates += 1

    def op_new_user(self) -> None:
        name = f"{self.rng.choice(LAST)} {self.rng.choice(FIRST)}"
        slug = name.lower().replace(" ", ".")
        self.pg.execute(
            "INSERT INTO users (full_name,email,phone,national_id,region) VALUES ("
            f"{_q(name)},"
            f"{_q(f'{slug}{self.rng.randint(1, 9999)}@example.com')},"
            f"{_q('+849' + str(self.rng.randint(10_000_000, 99_999_999)))},"
            f"{_q(str(self.rng.randint(10**11, 10**12 - 1)))},"
            f"{_q(self.rng.choice(REGIONS))})"
        )
        self.counters.users += 1
        self.max_user += 1

    def op_user_update(self) -> None:
        """Change a user's phone number and region -- a PII UPDATE.

        The before image carries the *old* phone. That is the value an SMT masking
        chain never sees (it masks `after` only), so this op is what turns ADR
        0004 from an argument into an observation on the PostgreSQL source.
        """
        self.pg.execute(
            f"UPDATE users SET phone = "
            f"{_q('+849' + str(self.rng.randint(10_000_000, 99_999_999)))}, "
            f"region = {_q(self.rng.choice(REGIONS))}, updated_at = now() "
            f"WHERE id = {self.rng.randint(1, self.max_user)}"
        )
        self.counters.user_updates += 1
