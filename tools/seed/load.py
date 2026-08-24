"""Bulk-load the generated CSV corpus into MySQL and PostgreSQL.

Uses each engine's native bulk path rather than a Python driver:

  PostgreSQL   psql \\copy ... FROM STDIN, streamed over `compose exec -T`
  MySQL        docker cp into secure_file_priv, then LOAD DATA INFILE

Two reasons. First, speed: at the `full` scale (10M+ rows, GR1 section 10.1) driver
round-trips take hours where the native loaders take minutes. Second, dependencies:
this keeps the seed path free of psycopg/pymysql, so the repository stays runnable
with a bare Python and Docker.

    python -m tools.seed.load --dir .seed
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from mosaic import COMPOSE, docker_bin, docker_env  # noqa: E402

# Load order matters: every table's foreign keys must already be satisfiable.
# categories is self-referencing, and the generator emits parents before children.
PG_TABLES = [
    ("categories", "id,name,parent_id,updated_at"),
    ("users",      "id,full_name,email,phone,national_id,region,created_at,updated_at"),
    ("products",   "id,sku,name,description,category_id,price,updated_at"),
    ("inventory",  "product_id,stock_qty,warehouse,updated_at"),
]

MYSQL_TABLES = [
    ("orders",      "order_id,user_id,status,total_amount,currency,region,"
                    "buyer_email,buyer_phone,created_at,updated_at"),
    ("order_items", "order_item_id,order_id,product_id,quantity,unit_price,created_at"),
    ("payments",    "payment_id,order_id,method,amount,raw_card_number,status,created_at"),
]

# BIGSERIAL sequences do not advance when ids are supplied explicitly. Without this
# the load generator's first INSERT collides with a seeded row on the primary key.
PG_SEQUENCES = [("categories", "id"), ("users", "id"), ("products", "id")]

MYSQL_FILES_DIR = "/var/lib/mysql-files"


def _compose(*args: str) -> list[str]:
    return [docker_bin(), "compose", "-f", str(COMPOSE), *args]


def _run(cmd: list[str], stdin=None, check: bool = True) -> subprocess.CompletedProcess:
    r = subprocess.run(cmd, stdin=stdin, capture_output=True, text=True,
                       env=docker_env())
    if check and r.returncode != 0:
        raise RuntimeError(
            f"command failed ({r.returncode}): {' '.join(cmd[:6])}...\n"
            f"stdout: {r.stdout[-1500:]}\nstderr: {r.stderr[-1500:]}"
        )
    return r


def psql(sql: str, database: str = "ecommerce_md") -> str:
    return _run(_compose("exec", "-T", "postgres",
                         "psql", "-U", "cdc", "-d", database,
                         "-v", "ON_ERROR_STOP=1", "-tAc", sql)).stdout.strip()


def mysql(sql: str, database: str = "ecommerce_tx") -> str:
    """Run SQL in MySQL as root.

    Not as `cdc`: server-side LOAD DATA INFILE needs the global FILE privilege,
    and `cdc` deliberately holds only the least-privilege replication grants that
    GR1 Appendix A specifies. Granting FILE to the replication account to make a
    one-off bulk load work would weaken the very thing that appendix is careful
    about. Seeding is an operator action, so it uses the operator account.

    Worth knowing: MySQL reports the missing FILE privilege as
    "ERROR 1045 (28000): Access denied for user ... (using password: YES)",
    which reads exactly like a wrong password and is not.
    """
    return _run(_compose("exec", "-T", "mysql",
                         "mysql", "-uroot", "-prootpw", "--batch", "--skip-column-names",
                         database, "-e", sql)).stdout.strip()


def load_postgres(seed_dir: Path) -> None:
    print("\nPostgreSQL (master data):")
    for table, cols in PG_TABLES:
        csv_path = seed_dir / f"{table}.csv"
        t0 = time.time()
        # \copy runs client-side, so the file streams over exec's stdin and never
        # has to exist inside the container.
        sql = (rf"\copy {table}({cols}) FROM STDIN "
               r"WITH (FORMAT csv, HEADER true)")
        with csv_path.open("rb") as fh:
            _run(_compose("exec", "-T", "postgres",
                          "psql", "-U", "cdc", "-d", "ecommerce_md",
                          "-v", "ON_ERROR_STOP=1", "-c", sql), stdin=fh)
        n = psql(f"SELECT count(*) FROM {table}")
        print(f"  {table:<12} {int(n):>10,} rows   {time.time() - t0:6.1f}s")

    for table, col in PG_SEQUENCES:
        psql(f"SELECT setval(pg_get_serial_sequence('{table}', '{col}'), "
             f"COALESCE((SELECT max({col}) FROM {table}), 1))")
    print(f"  sequences advanced past the seeded ids "
          f"({', '.join(t for t, _ in PG_SEQUENCES)})")


def load_mysql(seed_dir: Path) -> None:
    print("\nMySQL (transactions):")
    for table, cols in MYSQL_TABLES:
        csv_path = seed_dir / f"{table}.csv"
        t0 = time.time()
        # LOAD DATA INFILE is server-side, so the file has to be inside the
        # container and under secure_file_priv.
        _run([docker_bin(), "cp", str(csv_path), f"mosaic-mysql:{MYSQL_FILES_DIR}/"])
        sql = (
            "SET foreign_key_checks=0; SET unique_checks=0; "
            f"LOAD DATA INFILE '{MYSQL_FILES_DIR}/{table}.csv' INTO TABLE {table} "
            "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' "
            "LINES TERMINATED BY '\\n' IGNORE 1 LINES "
            f"({cols});"
        )
        mysql(sql)
        _run(_compose("exec", "-T", "mysql", "rm", "-f",
                      f"{MYSQL_FILES_DIR}/{table}.csv"), check=False)
        n = mysql(f"SELECT count(*) FROM {table}")
        print(f"  {table:<12} {int(n):>10,} rows   {time.time() - t0:6.1f}s")


def verify(seed_dir: Path) -> int:
    """Compare loaded row counts against the generator's manifest."""
    manifest_path = seed_dir / "manifest.json"
    if not manifest_path.exists():
        print("\nno manifest.json -- skipping verification")
        return 0
    expected = json.loads(manifest_path.read_text(encoding="utf-8"))["counts"]

    print("\nverification (loaded vs manifest):")
    bad = 0
    for table, _ in PG_TABLES:
        got, want = int(psql(f"SELECT count(*) FROM {table}")), expected[table]
        ok = got == want
        bad += not ok
        print(f"  {table:<12} {got:>10,} / {want:<10,} {'ok' if ok else 'MISMATCH'}")
    for table, _ in MYSQL_TABLES:
        got, want = int(mysql(f"SELECT count(*) FROM {table}")), expected[table]
        ok = got == want
        bad += not ok
        print(f"  {table:<12} {got:>10,} / {want:<10,} {'ok' if ok else 'MISMATCH'}")

    if bad:
        print(f"\n{bad} table(s) did not match the manifest.")
        return 1
    print("\nall tables match the manifest.")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dir", type=Path, default=Path(".seed"),
                   help="directory holding the generated CSVs")
    p.add_argument("--truncate", action="store_true",
                   help="empty the target tables first (re-loading otherwise "
                        "violates primary keys)")
    a = p.parse_args()

    if not (a.dir / "products.csv").exists():
        print(f"no corpus in {a.dir}/ -- run:  python mosaic.py generate")
        return 1

    if a.truncate:
        print("truncating target tables...")
        psql("TRUNCATE inventory, products, users, categories RESTART IDENTITY CASCADE")
        mysql("SET foreign_key_checks=0; TRUNCATE payments; TRUNCATE order_items; "
              "TRUNCATE orders; SET foreign_key_checks=1;")

    t0 = time.time()
    load_postgres(a.dir)
    load_mysql(a.dir)
    print(f"\nloaded in {time.time() - t0:.1f}s")
    return verify(a.dir)


if __name__ == "__main__":
    raise SystemExit(main())
