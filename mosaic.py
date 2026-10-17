#!/usr/bin/env python3
"""MOSAIC task runner.

Canonical entry point for operating the stack. Standard library only, so it works
identically on Windows, macOS and Linux -- `make` is not available everywhere this
project needs to run. A thin Makefile delegates here for platforms that have it.

    python mosaic.py up                 bring up the core vertical slice
    python mosaic.py up --profile gov   add governance (OPA + Marquez)
    python mosaic.py status             health of every running service
    python mosaic.py connectors         register Debezium connectors
    python mosaic.py cdc-verify         check masking/envelope on the Kafka topics
    python mosaic.py loadgen            continuous OLTP write load
    python mosaic.py stockout --top 3   force the L0 failure condition
    python mosaic.py embed refresh      rebuild the slow channel's vector index (M4)
    python mosaic.py register           register datasets in platform.datasets (M4)
    python mosaic.py check-abstraction  the L3 domain-purity gate
    python mosaic.py down / clean       stop / stop and destroy volumes
"""
from __future__ import annotations

import argparse
import functools
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
COMPOSE = ROOT / "docker" / "compose.yaml"
ENV_FILE = ROOT / "docker" / ".env"

# Published host ports. Container-internal ports are fixed; only what the stack
# exposes on the host is negotiable, because a developer machine may already be
# running something on a default port. Defaults match GR1 Appendix B.
PORTS: dict[str, tuple[int, str]] = {
    "MYSQL_PORT":           (3306,  "mysql"),
    "POSTGRES_PORT":        (5432,  "postgres"),
    "KAFKA_PORT":           (9092,  "kafka"),
    "SCHEMA_REGISTRY_PORT": (8081,  "schema-registry"),
    "CONNECT_PORT":         (8083,  "kafka-connect"),
    "MINIO_PORT":           (9000,  "minio (S3 API)"),
    "MINIO_CONSOLE_PORT":   (9001,  "minio (console)"),
    "NESSIE_PORT":          (19120, "nessie"),
    "TRINO_PORT":           (8090,  "trino"),
    "SPARK_UI_PORT":        (4040,  "spark ui (bronze stream)"),
    "OPA_PORT":             (8181,  "opa"),
    "MARQUEZ_PORT":         (5000,  "marquez"),
    "MARQUEZ_ADMIN_PORT":   (5001,  "marquez (admin)"),
    "AIRFLOW_PORT":         (8080,  "airflow"),
    "API_PORT":             (8000,  "mosaic api"),
}

# Docker Desktop on Windows ships docker.exe but does not always put its directory
# on the *Windows* PATH -- Git Bash resolves `docker` through a shell shim of its
# own, so a command that works in the terminal still fails from subprocess. Resolve
# the real executable ourselves rather than relying on PATH inheritance.
_DOCKER_CANDIDATES = [
    r"C:\Program Files\Docker\Docker\resources\bin\docker.exe",
    r"C:\Program Files\Docker\Docker\resources\docker.exe",
    "/usr/local/bin/docker",
    "/usr/bin/docker",
    "/opt/homebrew/bin/docker",
]

# Words that must never appear inside the generic core. See docs/PROBLEM.md, rung L3.
# Deliberately includes both domains: the core must not know about either.
DOMAIN_WORDS = [
    "product", "order", "sku", "shopper", "cart", "invento", "payment",
    "posting", "candidate", "recruiter", "salary", "vacancy",
]

# What the gate scans. platform/ from M0; pipelines/embeddings/ since M4, because the
# index runner is generic by decision (ADR 0006, M4-B2) and a decision nothing checks
# erodes. pipelines/transform/ is not listed: its generated models legitimately carry
# table names, and assemble.py is what keeps the project domain-free there.
GATED_DIRS = ["platform", "pipelines/embeddings"]

# SQL's ORDER BY is not domain vocabulary. Without this exemption the word "order"
# fails every ranking query -- and ranking is what the core exists to do.
_SQL_KEYWORDS = re.compile(r"\border\s+by\b", re.IGNORECASE)


# ------ helpers
@functools.lru_cache(maxsize=1)
def docker_bin() -> str:
    """Absolute path to the docker executable.

    Set MOSAIC_DOCKER_BIN to override if Docker lives somewhere unusual.
    """
    override = os.environ.get("MOSAIC_DOCKER_BIN")
    if override:
        return override
    found = shutil.which("docker")
    if found:
        return found
    for candidate in _DOCKER_CANDIDATES:
        if Path(candidate).is_file():
            return candidate
    sys.exit(
        "Cannot find the docker executable.\n"
        "  - Is Docker Desktop installed and running?\n"
        "  - If it is installed somewhere unusual, set MOSAIC_DOCKER_BIN to its full path.\n"
        "Note: on Windows, `docker` working in Git Bash does not mean it is on the\n"
        "Windows PATH -- Git Bash resolves it through a shim of its own."
    )


@functools.lru_cache(maxsize=1)
def docker_env() -> dict[str, str]:
    """Environment for docker subprocesses, with Docker's bin directory on PATH.

    Docker shells out to helpers that must be found on PATH -- notably
    `docker-credential-desktop`, used for every registry pull. Those helpers sit
    beside docker.exe in a directory Docker Desktop does not add to the Windows
    PATH, so resolving docker.exe by absolute path is not enough on its own: the
    pull then fails with "error getting credentials". Putting that directory on the
    child's PATH fixes the whole family of helper lookups at once.
    """
    env = dict(os.environ)
    bin_dir = str(Path(docker_bin()).parent)
    if bin_dir not in env.get("PATH", ""):
        env["PATH"] = bin_dir + os.pathsep + env.get("PATH", "")
    return env


def run(cmd: list[str], **kw) -> int:
    print(f"  $ {' '.join(cmd)}")
    kw.setdefault("env", docker_env())
    return subprocess.call(cmd, **kw)


@functools.lru_cache(maxsize=1)
def env_overrides() -> dict[str, str]:
    """Values from docker/.env, which carries this machine's port overrides."""
    out: dict[str, str] = {}
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
    return out


def host_port(var: str) -> int:
    """Effective host port: real environment, then docker/.env, then the default."""
    return int(os.environ.get(var) or env_overrides().get(var) or PORTS[var][0])


def _compose_cmd(profiles: list[str] | None = None) -> list[str]:
    cmd = [docker_bin(), "compose", "-f", str(COMPOSE)]
    if ENV_FILE.exists():
        cmd += ["--env-file", str(ENV_FILE)]
    for p in profiles or []:
        cmd += ["--profile", p]
    return cmd


def compose(*args: str, profiles: list[str] | None = None) -> int:
    return run(_compose_cmd(profiles) + list(args))


def compose_out(*args: str) -> str:
    cmd = _compose_cmd() + list(args)
    return subprocess.run(cmd, capture_output=True, text=True,
                          env=docker_env()).stdout


def http(url: str, method: str = "GET", body: dict | None = None, timeout: int = 10):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json"} if data else {},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read().decode()
        return r.status, (json.loads(raw) if raw.strip() else None)


# ---------- lifecycle
def cmd_up(args) -> int:
    rc = compose("up", "-d", profiles=args.profile)
    if rc != 0:
        return rc
    return wait_healthy(timeout=args.timeout, profiles=args.profile)


def wait_healthy(timeout: int = 300, profiles: list[str] | None = None) -> int:
    """Block until every container with a healthcheck reports healthy.

    `docker compose up -d` returns as soon as containers are *started*, which is
    not the same as usable -- Kafka Connect in particular accepts connections long
    before it will accept a connector. Everything downstream depends on this
    distinction, so `up` does not return until it is actually true.
    """
    expected = _expected_services(profiles)
    print(f"\nwaiting for {len(expected)} services to become healthy (timeout {timeout}s)...")
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        rows = _service_states()
        pending, crashed = [], []
        for svc in expected:
            state, exit_code = rows.get(svc, ("missing", None))
            if state == "healthy" or state == "running-no-healthcheck":
                continue
            if state == "exited":
                # A one-shot task that finished cleanly is done, not broken.
                (crashed if exit_code else []).append(f"{svc} (exit {exit_code})") \
                    if exit_code else None
                continue
            pending.append(svc)

        # A crashed service must never be mistaken for a healthy one. Earlier this
        # loop counted only what `compose ps` still listed, so a container that died
        # dropped out of the denominator and the wait reported success.
        if crashed:
            print(f"\nFAILED: {', '.join(crashed)}")
            for entry in crashed:
                svc = entry.split(" ")[0]
                print(f"\n--- last 15 log lines: {svc} ---")
                print("\n".join(compose_out("logs", "--tail", "15", svc).splitlines()[-15:]))
            return 1

        line = f"  healthy {len(expected) - len(pending)}/{len(expected)}"
        if pending:
            line += f"   waiting on: {', '.join(sorted(pending))}"
        if line != last:
            print(line)
            last = line
        if not pending:
            print("\nall services healthy.")
            _print_endpoints()
            return 0
        time.sleep(5)
    print("\nTIMEOUT. Current state:")
    cmd_status(None)
    return 1


def _expected_services(profiles: list[str] | None = None) -> list[str]:
    """Services compose would start, so a vanished container is still accounted for."""
    out = subprocess.run(_compose_cmd(profiles) + ["config", "--services"],
                         capture_output=True, text=True, env=docker_env()).stdout
    return sorted(s.strip() for s in out.splitlines() if s.strip())


def _service_states() -> dict[str, tuple[str, int | None]]:
    """Per-service (state, exit_code), including containers that have exited.

    `compose ps` without -a hides exited containers entirely, which is how a crashed
    service used to disappear from the health tally instead of failing the wait.
    """
    out = compose_out("ps", "-a", "--format", "json")
    states: dict[str, tuple[str, int | None]] = {}
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        name = row.get("Service") or row.get("Name", "?")
        health = (row.get("Health") or "").lower()
        state = (row.get("State") or "").lower()
        code = row.get("ExitCode")
        if state == "exited":
            states[name] = ("exited", int(code) if code is not None else None)
        elif not health:
            states[name] = ("running-no-healthcheck" if state == "running" else state, None)
        else:
            states[name] = (health, None)
    return states


def _print_endpoints() -> None:
    print(f"""
  Trino          http://localhost:{host_port('TRINO_PORT')}
  Kafka Connect  http://localhost:{host_port('CONNECT_PORT')}
  Schema Reg     http://localhost:{host_port('SCHEMA_REGISTRY_PORT')}
  MinIO console  http://localhost:{host_port('MINIO_CONSOLE_PORT')}    (minioadmin / minioadmin)
  Nessie         http://localhost:{host_port('NESSIE_PORT')}
  Spark UI       http://localhost:{host_port('SPARK_UI_PORT')}    (the Bronze stream)
  MySQL         localhost:{host_port('MYSQL_PORT')}           (cdc / cdcpw, db ecommerce_tx)
  PostgreSQL     localhost:{host_port('POSTGRES_PORT')}           (cdc / cdcpw, db ecommerce_md)

  next:  python mosaic.py connectors""")


def cmd_down(args) -> int:
    return compose("down", profiles=["gov", "orch", "console"])


def cmd_clean(args) -> int:
    print("This destroys all volumes: MySQL, PostgreSQL, MinIO and Kafka data.")
    if input("Type 'yes' to continue: ").strip() != "yes":
        print("aborted.")
        return 1
    return compose("down", "-v", profiles=["gov", "orch", "console"])


def cmd_status(args) -> int:
    states = _service_states()
    if not states:
        print("nothing running.  start with:  python mosaic.py up")
        return 0
    width = max(len(n) for n in states)
    for name in sorted(states):
        state, code = states[name]
        suffix = f" (exit {code})" if state == "exited" and code is not None else ""
        print(f"  {name.ljust(width)}  {state}{suffix}")
    return 0


def cmd_logs(args) -> int:
    return compose("logs", "-f", "--tail", "100", *(args.service or []))


# --------- connectors
def cmd_connectors(args) -> int:
    """Register (or update) every Debezium connector for a domain."""
    conn_dir = ROOT / "domains" / args.domain / "connectors"
    files = sorted(conn_dir.glob("*.json"))
    if not files:
        print(f"no connector configs under {conn_dir}")
        return 1

    connect_url = f"http://localhost:{host_port('CONNECT_PORT')}"
    try:
        http(f"{connect_url}/connectors")
    except (urllib.error.URLError, OSError) as e:
        print(f"Kafka Connect is not reachable at {connect_url} ({e}).")
        print("Is the stack up?  python mosaic.py status")
        return 1

    failed = 0
    for f in files:
        cfg = json.loads(f.read_text())
        name, config = cfg["name"], cfg["config"]
        # Strip the _comment_* documentation keys before submitting.
        config = {k: v for k, v in config.items() if not k.startswith("_comment")}
        try:
            status, _ = http(f"{connect_url}/connectors/{name}/config", "PUT", config)
            print(f"  {name:<24} registered (HTTP {status})")
        except urllib.error.HTTPError as e:
            print(f"  {name:<24} FAILED (HTTP {e.code})")
            print(f"      {e.read().decode()[:600]}")
            failed += 1

    if failed:
        return 1
    print("\nchecking connector status...")
    time.sleep(3)
    for f in files:
        name = json.loads(f.read_text())["name"]
        try:
            _, st = http(f"{connect_url}/connectors/{name}/status")
            tasks = ", ".join(t["state"] for t in st.get("tasks", [])) or "no tasks yet"
            print(f"  {name:<24} connector={st['connector']['state']}  tasks={tasks}")
        except urllib.error.HTTPError as e:
            print(f"  {name:<24} status unavailable (HTTP {e.code})")
    return 0


# ---------- abstraction gate
def cmd_check_abstraction(args) -> int:
    """L3 gate: no domain vocabulary may appear anywhere in the generic core.

    This is the mechanical form of the claim that MOSAIC is a platform rather than
    an e-commerce pipeline. If it fails, the abstraction has leaked and the leak is
    named precisely. See docs/PROBLEM.md.
    """
    pattern = re.compile("|".join(DOMAIN_WORDS), re.IGNORECASE)
    violations: list[str] = []
    scanned = 0
    for d in GATED_DIRS:
        target = ROOT / d
        if not target.exists():
            continue
        for path in sorted(target.rglob("*")):
            if not path.is_file() or "__pycache__" in path.parts:
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except (UnicodeDecodeError, OSError):
                continue
            scanned += 1
            for n, line in enumerate(text.splitlines(), 1):
                if pattern.search(_SQL_KEYWORDS.sub(" ", line)):
                    rel = path.relative_to(ROOT).as_posix()
                    violations.append(f"  {rel}:{n}: {line.strip()[:100]}")

    print(f"scanned {scanned} files under {', '.join(d + '/' for d in GATED_DIRS)} "
          f"for {len(DOMAIN_WORDS)} domain terms")
    if violations:
        print(f"\nFAIL -- {len(violations)} domain reference(s) leaked into the generic core:\n")
        print("\n".join(violations))
        print("\nMove the domain knowledge into domains/<domain>/ and re-run.")
        return 1
    print("PASS -- the generic core contains no domain vocabulary.")
    return 0


# --------- doctor
def _exec(container: str, *cmd: str) -> str | None:
    """Run a command in a container; None if it fails or the container is absent."""
    r = subprocess.run([docker_bin(), "exec", container, *cmd],
                       capture_output=True, text=True, env=docker_env())
    return r.stdout.strip() if r.returncode == 0 else None


def _mysql_q(sql: str) -> str | None:
    return _exec("mosaic-mysql", "mysql", "-uroot", "-prootpw", "--batch",
                 "--skip-column-names", "ecommerce_tx", "-e", sql)


def _pg_q(sql: str, db: str = "ecommerce_md") -> str | None:
    return _exec("mosaic-postgres", "psql", "-U", "cdc", "-d", db, "-tAc", sql)


def cmd_doctor(args) -> int:
    """One-shot state report: environment, stack, data, connectors.

    Exists so a session can answer "where is everything?" with one command instead of
    a dozen probes. Read docs/STATUS.md for what is *meant* to be true; this reports
    what actually is.
    """
    checks: list[tuple[str, bool | None, str]] = []

    def add(name, ok, detail=""):
        checks.append((name, ok, detail))

    add("docker binary", True, docker_bin())
    daemon = subprocess.run([docker_bin(), "info", "--format", "{{.ServerVersion}}"],
                            capture_output=True, text=True, env=docker_env())
    up = daemon.returncode == 0
    add("docker daemon", up, daemon.stdout.strip() if up else "not reachable -- start Docker Desktop")
    if not up:
        _render(checks)
        return 1

    states = _service_states()
    if not states:
        add("stack", False, "nothing running -- python mosaic.py up")
        _render(checks)
        return 1
    bad = {n: s for n, (s, c) in states.items()
           if not (s in ("healthy", "running-no-healthcheck") or (s == "exited" and not c))}
    add("stack health", not bad,
        f"{len(states) - len(bad)}/{len(states)} ok" + (f"  degraded: {', '.join(bad)}" if bad else ""))

    binlog = _mysql_q("SELECT CONCAT(@@binlog_format,'/',@@binlog_row_image,'/',@@gtid_mode)")
    add("mysql cdc prereqs", binlog == "ROW/FULL/ON", binlog or "unreachable")

    wal = _pg_q("SELECT current_setting('wal_level')||'/'||"
                "COALESCE((SELECT pubname FROM pg_publication LIMIT 1),'no-publication')")
    add("postgres cdc prereqs", wal == "logical/mosaic_pub", wal or "unreachable")

    pg_tables = _pg_q("SELECT count(*) FROM pg_tables WHERE schemaname='public'")
    plat = _pg_q("SELECT count(*) FROM information_schema.tables "
                 "WHERE table_schema='platform'", db="mosaic_platform")
    add("schemas", bool(pg_tables) and bool(plat),
        f"postgres public: {pg_tables} tables | platform: {plat} tables")

    cats = _exec("mosaic-trino", "trino", "--execute", "SHOW CATALOGS")
    cat_list = sorted(c.strip('"') for c in (cats or "").splitlines() if c.startswith('"'))
    add("trino catalogs", len(cat_list) >= 4, ", ".join(cat_list) or "unreachable")

    # Seed volume, per source.
    pg_rows = _pg_q("SELECT (SELECT count(*) FROM products)||' products, '||"
                    "(SELECT count(*) FROM users)||' users'")
    my_rows = _mysql_q("SELECT CONCAT((SELECT count(*) FROM orders),' orders, ',"
                       "(SELECT count(*) FROM order_items),' items')")
    seeded = bool(pg_rows) and not pg_rows.startswith("0 ")
    add("seed data", seeded, f"{pg_rows or '?'} | {my_rows or '?'}"
        if seeded else "empty -- python mosaic.py generate && python -m tools.seed.load")

    try:
        _, names = http(f"http://localhost:{host_port('CONNECT_PORT')}/connectors")
        if names:
            detail = []
            for n in names:
                _, st = http(f"http://localhost:{host_port('CONNECT_PORT')}/connectors/{n}/status")
                tasks = "/".join(t["state"] for t in st.get("tasks", [])) or "no-tasks"
                detail.append(f"{n}={st['connector']['state']}:{tasks}")
            add("debezium connectors", all("RUNNING" in d for d in detail), " | ".join(detail))
        else:
            add("debezium connectors", None, "none registered -- python mosaic.py connectors")
    except (urllib.error.URLError, OSError) as e:
        add("debezium connectors", False, f"Connect unreachable ({e})")

    status = _bronze_status()
    if status is None:
        add("bronze stream", False, "no status file -- python mosaic.py logs spark")
    else:
        age = time.time() - status.get("written_at", 0)
        live = bool(status.get("active")) and age < 60
        totals = status.get("totals", {})
        add("bronze stream", live,
            f"{'active' if live else 'STOPPED or stale'}, {len(status.get('topics', []))} topics, "
            f"{totals.get('written', 0)} rows landed since start, "
            f"backlog {status.get('backlog_records', '?')}")

    # The slow channel's index (M4). Absent is "--", not a failure: it exists only once
    # someone has run a refresh, exactly like Silver/Gold before the first dbt build.
    emb = _pg_q("SELECT CASE WHEN to_regclass('public.product_embeddings') IS NULL THEN 'absent' "
                "ELSE (SELECT count(*)||' rows, refreshed_at '||"
                "COALESCE(to_char(max(refreshed_at) AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS'),'-')"
                "||' UTC, '||COALESCE(round(extract(epoch FROM now()-max(refreshed_at)))::text,'?')"
                "||'s old' FROM product_embeddings) END")
    if emb is None or emb == "absent":
        add("embedding index", None, "not built -- python mosaic.py embed refresh")
    else:
        add("embedding index", not emb.startswith("0 "), emb)

    # Reuse the real gate rather than reimplementing it.
    import io, contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        gate = cmd_check_abstraction(None)
    add("abstraction gate", gate == 0,
        "generic core is domain-free" if gate == 0 else "LEAKED -- see check-abstraction")

    return _render(checks)


def _render(checks) -> int:
    width = max(len(n) for n, _, _ in checks)
    failed = 0
    for name, ok, detail in checks:
        mark = {True: "ok  ", False: "FAIL", None: "--  "}[ok]
        failed += ok is False
        print(f"  {mark}  {name.ljust(width)}  {detail}")
    print(f"\n  {'all checks passed' if not failed else f'{failed} check(s) failing'}"
          f"   |   intended state: docs/STATUS.md")
    return 1 if failed else 0


# --------------- port check
def cmd_check_ports(args) -> int:
    """Report which of the stack's host ports are already taken, and by what.

    A port collision surfaces from `compose up` only after every image has been
    pulled and most containers created, which is an expensive way to learn it.
    """
    import socket

    ours = set()
    owner: dict[int, str] = {}
    ps = subprocess.run([docker_bin(), "ps", "--format", "{{.Names}}\t{{.Ports}}"],
                        capture_output=True, text=True, env=docker_env()).stdout
    for line in ps.splitlines():
        if "\t" not in line:
            continue
        name, ports = line.split("\t", 1)
        for hp in re.findall(r":(\d+)->", ports):
            owner[int(hp)] = name
            if name.startswith("mosaic-"):
                ours.add(int(hp))

    conflicts = 0
    for var, (_, label) in PORTS.items():
        port = host_port(var)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(("0.0.0.0", port))
                print(f"  free       {port:>6}  {label}")
                continue
            except OSError:
                pass
        who = owner.get(port, "an unknown process")
        if port in ours:
            print(f"  ours       {port:>6}  {label}  (held by {who})")
        else:
            conflicts += 1
            print(f"  CONFLICT   {port:>6}  {label}  (held by {who})")

    if conflicts:
        print(f"\n{conflicts} conflict(s). Override the port in docker/.env, e.g.:")
        print("    MINIO_CONSOLE_PORT=9101")
        return 1
    print("\nno conflicts.")
    return 0


# ------------- seed data
def cmd_generate(args) -> int:
    """Generate the CSV corpus for a domain (no database needed)."""
    cmd = [sys.executable, "-m", f"tools.seed.generate",
           "--scale", args.scale, "--out", str(args.out)]
    if args.now:
        cmd += ["--now", args.now]
    return run(cmd, cwd=str(ROOT))


# ------------ live workload
def _tool(module: str, *args: str) -> int:
    return run([sys.executable, "-m", module, *args], cwd=str(ROOT))


def cmd_loadgen(args) -> int:
    """Continuous OLTP writes -- what manufactures the freshness asymmetry."""
    argv = ["start", "--rate", str(args.rate), "--duration", str(args.duration),
            "--max-lag", str(args.max_lag)]
    if args.seed is not None:
        argv += ["--seed", str(args.seed)]
    return _tool("tools.loadgen.run", *argv)


def cmd_stockout(args) -> int:
    """Force products out of stock: the L0 switch from docs/PROBLEM.md."""
    argv = ["stockout"]
    for pid in args.product_id or []:
        argv += ["--product-id", str(pid)]
    if args.random:
        argv += ["--random", str(args.random)]
    elif not args.product_id:
        argv += ["--top", str(args.top)]
    return _tool("tools.loadgen.run", *argv)


def cmd_restock(args) -> int:
    argv = ["restock", "--qty", str(args.qty)]
    for pid in args.product_id or []:
        argv += ["--product-id", str(pid)]
    if args.last:
        argv += ["--last", str(args.last)]
    return _tool("tools.loadgen.run", *argv)


def cmd_cdc_verify(args) -> int:
    """Check the Kafka topics against what the connectors promised."""
    argv = ["--domain", args.domain, "--messages", str(args.messages)]
    if args.require_update:
        argv.append("--require-update")
    return _tool("tools.cdc.verify", *argv)


def cmd_restart_safety(args) -> int:
    """Kill Connect mid-stream under load and prove the resume lost nothing (M2)."""
    return _tool("tools.cdc.restart", "restart", "--domain", args.domain,
                 "--mode", args.mode, "--rate", str(args.rate),
                 "--warmup", str(args.warmup), "--stall", str(args.stall),
                 "--drain", str(args.drain))


def cmd_wal_guard(args) -> int:
    """Exercise GR1 section 4.4's WAL-accumulation guard on the replication slot."""
    return _tool("tools.cdc.restart", "wal-guard", "--domain", args.domain,
                 "--mode", args.mode, "--idle", str(args.idle))


def cmd_resnapshot(args) -> int:
    """Re-emit every captured table's current rows as op=r events (M3, finding A10)."""
    argv = ["--domain", args.domain, "--timeout", str(args.timeout)]
    for table in args.table or []:
        argv += ["--table", table]
    return _tool("tools.cdc.snapshot", *argv)


# ------------------------------------------------------------------ lakehouse
BRONZE_STATUS = "/tmp/mosaic-bronze/status.json"


def _bronze_status() -> dict | None:
    """The Bronze stream's own status file, or None if the stream never wrote one."""
    raw = _exec("mosaic-spark", "cat", BRONZE_STATUS)
    try:
        return json.loads(raw) if raw else None
    except json.JSONDecodeError:
        return None


def cmd_bronze(args) -> int:
    """State of the Bronze stream: liveness, backlog, and what the last batch did."""
    status = _bronze_status()
    if status is None:
        print("the Bronze stream has not written a status file.")
        print("\n--- last 30 log lines: spark ---")
        print("\n".join(compose_out("logs", "--tail", "30", "spark").splitlines()[-30:]))
        return 1
    age = time.time() - status.get("written_at", 0)
    live = bool(status.get("active")) and age < 60
    totals, last = status.get("totals", {}), status.get("last_batch") or {}
    print(f"  stream     {'ACTIVE' if live else 'STOPPED or stale'}  "
          f"(status {age:.0f}s old, started {status.get('started_at', '?')})")
    print(f"  table      {status.get('table')}  <- {len(status.get('topics', []))} topic(s)")
    print(f"  backlog    {status.get('backlog_records', '?')} record(s) not yet consumed")
    print(f"  since start {totals.get('batches', 0)} batch(es), {totals.get('rows_in', 0)} in, "
          f"{totals.get('written', 0)} written, duplicates dropped: "
          f"{totals.get('dup_in_batch', 0)} in-batch + {totals.get('dup_in_bronze', 0)} already in Bronze")
    if last:
        print(f"  last batch #{last.get('batch_id')}: {last.get('rows_in')} in, "
              f"{last.get('written')} written, max _src_ts {last.get('max_src_ts')}")
    return 0 if live else 1


def cmd_dbt(args) -> int:
    """Silver + Gold: assemble the dbt project from domains/, then run dbt in its container."""
    dbt_args = [a for a in (args.dbt_args or []) if a != "--"] or ["build"]
    return compose("run", "--rm", "dbt", *dbt_args, profiles=["transform"])


def jobs(*args: str) -> int:
    """Run one platform job (`python -m <module> ...`) in the jobs image (M4)."""
    return compose("run", "--rm", "-T", "jobs", *args, profiles=["jobs"])


def cmd_embed(args) -> int:
    """The slow channel's index: refresh | status | query | recall (ADR 0006)."""
    extra = [a for a in (args.extra or []) if a != "--"]
    if "--domain" not in extra:
        extra = ["--domain", args.domain, *extra]
    return jobs("embeddings", args.action, *extra)


def cmd_register(args) -> int:
    """Probe and register a domain's datasets in platform.datasets (the N1 hand-off)."""
    return jobs("mosaic_platform.freshness.register", "--domain", args.domain,
                *(["--dry-run"] if args.dry_run else []))


def cmd_embed_verify(args) -> int:
    """M4's acceptance checks, ending in the first end-to-end reproduction of L0."""
    argv = ["--domain", args.domain]
    if args.with_refresh:
        argv.append("--with-refresh")
    return _tool("tools.embeddings.verify", *argv)


def cmd_bronze_verify(args) -> int:
    """Exactly-once landing (Connect- and Spark-side replay) and end-to-end latency."""
    argv = [args.check, "--domain", args.domain]
    if args.check in ("replay", "latency"):
        argv += ["--rate", str(args.rate)]
    if args.check == "latency":
        argv += ["--duration", str(args.duration)]
    return _tool("tools.bronze.verify", *argv)


# ------------------------------------------------------------------- shells
def cmd_sql(args) -> int:
    return compose("exec", "trino", "trino")


def cmd_psql(args) -> int:
    return compose("exec", "postgres", "psql", "-U", "cdc", "-d", args.database)


def cmd_mysql(args) -> int:
    return compose("exec", "mysql", "mysql", "-ucdc", "-pcdcpw", "ecommerce_tx")


# ---------------------------------------------------------------------- main
def main() -> int:
    p = argparse.ArgumentParser(prog="mosaic", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    up = sub.add_parser("up", help="start the stack and wait until healthy")
    up.add_argument("--profile", action="append", default=[],
                    choices=["gov", "orch", "console"],
                    help="optional profile; repeatable")
    up.add_argument("--timeout", type=int, default=300)
    up.set_defaults(func=cmd_up)

    sub.add_parser("down", help="stop the stack, keep data").set_defaults(func=cmd_down)
    sub.add_parser("clean", help="stop the stack and DESTROY all volumes").set_defaults(func=cmd_clean)
    sub.add_parser("status", help="health of every service").set_defaults(func=cmd_status)

    lg = sub.add_parser("logs", help="tail logs")
    lg.add_argument("service", nargs="*")
    lg.set_defaults(func=cmd_logs)

    cn = sub.add_parser("connectors", help="register Debezium connectors for a domain")
    cn.add_argument("--domain", default="ecommerce")
    cn.set_defaults(func=cmd_connectors)

    sub.add_parser("doctor",
                   help="one-shot state report: environment, stack, data, connectors"
                   ).set_defaults(func=cmd_doctor)

    sub.add_parser("check-ports",
                   help="report host port collisions before they cost a pull"
                   ).set_defaults(func=cmd_check_ports)

    sub.add_parser("check-abstraction",
                   help="L3 gate: no domain vocabulary under platform/"
                   ).set_defaults(func=cmd_check_abstraction)

    ge = sub.add_parser("generate", help="generate the CSV seed corpus")
    ge.add_argument("--scale", default="small", choices=["small", "medium", "full"])
    ge.add_argument("--out", type=Path, default=ROOT / ".seed")
    ge.add_argument("--now", default=None,
                    help="pin the clock anchor for a byte-reproducible corpus")
    ge.set_defaults(func=cmd_generate)

    lgen = sub.add_parser("loadgen", help="continuous OLTP write load")
    lgen.add_argument("--rate", type=float, default=10.0, help="operations per second")
    lgen.add_argument("--duration", type=int, default=0, help="seconds; 0 = until ctrl-c")
    lgen.add_argument("--seed", type=int, default=None)
    lgen.add_argument("--max-lag", type=int, default=2000)
    lgen.set_defaults(func=cmd_loadgen)

    so = sub.add_parser("stockout",
                        help="force products out of stock -- the L0 reproducer")
    so.add_argument("--product-id", type=int, action="append", help="repeatable")
    so.add_argument("--top", type=int, default=1, help="N best-selling products")
    so.add_argument("--random", type=int, default=0, help="N random in-stock products")
    so.set_defaults(func=cmd_stockout)

    rs = sub.add_parser("restock", help="undo a stock-out")
    rs.add_argument("--product-id", type=int, action="append")
    rs.add_argument("--last", type=int, default=0, help="the last N manual stock-outs")
    rs.add_argument("--qty", type=int, default=100)
    rs.set_defaults(func=cmd_restock)

    cv = sub.add_parser("cdc-verify",
                        help="check Kafka topics against what the connectors declared")
    cv.add_argument("--domain", default="ecommerce")
    cv.add_argument("--messages", type=int, default=300)
    cv.add_argument("--require-update", action="store_true",
                    help="fail unless an UPDATE before image was inspected")
    cv.set_defaults(func=cmd_cdc_verify)

    rsf = sub.add_parser("restart-safety",
                         help="kill Connect mid-stream under load, prove no gap (M2)")
    rsf.add_argument("--domain", default="ecommerce")
    rsf.add_argument("--mode", default="kill", choices=["kill", "stop", "pause"],
                     help="kill = SIGKILL (the honest crash); stop = graceful SIGTERM")
    rsf.add_argument("--rate", type=float, default=25.0)
    rsf.add_argument("--warmup", type=float, default=20.0)
    rsf.add_argument("--stall", type=float, default=45.0)
    rsf.add_argument("--drain", type=float, default=20.0)
    rsf.set_defaults(func=cmd_restart_safety)

    wg = sub.add_parser("wal-guard",
                        help="heartbeat / WAL-accumulation guard on the replication slot")
    wg.add_argument("--domain", default="ecommerce")
    wg.add_argument("--mode", default="kill", choices=["kill", "stop", "pause"])
    wg.add_argument("--idle", type=float, default=150.0,
                    help="idle seconds per phase; must exceed the 60s offset flush interval")
    wg.set_defaults(func=cmd_wal_guard)

    rsn = sub.add_parser("resnapshot",
                         help="re-emit captured tables as op=r events (Kafka retention recovery)")
    rsn.add_argument("--domain", default="ecommerce")
    rsn.add_argument("--table", action="append", help="restrict to a captured table; repeatable")
    rsn.add_argument("--timeout", type=float, default=900.0)
    rsn.set_defaults(func=cmd_resnapshot)

    sub.add_parser("bronze", help="Bronze stream state: liveness, backlog, last batch"
                   ).set_defaults(func=cmd_bronze)
    dbt = sub.add_parser("dbt", help="Silver/Gold via dbt-trino (default: build); "
                                     "extra args go to dbt, e.g. dbt -- run -s gold")
    dbt.add_argument("dbt_args", nargs=argparse.REMAINDER)
    dbt.set_defaults(func=cmd_dbt)

    bv = sub.add_parser("bronze-verify",
                        help="prove Bronze exactly-once (after restart-safety), Spark replay, "
                             "or measure commit->Bronze latency")
    bv.add_argument("check", choices=["exactly-once", "replay", "latency"])
    bv.add_argument("--domain", default="ecommerce")
    bv.add_argument("--rate", type=float, default=20.0)
    bv.add_argument("--duration", type=int, default=120)
    bv.set_defaults(func=cmd_bronze_verify)

    em = sub.add_parser("embed",
                        help="slow-channel index: refresh | status | query | recall; "
                             "extra args go to the job, e.g. embed query -- --text 'tent'")
    em.add_argument("action", choices=["refresh", "status", "query", "recall"])
    em.add_argument("--domain", default="ecommerce")
    em.add_argument("extra", nargs=argparse.REMAINDER)
    em.set_defaults(func=cmd_embed)

    rg = sub.add_parser("register",
                        help="probe + register a domain's datasets in platform.datasets")
    rg.add_argument("--domain", default="ecommerce")
    rg.add_argument("--dry-run", action="store_true")
    rg.set_defaults(func=cmd_register)

    ev = sub.add_parser("embed-verify",
                        help="M4 acceptance: counts, semantics, recall, guard, L0 stale hit")
    ev.add_argument("--domain", default="ecommerce")
    ev.add_argument("--with-refresh", action="store_true",
                    help="also rebuild Gold + the index and show the stale hit disappear")
    ev.set_defaults(func=cmd_embed_verify)

    sub.add_parser("sql", help="Trino CLI").set_defaults(func=cmd_sql)
    ps = sub.add_parser("psql", help="PostgreSQL shell")
    ps.add_argument("--database", default="ecommerce_md")
    ps.set_defaults(func=cmd_psql)
    sub.add_parser("mysql", help="MySQL shell").set_defaults(func=cmd_mysql)

    args = p.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
