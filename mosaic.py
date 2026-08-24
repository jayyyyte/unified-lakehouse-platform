#!/usr/bin/env python3
"""MOSAIC task runner.

Canonical entry point for operating the stack. Standard library only, so it works
identically on Windows, macOS and Linux -- `make` is not available everywhere this
project needs to run. A thin Makefile delegates here for platforms that have it.

    python mosaic.py up                 bring up the core vertical slice
    python mosaic.py up --profile gov   add governance (OPA + Marquez)
    python mosaic.py status             health of every running service
    python mosaic.py connectors         register Debezium connectors
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

# Words that must never appear inside platform/. See docs/PROBLEM.md, rung L3.
# Deliberately includes both domains: the core must not know about either.
DOMAIN_WORDS = [
    "product", "order", "sku", "shopper", "cart", "invento", "payment",
    "posting", "candidate", "recruiter", "salary", "vacancy",
]


# --------------------------------------------------------------------- helpers
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


# ------------------------------------------------------------------- lifecycle
def cmd_up(args) -> int:
    rc = compose("up", "-d", profiles=args.profile)
    if rc != 0:
        return rc
    return wait_healthy(timeout=args.timeout)


def wait_healthy(timeout: int = 300) -> int:
    """Block until every container with a healthcheck reports healthy.

    `docker compose up -d` returns as soon as containers are *started*, which is
    not the same as usable -- Kafka Connect in particular accepts connections long
    before it will accept a connector. Everything downstream depends on this
    distinction, so `up` does not return until it is actually true.
    """
    print(f"\nwaiting for services to become healthy (timeout {timeout}s)...")
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        rows = _service_states()
        pending = [n for n, s in rows.items() if s not in ("healthy", "no-healthcheck", "exited")]
        line = f"  healthy {len(rows) - len(pending)}/{len(rows)}"
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


def _service_states() -> dict[str, str]:
    out = compose_out("ps", "--format", "json")
    states: dict[str, str] = {}
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
        if state == "exited":
            states[name] = "exited"
        elif not health:
            states[name] = "no-healthcheck" if state == "running" else state
        else:
            states[name] = health
    return states


def _print_endpoints() -> None:
    print(f"""
  Trino          http://localhost:{host_port('TRINO_PORT')}
  Kafka Connect  http://localhost:{host_port('CONNECT_PORT')}
  Schema Reg     http://localhost:{host_port('SCHEMA_REGISTRY_PORT')}
  MinIO console  http://localhost:{host_port('MINIO_CONSOLE_PORT')}    (minioadmin / minioadmin)
  Nessie         http://localhost:{host_port('NESSIE_PORT')}
  MySQL          localhost:{host_port('MYSQL_PORT')}           (cdc / cdcpw, db ecommerce_tx)
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
        print(f"  {name.ljust(width)}  {states[name]}")
    return 0


def cmd_logs(args) -> int:
    return compose("logs", "-f", "--tail", "100", *(args.service or []))


# ------------------------------------------------------------------ connectors
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


# ------------------------------------------------------------- abstraction gate
def cmd_check_abstraction(args) -> int:
    """L3 gate: no domain vocabulary may appear anywhere under platform/.

    This is the mechanical form of the claim that MOSAIC is a platform rather than
    an e-commerce pipeline. If it fails, the abstraction has leaked and the leak is
    named precisely. See docs/PROBLEM.md.
    """
    target = ROOT / "platform"
    if not target.exists():
        print("platform/ does not exist yet -- nothing to check.")
        return 0

    pattern = re.compile("|".join(DOMAIN_WORDS), re.IGNORECASE)
    violations: list[str] = []
    scanned = 0
    for path in sorted(target.rglob("*")):
        if not path.is_file() or "__pycache__" in path.parts:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        scanned += 1
        for n, line in enumerate(text.splitlines(), 1):
            if pattern.search(line):
                rel = path.relative_to(ROOT).as_posix()
                violations.append(f"  {rel}:{n}: {line.strip()[:100]}")

    print(f"scanned {scanned} files under platform/ for {len(DOMAIN_WORDS)} domain terms")
    if violations:
        print(f"\nFAIL -- {len(violations)} domain reference(s) leaked into the generic core:\n")
        print("\n".join(violations))
        print("\nMove the domain knowledge into domains/<domain>/ and re-run.")
        return 1
    print("PASS -- the generic core contains no domain vocabulary.")
    return 0


# ------------------------------------------------------------------ port check
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


# ----------------------------------------------------------------- seed data
def cmd_generate(args) -> int:
    """Generate the CSV corpus for a domain (no database needed)."""
    cmd = [sys.executable, "-m", f"tools.seed.generate",
           "--scale", args.scale, "--out", str(args.out)]
    if args.now:
        cmd += ["--now", args.now]
    return run(cmd, cwd=str(ROOT))


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

    sub.add_parser("sql", help="Trino CLI").set_defaults(func=cmd_sql)
    ps = sub.add_parser("psql", help="PostgreSQL shell")
    ps.add_argument("--database", default="ecommerce_md")
    ps.set_defaults(func=cmd_psql)
    sub.add_parser("mysql", help="MySQL shell").set_defaults(func=cmd_mysql)

    args = p.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
