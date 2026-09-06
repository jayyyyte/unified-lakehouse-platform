"""Long-lived SQL sessions held open over `docker compose exec -T`.

Shared by tools/loadgen (writes) and tools/cdc (schema reads), which is why it
sits here rather than inside either.

Why not a database driver: the same reason tools/seed/load.py shells out. Keeping
psycopg and pymysql out of the tree means the repository runs with a bare Python
plus Docker, which is the environment the report claims it needs. See
tools/seed/load.py for the seed-path version of this argument.

Why a *persistent* session rather than one `exec` per statement: a container exec
costs roughly 200-600 ms on Windows, so a per-statement design tops out near two
operations per second -- far below the write rate needed to move a CDC watermark
in a way worth measuring. One client process per engine, fed over stdin, sustains
hundreds. Statements are therefore fire-and-forget; results come back through
separate one-shot `query()` calls, which are rare enough for exec cost not to
matter.

Failure handling matters here. Both clients are configured to abort on the first
error (`mysql --batch` does so by default; psql needs ON_ERROR_STOP=1), so a bad
statement kills the client and the next write hits a broken pipe. That is
deliberate: a load generator that silently swallowed failed writes would report a
throughput it never achieved, and every downstream freshness number would be
wrong. The dead process is detected, its captured output is printed, and the run
stops.
"""
from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from mosaic import COMPOSE, docker_bin, docker_env  # noqa: E402


class SessionDied(RuntimeError):
    """The client process exited -- almost always a rejected statement."""


class SqlSession:
    """A writer process fed over stdin, plus one-shot reads on the side.

    Writes run as an operator account, not as `cdc`. The replication account holds
    only the least-privilege grants of GR1 Appendix A (SELECT + REPLICATION on
    MySQL), so it cannot INSERT at all; generating load is an operator action in
    the same sense seeding is.
    """

    name = "?"
    default_database = ""

    def __init__(self, database: str | None = None) -> None:
        self.database = database or self.default_database
        self._proc: subprocess.Popen | None = None
        self._log = tempfile.TemporaryFile(mode="w+", encoding="utf-8", errors="replace")
        self.statements = 0

    def _writer_argv(self) -> list[str]:
        raise NotImplementedError

    def _reader_argv(self) -> list[str]:
        raise NotImplementedError

    # ------------------------------------------------------------- lifecycle
    def open(self) -> "SqlSession":
        self._proc = subprocess.Popen(
            self._compose(*self._writer_argv()),
            stdin=subprocess.PIPE, stdout=self._log, stderr=self._log,
            text=True, bufsize=1, env=docker_env(),
        )
        return self

    def close(self, timeout: float = 300.0) -> str:
        """Close stdin, wait for the client to drain, return its captured output.

        The wait is generous on purpose. Statements are written into a pipe, so at
        a high enough send rate the client is still executing a backlog when the
        run ends; killing it there silently discards committed-looking work. An
        earlier 30s limit did exactly that and the row-count check caught it.
        """
        if self._proc is None:
            return ""
        try:
            if self._proc.stdin and not self._proc.stdin.closed:
                self._proc.stdin.close()
            self._proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            print(f"  WARNING: {self.name} client still draining after {timeout:.0f}s; "
                  f"killing it. Row counts below will be short.")
            self._proc.kill()
        except OSError:
            self._proc.kill()
        self._proc = None
        return self._captured()

    def __enter__(self) -> "SqlSession":
        return self.open()

    def __exit__(self, *exc) -> None:
        self.close()

    # ---------------------------------------------------------------- writes
    def execute(self, sql: str) -> None:
        """Send one or more statements. Raises SessionDied if the client is gone."""
        if self._proc is None or self._proc.poll() is not None:
            raise SessionDied(
                f"{self.name} client exited (rc={self._proc.poll() if self._proc else 'n/a'}).\n"
                f"{self._captured()}"
            )
        try:
            assert self._proc.stdin is not None
            self._proc.stdin.write(sql.rstrip().rstrip(";") + ";\n")
            self._proc.stdin.flush()
        except (BrokenPipeError, OSError) as e:
            raise SessionDied(f"{self.name} client stopped accepting input ({e}).\n"
                              f"{self._captured()}") from e
        self.statements += 1

    # ----------------------------------------------------------------- reads
    def query(self, sql: str) -> str:
        """One-shot read on its own connection. Not for the hot path."""
        r = subprocess.run(self._compose(*self._reader_argv(), sql),
                           capture_output=True, text=True, env=docker_env())
        if r.returncode != 0:
            raise RuntimeError(f"{self.name} query failed: {sql[:120]}\n{r.stderr[-800:]}")
        return r.stdout.strip()

    def query_rows(self, sql: str, sep: str = "\t") -> list[list[str]]:
        out = self.query(sql)
        return [line.split(sep) for line in out.splitlines() if line.strip()]

    def scalar(self, sql: str, default: str = "0") -> str:
        rows = self.query_rows(sql)
        return rows[0][0] if rows and rows[0] else default

    # ------------------------------------------------------------- internals
    @staticmethod
    def _compose(*args: str) -> list[str]:
        return [docker_bin(), "compose", "-f", str(COMPOSE), *args]

    def _captured(self) -> str:
        try:
            self._log.seek(0)
            return self._log.read()[-2000:]
        except (OSError, ValueError):
            return ""


class MySqlSession(SqlSession):
    name = "mysql"
    default_database = "ecommerce_tx"

    def _writer_argv(self) -> list[str]:
        return ["exec", "-T", "mysql",
                "mysql", "-uroot", "-prootpw", "--batch", self.database]

    def _reader_argv(self) -> list[str]:
        return ["exec", "-T", "mysql",
                "mysql", "-uroot", "-prootpw", "--batch", "--skip-column-names",
                self.database, "-e"]


class PostgresSession(SqlSession):
    name = "postgres"
    default_database = "ecommerce_md"

    # -q suppresses the per-statement command tag; ON_ERROR_STOP turns a rejected
    # statement into an exit rather than a silently skipped write.
    def _writer_argv(self) -> list[str]:
        return ["exec", "-T", "postgres", "psql", "-U", "cdc", "-d", self.database,
                "-v", "ON_ERROR_STOP=1", "-q"]

    def _reader_argv(self) -> list[str]:
        return ["exec", "-T", "postgres", "psql", "-U", "cdc", "-d", self.database,
                "-v", "ON_ERROR_STOP=1", "-t", "-A", "-F", "\t", "-c"]
