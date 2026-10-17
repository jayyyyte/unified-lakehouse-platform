"""The embedding-input guard (N3): a column carrying a restricted tag may not be embedded.

GR1 section 1.1 names PII leaking through embeddings as a motivating gap, and both of
its masking layers stop at the relational boundary. This is the check that closes it:
before an index job reads a text column, it asks the tag store whether that column is
restricted, and refuses if so.

M4 ships the check and wires it into the only caller that exists (the index refresh).
What M4 does *not* ship is tag propagation along column lineage (M8): today
platform.column_tags holds only what has been written into it directly, so the guard
can refuse what it is told about and nothing more. The result says how many tags the
store knows in total so that an empty store is visible in every run's output rather
than reading as a clean bill of health. ADR 0006, decision M4-B3.
"""
from __future__ import annotations

from dataclasses import dataclass

RESTRICTED_TAGS = ("sensitive",)


@dataclass(frozen=True)
class GuardResult:
    dataset_id: str
    checked_columns: tuple[str, ...]
    blocked: tuple[tuple[str, str, str], ...]   # (column, tag, origin)
    tags_known: int

    @property
    def ok(self) -> bool:
        return not self.blocked

    def describe(self) -> str:
        if self.blocked:
            cols = ", ".join(f"{c} [{t}, {o}]" for c, t, o in self.blocked)
            return f"REFUSED: restricted column(s) in {self.dataset_id}: {cols}"
        note = "" if self.tags_known else " (tag store empty: N3 propagation lands in M8)"
        return (f"passed: {len(self.checked_columns)} column(s) of {self.dataset_id} carry no "
                f"restricted tag; {self.tags_known} tag(s) known{note}")


def check_embedding_inputs(platform_conn, dataset_id: str, columns) -> GuardResult:
    columns = tuple(columns)
    with platform_conn.cursor() as cur:
        cur.execute(
            "SELECT column_name, tag, origin FROM platform.column_tags "
            "WHERE dataset_id = %s AND column_name = ANY(%s) AND tag = ANY(%s) "
            "ORDER BY column_name, tag",
            (dataset_id, list(columns), list(RESTRICTED_TAGS)))
        blocked = tuple(tuple(r) for r in cur.fetchall())
        cur.execute("SELECT count(*) FROM platform.column_tags")
        known = cur.fetchone()[0]
    return GuardResult(dataset_id, columns, blocked, known)
