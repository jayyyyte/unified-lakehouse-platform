"""Load and validate domains/<domain>/retrieval.yaml -- the domain contract.

Everything a domain may tell the core passes through here. The validator is strict on
purpose: a spec it accepts must be runnable without the runner ever needing to know
what the domain is about, so every identifier is checked to be a plain SQL identifier
and every reference (an index's source, its target) must resolve to a declared dataset.

Identifiers are opaque strings from here on. The one piece of SQL a domain may write,
an index's `eligible` predicate, is inserted verbatim into a WHERE clause: it is
domain-authored and trusted to the same degree as the domain's Gold models.

Hand-written rather than JSON Schema: the jobs image would need another dependency,
and the error messages here name the offending key in domain terms.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
KINDS = ("source", "bronze", "silver", "gold", "vector")
SUPPORTED_VERSION = 1


class SpecError(ValueError):
    pass


# --------------------------------------------------------------------- datasets
@dataclass(frozen=True)
class Dataset:
    """One relation the platform can rank from or report freshness for.

    `id` doubles as the address. Three parts (catalog.schema.table) is a Trino
    relation; `postgres.<database>.<schema>.<table>` is a table in the PostgreSQL
    container, reached directly because Trino's connector cannot read vector columns.
    """
    id: str
    kind: str
    row_timestamp_column: str | None
    freshness_sla_seconds: int | None
    domain: str | None

    @property
    def engine(self) -> str:
        return "postgres" if self.id.startswith("postgres.") else "trino"

    @property
    def relation(self) -> str:
        """The name the engine knows it by."""
        return self.id.split(".", 1)[1] if self.engine == "postgres" else self.id

    @property
    def database(self) -> str | None:
        return self.id.split(".")[1] if self.engine == "postgres" else None

    @property
    def table(self) -> str:
        """schema.table within the engine's database (Postgres) or catalog (Trino)."""
        return ".".join(self.id.split(".")[-2:])


def _dataset_id(raw: str, where: str) -> str:
    parts = raw.split(".")
    if parts[0] == "postgres":
        if len(parts) != 4:
            raise SpecError(f"{where}: '{raw}' -- postgres ids are postgres.<db>.<schema>.<table>")
    elif len(parts) != 3:
        raise SpecError(f"{where}: '{raw}' -- Trino ids are <catalog>.<schema>.<table>")
    for p in parts:
        if not IDENT.match(p):
            raise SpecError(f"{where}: '{raw}' -- '{p}' is not a plain identifier")
    return raw


def _ident(raw, where: str) -> str:
    if not isinstance(raw, str) or not IDENT.match(raw):
        raise SpecError(f"{where}: {raw!r} is not a plain SQL identifier")
    return raw


def _positive_int(raw, where: str) -> int:
    if not isinstance(raw, int) or isinstance(raw, bool) or raw <= 0:
        raise SpecError(f"{where}: expected a positive integer, got {raw!r}")
    return raw


def parse_dataset(raw: dict, where: str, domain: str | None) -> Dataset:
    if not isinstance(raw, dict):
        raise SpecError(f"{where}: expected a mapping")
    unknown = set(raw) - {"id", "kind", "row_timestamp_column", "freshness_sla_seconds"}
    if unknown:
        raise SpecError(f"{where}: unknown key(s) {sorted(unknown)}")
    kind = raw.get("kind")
    if kind not in KINDS:
        raise SpecError(f"{where}: kind must be one of {KINDS}, got {kind!r}")
    ts = raw.get("row_timestamp_column")
    sla = raw.get("freshness_sla_seconds")
    return Dataset(
        id=_dataset_id(raw.get("id", ""), f"{where}.id"),
        kind=kind,
        row_timestamp_column=_ident(ts, f"{where}.row_timestamp_column") if ts else None,
        freshness_sla_seconds=_positive_int(sla, f"{where}.freshness_sla_seconds") if sla else None,
        domain=domain,
    )


# ------------------------------------------------------------------- embeddings
@dataclass(frozen=True)
class EmbeddingIndex:
    id: str
    model: str
    dimensions: int
    source: Dataset
    key: str
    text: tuple[str, ...]
    eligible: str | None
    snapshot: tuple[str, ...]
    target: Dataset
    m: int
    ef_construction: int
    freshness_sla_seconds: int

    # Columns the pipeline adds to every index table. Fixed names, so a reader (N1, the
    # retrieval runner) needs no per-domain configuration to find them.
    EMBEDDING = "embedding"
    REFRESHED_AT = "refreshed_at"   # watermark of the data read (M4-A3)
    EMBEDDED_AT = "embedded_at"     # wall clock of the job, for diagnosis only


@dataclass(frozen=True)
class Spec:
    domain: str
    entity_name: str
    entity_key: str
    datasets: tuple[Dataset, ...]
    embeddings: tuple[EmbeddingIndex, ...] = field(default=())

    def dataset(self, dataset_id: str) -> Dataset:
        for d in self.datasets:
            if d.id == dataset_id:
                return d
        raise KeyError(dataset_id)

    def index(self, index_id: str | None) -> EmbeddingIndex:
        if index_id is None:
            if len(self.embeddings) != 1:
                raise SpecError(f"domain '{self.domain}' declares {len(self.embeddings)} "
                                f"indexes; name one with --index")
            return self.embeddings[0]
        for e in self.embeddings:
            if e.id == index_id:
                return e
        raise SpecError(f"domain '{self.domain}' declares no index '{index_id}'")


def _parse_index(raw: dict, where: str, datasets: dict[str, Dataset], domain: str) -> EmbeddingIndex:
    for k in ("id", "model", "dimensions", "source", "target", "freshness_sla_seconds"):
        if k not in raw:
            raise SpecError(f"{where}: missing '{k}'")
    src = raw["source"] or {}
    for k in ("dataset", "key", "text"):
        if k not in src:
            raise SpecError(f"{where}.source: missing '{k}'")
    if src["dataset"] not in datasets:
        raise SpecError(f"{where}.source.dataset: '{src['dataset']}' is not declared under datasets")
    text = src["text"]
    if not isinstance(text, list) or not text:
        raise SpecError(f"{where}.source.text: expected a non-empty list of columns")
    snapshot = src.get("snapshot") or []
    eligible = src.get("eligible")
    if eligible is not None and (not isinstance(eligible, str) or ";" in eligible):
        raise SpecError(f"{where}.source.eligible: expected one SQL predicate without ';'")

    target_id = _dataset_id((raw["target"] or {}).get("dataset", ""), f"{where}.target.dataset")
    if not target_id.startswith("postgres."):
        raise SpecError(f"{where}.target.dataset: an index lives in PostgreSQL (pgvector); "
                        f"got '{target_id}'")
    sla = _positive_int(raw["freshness_sla_seconds"], f"{where}.freshness_sla_seconds")
    target = Dataset(id=target_id, kind="vector",
                     row_timestamp_column=EmbeddingIndex.REFRESHED_AT,
                     freshness_sla_seconds=sla, domain=domain)

    idx = raw.get("index") or {}
    if idx.get("method", "hnsw") != "hnsw":
        raise SpecError(f"{where}.index.method: only hnsw is supported")
    key = _ident(src["key"], f"{where}.source.key")
    reserved = {EmbeddingIndex.EMBEDDING, EmbeddingIndex.REFRESHED_AT, EmbeddingIndex.EMBEDDED_AT}
    for c in [*text, *snapshot]:
        _ident(c, f"{where}.source")
    clash = reserved & {key, *snapshot}
    if clash:
        raise SpecError(f"{where}.source: {sorted(clash)} are reserved index column names")
    if key in snapshot or len(set(snapshot)) != len(snapshot):
        raise SpecError(f"{where}.source.snapshot: repeats a column or the key")

    return EmbeddingIndex(
        id=_ident(raw["id"], f"{where}.id"),
        model=str(raw["model"]),
        dimensions=_positive_int(raw["dimensions"], f"{where}.dimensions"),
        source=datasets[src["dataset"]],
        key=key,
        text=tuple(text),
        eligible=eligible,
        snapshot=tuple(snapshot),
        target=target,
        m=_positive_int(idx.get("m", 16), f"{where}.index.m"),
        ef_construction=_positive_int(idx.get("ef_construction", 64), f"{where}.index.ef_construction"),
        freshness_sla_seconds=sla,
    )


def load(domains_dir: Path, domain: str) -> Spec:
    path = Path(domains_dir) / domain / "retrieval.yaml"
    if not path.is_file():
        raise SpecError(f"no spec at {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    where = path.as_posix()

    if raw.get("version") != SUPPORTED_VERSION:
        raise SpecError(f"{where}: version must be {SUPPORTED_VERSION}, got {raw.get('version')!r}")
    if raw.get("domain") != domain:
        raise SpecError(f"{where}: declares domain {raw.get('domain')!r}, expected '{domain}'")
    entity = raw.get("entity") or {}
    entity_name = _ident(entity.get("name"), f"{where}: entity.name")
    entity_key = _ident(entity.get("key"), f"{where}: entity.key")

    datasets: dict[str, Dataset] = {}
    for i, d in enumerate(raw.get("datasets") or []):
        ds = parse_dataset(d, f"{where}: datasets[{i}]", domain)
        if ds.id in datasets:
            raise SpecError(f"{where}: dataset '{ds.id}' declared twice")
        datasets[ds.id] = ds

    indexes = []
    for i, e in enumerate(raw.get("embeddings") or []):
        ix = _parse_index(e, f"{where}: embeddings[{i}]", datasets, domain)
        if ix.target.id in datasets:
            raise SpecError(f"{where}: '{ix.target.id}' is an index target; it is registered "
                            f"automatically and must not also be listed under datasets")
        if ix.key != entity_key:
            raise SpecError(f"{where}: embeddings[{i}].source.key '{ix.key}' differs from "
                            f"entity.key '{entity_key}'")
        indexes.append(ix)

    all_datasets = list(datasets.values()) + [ix.target for ix in indexes]
    return Spec(domain=domain, entity_name=entity_name, entity_key=entity_key,
                datasets=tuple(all_datasets), embeddings=tuple(indexes))


def load_platform_datasets(path: Path) -> list[Dataset]:
    """Datasets the platform owns itself (shared by every domain, domain = NULL)."""
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    return [parse_dataset(d, f"{Path(path).as_posix()}: datasets[{i}]", None)
            for i, d in enumerate(raw.get("datasets") or [])]
