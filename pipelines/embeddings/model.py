"""The sentence-embedding model, loaded from the copy baked into the jobs image.

docker/jobs/Dockerfile downloads each model at a pinned revision into
/opt/models/<org>__<name>/ and writes that revision beside it. At run time the Hugging
Face hub is switched off (HF_HUB_OFFLINE=1), so a refresh can never silently pick up a
newer upload: E3/E4 compare runs against an identical model or not at all (M4-C4).
"""
from __future__ import annotations

import os
from pathlib import Path

MODELS_DIR = Path(os.environ.get("MOSAIC_MODELS_DIR", "/opt/models"))


def model_dir(name: str) -> Path:
    return MODELS_DIR / name.replace("/", "__")


def revision(name: str) -> str:
    f = model_dir(name) / "REVISION"
    return f.read_text(encoding="utf-8").strip() if f.is_file() else "unknown"


class Encoder:
    def __init__(self, name: str, dimensions: int):
        path = model_dir(name)
        if not path.is_dir():
            baked = sorted(p.name for p in MODELS_DIR.iterdir()) if MODELS_DIR.is_dir() else []
            raise RuntimeError(f"model '{name}' is not baked into this image (have: {baked}); "
                               f"add it to docker/jobs/Dockerfile")
        import warnings
        # transformers 4.44 warns about a tokenizer default changing in 4.45; the
        # version is pinned, so the warning is noise in every run's output.
        warnings.filterwarnings("ignore", category=FutureWarning, module="transformers")
        from sentence_transformers import SentenceTransformer
        self.name = name
        self.revision = revision(name)
        self._model = SentenceTransformer(str(path), device="cpu")
        got = self._model.get_sentence_embedding_dimension()
        if got != dimensions:
            raise RuntimeError(f"model '{name}' produces {got}-d vectors; the spec declares "
                               f"{dimensions}")

    def encode(self, texts: list[str], batch_size: int = 128):
        # Unit-normalised, so cosine distance (pgvector `<=>`) and dot similarity agree.
        return self._model.encode(texts, batch_size=batch_size, normalize_embeddings=True,
                                  convert_to_numpy=True, show_progress_bar=False)


def to_pgvector(vec) -> str:
    return "[" + ",".join(f"{float(x):.7g}" for x in vec) + "]"
