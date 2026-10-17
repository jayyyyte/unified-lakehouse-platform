"""Append-only run ledger, same convention as every other experiments/results/*.jsonl:
one JSON object per line, failures and refusals included."""
from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path

RESULTS = Path(os.environ.get("MOSAIC_RESULTS_DIR", "/opt/mosaic/results"))
FILE = RESULTS / "embedding_events.jsonl"


def append(event: dict) -> None:
    row = {"ts": dt.datetime.now(dt.timezone.utc).isoformat(), **event}
    RESULTS.mkdir(parents=True, exist_ok=True)
    with FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, default=str) + "\n")
