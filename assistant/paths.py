"""Where the server finds the warehouse and the dbt documentation.

Defaults point inside the repository. ``WAREHOUSE_PATH`` and ``RAW_DIR`` are the
same variables the pipeline reads; ``DBT_MANIFEST_PATH`` moves the manifest.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Paths:
    warehouse: Path  # DuckDB file written by the pipeline
    manifest: Path  # dbt manifest: models, columns, descriptions, tests
    raw_dir: Path  # landing folder; only its control file is read, to name the dataset


def _resolve(variable: str, default: str) -> Path:
    value = os.environ.get(variable)
    return Path(value).expanduser().resolve() if value else REPO_ROOT / default


def paths() -> Paths:
    """Read on every call, so a test or a demo can point the server somewhere else."""
    return Paths(
        warehouse=_resolve("WAREHOUSE_PATH", "data/warehouse.duckdb"),
        manifest=_resolve("DBT_MANIFEST_PATH", "transform/target/manifest.json"),
        raw_dir=_resolve("RAW_DIR", "data/raw"),
    )
