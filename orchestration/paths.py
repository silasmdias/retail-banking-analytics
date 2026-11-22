"""Where the pipeline reads and writes.

Defaults point inside the repository. Each location can be moved with an
environment variable; ``WAREHOUSE_PATH`` is the same variable the dbt profile reads.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = REPO_ROOT / "transform"


@dataclass(frozen=True)
class Paths:
    raw_dir: Path  # landing folder the source systems deliver to
    warehouse: Path  # DuckDB file
    alerts_dir: Path  # where alerts are written


def _resolve(variable: str, default: str) -> Path:
    value = os.environ.get(variable)
    return Path(value).expanduser().resolve() if value else REPO_ROOT / default


def paths() -> Paths:
    """Read on every call, so a test or a demo can point the pipeline somewhere else."""
    return Paths(
        raw_dir=_resolve("RAW_DIR", "data/raw"),
        warehouse=_resolve("WAREHOUSE_PATH", "data/warehouse.duckdb"),
        alerts_dir=_resolve("ALERTS_DIR", "data/alerts"),
    )
