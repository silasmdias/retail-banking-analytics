"""Shared fixtures: a small generated dataset, copied fresh for every test."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from generator.simulation import Settings, month_index, simulate
from generator.writer import write_dataset

FIRST_MONTH = month_index(2023, 1)


def deliver(raw_dir: Path, end_month: tuple[int, int]) -> dict:
    """Writes the landing folder as the source systems would at the end of ``end_month``."""
    settings = Settings(start_month=FIRST_MONTH, end_month=month_index(*end_month), scale=0.15)
    return write_dataset(simulate(settings), settings, raw_dir)


@pytest.fixture(scope="session")
def _generated(tmp_path_factory) -> Path:
    raw_dir = tmp_path_factory.mktemp("generated") / "raw"
    deliver(raw_dir, (2023, 10))
    return raw_dir


@pytest.fixture
def raw_dir(_generated: Path, tmp_path: Path) -> Path:
    """Ten months of files, January to October 2023."""
    target = tmp_path / "raw"
    shutil.copytree(_generated, target)
    return target


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "warehouse.duckdb"


def set_control_rows(raw_dir: Path, relative_path: str, rows: int) -> None:
    """Rewrites one entry of the control file, as a source that miscounted would."""
    path = raw_dir / "_manifest.json"
    manifest = json.loads(path.read_text())
    manifest["files"][relative_path] = rows
    path.write_text(json.dumps(manifest))


def keep_rows(path: Path, rows: int) -> None:
    """Cuts a CSV file down to its header and the first ``rows`` records."""
    lines = path.read_text().splitlines(keepends=True)
    path.write_text("".join(lines[: rows + 1]))
