"""A private workspace for every test: landing folder, warehouse and alerts in a temp dir."""

from __future__ import annotations

from pathlib import Path

import pytest

from generator.simulation import Settings, month_index, simulate
from generator.writer import write_dataset
from orchestration.paths import Paths, paths


def deliver(raw_dir: Path, end_month: tuple[int, int]) -> None:
    """Writes the landing folder as the source systems would at the end of ``end_month``.

    Small on purpose: 30% of the customers, so a full run takes seconds.
    """
    settings = Settings(
        start_month=month_index(2023, 1), end_month=month_index(*end_month), scale=0.3
    )
    write_dataset(simulate(settings), settings, raw_dir)


@pytest.fixture
def workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Paths:
    monkeypatch.setenv("RAW_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("WAREHOUSE_PATH", str(tmp_path / "warehouse.duckdb"))
    monkeypatch.setenv("ALERTS_DIR", str(tmp_path / "alerts"))
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("DAGSTER_HOME", raising=False)
    return paths()
