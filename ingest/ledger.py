"""The load ledger: one row per source file the warehouse holds.

``raw_meta.loaded_files`` records the content hash and row count of every file
at the time it was loaded. Comparing it with the landing folder tells which
files are new or were delivered again with different content, which is what
makes the load incremental and safe to repeat.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb

LEDGER = '"raw_meta"."loaded_files"'

_CREATE = f"""
    create table if not exists {LEDGER} (
        relative_path varchar primary key,
        source_system varchar not null,
        table_name varchar not null,
        file_name varchar not null,
        period varchar,
        sha256 varchar not null,
        file_rows bigint not null,
        loaded_rows bigint not null,
        loaded_at timestamp not null,
        load_id varchar not null
    )
"""


@dataclass(frozen=True)
class LedgerEntry:
    relative_path: str
    source_system: str
    table_name: str
    file_name: str
    period: str | None
    sha256: str
    file_rows: int
    loaded_rows: int
    loaded_at: datetime
    load_id: str


def ensure_ledger(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute('create schema if not exists "raw_meta"')
    connection.execute(_CREATE)


def record(connection: duckdb.DuckDBPyConnection, entry: LedgerEntry) -> None:
    connection.execute(
        f"insert or replace into {LEDGER} values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            entry.relative_path,
            entry.source_system,
            entry.table_name,
            entry.file_name,
            entry.period,
            entry.sha256,
            entry.file_rows,
            entry.loaded_rows,
            entry.loaded_at,
            entry.load_id,
        ],
    )


def read_ledger(database: Path, load_id: str | None = None) -> dict[str, LedgerEntry]:
    """Ledger entries by relative path; empty when nothing was loaded yet.

    Opens the warehouse read-only. DuckDB allows one writing process at a time,
    so this raises ``duckdb.IOException`` while a load or a dbt run is in progress.
    """
    if not database.exists():
        return {}
    query = f"select * from {LEDGER}" + (" where load_id = ?" if load_id else "")
    with duckdb.connect(str(database), read_only=True) as connection:
        try:
            rows = connection.execute(query, [load_id] if load_id else []).fetchall()
        except duckdb.CatalogException:
            return {}
    return {row[0]: LedgerEntry(*row) for row in rows}
