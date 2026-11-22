"""Loads the raw CSV extracts into DuckDB, one table per source table.

    <raw-dir>/<source_system>/<table>/*.csv  ->  raw_<source_system>.<table>

Every column is loaded as text. Typing and cleaning belong to the dbt staging
models, so a malformed value never breaks the load. Two audit columns are added:
``_source_file`` (the file a row came from) and ``_loaded_at`` (load timestamp).

There are two ways to load:

- ``load`` replaces the raw tables with the files currently on disk. It is what
  ``make build`` uses to rebuild the warehouse from scratch.
- ``load_delivery`` loads only the files that are new or changed since the last
  load, in one transaction. A file delivered again replaces its own rows, so
  running it twice never duplicates data. It is what the orchestrated pipeline
  uses when a new month arrives.

Both record what they loaded in the ledger (``raw_meta.loaded_files``).
"""

from __future__ import annotations

import argparse
import uuid
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from ingest.delivery import Delivery, LandedFile, plan_delivery, scan
from ingest.ledger import LEDGER, LedgerEntry, ensure_ledger, record


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None, microsecond=0)


def _quoted(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def _entry(file: LandedFile, loaded_rows: int, loaded_at: datetime, load_id: str) -> LedgerEntry:
    return LedgerEntry(
        relative_path=file.relative_path,
        source_system=file.feed.system,
        table_name=file.feed.table,
        file_name=file.name,
        period=file.period or None,
        sha256=file.sha256,
        file_rows=file.rows,
        loaded_rows=loaded_rows,
        loaded_at=loaded_at,
        load_id=load_id,
    )


def load(raw_dir: Path, database: Path) -> dict[str, int]:
    """Full refresh: replaces every raw table with the files on disk."""
    database.parent.mkdir(parents=True, exist_ok=True)
    loaded_at = _now()
    load_id = f"full-refresh-{loaded_at:%Y%m%dT%H%M%S}"
    row_counts: dict[str, int] = {}
    with duckdb.connect(str(database)) as connection:
        for system_dir in sorted(path for path in raw_dir.iterdir() if path.is_dir()):
            schema = f"raw_{system_dir.name}"
            connection.execute(f'create schema if not exists "{schema}"')
            for table_dir in sorted(path for path in system_dir.iterdir() if path.is_dir()):
                relation = f'"{schema}"."{table_dir.name}"'
                connection.execute(
                    f"""
                    create or replace table {relation} as
                    select
                        * exclude (filename),
                        parse_filename(filename) as _source_file,
                        timestamp '{loaded_at}' as _loaded_at
                    from read_csv(
                        '{_quoted(table_dir / "*.csv")}',
                        header = true,
                        all_varchar = true,
                        union_by_name = true,
                        filename = true
                    )
                    """
                )
                count = connection.execute(f"select count(*) from {relation}").fetchone()[0]
                row_counts[f"{schema}.{table_dir.name}"] = count

        ensure_ledger(connection)
        connection.execute(f"delete from {LEDGER}")
        for file in scan(raw_dir):
            record(connection, _entry(file, _loaded_rows(connection, file), loaded_at, load_id))
    return row_counts


def _loaded_rows(connection: duckdb.DuckDBPyConnection, file: LandedFile) -> int:
    return connection.execute(
        f"select count(*) from {file.feed.relation} where _source_file = ?", [file.name]
    ).fetchone()[0]


def load_delivery(
    delivery: Delivery, database: Path, load_id: str | None = None
) -> list[LedgerEntry]:
    """Incremental load: loads the pending files of a delivery, all or nothing."""
    database.parent.mkdir(parents=True, exist_ok=True)
    loaded_at = _now()
    load_id = load_id or uuid.uuid4().hex
    entries = []
    with duckdb.connect(str(database)) as connection:
        connection.execute("begin transaction")
        try:
            ensure_ledger(connection)
            for file in delivery.pending:
                feed = file.feed
                columns = ", ".join(f'"{column}" varchar' for column in feed.columns)
                connection.execute(f'create schema if not exists "{feed.schema}"')
                connection.execute(
                    f"create table if not exists {feed.relation} "
                    f"({columns}, _source_file varchar, _loaded_at timestamp)"
                )
                connection.execute(
                    f"delete from {feed.relation} where _source_file = ?", [file.name]
                )
                connection.execute(
                    f"""
                    insert into {feed.relation} by name
                    select
                        * exclude (filename),
                        parse_filename(filename) as _source_file,
                        timestamp '{loaded_at}' as _loaded_at
                    from read_csv(
                        '{_quoted(file.path)}',
                        header = true,
                        all_varchar = true,
                        filename = true
                    )
                    """
                )
                entry = _entry(file, _loaded_rows(connection, file), loaded_at, load_id)
                record(connection, entry)
                entries.append(entry)
            connection.execute("commit")
        except Exception:
            connection.execute("rollback")
            raise
    return entries


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m ingest.load_raw",
        description="Load raw CSV extracts into the local DuckDB warehouse.",
    )
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--database", type=Path, default=Path("data/warehouse.duckdb"))
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="load only new or changed files instead of replacing the raw tables",
    )
    args = parser.parse_args()

    if not args.raw_dir.is_dir():
        parser.error(f"{args.raw_dir} does not exist. Run `python -m generator` first.")

    if args.incremental:
        delivery = plan_delivery(args.raw_dir, args.database)
        print(f"Delivery: {delivery.describe()}")
        for entry in load_delivery(delivery, args.database):
            print(f"{entry.relative_path:<70} {entry.loaded_rows:>10,} rows")
        return

    for relation, count in load(args.raw_dir, args.database).items():
        print(f"{relation:<45} {count:>10,} rows")


if __name__ == "__main__":
    main()
