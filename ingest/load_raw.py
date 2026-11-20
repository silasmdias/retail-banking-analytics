"""Loads the raw CSV extracts into DuckDB, one table per source table.

    <raw-dir>/<source_system>/<table>/*.csv  ->  raw_<source_system>.<table>

Every column is loaded as text. Typing and cleaning belong to the dbt staging
models, so a malformed value never breaks the load. Two audit columns are added:
``_source_file`` (the file a row came from) and ``_loaded_at`` (load timestamp).

Each run replaces the raw tables with the files currently on disk.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

import duckdb


def load(raw_dir: Path, database: Path) -> dict[str, int]:
    database.parent.mkdir(parents=True, exist_ok=True)
    loaded_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    row_counts: dict[str, int] = {}
    with duckdb.connect(str(database)) as connection:
        for system_dir in sorted(path for path in raw_dir.iterdir() if path.is_dir()):
            schema = f"raw_{system_dir.name}"
            connection.execute(f'create schema if not exists "{schema}"')
            for table_dir in sorted(path for path in system_dir.iterdir() if path.is_dir()):
                files = (table_dir / "*.csv").as_posix().replace("'", "''")
                relation = f'"{schema}"."{table_dir.name}"'
                connection.execute(
                    f"""
                    create or replace table {relation} as
                    select
                        * exclude (filename),
                        parse_filename(filename) as _source_file,
                        timestamp '{loaded_at}' as _loaded_at
                    from read_csv(
                        '{files}',
                        header = true,
                        all_varchar = true,
                        union_by_name = true,
                        filename = true
                    )
                    """
                )
                count = connection.execute(f"select count(*) from {relation}").fetchone()[0]
                row_counts[f"{schema}.{table_dir.name}"] = count
    return row_counts


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m ingest.load_raw",
        description="Load raw CSV extracts into the local DuckDB warehouse.",
    )
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--database", type=Path, default=Path("data/warehouse.duckdb"))
    args = parser.parse_args()

    if not args.raw_dir.is_dir():
        parser.error(f"{args.raw_dir} does not exist. Run `python -m generator` first.")
    for relation, count in load(args.raw_dir, args.database).items():
        print(f"{relation:<45} {count:>10,} rows")


if __name__ == "__main__":
    main()
