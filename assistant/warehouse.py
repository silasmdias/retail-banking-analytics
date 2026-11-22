"""A private, read-only copy of the marts.

The server never runs a query against the warehouse file. It copies the marts
into an in-memory DuckDB database, detaches the file and locks that database:

- The raw, staging and intermediate tables are not in the copy, so no query can
  reach them, whatever it says.
- File access, ``ATTACH``, ``COPY`` and extensions are switched off in the
  engine, and the switch cannot be turned back on.
- On top of that only one statement is accepted per call, and it has to be a
  ``SELECT``. The statement is classified by DuckDB's own parser, the same one
  that then runs it.

The copy also keeps the server out of the pipeline's way. DuckDB lets one
process write to a file, or several read it, never both. The server holds the
file only for the moment the copy takes; while the pipeline is writing, it keeps
answering from the last copy and says so.
"""

from __future__ import annotations

import math
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

SCHEMA = "marts"


class QueryRejected(Exception):
    """The statement is not allowed. The message says why and what is allowed."""


class QueryFailed(Exception):
    """DuckDB could not run the statement. The message is DuckDB's, with a hint."""


class WarehouseUnavailable(Exception):
    """There is no copy of the marts to answer from."""


@dataclass(frozen=True)
class QueryResult:
    columns: tuple[str, ...]
    rows: list[list[Any]]  # values are JSON-safe
    truncated: bool  # more rows existed than were asked for


@dataclass(frozen=True)
class Copy:
    """One copy of the marts, with what was learned while making it."""

    connection: duckdb.DuckDBPyConnection
    fingerprint: tuple[int, ...]  # of the warehouse file it was copied from
    copied_at: datetime
    row_counts: dict[str, int]  # table -> rows
    column_types: dict[str, dict[str, str]]  # table -> column -> type


class Marts:
    """Answers SELECT statements from an in-memory copy of the marts."""

    def __init__(
        self,
        warehouse: Path,
        tables: Sequence[str],
        *,
        quiet_seconds: float = 30.0,
        wait_seconds: float = 10.0,
        timeout_seconds: float = 15.0,
        memory_limit: str = "1GB",
    ) -> None:
        self.warehouse = warehouse
        self.tables = tuple(tables)
        # A warehouse modified less than this long ago is probably mid-run: the
        # pipeline writes in several steps and releases the file in between.
        # Copying then would show a half-finished run, and the copy holds the
        # file for an instant in which the next step could fail to open it.
        self.quiet_seconds = quiet_seconds
        # How long the first copy waits for a pipeline that is writing.
        self.wait_seconds = wait_seconds
        self.timeout_seconds = timeout_seconds
        self.memory_limit = memory_limit
        self._copy: Copy | None = None
        self._notice: str | None = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    # Queries
    # ------------------------------------------------------------------ #

    def query(self, sql: str, max_rows: int = 100) -> QueryResult:
        """Runs one SELECT statement and returns at most ``max_rows`` rows."""
        statement = self._only_select(sql)
        cursor = self.copy().connection.cursor()
        timer = threading.Timer(self.timeout_seconds, cursor.interrupt)
        timer.start()
        try:
            cursor.execute(statement)
            columns = tuple(column[0] for column in cursor.description or ())
            fetched = cursor.fetchmany(max_rows + 1)
        except duckdb.Error as error:
            raise self._explain(error) from error
        finally:
            timer.cancel()
            cursor.close()
        rows = [[json_safe(value) for value in row] for row in fetched[:max_rows]]
        return QueryResult(columns=columns, rows=rows, truncated=len(fetched) > max_rows)

    def _only_select(self, sql: str) -> duckdb.Statement:
        try:
            statements = duckdb.extract_statements(sql)
        except duckdb.Error as error:
            raise QueryFailed(f"DuckDB could not parse the SQL. {_message(error)}") from error
        if not statements:
            raise QueryRejected("The query is empty. Send one SELECT statement.")
        if len(statements) > 1:
            raise QueryRejected(
                f"Send one statement per call. This call had {len(statements)} statements."
            )
        kind = statements[0].type
        if kind != duckdb.StatementType.SELECT:
            raise QueryRejected(
                f"Only SELECT statements are allowed, and this is a {kind.name} statement. "
                "The server is read-only."
            )
        return statements[0]

    def _explain(self, error: duckdb.Error) -> Exception:
        if isinstance(error, duckdb.PermissionException):
            return QueryRejected(
                "Reading files, attaching databases and loading extensions are disabled. "
                f"Only these relations can be queried: {self._relations()}."
            )
        if isinstance(error, duckdb.InterruptException):
            return QueryFailed(
                f"The query ran for more than {self.timeout_seconds:g} seconds and was stopped. "
                "Aggregate or filter in SQL instead of fetching large results."
            )
        if isinstance(error, duckdb.OutOfMemoryException):
            return QueryFailed(
                f"The query needed more than {self.memory_limit} of memory and was stopped. "
                "Aggregate or filter in SQL."
            )
        message = _message(error)
        if isinstance(error, duckdb.CatalogException) and "Table with name" in message:
            return QueryFailed(
                f"{message}\nOnly the marts can be queried, as {SCHEMA}.<model>: "
                f"{self._relations()}. Raw, staging and intermediate tables are not available."
            )
        return QueryFailed(message)

    def _relations(self) -> str:
        return ", ".join(f"{SCHEMA}.{table}" for table in self.tables)

    # ------------------------------------------------------------------ #
    # The copy
    # ------------------------------------------------------------------ #

    @property
    def notice(self) -> str | None:
        """Set when the copy in use is older than the warehouse file."""
        return self._notice

    def copy(self) -> Copy:
        """The current copy of the marts, refreshed when the warehouse file has changed."""
        with self._lock:
            fingerprint = self._fingerprint()
            current = self._copy
            if current is not None:
                if fingerprint == current.fingerprint:
                    self._notice = None
                    return current
                if fingerprint is None:
                    self._notice = (
                        "The warehouse file is gone. These figures come from the copy made at "
                        f"{current.copied_at:%Y-%m-%d %H:%M} UTC."
                    )
                    return current
                if self._changed_moments_ago():
                    self._notice = _behind(current, "is being updated")
                    return current
                try:
                    self._copy = self._make_copy(fingerprint)
                    self._notice = None
                except _Busy:
                    self._notice = _behind(current, "is being written by the pipeline")
                except WarehouseUnavailable:
                    self._notice = _behind(current, "is being rebuilt")
                return self._copy
            if fingerprint is None:
                raise WarehouseUnavailable(
                    f"There is no warehouse at {self.warehouse}. Run `make build` to create it."
                )
            self._copy = self._first_copy()
            self._notice = None
            return self._copy

    def _first_copy(self) -> Copy:
        deadline = time.monotonic() + self.wait_seconds
        while True:
            try:
                return self._make_copy(self._fingerprint() or ())
            except _Busy:
                if time.monotonic() >= deadline:
                    raise WarehouseUnavailable(
                        "The pipeline is writing to the warehouse and no earlier copy of the "
                        "marts exists yet. Try again when the run has finished."
                    ) from None
                time.sleep(0.5)

    def _make_copy(self, fingerprint: tuple[int, ...]) -> Copy:
        connection = duckdb.connect(":memory:")
        try:
            path = self.warehouse.as_posix().replace("'", "''")
            connection.execute(f"ATTACH '{path}' AS source (READ_ONLY)")
        except duckdb.IOException as error:
            connection.close()
            if "lock" in str(error).lower():
                raise _Busy from error
            raise WarehouseUnavailable(
                f"The warehouse at {self.warehouse} could not be opened. {_message(error)}"
            ) from error

        built = {
            name
            for (name,) in connection.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_catalog = 'source' AND table_schema = ?",
                [SCHEMA],
            ).fetchall()
        }
        connection.execute(f'CREATE SCHEMA "{SCHEMA}"')
        row_counts = {}
        for table in self.tables:
            if table not in built:
                continue
            connection.execute(
                f'CREATE TABLE "{SCHEMA}"."{table}" AS SELECT * FROM source."{SCHEMA}"."{table}"'
            )
            row_counts[table] = connection.execute(
                f'SELECT count(*) FROM "{SCHEMA}"."{table}"'
            ).fetchall()[0][0]
        connection.execute("DETACH source")
        if not row_counts:
            connection.close()
            raise WarehouseUnavailable(
                f"The warehouse at {self.warehouse} has no marts yet. Run `make build`."
            )

        column_types: dict[str, dict[str, str]] = {table: {} for table in row_counts}
        for table, column, data_type in connection.execute(
            "SELECT table_name, column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = ? ORDER BY table_name, ordinal_position",
            [SCHEMA],
        ).fetchall():
            column_types[table][column] = data_type.lower()

        # From here on the database cannot reach anything outside itself, and
        # the last statement makes every one of these settings permanent.
        connection.execute(f"SET memory_limit = '{self.memory_limit}'")
        connection.execute("SET autoinstall_known_extensions = false")
        connection.execute("SET autoload_known_extensions = false")
        connection.execute("SET enable_external_access = false")
        connection.execute("SET lock_configuration = true")
        return Copy(
            connection=connection,
            fingerprint=fingerprint,
            copied_at=datetime.now(UTC).replace(tzinfo=None, microsecond=0),
            row_counts=row_counts,
            column_types=column_types,
        )

    def _fingerprint(self) -> tuple[int, ...] | None:
        """Changes whenever the warehouse file, or its write-ahead log, changes."""
        try:
            stat = self.warehouse.stat()
        except FileNotFoundError:
            return None
        fingerprint = [stat.st_mtime_ns, stat.st_size]
        try:
            log = self.warehouse.with_name(self.warehouse.name + ".wal").stat()
            fingerprint += [log.st_mtime_ns, log.st_size]
        except FileNotFoundError:
            pass
        return tuple(fingerprint)

    def _changed_moments_ago(self) -> bool:
        try:
            age = time.time() - self.warehouse.stat().st_mtime
        except FileNotFoundError:
            return False
        return age < self.quiet_seconds


class _Busy(Exception):
    """Another process holds the warehouse file for writing."""


def _behind(copy: Copy, reason: str) -> str:
    return (
        f"The warehouse {reason}. These figures come from the copy made at "
        f"{copy.copied_at:%Y-%m-%d %H:%M} UTC and may be one load behind."
    )


def _message(error: Exception, limit: int = 1200) -> str:
    text = str(error).strip()
    return text if len(text) <= limit else text[:limit].rstrip() + " ..."


def json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        return int(value) if value.as_tuple().exponent >= 0 else float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    return str(value)
