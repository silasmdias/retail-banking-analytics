"""Reads the landing folder, decides what a delivery contains and checks it.

A delivery is whatever sits in the landing folder that the warehouse has not
loaded yet: new files, and files delivered again with different content. The
checks here run before anything is loaded, on the files themselves:

- ``delivery_is_complete``: the files on disk are the ones the control file lists.
- ``headers_match_contract``: every new file has the columns its feed promises.
- ``row_counts_match_control_file``: every new file holds as many records as the
  source says it sent, which catches a file cut short in transit.
- ``monthly_volume_is_plausible``: a monthly file is not far smaller or larger
  than the months before it, which catches a source that sent a partial extract
  and counted it correctly.

Each record is expected on its own line, as the source systems write it.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

from ingest.contract import FEEDS, MONTHLY, MONTHLY_FEEDS, Feed
from ingest.ledger import LedgerEntry, read_ledger

CONTROL_FILE = "_manifest.json"

# A monthly file is compared with the average of the three months before it,
# once six earlier months exist. The first months of a feed grow too fast for
# the comparison to mean anything.
VOLUME_BASELINE_MONTHS = 3
VOLUME_MINIMUM_HISTORY = 6
VOLUME_LOWER_RATIO = 0.5
VOLUME_UPPER_RATIO = 2.0


@dataclass(frozen=True)
class LandedFile:
    feed: Feed
    path: Path
    relative_path: str
    period: str | None  # None when the file name does not follow the feed's pattern
    header: tuple[str, ...]
    rows: int
    sha256: str

    @property
    def name(self) -> str:
        return self.path.name


def _inspect(path: Path) -> tuple[tuple[str, ...], int, str]:
    """Header, number of data rows and content hash of a CSV file, in one pass."""
    digest = hashlib.sha256()
    rows = 0
    last_byte = b""
    with path.open("rb") as handle:
        first_line = handle.readline()
        digest.update(first_line)
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
            rows += chunk.count(b"\n")
            last_byte = chunk[-1:]
    if last_byte and last_byte != b"\n":
        rows += 1
    header_text = first_line.decode("utf-8", errors="replace").rstrip("\r\n")
    header = tuple(next(csv.reader([header_text]), ()))
    return header, rows, digest.hexdigest()


def scan(raw_dir: Path) -> tuple[LandedFile, ...]:
    """Every CSV file in the landing folder that belongs to a known feed."""
    landed = []
    for feed in FEEDS:
        for path in sorted((raw_dir / feed.directory).glob("*.csv")):
            header, rows, sha256 = _inspect(path)
            landed.append(
                LandedFile(
                    feed=feed,
                    path=path,
                    relative_path=f"{feed.directory}/{path.name}",
                    period=feed.period_of(path.name),
                    header=header,
                    rows=rows,
                    sha256=sha256,
                )
            )
    return tuple(landed)


def read_control_file(raw_dir: Path) -> dict[str, int] | None:
    """Rows per file as declared by the source, or None when there is no control file."""
    path = raw_dir / CONTROL_FILE
    if not path.is_file():
        return None
    try:
        files = json.loads(path.read_text(encoding="utf-8")).get("files")
    except (json.JSONDecodeError, AttributeError):
        return None
    if not isinstance(files, dict):
        return None
    return {str(name): int(rows) for name, rows in files.items()}


def _files(count: int, adjective: str = "") -> str:
    return " ".join(
        part for part in (str(count), adjective, "file" if count == 1 else "files") if part
    )


def _months(files: tuple[LandedFile, ...]) -> tuple[str, ...]:
    return tuple(sorted({f.period for f in files if f.feed.cadence == MONTHLY and f.period}))


@dataclass(frozen=True)
class Delivery:
    landed: tuple[LandedFile, ...]
    pending: tuple[LandedFile, ...]
    control: Mapping[str, int] | None
    loaded: Mapping[str, LedgerEntry]

    @property
    def new_files(self) -> tuple[LandedFile, ...]:
        """Pending files the warehouse has never loaded."""
        return tuple(file for file in self.pending if file.relative_path not in self.loaded)

    @property
    def changed_files(self) -> tuple[LandedFile, ...]:
        """Pending files that were loaded before and came again with different content."""
        return tuple(file for file in self.pending if file.relative_path in self.loaded)

    @property
    def new_months(self) -> tuple[str, ...]:
        """Months arriving for the first time."""
        return _months(self.new_files)

    @property
    def months(self) -> tuple[str, ...]:
        """Months with at least one new or changed monthly file."""
        return _months(self.pending)

    @property
    def fingerprint(self) -> str:
        """Identifies this exact set of pending files; changes when a file is delivered again."""
        digest = hashlib.sha256()
        for file in sorted(self.pending, key=lambda f: f.relative_path):
            digest.update(f"{file.relative_path}:{file.sha256}\n".encode())
        return digest.hexdigest()[:16]

    def describe(self) -> str:
        if not self.pending:
            return "nothing new in the landing folder"
        parts = []
        if self.new_files:
            months = self.new_months
            if not months:
                span = ""
            elif len(months) == 1:
                span = f" for {months[0]}"
            else:
                span = f" for {len(months)} months, {months[0]} to {months[-1]}"
            parts.append(f"{_files(len(self.new_files), 'new')}{span}")
        if self.changed_files:
            parts.append(f"{_files(len(self.changed_files))} delivered again with changes")
        return " and ".join(parts)


def plan_delivery(raw_dir: Path, database: Path) -> Delivery:
    """Compares the landing folder with the load ledger."""
    landed = scan(raw_dir)
    loaded = read_ledger(database)
    pending = tuple(
        file
        for file in landed
        if file.relative_path not in loaded or loaded[file.relative_path].sha256 != file.sha256
    )
    return Delivery(landed, pending, read_control_file(raw_dir), loaded)


# --------------------------------------------------------------------------- #
# Checks
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class CheckOutcome:
    passed: bool
    summary: str
    problems: tuple[str, ...] = ()


def _outcome(problems: list[str], ok: str, failed: str) -> CheckOutcome:
    if not problems:
        return CheckOutcome(True, ok)
    more = f" (and {len(problems) - 1} more)" if len(problems) > 1 else ""
    return CheckOutcome(False, f"{failed}: {problems[0]}{more}", tuple(problems))


def check_delivery_is_complete(delivery: Delivery) -> CheckOutcome:
    if delivery.control is None:
        return _outcome(
            [f"no readable control file ({CONTROL_FILE}) in the landing folder"],
            "",
            "Delivery is incomplete",
        )

    problems = []
    on_disk = {file.relative_path for file in delivery.landed}
    for relative_path in sorted(set(delivery.control) - on_disk):
        problems.append(f"{relative_path} is listed in the control file but was not delivered")
    for relative_path in sorted(on_disk - set(delivery.control)):
        problems.append(f"{relative_path} was delivered but is not listed in the control file")
    for file in delivery.landed:
        if file.period is None:
            problems.append(
                f"{file.relative_path} does not follow the name {file.feed.expected_file_name}"
            )

    delivered = {(f.feed.table, f.period) for f in delivery.landed if f.feed.cadence == MONTHLY}
    months = sorted({period for _, period in delivered if period})
    for month in months:
        for feed in MONTHLY_FEEDS:
            if (feed.table, month) not in delivered:
                problems.append(f"{month} has no {feed.table} file")

    return _outcome(
        problems,
        f"{len(on_disk)} files on disk match the control file",
        "Delivery is incomplete",
    )


def check_headers_match_contract(delivery: Delivery) -> CheckOutcome:
    problems = []
    for file in delivery.pending:
        if file.header == file.feed.columns:
            continue
        missing = [column for column in file.feed.columns if column not in file.header]
        unexpected = [column for column in file.header if column not in file.feed.columns]
        if missing or unexpected:
            detail = "; ".join(
                part
                for part in (
                    f"missing {', '.join(missing)}" if missing else "",
                    f"unexpected {', '.join(unexpected)}" if unexpected else "",
                )
                if part
            )
        else:
            detail = "columns are in a different order"
        problems.append(f"{file.relative_path}: {detail}")
    return _outcome(
        problems,
        f"{len(delivery.pending)} new files have the expected columns",
        "Columns differ from the contract",
    )


def check_row_counts_match_control_file(delivery: Delivery) -> CheckOutcome:
    if delivery.control is None:
        return CheckOutcome(True, "Not evaluated: there is no control file to compare with")
    problems = []
    for file in delivery.pending:
        declared = delivery.control.get(file.relative_path)
        if declared is not None and declared != file.rows:
            problems.append(
                f"{file.relative_path}: control file says {declared:,} rows, "
                f"the file has {file.rows:,}"
            )
    return _outcome(
        problems,
        f"{len(delivery.pending)} new files hold the row counts the source declared",
        "Row count differs from the control file",
    )


def check_monthly_volume_is_plausible(delivery: Delivery) -> CheckOutcome:
    problems = []
    evaluated = 0
    for feed in MONTHLY_FEEDS:
        rows_by_month = {
            file.period: file.rows for file in delivery.landed if file.feed is feed and file.period
        }
        for file in delivery.pending:
            if file.feed is not feed or not file.period:
                continue
            earlier = [rows for month, rows in sorted(rows_by_month.items()) if month < file.period]
            if len(earlier) < VOLUME_MINIMUM_HISTORY:
                continue
            evaluated += 1
            baseline = sum(earlier[-VOLUME_BASELINE_MONTHS:]) / VOLUME_BASELINE_MONTHS
            ratio = file.rows / baseline if baseline else float("inf")
            if not VOLUME_LOWER_RATIO <= ratio <= VOLUME_UPPER_RATIO:
                problems.append(
                    f"{file.relative_path}: {file.rows:,} rows is {ratio:.0%} of the average "
                    f"of the previous {VOLUME_BASELINE_MONTHS} months ({baseline:,.0f})"
                )
    return _outcome(
        problems,
        f"{evaluated} monthly files are within {VOLUME_LOWER_RATIO:.0%} to "
        f"{VOLUME_UPPER_RATIO:.0%} of recent months",
        "Unusual monthly volume",
    )


@dataclass(frozen=True)
class DeliveryCheck:
    name: str
    description: str
    blocking: bool  # a failed blocking check stops the load; the others only warn
    evaluate: Callable[[Delivery], CheckOutcome]


DELIVERY_CHECKS = (
    DeliveryCheck(
        "delivery_is_complete",
        "The files in the landing folder are exactly the ones the control file lists, and "
        "every month has all of its monthly files.",
        True,
        check_delivery_is_complete,
    ),
    DeliveryCheck(
        "headers_match_contract",
        "Every new file has the columns its feed promises.",
        True,
        check_headers_match_contract,
    ),
    DeliveryCheck(
        "row_counts_match_control_file",
        "Every new file holds as many records as the source declared in the control file.",
        True,
        check_row_counts_match_control_file,
    ),
    DeliveryCheck(
        "monthly_volume_is_plausible",
        "A new monthly file is between half and double the average of the three months before it.",
        False,
        check_monthly_volume_is_plausible,
    ),
)
