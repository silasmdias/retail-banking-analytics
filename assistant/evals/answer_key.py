"""The answer key: the right answer to every case, computed from the warehouse.

Reference answers are never typed in. Each one is the result of the case's
``reference_sql``, run through the same guarded path the server uses, which
also proves that every case can be answered with one SELECT over the marts.

Three checks run on the key before anything is graded with it:

- a case with ``check_sql`` gets the same answer through other tables;
- a case with ``naive_sql`` grades that query's result as wrong, so the case
  can tell the right answer from the plausible one;
- on the default dataset the key equals the one committed in
  ``expected/default_dataset.json``, so a change in a model or in the generator
  that moves an answer does not go unnoticed.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from assistant.evals.cases import Case
from assistant.evals.grading import CORRECT, WRONG, Submission, grade
from assistant.warehouse import Marts

COMMITTED_KEY = Path(__file__).with_name("expected") / "default_dataset.json"

# What `make build` generates. The committed key and the committed results belong to it.
DEFAULT_DATASET = {"seed": 42, "start_month": "2023-01", "end_month": "2025-12", "scale": 1.0}


class AnswerKeyError(Exception):
    """A case and the warehouse disagree. The message lists every problem found."""


def compute_key(cases: Sequence[Case], marts: Marts) -> dict[str, Any]:
    """The reference answer of every answerable case. Unanswerable cases have none."""
    key: dict[str, Any] = {}
    problems = []
    for case in cases:
        if not case.answerable:
            continue
        if case.expected is not None:
            key[case.id] = case.expected
            continue
        try:
            key[case.id] = _run(case, case.reference_sql, marts)
        except Exception as error:  # every broken case is reported, not just the first
            problems.append(f"{case.id}: reference_sql failed: {error}")
    if problems:
        raise AnswerKeyError("\n".join(problems))
    return key


def verify_key(cases: Sequence[Case], key: dict[str, Any], marts: Marts) -> list[str]:
    """Runs the second queries and the wrong queries. Returns one line per check passed."""
    passed, problems = [], []
    for case in cases:
        if case.check_sql:
            other = _run(case, case.check_sql, marts)
            if grade(case, key[case.id], Submission(True, other)) == CORRECT:
                passed.append(f"{case.id}: a second query through other tables agrees")
            else:
                problems.append(f"{case.id}: reference gives {key[case.id]}, check gives {other}")
        if case.naive_sql:
            naive = _run(case, case.naive_sql, marts)
            if grade(case, key[case.id], Submission(True, naive)) == WRONG:
                passed.append(
                    f"{case.id}: the plausible wrong query gives {_show(naive)}, "
                    f"not {_show(key[case.id])}, and is graded wrong"
                )
            else:
                problems.append(
                    f"{case.id}: the wrong query gives {naive}, which passes as {key[case.id]}"
                )
    if problems:
        raise AnswerKeyError("\n".join(problems))
    return passed


def compare_with_committed(key: dict[str, Any], committed: dict[str, Any]) -> None:
    """Raises when the computed key and the committed one differ."""
    problems = [
        f"{case_id}: committed {committed.get(case_id)!r}, warehouse gives {key.get(case_id)!r}"
        for case_id in sorted(key.keys() | committed.keys())
        if not _same(key.get(case_id), committed.get(case_id))
    ]
    if problems:
        raise AnswerKeyError("\n".join(problems))


def load_committed_key(path: Path = COMMITTED_KEY) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))["answers"]


def write_committed_key(key: dict[str, Any], path: Path = COMMITTED_KEY) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    document = {"dataset": DEFAULT_DATASET, "answers": dict(sorted(key.items()))}
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")


def delivered_dataset(raw_dir: Path) -> dict | None:
    """Settings of the dataset in the landing folder, from its control file."""
    control_file = raw_dir / "_manifest.json"
    if not control_file.is_file():
        return None
    return json.loads(control_file.read_text(encoding="utf-8")).get("settings")


def _run(case: Case, sql: str, marts: Marts) -> Any:
    result = marts.query(sql, max_rows=500)
    rows = result.rows
    if case.answer_type == "breakdown":
        if not rows or result.truncated or any(len(row) != 2 for row in rows):
            raise ValueError("a breakdown query returns rows of (label, number)")
        return {str(label): float(number) for label, number in rows}
    if len(rows) != 1 or len(rows[0]) != 1 or rows[0][0] is None:
        raise ValueError(f"expected one value, got {rows!r}")
    value = rows[0][0]
    if case.answer_type == "number":
        return float(value)
    if case.answer_type == "yes_no":
        return bool(value)
    return str(value)


def _same(left: Any, right: Any) -> bool:
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(_same(left[k], right[k]) for k in left)
    if isinstance(left, bool) or isinstance(right, bool):
        return left is right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return abs(left - right) <= 1e-6 * max(1.0, abs(left), abs(right))
    return left == right


def _show(value: Any) -> str:
    if not isinstance(value, float):
        return str(value)
    return f"{value:,.0f}" if abs(value) >= 1000 else f"{value:.4g}"
