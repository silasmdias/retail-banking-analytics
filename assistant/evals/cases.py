"""The evaluation set, read from ``questions.yml`` and validated.

A case is a question plus a way to know the right answer. The file's header
explains each field.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

QUESTIONS = Path(__file__).with_name("questions.yml")

CATEGORIES = ("lookup", "aggregation", "definition", "unanswerable")
ANSWER_TYPES = ("number", "label", "yes_no", "breakdown")

_FIELDS = {
    "id",
    "category",
    "question",
    "answer",
    "reference_sql",
    "check_sql",
    "naive_sql",
    "expected",
    "why",
}


class CaseError(ValueError):
    """``questions.yml`` breaks one of its own rules."""


@dataclass(frozen=True)
class Case:
    id: str
    category: str
    question: str
    answer_type: str
    tolerance: float = 0.0
    breakdown_by: str | None = None
    reference_sql: str | None = None
    check_sql: str | None = None
    naive_sql: str | None = None
    expected: Any = None
    why: str | None = None

    @property
    def answerable(self) -> bool:
        return self.category != "unanswerable"

    @property
    def answer_format(self) -> str:
        """What the model is told about the shape of the answer, and nothing about its value."""
        if self.answer_type == "number":
            return "Answer with one number."
        if self.answer_type == "yes_no":
            return "Answer yes or no."
        if self.answer_type == "label":
            return "Answer with one label, written exactly as it appears in the data."
        return (
            f"Answer with a breakdown: one entry per {self.breakdown_by}, each with its label, "
            "written exactly as it appears in the data, and its number."
        )

    @property
    def prompt(self) -> str:
        return f"{self.question.strip()}\n\n{self.answer_format}"


def load_cases(path: Path = QUESTIONS) -> list[Case]:
    entries = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(entries, list) or not entries:
        raise CaseError(f"{path.name} must be a list of cases")
    cases = [_case(entry) for entry in entries]
    repeated = {case.id for case in cases if sum(other.id == case.id for other in cases) > 1}
    if repeated:
        raise CaseError(f"case ids used more than once: {sorted(repeated)}")
    return cases


def _case(entry: dict) -> Case:
    name = entry.get("id", "<no id>")

    def require(condition: bool, message: str) -> None:
        if not condition:
            raise CaseError(f"case {name}: {message}")

    require(isinstance(entry.get("id"), str), "needs an id")
    require(not set(entry) - _FIELDS, f"unknown fields {sorted(set(entry) - _FIELDS)}")
    require(entry.get("category") in CATEGORIES, f"category must be one of {CATEGORIES}")
    require(isinstance(entry.get("question"), str), "needs a question")
    answer = entry.get("answer") or {}
    require(answer.get("type") in ANSWER_TYPES, f"answer.type must be one of {ANSWER_TYPES}")
    require(not set(answer) - {"type", "tolerance", "by"}, "answer accepts type, tolerance and by")
    case = Case(
        id=entry["id"],
        category=entry["category"],
        question=entry["question"],
        answer_type=answer["type"],
        tolerance=float(answer.get("tolerance", 0)),
        breakdown_by=answer.get("by"),
        reference_sql=entry.get("reference_sql"),
        check_sql=entry.get("check_sql"),
        naive_sql=entry.get("naive_sql"),
        expected=entry.get("expected"),
        why=entry.get("why"),
    )

    require(case.tolerance >= 0, "tolerance cannot be negative")
    require(
        (case.answer_type == "breakdown") == (case.breakdown_by is not None),
        "a breakdown, and only a breakdown, says what it is broken down `by`",
    )
    has_reference = case.reference_sql is not None
    has_fact = case.expected is not None
    if case.category == "unanswerable":
        require(
            not (has_reference or has_fact or case.check_sql or case.naive_sql),
            "an unanswerable case has no reference answer",
        )
    else:
        require(has_reference != has_fact, "needs either reference_sql or expected, not both")
    if case.category == "definition":
        require(has_fact, "a definition states its answer in `expected`")
    if case.category == "aggregation":
        require(case.naive_sql is not None, "an aggregation case shows the wrong query too")
    if has_fact:
        require(case.answer_type != "breakdown", "a breakdown comes from a query")
        kind = {"number": (int, float), "label": str, "yes_no": bool}[case.answer_type]
        is_bool = isinstance(case.expected, bool)
        require(
            isinstance(case.expected, kind) and (case.answer_type == "yes_no") == is_bool,
            f"expected must be a {case.answer_type}",
        )
    return case
