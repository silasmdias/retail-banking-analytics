"""Turns a submitted answer into a grade, with no model involved.

The model hands in its answer through one tool call with a fixed shape, so a
grade is a comparison: a number within the case's tolerance, a label that
matches, a yes or a no, or every entry of a breakdown.

A case ends in one of four ways:

    correct     the right answer, or "cannot be answered" for an unanswerable case
    wrong       an answer that is not right: the outcome that misleads someone
    abstained   "cannot be answered" for a case that can be: no harm done, no help either
    no_answer   the model stopped without handing in an answer
"""

from __future__ import annotations

import math
import random
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from assistant.evals.cases import Case

CORRECT, WRONG, ABSTAINED, NO_ANSWER = "correct", "wrong", "abstained", "no_answer"
OUTCOMES = (CORRECT, WRONG, ABSTAINED, NO_ANSWER)

# Float noise from summing and dividing, far below any tolerance a case asks for.
_EPSILON = 1e-9


@dataclass(frozen=True)
class Submission:
    """What the model handed in: either a value, or that the question cannot be answered."""

    answerable: bool
    value: Any = None  # float, str, bool, or {label: float} for a breakdown

    def to_json(self) -> dict:
        return {"answerable": self.answerable, "value": self.value}

    @classmethod
    def from_json(cls, data: dict | None) -> Submission | None:
        return None if data is None else cls(bool(data["answerable"]), data.get("value"))


class InvalidSubmission(ValueError):
    """The tool call does not carry an answer of the shape the case asks for."""


def read_submission(case: Case, tool_input: dict) -> Submission:
    """Reads the ``submit_answer`` tool call. Raises when the answer has the wrong shape."""
    if not isinstance(tool_input.get("answerable"), bool):
        raise InvalidSubmission("`answerable` must be true or false.")
    if not tool_input["answerable"]:
        return Submission(answerable=False)
    field = case.answer_type
    value = tool_input.get(field)
    if value is None:
        raise InvalidSubmission(
            f"This question asks for a {field.replace('_', '/')}: fill the `{field}` field, "
            "or set `answerable` to false."
        )
    if field == "number":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise InvalidSubmission("`number` must be a number.")
        return Submission(True, float(value))
    if field == "yes_no":
        if not isinstance(value, bool):
            raise InvalidSubmission("`yes_no` must be true or false.")
        return Submission(True, value)
    if field == "label":
        if not isinstance(value, str) or not value.strip():
            raise InvalidSubmission("`label` must be a non-empty text.")
        return Submission(True, value.strip())
    try:
        entries = {str(entry["label"]).strip(): float(entry["value"]) for entry in value}
    except (TypeError, KeyError, ValueError) as error:
        raise InvalidSubmission("`breakdown` must be a list of {label, value} entries.") from error
    if not entries or len(entries) != len(value):
        raise InvalidSubmission("`breakdown` needs one entry per label, each label once.")
    return Submission(True, entries)


def grade(case: Case, expected: Any, submission: Submission | None) -> str:
    """Compares a submission with the reference answer of the case."""
    if submission is None:
        return NO_ANSWER
    if not case.answerable:
        return WRONG if submission.answerable else CORRECT
    if not submission.answerable:
        return ABSTAINED
    return CORRECT if _matches(case, expected, submission.value) else WRONG


def _matches(case: Case, expected: Any, value: Any) -> bool:
    if case.answer_type == "number":
        return _close(expected, value, case.tolerance)
    if case.answer_type == "yes_no":
        return isinstance(value, bool) and value == expected
    if case.answer_type == "label":
        return isinstance(value, str) and _normal(value) == _normal(expected)
    if not isinstance(value, dict):
        return False
    given = {_normal(label): number for label, number in value.items()}
    wanted = {_normal(label): number for label, number in expected.items()}
    return given.keys() == wanted.keys() and all(
        _close(wanted[label], given[label], case.tolerance) for label in wanted
    )


def _close(expected: Any, value: Any, tolerance: float) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return False
    return abs(value - expected) <= tolerance + _EPSILON


def _normal(label: str) -> str:
    """`Mobile app`, `mobile_app` and `MOBILE-APP` are the same label."""
    return re.sub(r"[^a-z0-9]", "", label.lower())


# --------------------------------------------------------------------------- #
# Summaries
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Interval:
    value: float
    low: float
    high: float


def share(outcomes: Sequence[str], outcome: str) -> float:
    return sum(item == outcome for item in outcomes) / len(outcomes) if outcomes else math.nan


def interval(per_case: Sequence[float], resamples: int = 2000, seed: int = 0) -> Interval:
    """A mean over cases with a 95% bootstrap interval.

    Each case contributes one number, already averaged over its repetitions:
    repetitions of one question are not independent observations, so the cases
    are what gets resampled. The seed is fixed, so the interval is reproducible.
    """
    if not per_case:
        return Interval(math.nan, math.nan, math.nan)
    mean = sum(per_case) / len(per_case)
    generator = random.Random(seed)
    count = len(per_case)
    means = sorted(sum(generator.choices(per_case, k=count)) / count for _ in range(resamples))
    return Interval(mean, means[int(0.025 * resamples)], means[int(0.975 * resamples) - 1])
