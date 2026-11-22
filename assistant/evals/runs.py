"""Recorded runs: what is written while a model answers, and how it is summarized.

A run lives in ``results/<name>/``:

    run.json                          model, settings and dataset of the run
    <condition>/results.jsonl         one line per question and repetition: the answer handed in
    <condition>/traces/<id>_rep<k>.json   every tool call and result that led to it
    <condition>/errors.jsonl          attempts that never produced an answer to grade

A line of ``results.jsonl`` holds the answer, not the verdict that matters: the
grade is recomputed from the answer and the answer key every time a summary is
made, so anyone can check the published figures without calling a model.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from assistant.evals.cases import CATEGORIES, Case
from assistant.evals.grading import (
    ABSTAINED,
    CORRECT,
    NO_ANSWER,
    WRONG,
    Interval,
    Submission,
    grade,
    interval,
    score_interval,
    share,
)

RESULTS = Path(__file__).with_name("results")

# The two ways the server is run. The second strips the dbt documentation from
# the tools, which measures what the documentation is worth.
CONDITIONS = {
    "docs": "With the dbt documentation",
    "schema_only": "Table and column names only",
}

# US dollars per million tokens, as published in November 2026. A cache write
# costs 1.25 times the input price; the cost of a run is an estimate from its
# token counts and these figures.
PRICES = {
    "claude-fable-5-2": {"input": 8.00, "output": 40.00, "cache_read": 0.20},
    "claude-opus-5-5": {"input": 4.00, "output": 20.00, "cache_read": 0.20},
    "claude-sonnet-5-5": {"input": 2.00, "output": 10.00, "cache_read": 0.10},
    "claude-haiku-5-5": {"input": 0.10, "output": 0.50, "cache_read": 0.01},
}


def cost_usd(model: str, usage: dict[str, int]) -> float | None:
    price = PRICES.get(model)
    if price is None:
        return None
    return (
        usage.get("input_tokens", 0) * price["input"]
        + usage.get("cache_creation_input_tokens", 0) * price["input"] * 1.25
        + usage.get("cache_read_input_tokens", 0) * price["cache_read"]
        + usage.get("output_tokens", 0) * price["output"]
    ) / 1_000_000


# --------------------------------------------------------------------------- #
# Files
# --------------------------------------------------------------------------- #


def read_jsonl(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def append_jsonl(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(row, ensure_ascii=False) + "\n")


def recorded_runs() -> list[Path]:
    return sorted(path.parent for path in RESULTS.glob("*/run.json"))


def read_run(run: Path) -> tuple[dict, dict[str, list[dict]]]:
    """The settings of a run and its result lines, per condition."""
    settings = json.loads((run / "run.json").read_text(encoding="utf-8"))
    rows = {
        condition: read_jsonl(run / condition / "results.jsonl")
        for condition in CONDITIONS
        if (run / condition / "results.jsonl").is_file()
    }
    return settings, rows


# --------------------------------------------------------------------------- #
# Summaries
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Summary:
    questions: int
    answers: int  # questions times repetitions, less the attempts that errored
    correct: Interval
    wrong: float
    abstained: float
    no_answer: float
    by_category: dict[str, tuple[int, float]]  # category -> (questions, share correct)
    per_case: dict[str, float]  # case id -> share of its repetitions answered correctly
    not_correct: dict[str, list[str]]  # case id -> outcomes other than correct
    mismatches: list[str]  # lines whose recorded grade differs from the recomputed one
    tool_calls: float
    tokens: float
    seconds: float
    cost: float | None  # of the whole condition


def regrade(rows: Iterable[dict], cases: Sequence[Case], key: dict[str, Any]) -> list[str]:
    """The grade of every line, recomputed from the answer it holds."""
    by_id = {case.id: case for case in cases}
    return [
        grade(
            by_id[row["case_id"]],
            key.get(row["case_id"]),
            Submission.from_json(row["submission"]),
        )
        for row in rows
    ]


def summarize(rows: list[dict], cases: Sequence[Case], key: dict[str, Any]) -> Summary:
    by_id = {case.id: case for case in cases}
    rows = [row for row in rows if row["case_id"] in by_id]
    outcomes = regrade(rows, cases, key)
    answered: dict[str, list[str]] = {}
    for row, outcome in zip(rows, outcomes, strict=True):
        answered.setdefault(row["case_id"], []).append(outcome)
    per_case = {case_id: share(results, CORRECT) for case_id, results in answered.items()}
    by_category = {}
    for category in CATEGORIES:
        scores = [
            per_case[case.id] for case in cases if case.category == category if case.id in per_case
        ]
        if scores:
            by_category[category] = (len(scores), sum(scores) / len(scores))
    costs = [cost_usd(row["model"], row["usage"]) for row in rows]
    return Summary(
        questions=len(per_case),
        answers=len(rows),
        correct=score_interval(list(per_case.values())),
        wrong=share(outcomes, WRONG),
        abstained=share(outcomes, ABSTAINED),
        no_answer=share(outcomes, NO_ANSWER),
        by_category=by_category,
        per_case=per_case,
        not_correct={
            case_id: [outcome for outcome in results if outcome != CORRECT]
            for case_id, results in answered.items()
            if any(outcome != CORRECT for outcome in results)
        },
        mismatches=[
            f"{row['case_id']} repetition {row['rep']}: recorded {row['outcome']}, now {outcome}"
            for row, outcome in zip(rows, outcomes, strict=True)
            if row.get("outcome") != outcome
        ],
        tool_calls=_mean(len(row["tool_calls"]) for row in rows),
        tokens=_mean(sum(row["usage"].values()) for row in rows),
        seconds=_mean(row["latency_s"] for row in rows),
        cost=None if any(cost is None for cost in costs) else sum(costs),
    )


def difference(first: Summary, second: Summary) -> Interval:
    """How many points more often the first condition is correct, question by question."""
    shared = sorted(first.per_case.keys() & second.per_case.keys())
    return interval([first.per_case[case] - second.per_case[case] for case in shared])


def _mean(values: Iterable[float]) -> float:
    values = list(values)
    return sum(values) / len(values) if values else math.nan


# --------------------------------------------------------------------------- #
# The report
# --------------------------------------------------------------------------- #


def report(settings: dict, summaries: dict[str, Summary]) -> str:
    """The run as Markdown tables, the form the README quotes."""
    client = settings.get("client", "api")
    reached = "the model API" if client == "api" else client.replace("claude-code", "Claude Code")
    lines = [
        f"Run `{settings['name']}`: `{settings['model']}` through {reached}, "
        f"{settings['repetitions']} repetition(s) per question, effort {settings['effort']}.",
        "",
        "| Server | Questions | Answers | Correct | 95% interval | Wrong answer "
        "| Said it cannot answer | No answer |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for condition, summary in summaries.items():
        lines.append(
            f"| {CONDITIONS[condition]} | {summary.questions} | {summary.answers} "
            f"| {_pct(summary.correct.value)} "
            f"| {_pct(summary.correct.low)} to {_pct(summary.correct.high)} "
            f"| {_pct(summary.wrong)} | {_pct(summary.abstained)} | {_pct(summary.no_answer)} |"
        )

    lines += ["", "Correct answers by kind of question:", ""]
    lines.append("| Kind | Questions | " + " | ".join(CONDITIONS[c] for c in summaries) + " |")
    lines.append("| --- | ---: | " + " | ".join("---:" for _ in summaries) + " |")
    for category in CATEGORIES:
        counts = [summary.by_category.get(category) for summary in summaries.values()]
        if not any(counts):
            continue
        questions = next(count[0] for count in counts if count)
        cells = " | ".join(_pct(count[1]) if count else "" for count in counts)
        lines.append(f"| {category} | {questions} | {cells} |")

    lines += ["", "Per question, on average:", ""]
    lines.append("| Server | Tool calls | Tokens | Seconds | Cost (US$) | Cost of the run (US$) |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")
    for condition, summary in summaries.items():
        per_answer = "" if summary.cost is None else f"{summary.cost / summary.answers:.4f}"
        total = "" if summary.cost is None else f"{summary.cost:.2f}"
        lines.append(
            f"| {CONDITIONS[condition]} | {summary.tool_calls:.1f} | {summary.tokens:,.0f} "
            f"| {summary.seconds:.1f} | {per_answer} | {total} |"
        )
    if client != "api":
        lines += [
            "",
            "The cost is what these tokens cost at the API's list prices. This run went "
            "through Claude Code under a subscription, so nothing was billed per token.",
        ]

    if len(summaries) == 2:
        first, second = summaries.values()
        gap = difference(first, second)
        lines += [
            "",
            f"Documentation against names only: {gap.value * 100:+.1f} points "
            f"(95% interval {gap.low * 100:+.1f} to {gap.high * 100:+.1f}).",
        ]

    for condition, summary in summaries.items():
        if summary.not_correct:
            lines += ["", f"Not correct, {CONDITIONS[condition].lower()}:", ""]
            lines += [
                f"- `{case_id}`: {', '.join(outcomes)}"
                for case_id, outcomes in sorted(summary.not_correct.items())
            ]
    return "\n".join(lines)


def _pct(value: float) -> str:
    return "" if math.isnan(value) else f"{value * 100:.1f}%"
