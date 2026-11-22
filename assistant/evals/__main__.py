"""The evaluation, from the command line.

    python -m assistant.evals check     free: verifies the answer key and the grader, then
                                        regrades the recorded runs and prints their results
    python -m assistant.evals cases     prints the questions with their reference answers
    python -m assistant.evals live      paid: a model answers the questions through the server
    python -m assistant.evals report    prints the results of the recorded runs

Only ``live`` calls a language model. It needs Anthropic credentials in the
environment (``ANTHROPIC_API_KEY``) and stops when its spending limit is reached.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any

import anyio

from assistant.catalog import load_catalog
from assistant.evals import answer_key, runs
from assistant.evals.cases import CATEGORIES, Case, load_cases
from assistant.evals.grading import CORRECT, Submission, grade
from assistant.paths import REPO_ROOT, paths
from assistant.warehouse import Marts

DEFAULT_MODEL = "claude-opus-5-5"
CREDENTIALS = "Set ANTHROPIC_API_KEY in the environment and run the command again."


def heading(text: str) -> None:
    print(f"\n{text}\n{'-' * len(text)}")


def open_marts() -> Marts:
    where = paths()
    return Marts(where.warehouse, [model.name for model in load_catalog(where.manifest).marts])


def is_default_dataset() -> bool | None:
    """None when the landing folder has no control file to tell."""
    dataset = answer_key.delivered_dataset(paths().raw_dir)
    return None if dataset is None else dataset == answer_key.DEFAULT_DATASET


# --------------------------------------------------------------------------- #
# check
# --------------------------------------------------------------------------- #


def check() -> int:
    cases = load_cases()
    heading("The questions")
    counts = Counter(case.category for case in cases)
    print(
        f"{len(cases)} questions: "
        + ", ".join(f"{counts[category]} {category}" for category in CATEGORIES)
    )

    heading("The answer key")
    marts = open_marts()
    key = answer_key.compute_key(cases, marts)
    queries = sum(case.reference_sql is not None for case in cases)
    print(
        f"{queries} reference answers computed from the warehouse, one SELECT over the marts each"
    )
    print(f"{len(key) - queries} reference answers stated by the documentation")
    passed = answer_key.verify_key(cases, key, marts)
    print(f"{sum('second query' in line for line in passed)} confirmed by a second query")
    print(
        f"{sum('wrong query' in line for line in passed)} plausible wrong queries graded as wrong"
    )
    default = is_default_dataset()
    if default is False:
        print(
            "The warehouse does not hold the default dataset, so the committed answer key "
            "and the recorded runs do not apply to it. `make build` goes back to the default."
        )
    else:
        answer_key.compare_with_committed(key, answer_key.load_committed_key())
        print("Equal to the answer key committed for the default dataset")

    heading("The grader")
    for label, outcomes in baselines(cases, key).items():
        print(f"{label}: {outcomes.count(CORRECT)} of {len(cases)} correct")

    heading("Recorded runs")
    return print_runs(cases, runs.recorded_runs())


def baselines(cases: list[Case], key: dict[str, Any]) -> dict[str, list[str]]:
    """Three answer sheets whose score is known in advance."""
    empty = {"number": 0.0, "label": "unknown", "yes_no": False, "breakdown": {"unknown": 0.0}}
    return {
        "Handing in the reference answers": [
            grade(case, key.get(case.id), Submission(case.answerable, key.get(case.id)))
            for case in cases
        ],
        'Always saying "cannot be answered"': [
            grade(case, key.get(case.id), Submission(False)) for case in cases
        ],
        'Always answering zero, "no" or "unknown"': [
            grade(case, key.get(case.id), Submission(True, empty[case.answer_type]))
            for case in cases
        ],
    }


def print_runs(cases: list[Case], recorded: list[Path]) -> int:
    if not recorded:
        print("No run is recorded yet. `make eval-live` records one.")
        return 0
    key = answer_key.load_committed_key()
    failed = 0
    for run in recorded:
        settings, rows = runs.read_run(run)
        if settings.get("dataset") != answer_key.DEFAULT_DATASET:
            print(f"Run `{settings['name']}` was recorded on another dataset and is not regraded.")
            continue
        summaries = {
            condition: runs.summarize(lines, cases, key) for condition, lines in rows.items()
        }
        print(runs.report(settings, summaries))
        for summary in summaries.values():
            for line in summary.mismatches:
                failed += 1
                print(f"MISMATCH {line}")
        print()
    if failed:
        print(f"{failed} recorded grades differ from the grades recomputed from the answers.")
    return 1 if failed else 0


# --------------------------------------------------------------------------- #
# cases
# --------------------------------------------------------------------------- #


def print_cases() -> int:
    cases = load_cases()
    key = answer_key.compute_key(cases, open_marts())
    print("| Question | Kind | Reference answer |\n| --- | --- | --- |")
    for case in cases:
        print(f"| {case.question.strip()} | {case.category} | {_show(case, key)} |")
    return 0


def _show(case: Case, key: dict[str, Any]) -> str:
    if not case.answerable:
        return "cannot be answered"
    value = key[case.id]
    if isinstance(value, dict):
        return "; ".join(f"{label}: {number:,.0f}" for label, number in sorted(value.items()))
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        digits = 0 if case.tolerance >= 1 or value == int(value) else 2
        return f"{value:,.{digits}f}"
    return str(value)


# --------------------------------------------------------------------------- #
# live
# --------------------------------------------------------------------------- #


async def live(args: argparse.Namespace) -> int:
    # Imported here so the free commands do not need the model client.
    import anthropic
    from mcp import Client, StdioServerParameters

    from assistant.evals.agent import ServedByAnotherModel, answer, server_tools

    if args.model not in runs.PRICES:
        print(
            f"No price is known for {args.model}, so the spending limit cannot be enforced. "
            "Add the model to PRICES in assistant/evals/runs.py."
        )
        return 2
    llm = anthropic.AsyncAnthropic(max_retries=4, timeout=120.0)
    if not (llm.api_key or llm.auth_token or llm.credentials):
        print(f"No Anthropic credentials were found. {CREDENTIALS}")
        return 2
    cases = load_cases()
    marts = open_marts()
    key = answer_key.compute_key(cases, marts)
    answer_key.verify_key(cases, key, marts)
    if args.cases:
        unknown = set(args.cases) - {case.id for case in cases}
        if unknown:
            print(f"Unknown cases: {sorted(unknown)}")
            return 2
        cases = [case for case in cases if case.id in args.cases]
    cases = cases[: args.limit] if args.limit else cases

    args.name = args.name or args.model
    run_dir = runs.RESULTS / args.name
    settings = {
        "name": args.name,
        "model": args.model,
        "effort": args.effort,
        "repetitions": args.repetitions,
        "dataset": answer_key.delivered_dataset(paths().raw_dir),
        "started_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "packages": {name: version(name) for name in ("anthropic", "mcp", "duckdb")},
        "prices_usd_per_million_tokens": runs.PRICES[args.model],
    }
    settings_file = run_dir / "run.json"
    if settings_file.is_file():
        earlier = json.loads(settings_file.read_text(encoding="utf-8"))
        changed = [k for k in ("model", "effort", "dataset") if earlier[k] != settings[k]]
        if changed:
            print(f"Run {args.name} exists with another {', '.join(changed)}. Pick a new --name.")
            return 2
        settings = {**earlier, "repetitions": max(earlier["repetitions"], args.repetitions)}
    run_dir.mkdir(parents=True, exist_ok=True)
    settings_file.write_text(json.dumps(settings, indent=2) + "\n", encoding="utf-8")

    spent = 0.0
    counts: Counter[str] = Counter()
    fatal: list[str] = []  # a problem no other question would survive either

    async def one(condition: str, server: Client, tools: list[dict], case: Case, rep: int) -> None:
        nonlocal spent
        folder = run_dir / condition
        if fatal:
            return
        if spent >= args.max_usd:
            counts["not run: spending limit"] += 1
            return
        error = None
        try:
            with anyio.fail_after(args.timeout):
                attempt = await answer(
                    case, model=args.model, llm=llm, server=server, tools=tools, effort=args.effort
                )
        except TimeoutError:
            error = "timeout"
        except ServedByAnotherModel as failure:
            error = f"served by another model: {failure}"
        except anthropic.APIConnectionError as failure:
            error = f"connection error: {failure}"
        except anthropic.APIStatusError as failure:
            if failure.status_code in (401, 403):
                fatal.append(f"The model API refused the credentials: {failure.message}")
                return
            error = f"api error {failure.status_code}: {failure.message}"
        except anthropic.AnthropicError as failure:
            fatal.append(f"The model client could not send a request: {failure}")
            return
        if error:
            counts["errors"] += 1
            runs.append_jsonl(
                folder / "errors.jsonl", {"case_id": case.id, "rep": rep, "error": error}
            )
            print(f"{condition:12} {case.id:44} #{rep}  ERROR {error}")
            return
        outcome = grade(case, key.get(case.id), attempt.submission)
        cost = runs.cost_usd(args.model, attempt.usage) or 0.0
        spent += cost
        counts[outcome] += 1
        traces = folder / "traces"
        traces.mkdir(parents=True, exist_ok=True)
        (traces / f"{case.id}_rep{rep}.json").write_text(
            json.dumps(attempt.trace, indent=1, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        runs.append_jsonl(
            folder / "results.jsonl",
            {
                "case_id": case.id,
                "rep": rep,
                "category": case.category,
                "condition": condition,
                "model": args.model,
                "stop": attempt.stop,
                "submission": attempt.submission.to_json() if attempt.submission else None,
                "explanation": attempt.explanation,
                "outcome": outcome,
                "turns": attempt.turns,
                "tool_calls": attempt.tool_calls,
                # The SQL the model wrote: the first place to look when an answer is wrong.
                "queries": [
                    entry["content"].get("sql")
                    for entry in attempt.trace
                    if entry["role"] == "tool_call" and entry["name"] == "run_query"
                ],
                "usage": attempt.usage,
                "latency_s": attempt.latency_s,
            },
        )
        print(
            f"{condition:12} {case.id:44} #{rep}  {outcome:9} "
            f"{len(attempt.tool_calls)} calls  ${cost:.4f}  (${spent:.2f} so far)"
        )

    limiter = anyio.CapacityLimiter(args.concurrency)

    async def limited(*item: Any) -> None:
        async with limiter:
            await one(*item)

    for condition in args.conditions:
        done = {
            (row["case_id"], row["rep"])
            for row in runs.read_jsonl(run_dir / condition / "results.jsonl")
        }
        pending = [
            (case, rep)
            for rep in range(args.repetitions)
            for case in cases
            if (case.id, rep) not in done
        ]
        if not pending:
            continue
        flags = ["--no-docs"] if condition == "schema_only" else []
        launch = StdioServerParameters(
            command=sys.executable,
            args=["-m", "assistant", *flags],
            env=dict(os.environ),
            cwd=str(REPO_ROOT),
        )
        async with Client(launch) as server:
            tools = await server_tools(server)
            # The first question runs alone: bad credentials stop the run right there.
            await one(condition, server, tools, *pending[0])
            async with anyio.create_task_group() as group:
                for item in pending[1:]:
                    group.start_soon(limited, condition, server, tools, *item)
    if fatal:
        print(f"\n{fatal[0]}\n{CREDENTIALS}")
        return 2

    heading("This run")
    print(
        ", ".join(f"{count} {label}" for label, count in sorted(counts.items())) or "nothing to do"
    )
    print(f"Estimated cost of this command: US$ {spent:.2f}\n")
    settings, rows = runs.read_run(run_dir)
    print(
        runs.report(
            settings,
            {condition: runs.summarize(lines, cases, key) for condition, lines in rows.items()},
        )
    )
    if counts["not run: spending limit"]:
        print(f"\nThe limit of US$ {args.max_usd:.2f} was reached. Raise --max-usd and run again.")
        return 3
    if counts["errors"]:
        print("\nRun the same command again to retry the attempts that errored.")
        return 1
    return 0


# --------------------------------------------------------------------------- #
# Command line
# --------------------------------------------------------------------------- #


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m assistant.evals", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("check", help="verify the answer key and regrade the recorded runs")
    commands.add_parser("cases", help="print the questions with their reference answers")
    commands.add_parser("report", help="print the results of the recorded runs")
    commands.add_parser(
        "update-key", help="rewrite the committed answer key from the default dataset"
    )
    run = commands.add_parser("live", help="have a model answer the questions (paid)")
    run.add_argument("--model", default=DEFAULT_MODEL)
    run.add_argument(
        "--name", help="folder under results/ for this run; the model's name by default"
    )
    run.add_argument("--effort", default="medium", help="reasoning effort sent to the model")
    run.add_argument("--repetitions", type=int, default=1, help="answers per question")
    run.add_argument(
        "--conditions", nargs="+", choices=list(runs.CONDITIONS), default=list(runs.CONDITIONS)
    )
    run.add_argument("--cases", nargs="*", help="ids of the cases to run; all by default")
    run.add_argument("--limit", type=int, help="run only the first N cases, for a pilot")
    run.add_argument(
        "--max-usd", type=float, default=2.0, help="stop starting questions at this cost"
    )
    run.add_argument("--concurrency", type=int, default=4)
    run.add_argument("--timeout", type=float, default=300.0, help="seconds per question")
    args = parser.parse_args(argv)

    try:
        if args.command == "check":
            return check()
        if args.command == "cases":
            return print_cases()
        if args.command == "report":
            return print_runs(load_cases(), runs.recorded_runs())
        if args.command == "update-key":
            if is_default_dataset() is not True:
                print("The committed key belongs to the default dataset. Run `make build` first.")
                return 2
            cases = load_cases()
            marts = open_marts()
            key = answer_key.compute_key(cases, marts)
            answer_key.verify_key(cases, key, marts)
            answer_key.write_committed_key(key)
            print(f"Wrote {len(key)} reference answers to {answer_key.COMMITTED_KEY.name}")
            return 0
        return anyio.run(live, args)
    except answer_key.AnswerKeyError as error:
        print(f"\nThe answer key does not hold:\n{error}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
