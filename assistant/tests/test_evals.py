"""The evaluation: cases, answer key, grader, and the loop a model answers through.

No test calls a language model. Where one is needed, a scripted stand-in plays
it: it returns the tool calls a model would make, in order, and the real loop,
the real protocol and a real server do the rest.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import anyio
import pytest
from mcp import Client

from assistant.catalog import Catalog
from assistant.evals import __main__ as command
from assistant.evals import answer_key, runs
from assistant.evals.agent import REMINDER, ServedByAnotherModel, answer, server_tools
from assistant.evals.cases import Case, CaseError, load_cases
from assistant.evals.grading import (
    ABSTAINED,
    CORRECT,
    NO_ANSWER,
    WRONG,
    InvalidSubmission,
    Submission,
    grade,
    interval,
    read_submission,
    score_interval,
)
from assistant.server import build_server
from assistant.warehouse import Marts

TOTAL = "SELECT sum(total_revenue) FROM marts.revenue_by_product_channel"

REVENUE = Case(
    id="revenue",
    category="aggregation",
    question="What was total revenue? Answer in US dollars, to the nearest dollar.",
    answer_type="number",
    tolerance=1,
    reference_sql=TOTAL,
    check_sql="SELECT 100.50 + 200.25 + 300.00",
    naive_sql="SELECT avg(total_revenue) FROM marts.revenue_by_product_channel",
)
BY_CHANNEL = Case(
    id="by-channel",
    category="lookup",
    question="What was total revenue for each sales channel?",
    answer_type="breakdown",
    breakdown_by="sales channel",
    tolerance=1,
    reference_sql="SELECT sales_channel, sum(total_revenue) "
    "FROM marts.revenue_by_product_channel GROUP BY 1",
)
TOP_PRODUCT = Case(
    id="top-product",
    category="lookup",
    question="Which product has the most revenue?",
    answer_type="label",
    reference_sql="SELECT product_name FROM marts.dim_products WHERE product_id = 'CHK_STD'",
)
CAC_EXCLUDED = Case(
    id="cac-excluded",
    category="definition",
    question="Is acquisition cost excluded from contribution margin?",
    answer_type="yes_no",
    expected=True,
)
NO_SUCH_DATA = Case(
    id="no-such-data",
    category="unanswerable",
    question="What is the net promoter score?",
    answer_type="number",
)
CASES = [REVENUE, BY_CHANNEL, TOP_PRODUCT, CAC_EXCLUDED, NO_SUCH_DATA]


# --------------------------------------------------------------------------- #
# The questions
# --------------------------------------------------------------------------- #


def test_the_question_file_is_valid_and_balanced() -> None:
    cases = load_cases()
    assert len({case.id for case in cases}) == len(cases) >= 40
    kinds = {
        category: sum(case.category == category for case in cases)
        for category in command.CATEGORIES
    }
    assert all(count >= 8 for count in kinds.values()), kinds
    # Yes and no are both right somewhere, so neither can be answered blindly.
    assert {case.expected for case in cases if case.answer_type == "yes_no"} == {True, False}
    for case in cases:
        assert case.question.strip().endswith(("?", "."))
        assert case.answer_format in case.prompt


def test_the_answer_format_gives_nothing_away() -> None:
    assert NO_SUCH_DATA.answer_format == REVENUE.answer_format == "Answer with one number."
    assert "sales channel" in BY_CHANNEL.answer_format


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"category": "trivia"}, "category must be one of"),
        ({"answer": {"type": "essay"}}, "answer.type must be one of"),
        ({"surprise": 1}, "unknown fields"),
        ({"reference_sql": None}, "needs either reference_sql or expected"),
        ({"expected": 5}, "needs either reference_sql or expected"),
        ({"naive_sql": None}, "shows the wrong query too"),
        ({"category": "unanswerable"}, "has no reference answer"),
        ({"answer": {"type": "breakdown"}}, "broken down `by`"),
    ],
)
def test_a_case_that_breaks_the_rules_is_refused(
    tmp_path: Path, change: dict, message: str
) -> None:
    entry = {
        "id": "case",
        "category": "aggregation",
        "question": "How much?",
        "answer": {"type": "number", "tolerance": 1},
        "reference_sql": "SELECT 1",
        "naive_sql": "SELECT 2",
    }
    entry = {key: value for key, value in {**entry, **change}.items() if value is not None}
    path = tmp_path / "questions.yml"
    path.write_text(json.dumps([entry]), encoding="utf-8")  # JSON is YAML
    with pytest.raises(CaseError, match=message):
        load_cases(path)


def test_two_cases_cannot_share_an_id(tmp_path: Path) -> None:
    entry = {"id": "same", "category": "unanswerable", "question": "?", "answer": {"type": "label"}}
    path = tmp_path / "questions.yml"
    path.write_text(json.dumps([entry, entry]), encoding="utf-8")
    with pytest.raises(CaseError, match="more than once"):
        load_cases(path)


# --------------------------------------------------------------------------- #
# The grader
# --------------------------------------------------------------------------- #


def test_a_number_is_correct_within_the_tolerance_of_the_case() -> None:
    assert grade(REVENUE, 600.75, Submission(True, 601.0)) == CORRECT
    assert grade(REVENUE, 600.75, Submission(True, 600.0)) == CORRECT
    assert grade(REVENUE, 600.75, Submission(True, 602.0)) == WRONG
    assert grade(REVENUE, 600.75, Submission(True, float("nan"))) == WRONG
    assert grade(REVENUE, 600.75, Submission(True, "600.75")) == WRONG


def test_a_label_matches_whatever_its_spelling() -> None:
    assert grade(TOP_PRODUCT, "mobile_app", Submission(True, "Mobile app")) == CORRECT
    assert grade(TOP_PRODUCT, "mobile_app", Submission(True, "mobile")) == WRONG


def test_a_breakdown_needs_every_label_and_every_number() -> None:
    expected = {"branch": 400.5, "online": 200.25}
    assert grade(BY_CHANNEL, expected, Submission(True, {"Branch": 401, "ONLINE": 200})) == CORRECT
    assert grade(BY_CHANNEL, expected, Submission(True, {"branch": 400.5})) == WRONG
    assert grade(BY_CHANNEL, expected, Submission(True, {"branch": 400.5, "online": 250})) == WRONG
    extra = {"branch": 400.5, "online": 200.25, "partner": 0}
    assert grade(BY_CHANNEL, expected, Submission(True, extra)) == WRONG


def test_yes_and_no_are_exact() -> None:
    assert grade(CAC_EXCLUDED, True, Submission(True, True)) == CORRECT
    assert grade(CAC_EXCLUDED, True, Submission(True, False)) == WRONG
    assert grade(CAC_EXCLUDED, True, Submission(True, 1)) == WRONG


def test_the_four_outcomes_are_told_apart() -> None:
    assert grade(REVENUE, 600.75, Submission(False)) == ABSTAINED
    assert grade(REVENUE, 600.75, None) == NO_ANSWER
    assert grade(NO_SUCH_DATA, None, Submission(False)) == CORRECT
    # An invented figure for a question that has no answer is the worst outcome.
    assert grade(NO_SUCH_DATA, None, Submission(True, 42.0)) == WRONG
    assert grade(NO_SUCH_DATA, None, None) == NO_ANSWER


def test_a_submission_is_read_from_the_field_the_case_asks_for() -> None:
    assert read_submission(REVENUE, {"answerable": True, "number": 601}) == Submission(True, 601.0)
    assert read_submission(REVENUE, {"answerable": False, "number": 601}) == Submission(False)
    assert read_submission(CAC_EXCLUDED, {"answerable": True, "yes_no": False}).value is False
    assert read_submission(TOP_PRODUCT, {"answerable": True, "label": " Loan "}).value == "Loan"
    entries = [{"label": "branch", "value": 400.5}, {"label": "online", "value": 200}]
    assert read_submission(BY_CHANNEL, {"answerable": True, "breakdown": entries}).value == {
        "branch": 400.5,
        "online": 200.0,
    }


@pytest.mark.parametrize(
    ("case", "tool_input", "message"),
    [
        (REVENUE, {"number": 1}, "`answerable` must be true or false"),
        (REVENUE, {"answerable": True, "label": "600"}, "fill the `number` field"),
        (REVENUE, {"answerable": True, "number": True}, "must be a number"),
        (CAC_EXCLUDED, {"answerable": True, "yes_no": "no"}, "must be true or false"),
        (TOP_PRODUCT, {"answerable": True, "label": "  "}, "non-empty text"),
        (BY_CHANNEL, {"answerable": True, "breakdown": [{"label": "branch"}]}, "list of"),
        (BY_CHANNEL, {"answerable": True, "breakdown": []}, "one entry per label"),
    ],
)
def test_a_submission_of_the_wrong_shape_is_sent_back(
    case: Case, tool_input: dict, message: str
) -> None:
    with pytest.raises(InvalidSubmission, match=message):
        read_submission(case, tool_input)


def test_the_interval_is_reproducible_and_contains_the_mean() -> None:
    scores = [1.0] * 30 + [0.0] * 10
    first, second = interval(scores), interval(scores)
    assert first == second
    assert first.value == 0.75 and first.low < 0.75 < first.high
    assert 0.55 < first.low and first.high < 0.9
    assert interval([1.0, 1.0]).low == interval([1.0, 1.0]).high == 1.0


def test_a_perfect_score_still_has_an_interval() -> None:
    """44 right out of 44 does not show that the true rate is 100%."""
    perfect = score_interval([1.0] * 44)
    assert perfect.value == 1.0 and perfect.high == 1.0
    assert perfect.low == pytest.approx(0.920, abs=0.001)
    # A question answered right two times out of three counts as two thirds of a question.
    mixed = score_interval([1.0] * 41 + [2 / 3, 2 / 3, 0.0])
    assert mixed.value == pytest.approx(0.962, abs=0.001)
    assert (mixed.low, mixed.high) == pytest.approx((0.859, 0.991), abs=0.001)
    assert score_interval([0.0] * 10).low == 0.0
    assert score_interval([]).value != score_interval([]).value  # nan: nothing was answered


# --------------------------------------------------------------------------- #
# The answer key
# --------------------------------------------------------------------------- #


def test_the_key_is_computed_from_the_warehouse(marts: Marts) -> None:
    key = answer_key.compute_key(CASES, marts)
    assert key == {
        "revenue": 600.75,
        "by-channel": {"branch": 400.5, "online": 200.25},
        "top-product": "Everyday Checking",
        "cac-excluded": True,
    }
    passed = answer_key.verify_key(CASES, key, marts)
    assert len(passed) == 2 and "graded wrong" in passed[1]


def test_a_reference_query_outside_the_marts_is_reported(marts: Marts) -> None:
    raw = Case(
        "raw",
        "lookup",
        "?",
        "number",
        reference_sql="SELECT count(*) FROM raw_core_banking.customers",
    )
    many = Case("many", "lookup", "?", "number", reference_sql="SELECT * FROM marts.dim_products")
    with pytest.raises(answer_key.AnswerKeyError) as error:
        answer_key.compute_key([raw, many], marts)
    assert "raw: reference_sql failed" in str(error.value)
    assert "many: reference_sql failed: expected one value" in str(error.value)


def test_a_second_query_that_disagrees_is_reported(marts: Marts) -> None:
    case = Case(
        "revenue", "lookup", "?", "number", tolerance=1, reference_sql=TOTAL, check_sql="SELECT 1"
    )
    with pytest.raises(answer_key.AnswerKeyError, match="check gives 1.0"):
        answer_key.verify_key([case], answer_key.compute_key([case], marts), marts)


def test_a_wrong_query_the_grader_would_accept_is_reported(marts: Marts) -> None:
    case = Case(
        "revenue",
        "aggregation",
        "?",
        "number",
        tolerance=1,
        reference_sql=TOTAL,
        naive_sql="SELECT 601",
    )
    with pytest.raises(answer_key.AnswerKeyError, match="passes as 600.75"):
        answer_key.verify_key([case], answer_key.compute_key([case], marts), marts)


def test_a_key_that_moved_is_reported(tmp_path: Path) -> None:
    path = tmp_path / "key.json"
    answer_key.write_committed_key({"revenue": 600.75, "top": "Loan", "split": {"a": 1.0}}, path)
    committed = answer_key.load_committed_key(path)
    answer_key.compare_with_committed(
        {"revenue": 600.75, "top": "Loan", "split": {"a": 1.0}}, committed
    )
    with pytest.raises(answer_key.AnswerKeyError) as error:
        answer_key.compare_with_committed({"revenue": 601.0, "top": "Loan", "new": 1.0}, committed)
    assert "revenue: committed 600.75, warehouse gives 601.0" in str(error.value)
    assert "new: committed None" in str(error.value) and "split: committed" in str(error.value)


def test_the_dataset_is_named_by_its_control_file(tmp_path: Path) -> None:
    assert answer_key.delivered_dataset(tmp_path) is None
    (tmp_path / "_manifest.json").write_text(json.dumps({"settings": answer_key.DEFAULT_DATASET}))
    assert answer_key.delivered_dataset(tmp_path) == answer_key.DEFAULT_DATASET


# --------------------------------------------------------------------------- #
# The loop a model answers through
# --------------------------------------------------------------------------- #


def tool(tool_name: str, /, **arguments: Any) -> SimpleNamespace:
    return SimpleNamespace(type="tool_use", id=f"call_{tool_name}", name=tool_name, input=arguments)


def text(words: str) -> SimpleNamespace:
    return SimpleNamespace(type="text", text=words)


class ScriptedModel:
    """Stands in for the model client: answers each request with the next scripted turn."""

    def __init__(
        self,
        turns: list[list[SimpleNamespace]],
        *,
        served_by: str = "test-model",
        stop: str | None = None,
    ) -> None:
        self.turns = list(turns)
        self.served_by = served_by
        self.stop = stop
        self.requests: list[dict] = []
        self.messages = self  # so that `client.messages.create` resolves here

    async def create(self, **request: Any) -> SimpleNamespace:
        self.requests.append({**request, "messages": list(request["messages"])})
        content = self.turns.pop(0) if self.turns else [text("I have nothing to add.")]
        calls = any(block.type == "tool_use" for block in content)
        return SimpleNamespace(
            content=content,
            model=self.served_by,
            stop_reason=self.stop or ("tool_use" if calls else "end_turn"),
            usage=SimpleNamespace(
                input_tokens=100,
                output_tokens=10,
                cache_creation_input_tokens=0,
                cache_read_input_tokens=50,
            ),
        )


def attempt(case: Case, model: ScriptedModel, catalog: Catalog, marts: Marts, *, docs: bool = True):
    async def run():
        async with Client(build_server(catalog=catalog, marts=marts, include_docs=docs)) as server:
            tools = await server_tools(server)
            try:
                return await answer(case, model="test-model", llm=model, server=server, tools=tools)
            except ServedByAnotherModel as error:
                # Raised past the client it would arrive wrapped in an exception group.
                return error

    result = anyio.run(run)
    if isinstance(result, Exception):
        raise result
    return result


def test_a_model_that_uses_the_tools_and_submits_is_recorded(
    catalog: Catalog, marts: Marts
) -> None:
    model = ScriptedModel(
        [
            [text("Let me look."), tool("list_models")],
            [
                tool("describe_model", name="revenue_by_product_channel"),
                tool("run_query", sql=TOTAL),
            ],
            [tool("submit_answer", answerable=True, number=601, explanation="Summed the mart.")],
        ]
    )
    result = attempt(REVENUE, model, catalog, marts)
    assert result.stop == "submitted" and result.submission == Submission(True, 601.0)
    assert result.explanation == "Summed the mart."
    assert result.tool_calls == ["list_models", "describe_model", "run_query"]
    assert result.turns == 3
    assert result.usage == {
        "input_tokens": 300,
        "output_tokens": 30,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 150,
    }
    assert grade(REVENUE, 600.75, result.submission) == CORRECT

    first = model.requests[0]
    assert [entry["name"] for entry in first["tools"]] == [
        "list_models",
        "describe_model",
        "search_docs",
        "run_query",
        "submit_answer",
    ]
    assert "say so instead of estimating" in first["system"]  # the server's own instructions
    assert first["messages"] == [{"role": "user", "content": REVENUE.prompt}]
    # The query result reached the model as a tool result, through the protocol.
    results = model.requests[2]["messages"][-1]["content"]
    assert [entry["tool_use_id"] for entry in results] == ["call_describe_model", "call_run_query"]
    assert '"rows": [[600.75]]' in results[1]["content"] and results[1]["is_error"] is False
    roles = [entry["role"] for entry in result.trace]
    assert roles == ["system", "user", *["tool_call", "tool_result"] * 4]
    assert result.trace[2]["said"] == "Let me look."


def test_a_refused_query_comes_back_to_the_model_as_an_error(
    catalog: Catalog, marts: Marts
) -> None:
    model = ScriptedModel(
        [
            [tool("run_query", sql="SELECT * FROM raw_core_banking.customers")],
            [tool("nonexistent_tool")],
            [tool("submit_answer", answerable=False, explanation="No such data.")],
        ]
    )
    result = attempt(NO_SUCH_DATA, model, catalog, marts)
    refused = model.requests[1]["messages"][-1]["content"][0]
    assert refused["is_error"] is True and "Only the marts can be queried" in refused["content"]
    assert model.requests[2]["messages"][-1]["content"][0]["is_error"] is True
    assert result.submission == Submission(False)
    assert grade(NO_SUCH_DATA, None, result.submission) == CORRECT


def test_an_answer_of_the_wrong_shape_can_be_handed_in_again(
    catalog: Catalog, marts: Marts
) -> None:
    model = ScriptedModel(
        [
            [tool("submit_answer", answerable=True, label="601", explanation="As text.")],
            [tool("submit_answer", answerable=True, number=601, explanation="As a number.")],
        ]
    )
    result = attempt(REVENUE, model, catalog, marts)
    sent_back = model.requests[1]["messages"][-1]["content"][0]
    assert sent_back["is_error"] is True and "fill the `number` field" in sent_back["content"]
    assert result.submission == Submission(True, 601.0) and result.turns == 2


def test_a_model_that_only_talks_is_reminded_once(catalog: Catalog, marts: Marts) -> None:
    talks = ScriptedModel([[text("The revenue is about 600.")], [text("As I said, about 600.")]])
    result = attempt(REVENUE, talks, catalog, marts)
    assert result.stop == "no_answer" and result.submission is None and result.turns == 2
    assert talks.requests[1]["messages"][-1] == {"role": "user", "content": REMINDER}

    reminded = ScriptedModel(
        [
            [text("About 600.")],
            [tool("submit_answer", answerable=True, number=601, explanation="x")],
        ]
    )
    assert attempt(REVENUE, reminded, catalog, marts).stop == "submitted"


def test_a_model_that_never_stops_is_cut_off(catalog: Catalog, marts: Marts) -> None:
    model = ScriptedModel([[tool("list_models")]] * 50)
    result = attempt(REVENUE, model, catalog, marts)
    assert result.stop == "max_turns" and result.turns == 12 and result.submission is None


def test_a_refusal_and_a_cut_response_are_their_own_outcomes(
    catalog: Catalog, marts: Marts
) -> None:
    for stop in ("refusal", "max_tokens"):
        result = attempt(REVENUE, ScriptedModel([[text("...")]], stop=stop), catalog, marts)
        assert result.stop == stop and result.submission is None and result.turns == 1


def test_an_answer_from_another_model_is_not_accepted(catalog: Catalog, marts: Marts) -> None:
    with pytest.raises(ServedByAnotherModel, match="served by other-model"):
        attempt(
            REVENUE, ScriptedModel([[tool("list_models")]], served_by="other-model"), catalog, marts
        )


def test_without_docs_the_model_is_offered_three_tools(catalog: Catalog, marts: Marts) -> None:
    model = ScriptedModel([[tool("submit_answer", answerable=False, explanation="x")]])
    attempt(REVENUE, model, catalog, marts, docs=False)
    assert [entry["name"] for entry in model.requests[0]["tools"]] == [
        "list_models",
        "describe_model",
        "run_query",
        "submit_answer",
    ]
    assert (
        "say so instead of estimating" not in model.requests[0]["system"].split("data server:")[1]
    )


# --------------------------------------------------------------------------- #
# Recorded runs
# --------------------------------------------------------------------------- #


def row(case: Case, rep: int, submission: Submission | None, outcome: str) -> dict:
    return {
        "case_id": case.id,
        "rep": rep,
        "category": case.category,
        "model": "claude-opus-5-5",
        "submission": submission.to_json() if submission else None,
        "outcome": outcome,
        "tool_calls": ["list_models", "run_query"],
        "usage": {
            "input_tokens": 1000,
            "output_tokens": 100,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0,
        },
        "latency_s": 2.0,
    }


def test_a_summary_is_recomputed_from_the_answers(marts: Marts) -> None:
    key = answer_key.compute_key(CASES, marts)
    rows = [
        row(REVENUE, 0, Submission(True, 601.0), CORRECT),
        row(REVENUE, 1, Submission(True, 200.25), WRONG),
        row(TOP_PRODUCT, 0, Submission(False), ABSTAINED),
        row(TOP_PRODUCT, 1, None, NO_ANSWER),
        row(NO_SUCH_DATA, 0, Submission(False), CORRECT),
        row(NO_SUCH_DATA, 1, Submission(True, 42.0), CORRECT),  # recorded with the wrong grade
    ]
    summary = runs.summarize(rows, CASES, key)
    assert (summary.questions, summary.answers) == (3, 6)
    assert summary.correct.value == pytest.approx((0.5 + 0 + 0.5) / 3)
    assert (summary.wrong, summary.abstained, summary.no_answer) == (2 / 6, 1 / 6, 1 / 6)
    assert summary.by_category == {
        "lookup": (1, 0.0),
        "aggregation": (1, 0.5),
        "unanswerable": (1, 0.5),
    }
    assert summary.not_correct == {
        "revenue": [WRONG],
        "top-product": [ABSTAINED, NO_ANSWER],
        "no-such-data": [WRONG],
    }
    assert summary.mismatches == ["no-such-data repetition 1: recorded correct, now wrong"]
    assert summary.tool_calls == 2 and summary.tokens == 1100 and summary.seconds == 2.0
    # 1,000 input tokens at $4 and 100 output tokens at $20 per million, six times.
    assert summary.cost == pytest.approx(6 * (1000 * 4 + 100 * 20) / 1_000_000)


def test_the_report_compares_the_two_servers(marts: Marts) -> None:
    key = answer_key.compute_key(CASES, marts)
    good = [
        row(REVENUE, 0, Submission(True, 601.0), CORRECT),
        row(NO_SUCH_DATA, 0, Submission(False), CORRECT),
    ]
    poor = [
        row(REVENUE, 0, Submission(True, 200.25), WRONG),
        row(NO_SUCH_DATA, 0, Submission(False), CORRECT),
    ]
    summaries = {
        "docs": runs.summarize(good, CASES, key),
        "schema_only": runs.summarize(poor, CASES, key),
    }
    assert runs.difference(summaries["docs"], summaries["schema_only"]).value == 0.5
    text = runs.report(
        {"name": "test", "model": "claude-opus-5-5", "repetitions": 1, "effort": "medium"},
        summaries,
    )
    assert "| With the dbt documentation | 2 | 2 | 100.0% |" in text
    assert "| Table and column names only | 2 | 2 | 50.0% |" in text
    assert "| aggregation | 1 | 100.0% | 0.0% |" in text
    assert "Documentation against names only: +50.0 points" in text
    assert "- `revenue`: wrong" in text


def test_the_cost_of_an_unknown_model_is_not_guessed() -> None:
    assert runs.cost_usd("some-other-model", {"input_tokens": 1000}) is None
    usage = {
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_creation_input_tokens": 1_000_000,
        "cache_read_input_tokens": 1_000_000,
    }
    assert runs.cost_usd("claude-opus-5-5", usage) == pytest.approx(4 * 1.25 + 0.20)


# --------------------------------------------------------------------------- #
# The paid command, with the scripted model in place of the real one
# --------------------------------------------------------------------------- #


class OracleModel:
    """Answers every case with its reference query, as a model that makes no mistake would."""

    api_key = "not-a-real-key"
    auth_token = credentials = None

    def __init__(self, **_: Any) -> None:
        self.messages = self
        self.calls = 0

    async def create(self, *, messages: list[dict], model: str, **_: Any) -> SimpleNamespace:
        self.calls += 1
        question = messages[0]["content"]
        case = next(case for case in CASES if case.prompt == question)
        last = messages[-1]["content"]
        if not case.answerable:
            content = [tool("submit_answer", answerable=False, explanation="Not in the marts.")]
        elif case.expected is not None:
            content = [
                tool("submit_answer", answerable=True, yes_no=case.expected, explanation="Docs.")
            ]
        elif isinstance(last, str):
            content = [tool("run_query", sql=case.reference_sql)]
        else:
            rows = json.loads(last[0]["content"])["rows"]
            field = {
                "number": {"number": rows[0][0]},
                "label": {"label": rows[0][0]},
                "breakdown": {
                    "breakdown": [{"label": a, "value": b} for a, *rest in rows for b in rest]
                },
            }[case.answer_type]
            content = [tool("submit_answer", answerable=True, explanation="Queried.", **field)]
        return SimpleNamespace(
            content=content,
            model=model,
            stop_reason="tool_use",
            usage=SimpleNamespace(
                input_tokens=50_000,
                output_tokens=1_000,
                cache_creation_input_tokens=0,
                cache_read_input_tokens=0,
            ),
        )


@pytest.fixture
def workspace(
    tmp_path: Path, warehouse_file: Path, manifest_file: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    """Points the command at the small warehouse, the small cases and a scratch results folder."""
    import anthropic

    monkeypatch.setenv("WAREHOUSE_PATH", str(warehouse_file))
    monkeypatch.setenv("DBT_MANIFEST_PATH", str(manifest_file))
    monkeypatch.setenv("RAW_DIR", str(tmp_path / "raw"))
    monkeypatch.setattr(command, "load_cases", lambda: list(CASES))
    monkeypatch.setattr(runs, "RESULTS", tmp_path / "results")
    monkeypatch.setattr(anthropic, "AsyncAnthropic", OracleModel)
    return tmp_path / "results"


def test_a_run_records_answers_traces_and_settings(
    workspace: Path, capsys: pytest.CaptureFixture
) -> None:
    code = command.main(["live", "--name", "oracle", "--repetitions", "2", "--max-usd", "100"])
    printed = capsys.readouterr().out
    assert code == 0, printed
    settings, rows = runs.read_run(workspace / "oracle")
    assert settings["model"] == "claude-opus-5-5" and settings["repetitions"] == 2
    assert settings["dataset"] is None
    assert set(rows) == {"docs", "schema_only"}
    for lines in rows.values():
        assert len(lines) == len(CASES) * 2
        assert {line["outcome"] for line in lines} == {CORRECT}
    line = next(line for line in rows["docs"] if line["case_id"] == "by-channel")
    assert line["submission"] == {"answerable": True, "value": {"branch": 400.5, "online": 200.25}}
    assert line["tool_calls"] == ["run_query"] and line["stop"] == "submitted"
    trace = json.loads((workspace / "oracle" / "docs" / "traces" / "revenue_rep1.json").read_text())
    assert [entry["role"] for entry in trace] == [
        "system",
        "user",
        "tool_call",
        "tool_result",
        "tool_call",
        "tool_result",
    ]
    assert (
        "| With the dbt documentation | 5 | 10 | 100.0% | 56.6% to 100.0% | 0.0% | 0.0% | 0.0% |"
        in printed
    )
    assert "Documentation against names only: +0.0 points" in printed


def test_a_run_is_resumed_where_it_stopped(workspace: Path, capsys: pytest.CaptureFixture) -> None:
    assert (
        command.main(["live", "--name", "resume", "--conditions", "docs", "--cases", "revenue"])
        == 0
    )
    assert command.main(["live", "--name", "resume", "--conditions", "docs"]) == 0
    capsys.readouterr()
    _, rows = runs.read_run(workspace / "resume")
    assert sorted(line["case_id"] for line in rows["docs"]) == sorted(case.id for case in CASES)
    # A third time there is nothing left to ask.
    assert command.main(["live", "--name", "resume", "--conditions", "docs"]) == 0
    assert "nothing to do" in capsys.readouterr().out
    # The same name cannot be reused for another model.
    assert command.main(["live", "--name", "resume", "--model", "claude-haiku-5-5"]) == 2


def test_a_run_stops_at_its_spending_limit(workspace: Path, capsys: pytest.CaptureFixture) -> None:
    # The first scripted question takes two model calls: about 44 cents at the default prices.
    code = command.main(
        [
            "live",
            "--name",
            "capped",
            "--conditions",
            "docs",
            "--max-usd",
            "0.40",
            "--concurrency",
            "1",
        ]
    )
    printed = capsys.readouterr().out
    assert code == 3 and "The limit of US$ 0.40 was reached" in printed
    _, rows = runs.read_run(workspace / "capped")
    assert len(rows["docs"]) == 1
    assert "4 not run: spending limit" in printed


def test_an_attempt_that_errors_is_not_graded_and_is_retried(
    workspace: Path, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed call is plumbing, not a wrong answer: it must not count against the model."""
    import anthropic
    import httpx2

    answer_normally = OracleModel.create
    failing = {"on": True}

    async def fail_on_one_case(self, *, messages: list[dict], **request: Any) -> SimpleNamespace:
        if failing["on"] and messages[0]["content"] == TOP_PRODUCT.prompt:
            raise anthropic.APIConnectionError(request=httpx2.Request("POST", "https://example"))
        return await answer_normally(self, messages=messages, **request)

    monkeypatch.setattr(OracleModel, "create", fail_on_one_case)
    assert command.main(["live", "--name", "flaky", "--conditions", "docs"]) == 1
    printed = capsys.readouterr().out
    assert "ERROR connection error" in printed and "retry the attempts that errored" in printed
    _, rows = runs.read_run(workspace / "flaky")
    assert len(rows["docs"]) == len(CASES) - 1
    assert "top-product" not in [line["case_id"] for line in rows["docs"]]
    errors = runs.read_jsonl(workspace / "flaky" / "docs" / "errors.jsonl")
    assert [(error["case_id"], error["rep"]) for error in errors] == [("top-product", 0)]
    # The four that were answered are all correct: the error did not turn into a zero.
    assert "| With the dbt documentation | 4 | 4 | 100.0% |" in printed

    failing["on"] = False
    assert command.main(["live", "--name", "flaky", "--conditions", "docs"]) == 0
    _, rows = runs.read_run(workspace / "flaky")
    assert sorted(line["case_id"] for line in rows["docs"]) == sorted(case.id for case in CASES)


def test_a_question_that_takes_too_long_is_an_error_not_a_wrong_answer(
    workspace: Path, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def never_answers(self, **_: Any) -> SimpleNamespace:
        await anyio.sleep(60)

    monkeypatch.setattr(OracleModel, "create", never_answers)
    code = command.main(
        ["live", "--name", "slow", "--conditions", "docs", "--cases", "revenue", "--timeout", "0.2"]
    )
    assert code == 1 and "ERROR timeout" in capsys.readouterr().out
    assert not (workspace / "slow" / "docs" / "results.jsonl").exists()


def test_bad_credentials_stop_the_run_at_the_first_question(
    workspace: Path, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    import anthropic
    import httpx2

    calls = []

    async def refuse(self, **_: Any) -> SimpleNamespace:
        calls.append(1)
        request = httpx2.Request("POST", "https://example")
        raise anthropic.AuthenticationError(
            "invalid x-api-key", response=httpx2.Response(401, request=request), body=None
        )

    monkeypatch.setattr(OracleModel, "create", refuse)
    assert command.main(["live", "--name", "bad-key", "--conditions", "docs"]) == 2
    printed = capsys.readouterr().out
    assert "refused the credentials" in printed and "ANTHROPIC_API_KEY" in printed
    assert len(calls) == 1


def test_a_run_needs_credentials_and_a_known_price(
    workspace: Path, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert command.main(["live", "--name", "unknown", "--model", "some-other-model"]) == 2
    assert "No price is known" in capsys.readouterr().out
    monkeypatch.setattr(OracleModel, "api_key", None)
    assert command.main(["live", "--name", "no-key"]) == 2
    assert "No Anthropic credentials were found" in capsys.readouterr().out
    assert not (workspace / "no-key").exists()


def test_recorded_runs_are_regraded_and_a_tampered_grade_is_caught(
    workspace: Path, capsys: pytest.CaptureFixture, marts: Marts, monkeypatch: pytest.MonkeyPatch
) -> None:
    key = answer_key.compute_key(CASES, marts)
    monkeypatch.setattr(answer_key, "load_committed_key", lambda: key)
    monkeypatch.setattr(answer_key, "delivered_dataset", lambda _: answer_key.DEFAULT_DATASET)
    assert command.main(["live", "--name", "published", "--conditions", "docs"]) == 0
    capsys.readouterr()
    assert command.main(["report"]) == 0
    assert "| With the dbt documentation | 5 | 5 | 100.0% |" in capsys.readouterr().out

    results = workspace / "published" / "docs" / "results.jsonl"
    lines = [json.loads(line) for line in results.read_text().splitlines()]
    lines[0]["submission"] = {
        "answerable": True,
        "value": 1.0,
    }  # the answer changes, the grade stays
    results.write_text("".join(json.dumps(line) + "\n" for line in lines))
    assert command.main(["report"]) == 1
    assert "recorded correct, now wrong" in capsys.readouterr().out
