"""The second way to a model: Claude Code as the client, read back from its transcript.

No test runs the real client. The transcripts are written by hand in its
stream-json format, and the end-to-end tests put a small script in its place.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import anyio
import pytest
from mcp import Client

from assistant.evals import __main__ as command
from assistant.evals import answer_sheet, claude_code, runs
from assistant.evals.agent import SUBMIT_ANSWER, ServedByAnotherModel
from assistant.evals.claude_code import ClaudeCodeError, read_transcript
from assistant.evals.grading import CORRECT, Submission
from assistant.tests.test_evals import CASES, NO_SUCH_DATA, REVENUE, TOTAL

MODEL = "claude-haiku-5-5"


# --------------------------------------------------------------------------- #
# The answer sheet
# --------------------------------------------------------------------------- #


def sheet(**arguments: Any):
    async def run():
        async with Client(answer_sheet.build_server()) as client:
            tools = (await client.list_tools()).tools
            return tools, await client.call_tool("submit_answer", arguments)

    return anyio.run(run)


def test_the_answer_sheet_offers_the_same_tool_as_the_api_harness() -> None:
    tools, result = sheet(answerable=True, number=601, explanation="Summed the mart.")
    assert [tool.name for tool in tools] == ["submit_answer"]
    assert tools[0].description == SUBMIT_ANSWER["description"]
    offered = tools[0].input_schema["properties"]
    wanted = SUBMIT_ANSWER["input_schema"]["properties"]
    assert offered.keys() == wanted.keys()
    for name, field in wanted.items():
        assert offered[name]["description"] == field["description"]
    assert tools[0].input_schema["required"] == ["answerable", "explanation"]
    assert not result.is_error and "Recorded" in result.content[0].text


def test_the_answer_sheet_sends_back_an_answer_with_no_value() -> None:
    _, empty = sheet(answerable=True, explanation="Forgot the value.")
    assert empty.is_error and "The answer is missing" in empty.content[0].text
    _, cannot = sheet(answerable=False, explanation="Not in the marts.")
    assert not cannot.is_error
    entries = [{"label": "branch", "value": 400.5}]
    _, breakdown = sheet(answerable=True, breakdown=entries, explanation="By channel.")
    assert not breakdown.is_error


# --------------------------------------------------------------------------- #
# How the client is launched
# --------------------------------------------------------------------------- #


def test_the_client_is_run_with_nothing_but_the_two_servers(tmp_path: Path) -> None:
    line = claude_code.command(
        REVENUE,
        executable="claude",
        model=MODEL,
        config=tmp_path / "mcp.json",
        include_docs=True,
        effort="medium",
    )
    assert line[:3] == ["claude", "--print", REVENUE.prompt]
    assert line[line.index("--model") + 1] == MODEL
    assert line[line.index("--tools") + 1] == ""
    assert line[line.index("--setting-sources") + 1] == "project"
    assert "--strict-mcp-config" in line and "--no-session-persistence" in line
    assert line[-2:] == ["--effort", "medium"]
    system = line[line.index("--system-prompt") + 1]
    assert "call submit_answer exactly once" in system
    assert "say so instead of estimating" in system  # the data server's own instructions
    allowed = line[line.index("--allowedTools") + 1 : line.index("--no-session-persistence")]
    assert allowed == [
        "mcp__retail-banking__list_models",
        "mcp__retail-banking__describe_model",
        "mcp__retail-banking__search_docs",
        "mcp__retail-banking__run_query",
        "mcp__answer-sheet__submit_answer",
    ]


def test_without_docs_the_client_gets_no_search_and_other_instructions(tmp_path: Path) -> None:
    line = claude_code.command(
        REVENUE,
        executable="claude",
        model=MODEL,
        config=tmp_path / "mcp.json",
        include_docs=False,
        effort=None,
    )
    assert "mcp__retail-banking__search_docs" not in line
    assert "--effort" not in line
    assert (
        "instead of estimating"
        not in line[line.index("--system-prompt") + 1].split("data server:")[1]
    )
    servers = claude_code.mcp_config(include_docs=False)["mcpServers"]
    assert servers["retail-banking"]["args"] == ["-m", "assistant", "--no-docs"]


def test_the_servers_the_client_launches_see_the_same_warehouse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WAREHOUSE_PATH", "/somewhere/else.duckdb")
    monkeypatch.delenv("DBT_MANIFEST_PATH", raising=False)
    servers = claude_code.mcp_config(include_docs=True)["mcpServers"]
    assert set(servers) == {"retail-banking", "answer-sheet"}
    data = servers["retail-banking"]
    assert data["command"] == sys.executable and data["args"] == ["-m", "assistant"]
    assert data["env"]["WAREHOUSE_PATH"] == "/somewhere/else.duckdb"
    assert "DBT_MANIFEST_PATH" not in data["env"] and "PYTHONPATH" in data["env"]
    assert servers["answer-sheet"]["args"] == ["-m", "assistant.evals.answer_sheet"]


# --------------------------------------------------------------------------- #
# Reading the transcript
# --------------------------------------------------------------------------- #

STARTED = {
    "type": "system",
    "subtype": "init",
    "mcp_servers": [
        {"name": "retail-banking", "status": "connected"},
        {"name": "answer-sheet", "status": "connected"},
    ],
}


def says(*blocks: dict, model: str = MODEL) -> dict:
    return {"type": "assistant", "message": {"model": model, "content": list(blocks)}}


def calls(tool: str, server: str = "retail-banking", **arguments: Any) -> dict:
    return {
        "type": "tool_use",
        "id": f"call_{tool}",
        "name": f"mcp__{server}__{tool}",
        "input": arguments,
    }


def submits(**arguments: Any) -> dict:
    return calls("submit_answer", "answer-sheet", **arguments)


def returns(text: str | list, is_error: bool = False) -> dict:
    block = {"type": "tool_result", "tool_use_id": "x", "content": text, "is_error": is_error}
    return {"type": "user", "message": {"content": [block]}}


def finished(**changes: Any) -> dict:
    result = {
        "type": "result",
        "subtype": "success",
        "is_error": False,
        "num_turns": 5,
        "duration_ms": 12440,
        "stop_reason": "end_turn",
        "usage": {
            "input_tokens": 10,
            "output_tokens": 741,
            "cache_creation_input_tokens": 5659,
            "cache_read_input_tokens": 26031,
            "service_tier": "standard",
        },
    }
    return {**result, **changes}


def transcript(*events: dict) -> list[str]:
    return [json.dumps(event) for event in events]


def test_a_transcript_becomes_an_attempt() -> None:
    attempt = read_transcript(
        transcript(
            STARTED,
            says({"type": "thinking", "thinking": ""}),
            says({"type": "text", "text": "Let me look."}, calls("list_models")),
            returns('{"models": []}'),
            says(calls("run_query", sql=TOTAL)),
            returns([{"type": "text", "text": '{"rows": [[600.75]]}'}]),
            says(submits(answerable=True, number=601, explanation="Summed the mart.")),
            returns('{"result": "Recorded."}'),
            says({"type": "text", "text": "The answer is 601."}),
            finished(),
        ),
        REVENUE,
        MODEL,
    )
    assert attempt.stop == "submitted" and attempt.submission == Submission(True, 601.0)
    assert attempt.explanation == "Summed the mart."
    assert attempt.tool_calls == ["list_models", "run_query"]
    assert (attempt.turns, attempt.latency_s) == (5, 12.44)
    assert attempt.usage == {
        "input_tokens": 10,
        "output_tokens": 741,
        "cache_creation_input_tokens": 5659,
        "cache_read_input_tokens": 26031,
    }
    assert [entry["role"] for entry in attempt.trace] == [
        "system",
        "user",
        "tool_call",
        "tool_result",
        "tool_call",
        "tool_result",
        "tool_call",
        "tool_result",
        "assistant",
    ]
    assert attempt.trace[2] == {
        "role": "tool_call",
        "name": "list_models",
        "content": {},
        "said": "Let me look.",
    }
    assert attempt.trace[4]["content"] == {"sql": TOTAL}
    assert attempt.trace[5]["content"] == '{"rows": [[600.75]]}'


def test_the_first_answer_of_the_right_shape_counts() -> None:
    attempt = read_transcript(
        transcript(
            STARTED,
            says(submits(answerable=True, label="601", explanation="As text.")),
            returns("Recorded."),
            says(submits(answerable=True, number=601, explanation="As a number.")),
            returns("Recorded."),
            says(submits(answerable=True, number=999, explanation="Changed my mind.")),
            returns("Recorded."),
            finished(),
        ),
        REVENUE,
        MODEL,
    )
    assert attempt.submission == Submission(True, 601.0)
    assert attempt.explanation == "As a number." and attempt.tool_calls == []


def test_a_transcript_without_an_answer_is_a_no_answer() -> None:
    talked = read_transcript(
        transcript(STARTED, says({"type": "text", "text": "About 600."}), finished()),
        REVENUE,
        MODEL,
    )
    assert talked.stop == "no_answer" and talked.submission is None
    assert talked.trace[-1] == {"role": "assistant", "content": "About 600."}
    wrong_shape = read_transcript(
        transcript(
            STARTED, says(submits(answerable=True, label="601", explanation="x")), finished()
        ),
        REVENUE,
        MODEL,
    )
    assert wrong_shape.stop == "no_answer"
    refused = read_transcript(
        transcript(STARTED, finished(stop_reason="refusal")), NO_SUCH_DATA, MODEL
    )
    assert refused.stop == "refusal"


@pytest.mark.parametrize(
    ("lines", "message"),
    [
        (["Error: not logged in", "run /login"], "stopped without a result: Error: not logged in"),
        ([], "nothing was printed"),
        (
            transcript(STARTED, finished(is_error=True, subtype="error", result="Usage limit hit")),
            "reported an error: Usage limit hit",
        ),
        (
            transcript(
                {**STARTED, "mcp_servers": [{"name": "retail-banking", "status": "failed"}]},
                finished(),
            ),
            r"MCP server did not start: retail-banking \(failed\)",
        ),
    ],
)
def test_a_run_of_the_client_that_failed_is_an_error_not_an_answer(
    lines: list[str], message: str
) -> None:
    with pytest.raises(ClaudeCodeError, match=message):
        read_transcript(lines, REVENUE, MODEL)


def test_an_answer_from_another_model_is_not_accepted() -> None:
    other = transcript(STARTED, says(calls("list_models"), model="claude-sonnet-5-5"), finished())
    with pytest.raises(ServedByAnotherModel, match="served by claude-sonnet-5-5"):
        read_transcript(other, REVENUE, MODEL)
    # A message the client writes itself, such as a notice, has no model behind it.
    notice = transcript(
        STARTED, says({"type": "text", "text": "notice"}, model="<synthetic>"), finished()
    )
    assert read_transcript(notice, REVENUE, MODEL).stop == "no_answer"


# --------------------------------------------------------------------------- #
# The command, with a script standing in for the client
# --------------------------------------------------------------------------- #

STAND_IN = """\
import json, os, sys

if "--version" in sys.argv:
    print("9.9.9 (Claude Code)")
    raise SystemExit

def emit(event):
    print(json.dumps(event), flush=True)

arguments = sys.argv[1:]
prompt = arguments[arguments.index("--print") + 1]
config = arguments[arguments.index("--mcp-config") + 1]
servers = json.load(open(config))["mcpServers"]
emit({{"type": "system", "subtype": "init",
       "mcp_servers": [{{"name": name, "status": "connected"}} for name in servers]}})
usage = {{"input_tokens": 10, "output_tokens": 100,
          "cache_creation_input_tokens": 0, "cache_read_input_tokens": 1000}}
if {fail}:
    emit({{"type": "result", "subtype": "error", "is_error": True, "result": "Usage limit hit"}})
    raise SystemExit(1)
# An empty folder, and nothing of the user's: that is what keeps the run clean.
alone = os.listdir(".") == ["mcp.json"] and arguments[arguments.index("--tools") + 1] == ""
emit({{"type": "assistant", "message": {{"model": "claude-haiku-5-5", "content": [
    {{"type": "tool_use", "id": "1", "name": "mcp__answer-sheet__submit_answer",
      "input": {{"answerable": not alone, "number": 1, "explanation": prompt[:20]}}}}]}}}})
emit({{"type": "user", "message": {{"content": [
    {{"type": "tool_result", "tool_use_id": "1", "content": "Recorded."}}]}}}})
emit({{"type": "result", "subtype": "success", "is_error": False, "num_turns": 2,
       "duration_ms": 1500, "stop_reason": "end_turn", "usage": usage}})
"""


def stand_in(folder: Path, *, fail: bool = False) -> str:
    """An executable named ``claude`` that prints a transcript the way the client does."""
    program = folder / "stand_in.py"
    program.write_text(STAND_IN.format(fail=fail), encoding="utf-8")
    # A shell wrapper, because a path with spaces cannot go on a `#!` line.
    script = folder / "claude"
    script.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{program}" "$@"\n', encoding="utf-8")
    script.chmod(0o755)
    return str(script)


@pytest.fixture
def workspace(
    tmp_path: Path, warehouse_file: Path, manifest_file: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    monkeypatch.setenv("WAREHOUSE_PATH", str(warehouse_file))
    monkeypatch.setenv("DBT_MANIFEST_PATH", str(manifest_file))
    monkeypatch.setenv("RAW_DIR", str(tmp_path / "raw"))
    monkeypatch.setattr(command, "load_cases", lambda: list(CASES))
    monkeypatch.setattr(runs, "RESULTS", tmp_path / "results")
    return tmp_path / "results"


def test_a_run_through_the_client_is_recorded_like_any_other(
    workspace: Path, tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    code = command.main(
        ["live", "--client", "claude-code", "--claude", stand_in(tmp_path), "--model", MODEL]
    )
    printed = capsys.readouterr().out
    assert code == 0, printed
    settings, rows = runs.read_run(workspace / MODEL)
    assert settings["client"] == "claude-code 9.9.9" and settings["model"] == MODEL
    assert set(rows) == {"docs", "schema_only"}
    # The stand-in says "cannot be answered" to everything, which is right once in five.
    for lines in rows.values():
        assert len(lines) == len(CASES)
        assert [line["case_id"] for line in lines if line["outcome"] == CORRECT] == ["no-such-data"]
        assert {line["stop"] for line in lines} == {"submitted"}
        assert lines[0]["usage"]["cache_read_input_tokens"] == 1000 and lines[0]["latency_s"] == 1.5
    assert f"`{MODEL}` through Claude Code 9.9.9" in printed
    assert "nothing was billed per token" in printed
    assert "| With the dbt documentation | 5 | 5 | 20.0% |" in printed
    # The same name cannot be continued through the other client.
    assert command.main(["live", "--client", "api", "--model", MODEL]) == 2


def test_a_client_that_keeps_failing_stops_the_run(
    workspace: Path, tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    failing = stand_in(tmp_path, fail=True)
    code = command.main(
        ["live", "--client", "claude-code", "--claude", failing, "--model", MODEL, "--name", "down"]
    )
    printed = capsys.readouterr().out
    assert code == 2 and "Five attempts in a row failed" in printed
    assert "the client reported an error: Usage limit hit" in printed
    errors = runs.read_jsonl(workspace / "down" / "docs" / "errors.jsonl")
    assert len(errors) == 5
    assert not (workspace / "down" / "docs" / "results.jsonl").exists()
    assert not (workspace / "down" / "schema_only").exists()


def test_a_missing_client_is_reported_before_anything_runs(
    workspace: Path, capsys: pytest.CaptureFixture
) -> None:
    code = command.main(["live", "--client", "claude-code", "--claude", "no-such-client"])
    assert code == 2 and "Claude Code was not found" in capsys.readouterr().out
    assert not workspace.exists()
