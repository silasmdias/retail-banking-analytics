"""The same questions, answered through Claude Code instead of the model API.

Claude Code is an MCP client with a model behind it, and in print mode it
answers one prompt and exits. That makes it a second way to reach a model: with
a Claude subscription instead of an API key.

It is run as a thin client, so the two ways stay comparable: its built-in tools
are switched off, its system prompt is replaced by the evaluation's own, and it
starts in an empty folder with no user or project settings. The model gets the
data server's tools and ``submit_answer``, which here comes from a second MCP
server (``answer_sheet``). Everything that happened is read back from the
transcript the client prints.

What still differs from the API: the client decides how the conversation is
packed and cached, a question gets one turn with no reminder to hand in an
answer, and an answer of the wrong shape is not sent back for correction.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterable
from pathlib import Path

import anyio

from assistant.evals.agent import SUBMIT_ANSWER, SYSTEM, USAGE_FIELDS, Attempt, ServedByAnotherModel
from assistant.evals.answer_sheet import SERVER_NAME as ANSWER_SHEET
from assistant.evals.cases import Case
from assistant.evals.grading import InvalidSubmission, read_submission
from assistant.paths import REPO_ROOT
from assistant.server import INSTRUCTIONS, INSTRUCTIONS_WITHOUT_DOCS

DATA_SERVER = "retail-banking"
DATA_TOOLS = ("list_models", "describe_model", "search_docs", "run_query")

# Settings a test or a demo may have moved; the servers the client launches need them too.
_PASSED_ON = ("WAREHOUSE_PATH", "DBT_MANIFEST_PATH", "RAW_DIR")


class ClaudeCodeError(RuntimeError):
    """The client did not produce a transcript that can be graded."""


def system_prompt(include_docs: bool) -> str:
    return SYSTEM.format(instructions=INSTRUCTIONS if include_docs else INSTRUCTIONS_WITHOUT_DOCS)


def mcp_config(include_docs: bool) -> dict:
    """The two servers, as the client's ``--mcp-config`` file names them."""
    environment = {"PYTHONPATH": str(REPO_ROOT)}
    environment |= {name: os.environ[name] for name in _PASSED_ON if name in os.environ}
    flags = [] if include_docs else ["--no-docs"]
    return {
        "mcpServers": {
            DATA_SERVER: {
                "command": sys.executable,
                "args": ["-m", "assistant", *flags],
                "env": environment,
            },
            ANSWER_SHEET: {
                "command": sys.executable,
                "args": ["-m", "assistant.evals.answer_sheet"],
                "env": environment,
            },
        }
    }


def command(
    case: Case,
    *,
    executable: str,
    model: str,
    config: Path,
    include_docs: bool,
    effort: str | None,
) -> list[str]:
    tools = [tool for tool in DATA_TOOLS if include_docs or tool != "search_docs"]
    allowed = [f"mcp__{DATA_SERVER}__{tool}" for tool in tools]
    allowed.append(f"mcp__{ANSWER_SHEET}__{SUBMIT_ANSWER['name']}")
    line = [
        executable,
        "--print",
        case.prompt,
        "--model",
        model,
        "--system-prompt",
        system_prompt(include_docs),
        "--mcp-config",
        str(config),
        "--strict-mcp-config",  # no MCP server but these two
        "--tools",
        "",  # no built-in tool: the servers are the model's only source
        "--setting-sources",
        "project",  # and the folder is empty, so no settings and no memory files
        "--allowedTools",
        *allowed,
        "--no-session-persistence",
        "--output-format",
        "stream-json",
        "--verbose",
    ]
    return [*line, "--effort", effort] if effort else line


async def answer(
    case: Case,
    *,
    model: str,
    executable: str,
    config: Path,
    include_docs: bool,
    effort: str | None = "medium",
) -> Attempt:
    """Runs the client on one case and reads the attempt from what it printed."""
    line = command(
        case,
        executable=executable,
        model=model,
        config=config,
        include_docs=include_docs,
        effort=effort,
    )
    async with await anyio.open_process(
        line,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=config.parent,
    ) as process:
        try:
            printed = b"".join([chunk async for chunk in process.stdout])
            await process.wait()
        except BaseException:
            process.kill()
            raise
    return read_transcript(printed.decode("utf-8", errors="replace").splitlines(), case, model)


def read_transcript(lines: Iterable[str], case: Case, model: str) -> Attempt:
    """Turns the client's stream of JSON events into an attempt."""
    events, stray = [], []
    for line in lines:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            stray.append(line)
            continue
        if isinstance(event, dict) and "type" in event:
            events.append(event)

    result = next((event for event in events if event["type"] == "result"), None)
    if result is None:
        tail = " ".join(stray[-3:]).strip() or "nothing was printed"
        raise ClaudeCodeError(f"the client stopped without a result: {tail[:300]}")
    started = next((event for event in events if event.get("subtype") == "init"), {})
    down = [
        f"{server.get('name')} ({server.get('status')})"
        for server in started.get("mcp_servers", [])
        if server.get("status") != "connected"
    ]
    if down:
        raise ClaudeCodeError(f"an MCP server did not start: {', '.join(down)}")
    if result.get("is_error"):
        reason = result.get("result") or result.get("subtype") or "unknown error"
        raise ClaudeCodeError(f"the client reported an error: {str(reason)[:300]}")

    attempt = Attempt(
        stop="refusal" if result.get("stop_reason") == "refusal" else "no_answer",
        turns=int(result.get("num_turns") or 0),
        latency_s=round((result.get("duration_ms") or 0) / 1000, 2),
        trace=[
            {"role": "system", "content": "(the evaluation's system prompt)"},
            {"role": "user", "content": case.prompt},
        ],
    )
    usage = result.get("usage") or {}
    attempt.usage = {name: int(usage.get(name) or 0) for name in USAGE_FIELDS}

    for event in events:
        message = event.get("message") or {}
        blocks = message.get("content")
        if not isinstance(blocks, list):
            continue
        if event["type"] == "assistant":
            served_by = message.get("model") or ""
            if served_by != "<synthetic>" and not served_by.startswith(model):
                raise ServedByAnotherModel(f"asked for {model}, served by {served_by}")
            said = "".join(b.get("text", "") for b in blocks if b.get("type") == "text").strip()
            calls = [block for block in blocks if block.get("type") == "tool_use"]
            if said and not calls:
                attempt.trace.append({"role": "assistant", "content": said})
            for index, call in enumerate(calls):
                name = call["name"].split("__")[-1]
                arguments = call.get("input") or {}
                entry = {"role": "tool_call", "name": name, "content": arguments}
                if said and index == 0:
                    entry["said"] = said
                attempt.trace.append(entry)
                if name != SUBMIT_ANSWER["name"]:
                    attempt.tool_calls.append(name)
                elif attempt.submission is None:
                    try:
                        attempt.submission = read_submission(case, arguments)
                        attempt.explanation = str(arguments.get("explanation") or "") or None
                        attempt.stop = "submitted"
                    except InvalidSubmission:
                        pass  # a later call may still carry an answer of the right shape
        elif event["type"] == "user":
            for block in blocks:
                if isinstance(block, dict) and block.get("type") == "tool_result":
                    attempt.trace.append(
                        {
                            "role": "tool_result",
                            "content": _text(block.get("content")),
                            "is_error": bool(block.get("is_error")),
                        }
                    )
    return attempt


def _text(content: object) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(part.get("text", "") for part in content if isinstance(part, dict))
    return "" if content is None else str(content)
