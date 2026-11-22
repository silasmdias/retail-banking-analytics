"""One case, answered by a language model through the MCP server.

The model gets the server's tools, exactly as an MCP client would offer them,
plus one tool of the harness: ``submit_answer``, which is how it hands in the
answer in a shape a program can grade. Every tool call goes through the real
protocol to a real server process.

Nothing here is specific to a case: the system prompt is the same for all of
them and says nothing about the bank beyond what the server itself tells a
client in its instructions.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from mcp import Client, MCPError
from mcp.types import TextContent

from assistant.evals.cases import Case
from assistant.evals.grading import InvalidSubmission, Submission, read_submission

if TYPE_CHECKING:
    import anthropic

MAX_TOKENS = 16000
MAX_TURNS = 12

SYSTEM = """\
You answer one question about a retail bank, using the tools of the data server \
connected to this conversation.

Work out the answer with the tools, then call submit_answer exactly once. Only \
that call is read; anything else you write is ignored.

If the tools cannot answer the question, do not estimate: call submit_answer \
with answerable set to false.

Instructions from the data server:

{instructions}"""

REMINDER = "Call submit_answer now, with your answer or with answerable set to false."

SUBMIT_ANSWER = {
    "name": "submit_answer",
    "description": (
        "Hands in the final answer. Call it once, when you are done. Fill the one field "
        "that matches the format the question asks for, or set answerable to false when "
        "the tools cannot answer the question."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "answerable": {
                "type": "boolean",
                "description": "False when the available tables and documentation cannot "
                "answer the question.",
            },
            "number": {
                "type": "number",
                "description": "For a question that asks for one number, in the unit and "
                "precision the question asks for.",
            },
            "label": {
                "type": "string",
                "description": "For a question that asks for one label.",
            },
            "yes_no": {
                "type": "boolean",
                "description": "For a yes or no question: true for yes, false for no.",
            },
            "breakdown": {
                "type": "array",
                "description": "For a question that asks for a breakdown.",
                "items": {
                    "type": "object",
                    "properties": {"label": {"type": "string"}, "value": {"type": "number"}},
                    "required": ["label", "value"],
                    "additionalProperties": False,
                },
            },
            "explanation": {
                "type": "string",
                "description": "One or two sentences: how the answer was obtained, or why "
                "the question cannot be answered.",
            },
        },
        "required": ["answerable", "explanation"],
        "additionalProperties": False,
    },
}

USAGE_FIELDS = (
    "input_tokens",
    "output_tokens",
    "cache_creation_input_tokens",
    "cache_read_input_tokens",
)


class ServedByAnotherModel(RuntimeError):
    """The response came from a model other than the one asked for."""


@dataclass
class Attempt:
    """What happened on one case: the answer handed in, and everything that led to it."""

    stop: str = "max_turns"  # submitted | no_answer | max_turns | refusal | max_tokens
    submission: Submission | None = None
    explanation: str | None = None
    turns: int = 0
    tool_calls: list[str] = field(default_factory=list)  # server tools, in the order called
    usage: dict[str, int] = field(default_factory=lambda: dict.fromkeys(USAGE_FIELDS, 0))
    latency_s: float = 0.0
    trace: list[dict] = field(default_factory=list)


async def server_tools(server: Client) -> list[dict]:
    """The server's tools as the Messages API takes them, followed by ``submit_answer``."""
    listed = await server.list_tools()
    tools = [
        {
            "name": tool.name,
            "description": tool.description or "",
            "input_schema": tool.input_schema,
        }
        for tool in listed.tools
    ]
    return [*tools, SUBMIT_ANSWER]


async def answer(
    case: Case,
    *,
    model: str,
    llm: anthropic.AsyncAnthropic,
    server: Client,
    tools: list[dict],
    effort: str | None = "medium",
) -> Attempt:
    """Lets the model work on one case until it hands in an answer or gives up."""
    system = SYSTEM.format(instructions=server.instructions or "(none)")
    messages: list[dict[str, Any]] = [{"role": "user", "content": case.prompt}]
    attempt = Attempt(
        trace=[{"role": "system", "content": system}, {"role": "user", "content": case.prompt}]
    )
    reminded = False
    started = time.monotonic()
    request: dict[str, Any] = {
        "model": model,
        "max_tokens": MAX_TOKENS,
        "system": system,
        "tools": tools,
        # The tools and the system prompt repeat on every call of every case.
        "cache_control": {"type": "ephemeral"},
    }
    if effort:
        request["output_config"] = {"effort": effort}

    while attempt.turns < MAX_TURNS:
        response = await llm.messages.create(messages=messages, **request)
        attempt.turns += 1
        attempt.latency_s = round(time.monotonic() - started, 2)
        for name in USAGE_FIELDS:
            attempt.usage[name] += getattr(response.usage, name, None) or 0
        # A silent substitution would make the result a measurement of another model.
        if not response.model.startswith(model):
            raise ServedByAnotherModel(f"asked for {model}, served by {response.model}")
        if response.stop_reason in ("refusal", "max_tokens", "model_context_window_exceeded"):
            attempt.stop = response.stop_reason
            return attempt

        said = "".join(block.text for block in response.content if block.type == "text").strip()
        calls = [block for block in response.content if block.type == "tool_use"]
        messages.append({"role": "assistant", "content": response.content})
        if not calls:
            attempt.trace.append({"role": "assistant", "content": said})
            if reminded:
                attempt.stop = "no_answer"
                return attempt
            reminded = True
            messages.append({"role": "user", "content": REMINDER})
            attempt.trace.append({"role": "user", "content": REMINDER})
            continue

        results = []
        for index, call in enumerate(calls):
            arguments = dict(call.input)
            entry = {"role": "tool_call", "name": call.name, "content": arguments}
            if said and index == 0:
                entry["said"] = said
            attempt.trace.append(entry)
            if call.name == SUBMIT_ANSWER["name"]:
                try:
                    attempt.submission = read_submission(case, arguments)
                    attempt.explanation = str(arguments.get("explanation") or "") or None
                    text, failed = "Recorded.", False
                except InvalidSubmission as error:
                    text, failed = str(error), True
            else:
                attempt.tool_calls.append(call.name)
                text, failed = await _call_server(server, call.name, arguments)
            attempt.trace.append({"role": "tool_result", "content": text, "is_error": failed})
            results.append(
                {"type": "tool_result", "tool_use_id": call.id, "content": text, "is_error": failed}
            )
        if attempt.submission is not None:
            attempt.stop = "submitted"
            return attempt
        messages.append({"role": "user", "content": results})
    return attempt


async def _call_server(server: Client, name: str, arguments: dict) -> tuple[str, bool]:
    try:
        result = await server.call_tool(name, arguments)
    except MCPError as error:
        return f"The server rejected the call: {error.message}", True
    text = "\n".join(block.text for block in result.content if isinstance(block, TextContent))
    return text, bool(result.is_error)
