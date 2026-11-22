"""A one-tool MCP server: the answer sheet of the evaluation.

    python -m assistant.evals.answer_sheet

When the questions are answered through an MCP client instead of the model API,
the harness cannot add a tool of its own to the conversation. This server offers
the same ``submit_answer`` tool, with the same words, next to the data server.
It records nothing: the harness reads the call from the client's transcript.
"""

from __future__ import annotations

from typing import Annotated

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import BaseModel, Field

from assistant.evals.agent import SUBMIT_ANSWER

SERVER_NAME = "answer-sheet"
RECORDED = "Recorded. You are done: do not call any other tool."

_FIELDS = SUBMIT_ANSWER["input_schema"]["properties"]


class Entry(BaseModel):
    label: str
    value: float


def _field(name: str) -> Field:
    return Field(description=_FIELDS[name]["description"])


def build_server() -> MCPServer:
    server = MCPServer(SERVER_NAME, log_level="WARNING")

    @server.tool(name=SUBMIT_ANSWER["name"], description=SUBMIT_ANSWER["description"])
    def submit_answer(
        answerable: Annotated[bool, _field("answerable")],
        explanation: Annotated[str, _field("explanation")],
        number: Annotated[float | None, _field("number")] = None,
        label: Annotated[str | None, _field("label")] = None,
        yes_no: Annotated[bool | None, _field("yes_no")] = None,
        breakdown: Annotated[list[Entry] | None, _field("breakdown")] = None,
    ) -> str:
        if answerable and all(value is None for value in (number, label, yes_no, breakdown)):
            raise ToolError(
                "The answer is missing. Fill the field that matches the format the question "
                "asks for, or set answerable to false."
            )
        return RECORDED

    return server


if __name__ == "__main__":
    build_server().run(transport="stdio")
