"""The tools, called through the MCP protocol by a real client."""

from __future__ import annotations

import os
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path

import anyio
from mcp import Client, StdioServerParameters
from mcp.server.mcpserver import MCPServer

from assistant.catalog import Catalog
from assistant.paths import REPO_ROOT
from assistant.server import MAX_ROWS, build_server
from assistant.tests.conftest import SECRET
from assistant.warehouse import Marts


def call(server: MCPServer | StdioServerParameters, tool: str, **arguments):
    """One tool call over the protocol; returns the result as the client sees it."""

    async def run():
        async with Client(server) as client:
            return await client.call_tool(tool, arguments)

    return anyio.run(run)


def with_client(server: MCPServer, use: Callable[[Client], Awaitable]):
    async def run():
        async with Client(server) as client:
            return await use(client)

    return anyio.run(run)


def test_the_server_offers_four_read_only_tools(catalog: Catalog, marts: Marts) -> None:
    server = build_server(catalog=catalog, marts=marts)

    async def inspect(client: Client):
        return client.server_info, client.instructions, (await client.list_tools()).tools

    info, instructions, tools = with_client(server, inspect)
    assert info.name == "retail-banking-analytics"
    assert "say so instead of estimating" in instructions
    assert [tool.name for tool in tools] == [
        "list_models",
        "describe_model",
        "search_docs",
        "run_query",
    ]
    for tool in tools:
        assert tool.annotations.read_only_hint is True
        assert tool.annotations.open_world_hint is False
        assert tool.description and tool.output_schema


def test_list_models_shows_the_marts_and_the_period(catalog: Catalog, marts: Marts) -> None:
    result = call(build_server(catalog=catalog, marts=marts), "list_models")
    listed = result.structured_content
    assert [model["relation"] for model in listed["models"]] == [
        "marts.dim_products",
        "marts.fct_account_monthly_profitability",
        "marts.revenue_by_product_channel",
    ]
    assert listed["models"][2]["summary"] == "Monthly revenue by product and sales channel."
    assert listed["models"][0]["rows"] == 2
    assert listed["data_period"] == "2024-11 to 2025-02"
    assert "notice" not in listed


def test_describe_model_merges_the_documentation_with_the_real_table(
    catalog: Catalog, marts: Marts
) -> None:
    result = call(build_server(catalog=catalog, marts=marts), "describe_model", name="dim_products")
    described = result.structured_content
    columns = {column["name"]: column for column in described["columns"]}
    assert list(columns) == ["product_id", "product_name", "product_category"]
    # No contract on this model: the type comes from the table itself.
    assert columns["product_id"]["type"] == "varchar"
    assert columns["product_id"]["constraints"] == ["not_null", "unique"]
    assert columns["product_category"]["accepted_values"] == ["checking", "credit_card"]
    assert described["rows"] == 2 and described["contract_enforced"] is False


def test_describe_model_refuses_what_is_not_a_mart(catalog: Catalog, marts: Marts) -> None:
    server = build_server(catalog=catalog, marts=marts)
    upstream = call(server, "describe_model", name="stg_core_banking__customers")
    assert upstream.is_error and "cannot be queried" in upstream.content[0].text
    unknown = call(server, "describe_model", name="raw_core_banking.customers")
    assert unknown.is_error and "dim_products" in unknown.content[0].text


def test_search_docs_returns_definitions_in_full(catalog: Catalog, marts: Marts) -> None:
    result = call(
        build_server(catalog=catalog, marts=marts), "search_docs", query="contribution margin"
    )
    found = result.structured_content
    assert found["results"][0]["kind"] == "definition"
    assert "reported separately as CAC" in found["results"][0]["text"]
    assert found["definitions"] == ["sales_channel", "contribution_margin"]


def test_run_query_returns_rows_as_text_and_as_structure(catalog: Catalog, marts: Marts) -> None:
    result = call(
        build_server(catalog=catalog, marts=marts),
        "run_query",
        sql="SELECT sales_channel, sum(total_revenue) AS revenue "
        "FROM marts.revenue_by_product_channel GROUP BY 1 ORDER BY 2 DESC",
    )
    assert not result.is_error
    assert result.structured_content == {
        "columns": ["sales_channel", "revenue"],
        "rows": [["branch", 400.5], ["online", 200.25]],
        "row_count": 2,
        "truncated": False,
    }
    assert '"rows": [["branch", 400.5], ["online", 200.25]]' in result.content[0].text


def test_run_query_caps_the_rows_it_returns(catalog: Catalog, marts: Marts) -> None:
    result = call(
        build_server(catalog=catalog, marts=marts),
        "run_query",
        sql="SELECT * FROM range(100000)",
        max_rows=100000,
    )
    assert result.structured_content["row_count"] == MAX_ROWS
    assert result.structured_content["truncated"] is True


def test_run_query_refuses_raw_tables_and_writes(catalog: Catalog, marts: Marts) -> None:
    server = build_server(catalog=catalog, marts=marts)
    raw = call(server, "run_query", sql="SELECT * FROM raw_core_banking.customers")
    assert raw.is_error and "Only the marts can be queried" in raw.content[0].text
    assert SECRET not in raw.content[0].text
    write = call(server, "run_query", sql="DELETE FROM marts.dim_products")
    assert write.is_error and "read-only" in write.content[0].text
    count = call(server, "run_query", sql="SELECT count(*) FROM marts.dim_products")
    assert count.structured_content["rows"] == [[2]]


def test_a_missing_warehouse_is_an_error_the_model_can_read(
    catalog: Catalog, tmp_path: Path
) -> None:
    marts = Marts(tmp_path / "missing.duckdb", ["dim_products"])
    result = call(build_server(catalog=catalog, marts=marts), "list_models")
    assert result.is_error and "make build" in result.content[0].text


def test_without_docs_the_tools_return_names_and_types_only(catalog: Catalog, marts: Marts) -> None:
    server = build_server(catalog=catalog, marts=marts, include_docs=False)

    async def inspect(client: Client):
        return client.instructions, [tool.name for tool in (await client.list_tools()).tools]

    instructions, tools = with_client(server, inspect)
    assert tools == ["list_models", "describe_model", "run_query"]
    assert "estimating" not in instructions

    listed = call(server, "list_models").structured_content
    assert listed["models"][0] == {
        "name": "dim_products",
        "relation": "marts.dim_products",
        "rows": 2,
    }
    described = call(server, "describe_model", name="dim_products").structured_content
    assert described["columns"][2] == {"name": "product_category", "type": "varchar"}
    assert "description" not in described


def test_the_server_answers_over_stdio_as_a_client_would_launch_it(
    warehouse_file: Path, manifest_file: Path
) -> None:
    environment = {
        **os.environ,
        "WAREHOUSE_PATH": str(warehouse_file),
        "DBT_MANIFEST_PATH": str(manifest_file),
    }
    server = StdioServerParameters(
        command=sys.executable, args=["-m", "assistant"], env=environment, cwd=str(REPO_ROOT)
    )
    result = call(server, "run_query", sql="SELECT count(*) AS products FROM marts.dim_products")
    assert result.structured_content["rows"] == [[2]]
