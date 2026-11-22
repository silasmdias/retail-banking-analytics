"""The MCP server: four read-only tools over the marts and the dbt documentation.

    list_models      which tables can be queried, and the period they cover
    describe_model   columns, types and how each one may be aggregated
    search_docs      business definitions, from the dbt docs blocks
    run_query        one SELECT statement against the marts

``build_server`` takes the two things the tools read from, so a test can hand it
a small warehouse, and ``include_docs`` so the evaluation can measure what the
documentation is worth: without it the tools return names and types only.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import CallToolResult, TextContent, ToolAnnotations
from pydantic import BaseModel, Field

from assistant.catalog import Catalog, load_catalog
from assistant.paths import paths
from assistant.warehouse import Marts, QueryFailed, QueryRejected, WarehouseUnavailable

SERVER_NAME = "retail-banking-analytics"
SERVER_VERSION = "0.1.0"

DEFAULT_ROWS = 100
MAX_ROWS = 500

# The fact every finance figure sums up from; its months are the period covered.
PERIOD_TABLE, PERIOD_COLUMN = "fct_account_monthly_profitability", "reporting_month"

INSTRUCTIONS = """\
Answers business questions about a fictional retail bank from its dbt marts. \
All data is synthetic and amounts are in US dollars.

1. list_models shows the tables you can query and the period the data covers.
2. describe_model gives a table's columns and says how they may be aggregated. \
Read it before writing SQL: several columns are ratios or per-customer figures \
that must be recomputed from totals, not summed or averaged.
3. search_docs returns the business definitions, such as what counts as revenue \
or how customer lifetime value is calculated.
4. run_query runs one SELECT statement against marts.<model>.

Only the marts are available, and only for reading. When they cannot answer a \
question, say so instead of estimating."""

INSTRUCTIONS_WITHOUT_DOCS = """\
Answers questions from the tables of a retail bank. list_models shows the \
tables, describe_model their columns, run_query runs one SELECT statement \
against marts.<model>. Only these tables are available, and only for reading."""

READ_ONLY = ToolAnnotations(read_only_hint=True, idempotent_hint=True, open_world_hint=False)


# --------------------------------------------------------------------------- #
# What the tools return
# --------------------------------------------------------------------------- #


class ModelSummary(BaseModel):
    name: str
    relation: str = Field(description="Name to use in SQL.")
    area: str | None = Field(None, description="Subject area: finance, core or data_quality.")
    summary: str | None = Field(None, description="What one row is and what the table answers.")
    rows: int | None = Field(None, description="Rows in the table. Null if it is not built yet.")


class ModelList(BaseModel):
    models: list[ModelSummary]
    data_period: str | None = Field(
        None, description="First and last month of data, as YYYY-MM to YYYY-MM."
    )
    notice: str | None = Field(None, description="Set when the data may be one load behind.")


class ColumnDetails(BaseModel):
    name: str
    type: str | None = None
    description: str | None = None
    accepted_values: list[str] | None = Field(
        None, description="Every value the column can hold, enforced by a dbt test."
    )
    constraints: list[str] | None = Field(None, description="unique, not_null: dbt tests.")


class ModelDetails(BaseModel):
    name: str
    relation: str = Field(description="Name to use in SQL.")
    description: str | None = None
    contract_enforced: bool | None = Field(
        None, description="True when dbt fails the build if a column name or type changes."
    )
    built_from: list[str] | None = Field(
        None, description="Upstream dbt models. Only the marts among them can be queried."
    )
    rows: int | None = None
    columns: list[ColumnDetails]


class DocResult(BaseModel):
    kind: str = Field(description="definition, model or column.")
    name: str
    text: str
    queryable: bool = Field(description="False for definitions and for upstream models.")


class DocSearch(BaseModel):
    results: list[DocResult]
    definitions: list[str] = Field(description="Every business definition that can be searched.")


class QueryOutput(BaseModel):
    columns: list[str]
    rows: list[list[Any]] = Field(description="One list per row, in the order of `columns`.")
    row_count: int
    truncated: bool = Field(description="True when more rows existed than `max_rows`.")
    notice: str | None = Field(None, description="Set when the data may be one load behind.")


# --------------------------------------------------------------------------- #
# The server
# --------------------------------------------------------------------------- #


def build_server(
    *,
    catalog: Catalog | None = None,
    marts: Marts | None = None,
    include_docs: bool = True,
) -> MCPServer:
    """Builds the server. With no arguments it reads the repository's manifest and warehouse."""
    if catalog is None:
        catalog = load_catalog(paths().manifest)
    if marts is None:
        marts = Marts(paths().warehouse, [model.name for model in catalog.marts])

    server = MCPServer(
        SERVER_NAME,
        version=SERVER_VERSION,
        instructions=INSTRUCTIONS if include_docs else INSTRUCTIONS_WITHOUT_DOCS,
        log_level="WARNING",
    )
    names = ", ".join(model.name for model in catalog.marts)

    def current_copy():
        try:
            return marts.copy()
        except WarehouseUnavailable as error:
            raise ToolError(str(error)) from error

    @server.tool(title="List the tables", annotations=READ_ONLY)
    def list_models() -> Annotated[CallToolResult, ModelList]:
        """Lists the tables that can be queried and the period the data covers.

        Start here. Every other table of the warehouse (raw, staging,
        intermediate) is out of reach by design.
        """
        copy = current_copy()
        return _result(
            ModelList(
                models=[
                    ModelSummary(
                        name=model.name,
                        relation=model.relation,
                        area=model.area if include_docs else None,
                        summary=model.summary if include_docs else None,
                        rows=copy.row_counts.get(model.name),
                    )
                    for model in catalog.marts
                ],
                data_period=_data_period(marts),
                notice=marts.notice,
            )
        )

    @server.tool(title="Describe a table", annotations=READ_ONLY)
    def describe_model(
        name: Annotated[str, Field(description="Table name, as returned by list_models.")],
    ) -> Annotated[CallToolResult, ModelDetails]:
        """Returns the columns of one table: name, type, meaning and accepted values.

        Read it before querying a table. The descriptions say which columns are
        ratios or per-customer figures that cannot be summed or averaged across
        rows, and how to recompute them.
        """
        model = catalog.get(name)
        if model is None:
            raise ToolError(f"There is no table named {name!r}. The tables are: {names}.")
        if not model.queryable:
            raise ToolError(
                f"{model.name} is an upstream dbt model ({model.area}) and cannot be queried. "
                f"The tables are: {names}."
            )
        copy = current_copy()
        types = copy.column_types.get(model.name, {})
        documented = {column.name: column for column in model.columns}
        columns = []
        # The table's own column order; a documented column the table lacks comes last.
        for column_name in [*types, *(column for column in documented if column not in types)]:
            column = documented.get(column_name)
            details = ColumnDetails(
                name=column_name, type=types.get(column_name) or (column and column.data_type)
            )
            if include_docs and column is not None:
                details.description = column.description or None
                details.accepted_values = list(column.accepted_values) or None
                details.constraints = list(column.constraints) or None
            columns.append(details)
        return _result(
            ModelDetails(
                name=model.name,
                relation=model.relation,
                description=model.description if include_docs else None,
                contract_enforced=model.contract_enforced if include_docs else None,
                built_from=list(model.built_from) if include_docs else None,
                rows=copy.row_counts.get(model.name),
                columns=columns,
            )
        )

    if include_docs:

        @server.tool(title="Search the documentation", annotations=READ_ONLY)
        def search_docs(
            query: Annotated[
                str, Field(description="Words to look for, such as 'contribution margin'.")
            ],
            limit: Annotated[int, Field(ge=1, le=10, description="Results to return.")] = 5,
        ) -> Annotated[CallToolResult, DocSearch]:
            """Searches the business definitions and the descriptions of tables and columns.

            Use it for questions about meaning: what counts as revenue, which
            channel a figure is attributed to, how customer lifetime value is
            calculated and what it leaves out. Definitions are returned in full.
            """
            return _result(
                DocSearch(
                    results=[
                        DocResult(
                            kind=hit.kind, name=hit.name, text=hit.text, queryable=hit.queryable
                        )
                        for hit in catalog.search(query, limit)
                    ],
                    definitions=[definition.name for definition in catalog.definitions],
                )
            )

    @server.tool(title="Run a query", annotations=READ_ONLY)
    def run_query(
        sql: Annotated[
            str,
            Field(description="One SELECT statement in DuckDB SQL, over marts.<model> tables."),
        ],
        max_rows: Annotated[
            int, Field(ge=1, description=f"Rows to return, {MAX_ROWS} at most.")
        ] = DEFAULT_ROWS,
    ) -> Annotated[CallToolResult, QueryOutput]:
        """Runs one read-only SELECT statement against the marts and returns the rows.

        Aggregate in SQL and ask for the few rows that answer the question.
        Anything other than a single SELECT is refused, and so is any table
        outside the marts.
        """
        try:
            result = marts.query(sql, max_rows=min(max_rows, MAX_ROWS))
        except (QueryRejected, QueryFailed, WarehouseUnavailable) as error:
            raise ToolError(str(error)) from error
        return _result(
            QueryOutput(
                columns=list(result.columns),
                rows=result.rows,
                row_count=len(result.rows),
                truncated=result.truncated,
                notice=marts.notice,
            )
        )

    return server


def _result(payload: BaseModel) -> CallToolResult:
    """The same data twice: as compact JSON text for the model and as structured content.

    Empty fields are left out. The SDK's default would pretty-print them, which
    costs a model several times the tokens for the same information.
    """
    data = payload.model_dump(mode="json", exclude_none=True)
    return CallToolResult(
        content=[TextContent(type="text", text=json.dumps(data, ensure_ascii=False))],
        structured_content=data,
    )


def _data_period(marts: Marts) -> str | None:
    if PERIOD_TABLE not in marts.copy().row_counts:
        return None
    first, last = marts.query(
        f"SELECT strftime(min({PERIOD_COLUMN}), '%Y-%m'), strftime(max({PERIOD_COLUMN}), '%Y-%m') "
        f"FROM marts.{PERIOD_TABLE}"
    ).rows[0]
    return f"{first} to {last}" if first else None
