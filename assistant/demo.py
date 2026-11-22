"""Asks the server four business questions and tries three things it must refuse.

    python -m assistant.demo                     launches the server over stdio
    python -m assistant.demo --url URL           talks to a server started with --http

There is no language model here. The script plays the client: it makes the tool
calls a model would make, through the real protocol, so anyone can repeat it
without an account or a key.

Every figure the server returns is compared with a direct read of the warehouse
file, and every refusal is checked. The script stops with an error when
something differs, which makes it an end-to-end test as well.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any

import anyio
import duckdb
from mcp import Client, StdioServerParameters

from assistant.paths import REPO_ROOT, paths
from assistant.warehouse import json_safe


class DemoFailed(SystemExit):
    def __init__(self, message: str) -> None:
        super().__init__(f"\nThe demo did not behave as designed: {message}")


def expect(condition: bool, message: str) -> None:
    if not condition:
        raise DemoFailed(message)


def heading(text: str) -> None:
    print(f"\n{text}\n{'-' * len(text)}")


# --------------------------------------------------------------------------- #
# The questions
# --------------------------------------------------------------------------- #

REVENUE_BY_PRODUCT = """\
SELECT product_name,
       sum(net_interest_income) AS net_interest_income,
       sum(fee_revenue) AS fees,
       sum(interchange_revenue) AS interchange,
       sum(total_revenue) AS total
FROM marts.revenue_by_product_channel
WHERE year(reporting_month) = {year}
GROUP BY product_name
ORDER BY total DESC"""

# The per-customer columns of this table cannot be averaged across cohorts.
# The documentation says to sum the totals and divide, and that is what this does.
LIFETIME_VALUE_BY_CHANNEL = """\
SELECT acquisition_channel,
       sum(customers_acquired) AS customers,
       sum(acquisition_spend) / sum(customers_acquired) AS cac,
       sum(realized_discounted_margin + projected_discounted_margin)
           / sum(customers_acquired) AS clv,
       sum(realized_discounted_margin + projected_discounted_margin)
           / sum(acquisition_spend) AS clv_to_cac
FROM marts.customer_lifetime_value
GROUP BY acquisition_channel
ORDER BY clv DESC"""

BUDGET_BY_PRODUCT = """\
SELECT product_name,
       sum(budget_revenue) AS budget,
       sum(actual_revenue) AS actual,
       sum(variance_amount) AS variance,
       100 * sum(variance_amount) / sum(budget_revenue) AS variance_pct
FROM marts.budget_vs_actual
WHERE year(reporting_month) = {year}
GROUP BY product_name
ORDER BY variance_pct"""

REJECTED_BY_REASON = """\
SELECT source_table, rejection_reason, sum(rejected_records) AS records
FROM marts.dq_rejected_records
GROUP BY source_table, rejection_reason
ORDER BY records DESC, source_table, rejection_reason"""

REFUSALS = [
    (
        "Read a raw table",
        "SELECT * FROM raw_core_banking.transactions LIMIT 5",
        "Only the marts can be queried",
    ),
    (
        "Change the data",
        "DELETE FROM marts.dim_customers",
        "Only SELECT statements are allowed",
    ),
    (
        "Read a file from disk",
        "SELECT * FROM read_csv('data/raw/core_banking/customers/customers.csv')",
        "Reading files, attaching databases and loading extensions are disabled",
    ),
]


# --------------------------------------------------------------------------- #
# The client
# --------------------------------------------------------------------------- #


class Session:
    """The tool calls, with each query checked against the warehouse file."""

    def __init__(self, client: Client) -> None:
        self.client = client
        self.checked = 0

    async def call(self, tool: str, **arguments: Any) -> dict:
        result = await self.client.call_tool(tool, arguments)
        expect(not result.is_error, f"{tool} failed: {result.content[0].text}")
        return result.structured_content

    async def query(self, sql: str) -> list[list]:
        print("run_query")
        print("\n".join(f"    {line}" for line in sql.splitlines()))
        answer = await self.call("run_query", sql=sql)
        expect(not answer["truncated"], "the result was cut short")
        expect(answer["rows"] == read_directly(sql), "the server and the warehouse disagree")
        self.checked += 1
        return answer["rows"]

    async def refused(self, sql: str, reason: str) -> str:
        result = await self.client.call_tool("run_query", {"sql": sql})
        message = result.content[0].text
        expect(result.is_error is True, f"the server ran a statement it must refuse: {sql}")
        expect(reason in message, f"refused for an unexpected reason: {message}")
        return message.removeprefix("Error executing tool run_query: ")


def read_directly(sql: str) -> list[list]:
    """The same statement, run on the warehouse file without the server."""
    try:
        with duckdb.connect(str(paths().warehouse), read_only=True) as connection:
            rows = connection.execute(sql).fetchall()
    except duckdb.IOException as error:
        raise DemoFailed(
            f"the warehouse file could not be opened to check a figure: {error}"
        ) from error
    return [[json_safe(value) for value in row] for row in rows]


def table(headers: list[str], rows: list[list], formats: list[str]) -> None:
    cells = [
        [format(value, spec) for value, spec in zip(row, formats, strict=True)] for row in rows
    ]
    widths = [max(len(text) for text in column) for column in zip(headers, *cells, strict=True)]
    for line in [headers, *cells]:
        first, *rest = (
            text.ljust(width) if index == 0 else text.rjust(width)
            for index, (text, width) in enumerate(zip(line, widths, strict=True))
        )
        print("  " + "  ".join([first, *rest]).rstrip())


def sentence_with(text: str, words: str) -> str:
    """The sentence of a description that holds ``words``, on one line."""
    flat = " ".join(text.split())
    for sentence in flat.split(". "):
        if words in sentence:
            return sentence.strip().removeprefix("* ").removesuffix(".") + "."
    return ""


async def play(client: Client) -> None:
    session = Session(client)

    heading("The server")
    tools = [tool.name for tool in (await client.list_tools()).tools]
    print(f"{client.server_info.name} {client.server_info.version}")
    print(f"Tools: {', '.join(tools)}")
    listed = await session.call("list_models")
    period = listed["data_period"]
    print(f"Data:  {len(listed['models'])} marts, {period}")
    expect(period is not None, "the marts hold no data")
    expect(
        all(model["relation"].startswith("marts.") for model in listed["models"]),
        "list_models returned something that is not a mart",
    )
    last_year, last_month = (int(part) for part in period.split(" to ")[1].split("-"))
    year = last_year if last_month == 12 else last_year - 1

    heading(f"1. Where did {year} revenue come from?")
    described = await session.call("describe_model", name="revenue_by_product_channel")
    print(f"describe_model revenue_by_product_channel -> {len(described['columns'])} columns")
    rows = await session.query(REVENUE_BY_PRODUCT.format(year=year))
    expect(len(rows) > 0, f"no revenue in {year}")
    print()
    table(
        ["Product", "Net interest income", "Fees", "Interchange", "Total"],
        rows,
        ["", ",.0f", ",.0f", ",.0f", ",.0f"],
    )

    heading("2. What is a customer worth, by acquisition channel?")
    described = await session.call("describe_model", name="customer_lifetime_value")
    rule = sentence_with(described["description"], "do not average")
    expect(rule != "", "the documentation lost its rule on how to aggregate lifetime value")
    print(f'describe_model customer_lifetime_value -> "{rule}"')
    found = await session.call("search_docs", query="does lifetime value subtract acquisition cost")
    definition = next(
        (hit for hit in found["results"] if hit["name"] == "customer_lifetime_value"), None
    )
    expect(definition is not None, "search_docs did not find the lifetime value definition")
    print(
        "search_docs -> definition customer_lifetime_value: "
        f'"{sentence_with(definition["text"], "does not subtract acquisition cost")}"'
    )
    rows = await session.query(LIFETIME_VALUE_BY_CHANNEL)
    print()
    table(
        ["Channel", "Customers", "CAC", "CLV (60 months)", "CLV to CAC"],
        rows,
        ["", ",.0f", ",.0f", ",.0f", ".1f"],
    )

    heading(f"3. Was {year} revenue on plan?")
    rows = await session.query(BUDGET_BY_PRODUCT.format(year=year))
    print()
    table(
        ["Product", "Budget", "Actual", "Variance", "Variance %"],
        rows,
        ["", ",.0f", ",.0f", ",.0f", "+.1f"],
    )

    heading("4. Which records did the pipeline refuse to use?")
    rows = await session.query(REJECTED_BY_REASON)
    print()
    table(["Source", "Reason", "Records"], rows, ["", "", ",.0f"])

    heading("What the server refuses")
    for title, sql, reason in REFUSALS:
        message = await session.refused(sql, reason)
        first_sentence = message.splitlines()[0].split(". ")[0].removesuffix(".") + "."
        print(f"{title}\n    {sql}\n    refused: {first_sentence}")

    print(
        f"\nThe {session.checked} query results matched a direct read of the warehouse file, "
        f"and the {len(REFUSALS)} statements were refused."
    )


async def main(url: str | None) -> None:
    server = url or StdioServerParameters(
        command=sys.executable,
        args=["-m", "assistant"],
        env=dict(os.environ),
        cwd=str(REPO_ROOT),
    )
    async with Client(server) as client:
        await play(client)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="python -m assistant.demo", description=__doc__)
    parser.add_argument(
        "--url", help="address of a server started with --http, instead of launching one"
    )
    anyio.run(main, parser.parse_args().url)
