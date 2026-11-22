"""Starts the MCP server.

    python -m assistant                 over stdio, the way an MCP client launches it
    python -m assistant --http          over HTTP, at http://127.0.0.1:8765/mcp

Over stdio the protocol owns standard output, so everything this module says
goes to standard error.
"""

from __future__ import annotations

import argparse
import sys

from assistant.catalog import CatalogError, load_catalog
from assistant.paths import paths
from assistant.server import build_server
from assistant.warehouse import Marts, WarehouseUnavailable

DEFAULT_PORT = 8765


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m assistant",
        description="MCP server that answers from the dbt marts and their documentation.",
    )
    parser.add_argument(
        "--http", action="store_true", help="serve over HTTP instead of stdio, on localhost"
    )
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="port for --http")
    parser.add_argument(
        "--no-docs",
        action="store_true",
        help="return table and column names only, without the dbt documentation "
        "(used by the evaluation to measure what the documentation is worth)",
    )
    args = parser.parse_args(argv)

    where = paths()
    try:
        catalog = load_catalog(where.manifest)
    except CatalogError as error:
        print(error, file=sys.stderr)
        return 1
    marts = Marts(where.warehouse, [model.name for model in catalog.marts])
    try:
        copy = marts.copy()
        print(
            f"Serving {len(copy.row_counts)} marts from a private copy of {where.warehouse}",
            file=sys.stderr,
        )
    except WarehouseUnavailable as error:
        # The server still starts: the copy is made on the first call that finds a warehouse.
        print(f"Starting without data. {error}", file=sys.stderr)

    server = build_server(catalog=catalog, marts=marts, include_docs=not args.no_docs)
    if args.http:
        print(f"Listening on http://127.0.0.1:{args.port}/mcp", file=sys.stderr)
        server.run(transport="streamable-http", host="127.0.0.1", port=args.port)
    else:
        server.run(transport="stdio")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
