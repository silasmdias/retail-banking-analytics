.DEFAULT_GOAL := build
.PHONY: build setup generate load dbt test lint docs clean \
	orchestrate demo-new-month demo-broken-month pipeline test-orchestration \
	assistant demo-assistant test-assistant \
	setup-orchestration setup-assistant manifest

# Dagster keeps its run history here, so runs started from the command line and
# from the UI show up in the same place.
export DAGSTER_HOME := $(CURDIR)/data/dagster

ORCHESTRATION := uv run --group orchestration
ASSISTANT := uv run --group assistant

## build: install dependencies, generate the data, load it and run every dbt model and test
build: setup generate load dbt

## setup: create the virtual environment with the pinned dependencies
setup:
	uv sync --inexact

## generate: write the synthetic source files to data/raw
generate:
	uv run python -m generator

## load: load the source files into data/warehouse.duckdb as raw tables
load:
	uv run python -m ingest.load_raw

## dbt: build and test every model
dbt: transform/dbt_packages
	cd transform && uv run dbt build

## test: run the generator and loader tests
test:
	uv run pytest -q

## lint: check Python style and formatting
lint:
	uv run ruff check . && uv run ruff format --check .

## docs: build the dbt documentation site and serve it locally
docs: transform/dbt_packages
	cd transform && uv run dbt docs generate && uv run dbt docs serve

## orchestrate: start Dagster at http://localhost:3000; it loads whatever lands in data/raw
orchestrate: setup-orchestration manifest data/raw/_manifest.json
	$(ORCHESTRATION) dg dev

## demo-new-month: deliver the next month and run the pipeline on it, without the UI
demo-new-month: setup-orchestration manifest
	$(ORCHESTRATION) python -m orchestration.demo new-month

## demo-broken-month: deliver the next month broken in three ways, then fixed
demo-broken-month: setup-orchestration manifest
	$(ORCHESTRATION) python -m orchestration.demo broken-month

## pipeline: check, load and build whatever is in data/raw, once, without the UI
pipeline: setup-orchestration manifest
	$(ORCHESTRATION) python -m orchestration.run_once

## test-orchestration: run the orchestration tests, including two end-to-end runs
test-orchestration: setup-orchestration manifest
	$(ORCHESTRATION) pytest -q orchestration/tests

## assistant: start the MCP server at http://127.0.0.1:8765/mcp; it answers from the marts only
assistant: setup-assistant manifest data/warehouse.duckdb
	$(ASSISTANT) python -m assistant --http

## demo-assistant: ask the server four business questions and try three things it must refuse
demo-assistant: setup-assistant manifest data/warehouse.duckdb
	$(ASSISTANT) python -m assistant.demo

## test-assistant: run the tests of the server and of the evaluation
test-assistant: setup-assistant manifest
	$(ASSISTANT) pytest -q assistant/tests

## clean: remove generated data, the local warehouse, run history and dbt artifacts
clean:
	rm -rf data/raw data/warehouse.duckdb data/warehouse.duckdb.wal data/dagster data/alerts \
		transform/target transform/dbt_packages transform/logs

# ---------------------------------------------------------------------------
# Helpers the targets above depend on
# ---------------------------------------------------------------------------

setup-orchestration:
	uv sync --inexact --group orchestration
	mkdir -p "$(DAGSTER_HOME)"
	cp orchestration/dagster.yaml "$(DAGSTER_HOME)/dagster.yaml"

setup-assistant:
	uv sync --inexact --group assistant

# Dagster reads the dbt project from its manifest, and so does the MCP server.
manifest: transform/dbt_packages
	cd transform && uv run dbt parse --quiet

# dbt packages are downloaded once, not on every build.
transform/dbt_packages: transform/packages.yml transform/package-lock.yml
	cd transform && uv run dbt deps

# The first delivery, for a landing folder that is still empty.
data/raw/_manifest.json:
	uv run python -m generator

# The MCP server reads the marts, so a clone without a warehouse builds one first.
data/warehouse.duckdb:
	$(MAKE) build
