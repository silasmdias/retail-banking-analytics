.DEFAULT_GOAL := build
.PHONY: build setup generate load dbt test lint docs clean

## build: install dependencies, generate the data, load it and run every dbt model and test
build: setup generate load dbt

## setup: create the virtual environment with the pinned dependencies
setup:
	uv sync

## generate: write the synthetic source files to data/raw
generate:
	uv run python -m generator

## load: load the source files into data/warehouse.duckdb as raw tables
load:
	uv run python -m ingest.load_raw

## dbt: install dbt packages, then build and test every model
dbt:
	cd transform && uv run dbt deps && uv run dbt build

## test: run the generator's unit tests
test:
	uv run pytest -q

## lint: check Python style and formatting
lint:
	uv run ruff check . && uv run ruff format --check .

## docs: build the dbt documentation site and serve it locally
docs:
	cd transform && uv run dbt docs generate && uv run dbt docs serve

## clean: remove generated data, the local warehouse and dbt artifacts
clean:
	rm -rf data/raw data/warehouse.duckdb data/warehouse.duckdb.wal \
		transform/target transform/dbt_packages transform/logs
