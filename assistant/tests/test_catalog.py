"""The catalog: what the dbt manifest says can be queried and what can be quoted."""

from __future__ import annotations

from pathlib import Path

import pytest

from assistant.catalog import Catalog, CatalogError, load_catalog
from assistant.paths import REPO_ROOT


def test_only_the_models_in_the_marts_schema_can_be_queried(catalog: Catalog) -> None:
    assert [model.name for model in catalog.marts] == [
        "dim_products",
        "fct_account_monthly_profitability",
        "revenue_by_product_channel",
    ]
    staging = catalog.get("stg_core_banking__customers")
    assert staging is not None and not staging.queryable


def test_a_model_is_found_with_or_without_its_schema(catalog: Catalog) -> None:
    assert catalog.get("marts.dim_products") is catalog.get("dim_products")
    assert catalog.get(' "DIM_PRODUCTS" ') is catalog.get("dim_products")
    assert catalog.get("raw_core_banking.customers") is None


def test_column_facts_come_from_the_dbt_tests(catalog: Catalog) -> None:
    columns = {column.name: column for column in catalog.get("dim_products").columns}
    assert columns["product_category"].accepted_values == ("checking", "credit_card")
    assert columns["product_id"].constraints == ("not_null", "unique")
    assert columns["product_name"].constraints == ()


def test_contract_types_lineage_and_summary_are_read(catalog: Catalog) -> None:
    revenue = catalog.get("revenue_by_product_channel")
    assert revenue.contract_enforced and revenue.area == "finance"
    assert revenue.built_from == ("dim_products", "fct_account_monthly_profitability")
    assert revenue.summary == "Monthly revenue by product and sales channel."
    assert {column.name: column.data_type for column in revenue.columns}["total_revenue"] == (
        "decimal(18,2)"
    )
    assert not catalog.get("dim_products").contract_enforced


def test_definitions_are_the_project_docs_blocks(catalog: Catalog) -> None:
    assert [definition.name for definition in catalog.definitions] == [
        "sales_channel",
        "contribution_margin",
    ]


def test_search_puts_the_definition_first_and_does_not_repeat_it(catalog: Catalog) -> None:
    hits = catalog.search("sales channel")
    assert (hits[0].kind, hits[0].name) == ("definition", "sales_channel")
    # The column that quotes the definition word for word is not a second hit.
    assert "revenue_by_product_channel.sales_channel" not in [hit.name for hit in hits]


def test_search_finds_plurals_and_text_matches(catalog: Catalog) -> None:
    assert catalog.search("products")[0].name == "dim_products"
    assert catalog.search("is acquisition cost excluded?")[0].name == "contribution_margin"


def test_search_tells_documentation_from_queryable_tables(catalog: Catalog) -> None:
    hits = {hit.name: hit for hit in catalog.search("customer deduplicated", limit=10)}
    assert not hits["stg_core_banking__customers"].queryable


def test_search_without_a_match_returns_nothing(catalog: Catalog) -> None:
    assert catalog.search("weather forecast") == []
    assert catalog.search("the of and") == []


def test_a_missing_manifest_says_how_to_create_it(tmp_path: Path) -> None:
    with pytest.raises(CatalogError, match="make build"):
        load_catalog(tmp_path / "manifest.json")


def test_a_file_that_is_not_a_manifest_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text('{"nodes": []}', encoding="utf-8")
    with pytest.raises(CatalogError, match="not a dbt manifest"):
        load_catalog(path)


def test_every_mart_and_column_of_the_real_project_is_documented() -> None:
    """The server has nothing else to explain a figure with, so a gap here is a bug."""
    catalog = load_catalog(REPO_ROOT / "transform" / "target" / "manifest.json")
    assert len(catalog.marts) == 10
    for model in catalog.marts:
        assert model.description, f"{model.name} has no description"
        assert model.columns, f"{model.name} documents no column"
        undocumented = [column.name for column in model.columns if not column.description]
        assert not undocumented, f"{model.name} has undocumented columns: {undocumented}"
    assert "customer_lifetime_value" in [definition.name for definition in catalog.definitions]
