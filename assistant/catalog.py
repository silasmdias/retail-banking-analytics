"""The dbt documentation, read from the manifest.

The manifest is what ``dbt parse`` and ``dbt build`` write to
``transform/target/manifest.json``: every model with its columns, descriptions
and tests, and the docs blocks that hold the business definitions.

Two things come out of it:

- the marts, which are the only relations the server may query;
- the documentation of every model and every docs block, which the server may
  quote. Upstream models (staging, intermediate) are documentation only.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

MARTS_SCHEMA = "marts"

# Words too common to help a search.
_STOPWORDS = frozenset(
    "a an and are as at be by do does for from how in is it of on or the to what when "
    "which who why with".split()
)


class CatalogError(RuntimeError):
    """The manifest is missing or is not a dbt manifest."""


@dataclass(frozen=True)
class Column:
    name: str
    description: str
    data_type: str | None  # declared in the model contract; None when there is no contract
    accepted_values: tuple[str, ...] = ()
    constraints: tuple[str, ...] = ()  # "unique", "not_null": taken from the dbt tests


@dataclass(frozen=True)
class Model:
    name: str
    schema: str
    area: str  # folder under models/: "finance", "core", "data_quality", "staging", ...
    description: str
    columns: tuple[Column, ...]
    contract_enforced: bool
    built_from: tuple[str, ...]  # names of the dbt models and seeds it selects from

    @property
    def queryable(self) -> bool:
        return self.schema == MARTS_SCHEMA

    @property
    def relation(self) -> str:
        return f"{self.schema}.{self.name}"

    @property
    def summary(self) -> str:
        """First paragraph of the description."""
        return self.description.split("\n\n", 1)[0].strip()


@dataclass(frozen=True)
class Definition:
    """A dbt docs block: one business definition, written once and reused by many columns."""

    name: str
    text: str


@dataclass(frozen=True)
class SearchHit:
    kind: str  # "definition", "model" or "column"
    name: str
    text: str
    queryable: bool
    score: float = field(compare=False, default=0.0)


@dataclass(frozen=True)
class Catalog:
    project: str
    models: tuple[Model, ...]
    definitions: tuple[Definition, ...]

    @property
    def marts(self) -> tuple[Model, ...]:
        return tuple(model for model in self.models if model.queryable)

    def get(self, name: str) -> Model | None:
        wanted = name.strip().strip('"').lower().removeprefix(f"{MARTS_SCHEMA}.")
        return next((model for model in self.models if model.name == wanted), None)

    def search(self, query: str, limit: int = 5) -> list[SearchHit]:
        """Ranks definitions, models and mart columns by how well they match the words."""
        terms = _terms(query)
        if not terms:
            return []
        hits = [
            SearchHit(hit.kind, hit.name, hit.text, hit.queryable, score)
            for hit in self._entries()
            if (score := _score(terms, hit)) > 0
        ]
        hits.sort(key=lambda hit: (-hit.score, hit.kind, hit.name))
        return hits[:limit]

    def _entries(self) -> list[SearchHit]:
        entries = [
            SearchHit("definition", definition.name, definition.text, False)
            for definition in self.definitions
        ]
        # A column described by a docs block repeats the definition word for word.
        defined = {definition.text for definition in self.definitions}
        for model in self.models:
            entries.append(SearchHit("model", model.name, model.description, model.queryable))
            if model.queryable:
                entries.extend(
                    SearchHit("column", f"{model.name}.{column.name}", column.description, True)
                    for column in model.columns
                    if column.description and column.description not in defined
                )
        return entries


def _terms(text: str) -> list[str]:
    words = re.findall(r"[a-z0-9]+", text.lower().replace("_", " "))
    return [_singular(word) for word in words if word not in _STOPWORDS and len(word) > 1]


def _singular(word: str) -> str:
    """Enough stemming for "reasons" to find "reason"."""
    return word[:-1] if len(word) > 3 and word.endswith("s") and not word.endswith("ss") else word


def _score(terms: list[str], entry: SearchHit) -> float:
    """Counts the query words found; a word in the name is worth three in the text.

    A definition outranks a column that merely repeats it, because the column
    descriptions are rendered from the definitions.
    """
    name_words = set(_terms(entry.name))
    text_words = set(_terms(entry.text))
    found = [term for term in terms if term in name_words or term in text_words]
    matched = sum(3 if term in name_words else 1 for term in found)
    if not matched:
        return 0.0
    weight = {"definition": 1.5, "model": 1.2, "column": 1.0}[entry.kind]
    return matched * weight


# --------------------------------------------------------------------------- #
# Reading the manifest
# --------------------------------------------------------------------------- #


def load_catalog(manifest_path: Path) -> Catalog:
    if not manifest_path.is_file():
        raise CatalogError(
            f"The dbt manifest was not found at {manifest_path}. "
            "Run `make build` (or `make manifest`) to create it."
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        project = manifest["metadata"]["project_name"]
        nodes = manifest["nodes"]
    except (json.JSONDecodeError, KeyError, TypeError) as error:
        raise CatalogError(f"{manifest_path} is not a dbt manifest: {error}") from error

    column_tests = _column_tests(nodes)
    names = {
        unique_id: node["name"]
        for unique_id, node in nodes.items()
        if node["resource_type"] in ("model", "seed")
    }
    models = tuple(
        _model(node, names, column_tests.get(unique_id, {}))
        for unique_id, node in sorted(nodes.items())
        if node["resource_type"] == "model" and node["package_name"] == project
    )
    definitions = tuple(
        Definition(doc["name"], doc["block_contents"].strip())
        for doc in manifest.get("docs", {}).values()
        if doc["package_name"] == project and not doc["name"].startswith("__")
    )
    return Catalog(project=project, models=models, definitions=definitions)


def _model(node: dict, names: dict[str, str], tests: dict[str, dict]) -> Model:
    columns = tuple(
        Column(
            name=column["name"],
            description=column.get("description", "").strip(),
            data_type=column.get("data_type"),
            accepted_values=tuple(tests.get(column["name"], {}).get("accepted_values", ())),
            constraints=tuple(sorted(tests.get(column["name"], {}).get("constraints", ()))),
        )
        for column in node["columns"].values()
    )
    fqn = node["fqn"]  # [project, layer, (area,) model]
    return Model(
        name=node["name"],
        schema=node["schema"],
        area=fqn[-2],
        description=node.get("description", "").strip(),
        columns=columns,
        contract_enforced=bool(node.get("contract", {}).get("enforced")),
        built_from=tuple(
            sorted(names[parent] for parent in node["depends_on"]["nodes"] if parent in names)
        ),
    )


def _column_tests(nodes: dict) -> dict[str, dict[str, dict]]:
    """What the generic dbt tests say about each column: {model id: {column: facts}}."""
    facts: dict[str, dict[str, dict]] = {}
    for node in nodes.values():
        metadata = node.get("test_metadata")
        model_id = node.get("attached_node")
        column = node.get("column_name")
        if node["resource_type"] != "test" or not (metadata and model_id and column):
            continue
        entry = facts.setdefault(model_id, {}).setdefault(column, {})
        if metadata["name"] == "accepted_values":
            entry["accepted_values"] = [str(value) for value in metadata["kwargs"]["values"]]
        elif metadata["name"] in ("unique", "not_null"):
            entry.setdefault("constraints", set()).add(metadata["name"])
    return facts
