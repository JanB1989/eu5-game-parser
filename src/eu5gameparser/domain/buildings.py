from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl

from eu5gameparser.clausewitz.parser import parse_file
from eu5gameparser.clausewitz.syntax import CDocument, CEntry, CList, Scalar, Value
from eu5gameparser.config import ParserConfig
from eu5gameparser.scanner import iter_text_files

PRODUCTION_METHOD_METADATA = {"category", "produced", "output", "potential", "allow", "no_upkeep"}


@dataclass(frozen=True)
class GoodsInput:
    goods: str
    amount: float


@dataclass(frozen=True)
class ProductionMethod:
    name: str
    category: str | None
    produced: str | None
    output: float | None
    inputs: list[GoodsInput]
    no_upkeep: bool
    source_kind: str
    source_file: str
    source_line: int
    building: str | None = None
    potential: Any | None = None
    allow: Any | None = None


@dataclass(frozen=True)
class Building:
    name: str
    category: str | None
    pop_type: str | None
    max_levels: str | int | float | bool | None
    possible_production_methods: list[str]
    unique_production_methods: list[str]
    source_file: str
    source_line: int


@dataclass(frozen=True)
class BuildingData:
    categories: pl.DataFrame
    buildings: pl.DataFrame
    production_methods: pl.DataFrame
    goods_flow_nodes: pl.DataFrame
    goods_flow_edges: pl.DataFrame
    unresolved_production_methods: pl.DataFrame
    duplicate_production_methods: pl.DataFrame
    warnings: list[str] = field(default_factory=list)


def load_building_data(config: ParserConfig | None = None) -> BuildingData:
    config = config or ParserConfig.from_env()
    categories = _load_categories(config.building_categories_dir)
    global_methods = _load_global_production_methods(config.production_methods_dir)
    buildings, inline_methods = _load_buildings(config.building_types_dir)

    all_methods = [*global_methods, *inline_methods]
    method_names = {method.name for method in all_methods}
    unresolved = sorted(
        {
            (building.name, method)
            for building in buildings
            for method in building.possible_production_methods
            if method not in method_names
        }
    )
    duplicates = _find_duplicates(all_methods)
    warnings = [
        f"{building} references missing production method {method}"
        for building, method in unresolved
    ]
    warnings.extend(f"duplicate production method {name}" for name in duplicates)

    categories_df = pl.DataFrame(
        [_category_row(entry) for entry in categories],
        schema={
            "name": pl.String,
            "source_file": pl.String,
            "source_line": pl.Int64,
        },
    )
    buildings_df = pl.DataFrame(
        [_building_row(building) for building in buildings],
        schema={
            "name": pl.String,
            "category": pl.String,
            "pop_type": pl.String,
            "max_levels": pl.String,
            "possible_production_methods": pl.List(pl.String),
            "unique_production_methods": pl.List(pl.String),
            "source_file": pl.String,
            "source_line": pl.Int64,
        },
    )
    methods_df = pl.DataFrame(
        [_production_method_row(method) for method in all_methods],
        schema={
            "name": pl.String,
            "category": pl.String,
            "produced": pl.String,
            "output": pl.Float64,
            "input_goods": pl.List(pl.String),
            "input_amounts": pl.List(pl.Float64),
            "no_upkeep": pl.Boolean,
            "source_kind": pl.String,
            "building": pl.String,
            "source_file": pl.String,
            "source_line": pl.Int64,
        },
    )
    unresolved_df = pl.DataFrame(
        [{"building": building, "production_method": method} for building, method in unresolved],
        schema={"building": pl.String, "production_method": pl.String},
    )
    duplicate_df = pl.DataFrame(
        [{"production_method": name} for name in duplicates],
        schema={"production_method": pl.String},
    )

    nodes, edges = build_goods_flow_tables(buildings, all_methods)

    return BuildingData(
        categories=categories_df,
        buildings=buildings_df,
        production_methods=methods_df,
        goods_flow_nodes=nodes,
        goods_flow_edges=edges,
        unresolved_production_methods=unresolved_df,
        duplicate_production_methods=duplicate_df,
        warnings=warnings,
    )


def build_goods_flow_tables(
    buildings: list[Building], production_methods: list[ProductionMethod]
) -> tuple[pl.DataFrame, pl.DataFrame]:
    nodes: dict[str, dict[str, str]] = {}
    edges: list[dict[str, Any]] = []

    def add_node(node_id: str, node_type: str, label: str) -> None:
        nodes.setdefault(node_id, {"id": node_id, "type": node_type, "label": label})

    for building in buildings:
        building_id = f"building:{building.name}"
        add_node(building_id, "building", building.name)

    for method in production_methods:
        method_id = f"production_method:{method.name}"
        add_node(method_id, "production_method", method.name)
        if method.building:
            building_id = f"building:{method.building}"
            add_node(building_id, "building", method.building)
            edges.append(
                {
                    "source": building_id,
                    "target": method_id,
                    "kind": "uses_production_method",
                    "amount": None,
                    "building": method.building,
                    "production_method": method.name,
                    "goods": None,
                }
            )
        for goods_input in method.inputs:
            goods_id = f"goods:{goods_input.goods}"
            add_node(goods_id, "goods", goods_input.goods)
            edges.append(
                {
                    "source": goods_id,
                    "target": method_id,
                    "kind": "consumes",
                    "amount": goods_input.amount,
                    "building": method.building,
                    "production_method": method.name,
                    "goods": goods_input.goods,
                }
            )
        if method.produced:
            goods_id = f"goods:{method.produced}"
            add_node(goods_id, "goods", method.produced)
            edges.append(
                {
                    "source": method_id,
                    "target": goods_id,
                    "kind": "produces",
                    "amount": method.output,
                    "building": method.building,
                    "production_method": method.name,
                    "goods": method.produced,
                }
            )

    nodes_df = pl.DataFrame(
        list(nodes.values()),
        schema={"id": pl.String, "type": pl.String, "label": pl.String},
    )
    edges_df = pl.DataFrame(
        edges,
        schema={
            "source": pl.String,
            "target": pl.String,
            "kind": pl.String,
            "amount": pl.Float64,
            "building": pl.String,
            "production_method": pl.String,
            "goods": pl.String,
        },
    )
    return nodes_df, edges_df


def _load_categories(root: Path) -> list[CEntry]:
    return [entry for document in _parse_directory(root) for entry in document.entries]


def _load_global_production_methods(root: Path) -> list[ProductionMethod]:
    methods: list[ProductionMethod] = []
    for document in _parse_directory(root):
        for entry in document.entries:
            if isinstance(entry.value, CList):
                methods.append(
                    _production_method_from_entry(entry, source_kind="global", building=None)
                )
    return methods


def _load_buildings(root: Path) -> tuple[list[Building], list[ProductionMethod]]:
    buildings: list[Building] = []
    inline_methods: list[ProductionMethod] = []
    for document in _parse_directory(root):
        for entry in document.entries:
            if not isinstance(entry.value, CList):
                continue
            unique_methods = _unique_production_methods(entry.value)
            buildings.append(_building_from_entry(entry, unique_methods))
            inline_methods.extend(
                _production_method_from_entry(
                    method_entry, source_kind="inline", building=entry.key
                )
                for method_entry in unique_methods
            )
    return buildings, inline_methods


def _parse_directory(root: Path) -> list[CDocument]:
    return [parse_file(path) for path in iter_text_files(root)]


def _building_from_entry(entry: CEntry, unique_methods: list[CEntry]) -> Building:
    block = _as_block(entry.value)
    possible = _scalar_list(block.first("possible_production_methods"))
    return Building(
        name=entry.key,
        category=_scalar_string(block.first("category")),
        pop_type=_scalar_string(block.first("pop_type")),
        max_levels=_scalar(block.first("max_levels")),
        possible_production_methods=possible,
        unique_production_methods=[method.key for method in unique_methods],
        source_file=str(entry.location.path or ""),
        source_line=entry.location.line,
    )


def _unique_production_methods(block: CList) -> list[CEntry]:
    unique_block = block.first("unique_production_methods")
    if not isinstance(unique_block, CList):
        return []
    return [entry for entry in unique_block.entries if isinstance(entry.value, CList)]


def _production_method_from_entry(
    entry: CEntry, source_kind: str, building: str | None
) -> ProductionMethod:
    block = _as_block(entry.value)
    inputs: list[GoodsInput] = []
    for child in block.entries:
        if child.key in PRODUCTION_METHOD_METADATA or child.op != "=":
            continue
        value = _scalar(child.value)
        if isinstance(value, int | float):
            inputs.append(GoodsInput(goods=child.key, amount=float(value)))

    return ProductionMethod(
        name=entry.key,
        category=_scalar_string(block.first("category")),
        produced=_scalar_string(block.first("produced")),
        output=_scalar_float(block.first("output")),
        inputs=inputs,
        no_upkeep=bool(_scalar(block.first("no_upkeep", False))),
        source_kind=source_kind,
        building=building,
        potential=_to_python(block.first("potential")),
        allow=_to_python(block.first("allow")),
        source_file=str(entry.location.path or ""),
        source_line=entry.location.line,
    )


def _find_duplicates(methods: list[ProductionMethod]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for method in methods:
        if method.name in seen:
            duplicates.add(method.name)
        seen.add(method.name)
    return sorted(duplicates)


def _category_row(entry: CEntry) -> dict[str, Any]:
    return {
        "name": entry.key,
        "source_file": str(entry.location.path or ""),
        "source_line": entry.location.line,
    }


def _building_row(building: Building) -> dict[str, Any]:
    return {
        "name": building.name,
        "category": building.category,
        "pop_type": building.pop_type,
        "max_levels": None if building.max_levels is None else str(building.max_levels),
        "possible_production_methods": building.possible_production_methods,
        "unique_production_methods": building.unique_production_methods,
        "source_file": building.source_file,
        "source_line": building.source_line,
    }


def _production_method_row(method: ProductionMethod) -> dict[str, Any]:
    return {
        "name": method.name,
        "category": method.category,
        "produced": method.produced,
        "output": method.output,
        "input_goods": [item.goods for item in method.inputs],
        "input_amounts": [item.amount for item in method.inputs],
        "no_upkeep": method.no_upkeep,
        "source_kind": method.source_kind,
        "building": method.building,
        "source_file": method.source_file,
        "source_line": method.source_line,
    }


def _as_block(value: Value) -> CList:
    if not isinstance(value, CList):
        raise TypeError(f"Expected block, got {type(value).__name__}")
    return value


def _scalar(value: Value | None) -> Scalar | None:
    if isinstance(value, CList):
        return None
    return value


def _scalar_string(value: Value | None) -> str | None:
    scalar = _scalar(value)
    return None if scalar is None else str(scalar)


def _scalar_float(value: Value | None) -> float | None:
    scalar = _scalar(value)
    if isinstance(scalar, int | float):
        return float(scalar)
    return None


def _scalar_list(value: Value | None) -> list[str]:
    if isinstance(value, CList):
        return [str(item) for item in value.items if not isinstance(item, CList)]
    scalar = _scalar(value)
    return [] if scalar is None else [str(scalar)]


def _to_python(value: Value | None) -> Any:
    if isinstance(value, CList):
        return {
            "entries": [
                {"key": entry.key, "op": entry.op, "value": _to_python(entry.value)}
                for entry in value.entries
            ],
            "items": [_to_python(item) for item in value.items],
        }
    return value
