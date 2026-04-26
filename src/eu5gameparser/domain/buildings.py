from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl

from eu5gameparser.clausewitz.syntax import CEntry, CList, Scalar, Value
from eu5gameparser.config import ParserConfig
from eu5gameparser.load_order import (
    DEFAULT_LOAD_ORDER_PATH,
    DataProfile,
    GameLayer,
    MergedDirectory,
    MergedEntry,
    load_merged_directory,
    load_profile,
)

PRODUCTION_METHOD_METADATA = {
    "allow",
    "category",
    "debug_max_profit",
    "no_upkeep",
    "output",
    "potential",
    "produced",
}


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
    source_layer: str
    source_mod: str | None
    source_mode: str
    source_history: str
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
    source_layer: str
    source_mod: str | None
    source_mode: str
    source_history: str


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


def load_building_data(
    config: ParserConfig | None = None,
    *,
    profile: str | DataProfile | None = None,
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
) -> BuildingData:
    profile_config = _resolve_profile(config, profile, load_order_path)
    categories_dir = load_merged_directory(profile_config, "building_categories")
    methods_dir = load_merged_directory(profile_config, "production_methods")
    buildings_dir = load_merged_directory(profile_config, "building_types")

    categories = _load_categories(categories_dir)
    global_methods = _load_global_production_methods(methods_dir)
    buildings, inline_methods = _load_buildings(buildings_dir)

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
    warnings.extend(categories_dir.warnings)
    warnings.extend(methods_dir.warnings)
    warnings.extend(buildings_dir.warnings)

    categories_df = pl.DataFrame(
        [_category_row(entry) for entry in categories],
        schema=_category_schema(),
    )
    buildings_df = pl.DataFrame(
        [_building_row(building) for building in buildings],
        schema=_building_schema(),
    )
    methods_df = pl.DataFrame(
        [_production_method_row(method) for method in all_methods],
        schema=_production_method_schema(),
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
    nodes: dict[str, dict[str, str | None]] = {}
    edges: list[dict[str, Any]] = []

    def add_node(
        node_id: str,
        node_type: str,
        label: str,
        source_layer: str | None = None,
        source_mod: str | None = None,
    ) -> None:
        nodes.setdefault(
            node_id,
            {
                "id": node_id,
                "type": node_type,
                "label": label,
                "source_layer": source_layer,
                "source_mod": source_mod,
            },
        )

    building_by_name = {building.name: building for building in buildings}
    for building in buildings:
        add_node(
            f"building:{building.name}",
            "building",
            building.name,
            building.source_layer,
            building.source_mod,
        )

    for method in production_methods:
        method_id = f"production_method:{method.name}"
        add_node(
            method_id,
            "production_method",
            method.name,
            method.source_layer,
            method.source_mod,
        )
        if method.building:
            building = building_by_name.get(method.building)
            building_id = f"building:{method.building}"
            add_node(
                building_id,
                "building",
                method.building,
                None if building is None else building.source_layer,
                None if building is None else building.source_mod,
            )
            edges.append(
                {
                    "source": building_id,
                    "target": method_id,
                    "kind": "uses_production_method",
                    "amount": None,
                    "building": method.building,
                    "production_method": method.name,
                    "goods": None,
                    "source_layer": method.source_layer,
                    "source_mod": method.source_mod,
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
                    "source_layer": method.source_layer,
                    "source_mod": method.source_mod,
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
                    "source_layer": method.source_layer,
                    "source_mod": method.source_mod,
                }
            )

    nodes_df = pl.DataFrame(list(nodes.values()), schema=_goods_flow_node_schema())
    edges_df = pl.DataFrame(edges, schema=_goods_flow_edge_schema())
    return nodes_df, edges_df


def _resolve_profile(
    config: ParserConfig | None,
    profile: str | DataProfile | None,
    load_order_path: str | Path,
) -> DataProfile:
    if isinstance(profile, DataProfile):
        return profile
    if isinstance(profile, str):
        return load_profile(profile, load_order_path)
    config = config or ParserConfig.from_env()
    return DataProfile(
        name="vanilla",
        layers=(GameLayer(id="vanilla", name="Vanilla", root=config.game_root, kind="vanilla"),),
    )


def _load_categories(directory: MergedDirectory) -> list[MergedEntry]:
    return directory.entries


def _load_global_production_methods(directory: MergedDirectory) -> list[ProductionMethod]:
    return [
        _production_method_from_entry(entry, source_kind="global", building=None)
        for entry in directory.entries
    ]


def _load_buildings(directory: MergedDirectory) -> tuple[list[Building], list[ProductionMethod]]:
    buildings: list[Building] = []
    inline_methods: list[ProductionMethod] = []
    for entry in directory.entries:
        unique_methods = _unique_production_methods(entry.value)
        buildings.append(_building_from_entry(entry, unique_methods))
        inline_methods.extend(
            _production_method_from_entry(method_entry, source_kind="inline", building=entry.key)
            for method_entry in unique_methods
        )
    return buildings, inline_methods


def _building_from_entry(entry: MergedEntry, unique_methods: list[CEntry]) -> Building:
    block = _as_block(entry.value)
    return Building(
        name=entry.key,
        category=_scalar_string(_last(block, "category")),
        pop_type=_scalar_string(_last(block, "pop_type")),
        max_levels=_scalar(_last(block, "max_levels")),
        possible_production_methods=_scalar_list(_last(block, "possible_production_methods")),
        unique_production_methods=[method.key for method in unique_methods],
        source_file=entry.source_file,
        source_line=entry.source_line,
        source_layer=entry.source_layer,
        source_mod=entry.source_mod,
        source_mode=entry.source_mode,
        source_history=entry.source_history_json(),
    )


def _unique_production_methods(block: CList) -> list[CEntry]:
    methods: list[CEntry] = []
    for unique_block in block.values("unique_production_methods"):
        if isinstance(unique_block, CList):
            methods.extend(
                entry for entry in unique_block.entries if isinstance(entry.value, CList)
            )
    return methods


def _production_method_from_entry(
    entry: MergedEntry | CEntry, source_kind: str, building: str | None
) -> ProductionMethod:
    block = _as_block(entry.value)
    inputs: list[GoodsInput] = []
    for child in block.entries:
        if child.key in PRODUCTION_METHOD_METADATA or child.op != "=":
            continue
        value = _scalar(child.value)
        if isinstance(value, int | float):
            inputs.append(GoodsInput(goods=child.key, amount=float(value)))

    source_file, source_line, source_layer, source_mod, source_mode, source_history = _source(entry)
    return ProductionMethod(
        name=entry.key,
        category=_scalar_string(_last(block, "category")),
        produced=_scalar_string(_last(block, "produced")),
        output=_scalar_float(_last(block, "output")),
        inputs=inputs,
        no_upkeep=bool(_scalar(_last(block, "no_upkeep", False))),
        source_kind=source_kind,
        source_file=source_file,
        source_line=source_line,
        source_layer=source_layer,
        source_mod=source_mod,
        source_mode=source_mode,
        source_history=source_history,
        building=building,
        potential=_to_python(_last(block, "potential")),
        allow=_to_python(_last(block, "allow")),
    )


def _source(entry: MergedEntry | CEntry) -> tuple[str, int, str, str | None, str, str]:
    if isinstance(entry, MergedEntry):
        return (
            entry.source_file,
            entry.source_line,
            entry.source_layer,
            entry.source_mod,
            entry.source_mode,
            entry.source_history_json(),
        )
    return (str(entry.location.path or ""), entry.location.line, "vanilla", None, "CREATE", "[]")


def _find_duplicates(methods: list[ProductionMethod]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for method in methods:
        if method.name in seen:
            duplicates.add(method.name)
        seen.add(method.name)
    return sorted(duplicates)


def _category_row(entry: MergedEntry) -> dict[str, Any]:
    return {
        "name": entry.key,
        "source_file": entry.source_file,
        "source_line": entry.source_line,
        "source_layer": entry.source_layer,
        "source_mod": entry.source_mod,
        "source_mode": entry.source_mode,
        "source_history": entry.source_history_json(),
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
        "source_layer": building.source_layer,
        "source_mod": building.source_mod,
        "source_mode": building.source_mode,
        "source_history": building.source_history,
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
        "source_layer": method.source_layer,
        "source_mod": method.source_mod,
        "source_mode": method.source_mode,
        "source_history": method.source_history,
    }


def _category_schema() -> dict[str, Any]:
    return {
        "name": pl.String,
        "source_file": pl.String,
        "source_line": pl.Int64,
        "source_layer": pl.String,
        "source_mod": pl.String,
        "source_mode": pl.String,
        "source_history": pl.String,
    }


def _building_schema() -> dict[str, Any]:
    return {
        "name": pl.String,
        "category": pl.String,
        "pop_type": pl.String,
        "max_levels": pl.String,
        "possible_production_methods": pl.List(pl.String),
        "unique_production_methods": pl.List(pl.String),
        "source_file": pl.String,
        "source_line": pl.Int64,
        "source_layer": pl.String,
        "source_mod": pl.String,
        "source_mode": pl.String,
        "source_history": pl.String,
    }


def _production_method_schema() -> dict[str, Any]:
    return {
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
        "source_layer": pl.String,
        "source_mod": pl.String,
        "source_mode": pl.String,
        "source_history": pl.String,
    }


def _goods_flow_node_schema() -> dict[str, Any]:
    return {
        "id": pl.String,
        "type": pl.String,
        "label": pl.String,
        "source_layer": pl.String,
        "source_mod": pl.String,
    }


def _goods_flow_edge_schema() -> dict[str, Any]:
    return {
        "source": pl.String,
        "target": pl.String,
        "kind": pl.String,
        "amount": pl.Float64,
        "building": pl.String,
        "production_method": pl.String,
        "goods": pl.String,
        "source_layer": pl.String,
        "source_mod": pl.String,
    }


def _as_block(value: Value) -> CList:
    if not isinstance(value, CList):
        raise TypeError(f"Expected block, got {type(value).__name__}")
    return value


def _last(block: CList, key: str, default: Value | None = None) -> Value | None:
    values = block.values(key)
    return values[-1] if values else default


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
