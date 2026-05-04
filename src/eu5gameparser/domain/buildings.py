from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl

from eu5gameparser.clausewitz.syntax import CEntry, CList, Scalar, Value
from eu5gameparser.config import ParserConfig
from eu5gameparser.domain.goods import GoodsData, load_goods_data
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

BASELINE_BUILDING_PRICE_BY_AGE = {
    "age_1_traditions": "p_building_age_1_traditions",
    "age_2_renaissance": "p_building_age_2_renaissance",
    "age_3_discovery": "p_building_age_3_discovery",
    "age_4_reformation": "p_building_age_4_reformation",
    "age_5_absolutism": "p_building_age_5_absolutism",
    "age_6_revolutions": "p_building_age_6_revolutions",
}


@dataclass(frozen=True)
class PriceInfo:
    key: str
    gold: float | None
    source: str | None


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
    required_pop_type: str | None
    required_pop_amount: float | None
    source_kind: str
    source_file: str
    source_line: int
    source_layer: str
    source_mod: str | None
    source_mode: str
    source_history: str
    building: str | None = None
    production_method_group: str | None = None
    production_method_group_index: int | None = None
    potential: Any | None = None
    allow: Any | None = None


@dataclass(frozen=True)
class Building:
    name: str
    category: str | None
    icon: str | None
    price: str | None
    price_gold: float | None
    price_source: str | None
    effective_price: str | None
    effective_price_gold: float | None
    effective_price_source: str | None
    price_kind: str
    pop_type: str | None
    employment_size: float | None
    max_levels: str | int | float | bool | None
    obsolete_buildings: list[str]
    possible_production_methods: list[str]
    unique_production_methods: list[str]
    unique_production_method_groups: list[list[str]]
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
    baseline_prices: dict[str, PriceInfo] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def load_building_data(
    config: ParserConfig | None = None,
    *,
    profile: str | DataProfile | None = None,
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
    goods_data: GoodsData | None = None,
) -> BuildingData:
    profile_config = _resolve_profile(config, profile, load_order_path)
    goods_data = goods_data or load_goods_data(
        config, profile=profile_config, load_order_path=load_order_path
    )
    categories_dir = load_merged_directory(profile_config, "building_categories")
    methods_dir = load_merged_directory(profile_config, "production_methods")
    buildings_dir = load_merged_directory(profile_config, "building_types")
    prices_dir = load_merged_directory(profile_config, "prices")
    script_values_dir = load_merged_directory(
        profile_config,
        "script_values",
        scope="main_menu",
        include_scalars=True,
    )

    categories = _load_categories(categories_dir)
    global_methods = _load_global_production_methods(methods_dir)
    script_values = _load_script_values(script_values_dir)
    prices = _load_prices(prices_dir, script_values)
    baseline_prices = _baseline_prices(prices)
    buildings, inline_methods = _load_buildings(buildings_dir, prices)
    generated_methods = _generate_rgo_methods(goods_data)

    all_methods = [*global_methods, *inline_methods, *generated_methods]
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
    warnings.extend(prices_dir.warnings)
    warnings.extend(script_values_dir.warnings)

    categories_df = pl.DataFrame(
        [_category_row(entry) for entry in categories],
        schema=_category_schema(),
    )
    buildings_df = pl.DataFrame(
        [_building_row(building) for building in buildings],
        schema=_building_schema(),
    )
    price_by_good = _good_prices(goods_data)
    employment_by_building = {
        building.name: building.employment_size or 1.0 for building in buildings
    }
    methods_df = pl.DataFrame(
        [
            _production_method_row(method, price_by_good, employment_by_building)
            for method in all_methods
        ],
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
        baseline_prices=baseline_prices,
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
        source_mode: str | None = None,
        source_history: str | None = None,
    ) -> None:
        nodes.setdefault(
            node_id,
            {
                "id": node_id,
                "type": node_type,
                "label": label,
                "source_layer": source_layer,
                "source_mod": source_mod,
                "source_mode": source_mode,
                "source_history": source_history,
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
            building.source_mode,
            building.source_history,
        )

    for method in production_methods:
        method_id = f"production_method:{method.name}"
        add_node(
            method_id,
            "production_method",
            method.name,
            method.source_layer,
            method.source_mod,
            method.source_mode,
            method.source_history,
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
                None if building is None else building.source_mode,
                None if building is None else building.source_history,
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


def _load_script_values(directory: MergedDirectory) -> dict[str, float]:
    values: dict[str, float] = {}
    for entry in directory.entries:
        value = _scalar_float(entry.value)
        if value is not None:
            values[entry.key] = value
    return values


def _load_prices(
    directory: MergedDirectory, script_values: dict[str, float]
) -> dict[str, PriceInfo]:
    prices: dict[str, PriceInfo] = {}
    for entry in directory.entries:
        gold: float | None = None
        if isinstance(entry.value, CList):
            gold_value = _last(entry.value, "gold")
            gold = _scalar_float(gold_value)
            if gold is None:
                script_value_key = _scalar_string(gold_value)
                if script_value_key is not None:
                    gold = script_values.get(script_value_key)
        prices[entry.key] = PriceInfo(entry.key, gold, entry.source_file)
    return prices


def _baseline_prices(prices: dict[str, PriceInfo]) -> dict[str, PriceInfo]:
    return {
        age: prices[price_key]
        for age, price_key in BASELINE_BUILDING_PRICE_BY_AGE.items()
        if price_key in prices
    }


def _load_buildings(
    directory: MergedDirectory,
    prices: dict[str, PriceInfo],
) -> tuple[list[Building], list[ProductionMethod]]:
    buildings: list[Building] = []
    inline_methods: list[ProductionMethod] = []
    for entry in directory.entries:
        method_groups = _unique_production_method_groups(entry.value)
        buildings.append(_building_from_entry(entry, method_groups, prices))
        for group_index, group in enumerate(method_groups):
            inline_methods.extend(
                _production_method_from_entry(
                    method_entry,
                    source_kind="inline",
                    building=entry.key,
                    source_entry=entry,
                    production_method_group=f"{entry.key}:unique_production_methods:{group_index}",
                    production_method_group_index=group_index,
                )
                for method_entry in group
            )
    return buildings, inline_methods


def _generate_rgo_methods(goods_data: GoodsData) -> list[ProductionMethod]:
    methods: list[ProductionMethod] = []
    for good in goods_data.goods.filter(pl.col("category") == "raw_material").to_dicts():
        methods.append(
            ProductionMethod(
                name=f"rgo_{good['name']}",
                category=None,
                produced=good["name"],
                output=1.0,
                inputs=[],
                no_upkeep=False,
                required_pop_type="laborers",
                required_pop_amount=1.0,
                source_kind="generated_rgo",
                source_file=good["source_file"],
                source_line=good["source_line"],
                source_layer=good["source_layer"],
                source_mod=good["source_mod"],
                source_mode=good["source_mode"],
                source_history=good["source_history"],
                building=None,
            )
        )
    return methods


def _building_from_entry(
    entry: MergedEntry,
    method_groups: list[list[CEntry]],
    prices: dict[str, PriceInfo],
) -> Building:
    block = _as_block(entry.value)
    unique_methods = [method for group in method_groups for method in group]
    price = _scalar_string(_last(block, "price"))
    price_info = prices.get(price) if price else None
    price_gold = None if price_info is None else price_info.gold
    price_source = None if price_info is None else price_info.source
    return Building(
        name=entry.key,
        category=_scalar_string(_last(block, "category")),
        icon=_scalar_string(_last(block, "icon")),
        price=price,
        price_gold=price_gold,
        price_source=price_source,
        effective_price=price,
        effective_price_gold=price_gold,
        effective_price_source=price_source,
        price_kind="explicit" if price else "unresolved",
        pop_type=_scalar_string(_last(block, "pop_type")),
        employment_size=_scalar_float(_last(block, "employment_size")),
        max_levels=_scalar(_last(block, "max_levels")),
        obsolete_buildings=_scalar_values(block, "obsolete"),
        possible_production_methods=_scalar_list(_last(block, "possible_production_methods")),
        unique_production_methods=[method.key for method in unique_methods],
        unique_production_method_groups=[
            [method.key for method in group] for group in method_groups
        ],
        source_file=entry.source_file,
        source_line=entry.source_line,
        source_layer=entry.source_layer,
        source_mod=entry.source_mod,
        source_mode=entry.source_mode,
        source_history=entry.source_history_json(),
    )


def _unique_production_method_groups(block: CList) -> list[list[CEntry]]:
    groups: list[list[CEntry]] = []
    for unique_block in block.values("unique_production_methods"):
        if isinstance(unique_block, CList):
            groups.append(
                [entry for entry in unique_block.entries if isinstance(entry.value, CList)]
            )
    return groups


def _production_method_from_entry(
    entry: MergedEntry | CEntry,
    source_kind: str,
    building: str | None,
    source_entry: MergedEntry | None = None,
    production_method_group: str | None = None,
    production_method_group_index: int | None = None,
) -> ProductionMethod:
    block = _as_block(entry.value)
    inputs: list[GoodsInput] = []
    for child in block.entries:
        if child.key in PRODUCTION_METHOD_METADATA or child.op != "=":
            continue
        value = _scalar(child.value)
        if isinstance(value, int | float):
            inputs.append(GoodsInput(goods=child.key, amount=float(value)))

    source_file, source_line, source_layer, source_mod, source_mode, source_history = _source(
        source_entry or entry
    )
    return ProductionMethod(
        name=entry.key,
        category=_scalar_string(_last(block, "category")),
        produced=_scalar_string(_last(block, "produced")),
        output=_scalar_float(_last(block, "output")),
        inputs=inputs,
        no_upkeep=bool(_scalar(_last(block, "no_upkeep", False))),
        required_pop_type=None,
        required_pop_amount=None,
        source_kind=source_kind,
        source_file=source_file,
        source_line=source_line,
        source_layer=source_layer,
        source_mod=source_mod,
        source_mode=source_mode,
        source_history=source_history,
        building=building,
        production_method_group=production_method_group,
        production_method_group_index=production_method_group_index,
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
        "icon": building.icon,
        "price": building.price,
        "price_gold": building.price_gold,
        "price_source": building.price_source,
        "effective_price": building.effective_price,
        "effective_price_gold": building.effective_price_gold,
        "effective_price_source": building.effective_price_source,
        "price_kind": building.price_kind,
        "pop_type": building.pop_type,
        "employment_size": building.employment_size,
        "max_levels": None if building.max_levels is None else str(building.max_levels),
        "obsolete_buildings": building.obsolete_buildings,
        "possible_production_methods": building.possible_production_methods,
        "unique_production_methods": building.unique_production_methods,
        "unique_production_method_groups": building.unique_production_method_groups,
        "source_file": building.source_file,
        "source_line": building.source_line,
        "source_layer": building.source_layer,
        "source_mod": building.source_mod,
        "source_mode": building.source_mode,
        "source_history": building.source_history,
    }


def _production_method_row(
    method: ProductionMethod,
    price_by_good: dict[str, float],
    employment_by_building: dict[str, float],
) -> dict[str, Any]:
    metrics = _production_method_metrics(method, price_by_good, employment_by_building)
    return {
        "name": method.name,
        "category": method.category,
        "produced": method.produced,
        "output": method.output,
        "input_goods": [item.goods for item in method.inputs],
        "input_amounts": [item.amount for item in method.inputs],
        "no_upkeep": method.no_upkeep,
        "required_pop_type": method.required_pop_type,
        "required_pop_amount": method.required_pop_amount,
        "source_kind": method.source_kind,
        "building": method.building,
        "production_method_group": method.production_method_group,
        "production_method_group_index": method.production_method_group_index,
        **metrics,
        "source_file": method.source_file,
        "source_line": method.source_line,
        "source_layer": method.source_layer,
        "source_mod": method.source_mod,
        "source_mode": method.source_mode,
        "source_history": method.source_history,
    }


def _production_method_metrics(
    method: ProductionMethod,
    price_by_good: dict[str, float],
    employment_by_building: dict[str, float],
) -> dict[str, float | list[str] | None]:
    production_efficiency_modifier = 0.0
    adjusted_output = (
        None if method.output is None else method.output * (1.0 + production_efficiency_modifier)
    )
    population_basis = (
        employment_by_building.get(method.building, 1.0)
        if method.building is not None
        else 1.0
    )
    if adjusted_output is None or population_basis == 0:
        output_per_population = None
    else:
        output_per_population = adjusted_output / population_basis

    missing_price_goods: list[str] = []
    output_value = 0.0
    if method.produced is not None and adjusted_output is not None:
        produced_price = price_by_good.get(method.produced)
        if produced_price is None:
            missing_price_goods.append(method.produced)
            output_value = None
        else:
            output_value = adjusted_output * produced_price

    input_cost = 0.0
    for item in method.inputs:
        input_price = price_by_good.get(item.goods)
        if input_price is None:
            missing_price_goods.append(item.goods)
        else:
            input_cost += item.amount * input_price
    if any(item.goods in missing_price_goods for item in method.inputs):
        input_cost = None

    profit = None
    if output_value is not None and input_cost is not None:
        profit = output_value - input_cost
    profit_margin_percent = None
    if profit is not None and input_cost is not None and input_cost > 0:
        profit_margin_percent = (profit / input_cost) * 100.0

    return {
        "production_efficiency_modifier": production_efficiency_modifier,
        "adjusted_output": adjusted_output,
        "output_value": output_value,
        "input_cost": input_cost,
        "profit": profit,
        "profit_margin_percent": profit_margin_percent,
        "missing_price_goods": sorted(set(missing_price_goods)),
        "population_basis": population_basis,
        "output_per_population": output_per_population,
    }


def _good_prices(goods_data: GoodsData) -> dict[str, float]:
    return {
        row["name"]: row["default_market_price"]
        for row in goods_data.goods.to_dicts()
        if row["default_market_price"] is not None
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
        "icon": pl.String,
        "price": pl.String,
        "price_gold": pl.Float64,
        "price_source": pl.String,
        "effective_price": pl.String,
        "effective_price_gold": pl.Float64,
        "effective_price_source": pl.String,
        "price_kind": pl.String,
        "pop_type": pl.String,
        "employment_size": pl.Float64,
        "max_levels": pl.String,
        "obsolete_buildings": pl.List(pl.String),
        "possible_production_methods": pl.List(pl.String),
        "unique_production_methods": pl.List(pl.String),
        "unique_production_method_groups": pl.List(pl.List(pl.String)),
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
        "required_pop_type": pl.String,
        "required_pop_amount": pl.Float64,
        "source_kind": pl.String,
        "building": pl.String,
        "production_method_group": pl.String,
        "production_method_group_index": pl.Int64,
        "production_efficiency_modifier": pl.Float64,
        "adjusted_output": pl.Float64,
        "output_value": pl.Float64,
        "input_cost": pl.Float64,
        "profit": pl.Float64,
        "profit_margin_percent": pl.Float64,
        "missing_price_goods": pl.List(pl.String),
        "population_basis": pl.Float64,
        "output_per_population": pl.Float64,
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
        "source_mode": pl.String,
        "source_history": pl.String,
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


def _scalar_values(block: CList, key: str) -> list[str]:
    values: list[str] = []
    for value in block.values(key):
        values.extend(_scalar_list(value))
    return values


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
