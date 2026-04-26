from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl

from eu5gameparser.clausewitz.syntax import CList, Scalar, Value
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

DEFAULT_TRANSPORT_COST = 1.0


@dataclass(frozen=True)
class Good:
    name: str
    method: str | None
    category: str | None
    color: str | None
    default_market_price: float | None
    transport_cost: float | None
    food: float | None
    block_rgo_upgrade: bool | None
    development_threshold: float | None
    origin_in_old_world: bool | None
    origin_in_new_world: bool | None
    custom_tags: list[str]
    demand_add: dict[str, float]
    demand_multiply: dict[str, float]
    wealth_impact_threshold: dict[str, float]
    data: dict[str, Any]
    source_file: str
    source_line: int
    source_layer: str
    source_mod: str | None
    source_mode: str
    source_history: str


@dataclass(frozen=True)
class GoodsData:
    goods: pl.DataFrame
    warnings: list[str] = field(default_factory=list)


def build_goods_summary(goods: pl.DataFrame, production_methods: pl.DataFrame) -> pl.DataFrame:
    input_counts = _goods_count_table(production_methods, "input_goods", "input_method_count")
    output_counts = (
        production_methods.filter(pl.col("produced").is_not_null())
        .group_by("produced")
        .agg(pl.len().alias("output_method_count"))
        .rename({"produced": "name"})
    )
    summary = (
        goods.select(
            [
                "name",
                "category",
                "default_market_price",
                "transport_cost",
                "food",
                "source_layer",
                "source_mod",
                "source_mode",
                "source_history",
            ]
        )
        .join(input_counts, on="name", how="left")
        .join(output_counts, on="name", how="left")
        .with_columns(
            [
                pl.col("input_method_count").fill_null(0).cast(pl.Int64),
                pl.col("output_method_count").fill_null(0).cast(pl.Int64),
                pl.struct(
                    ["source_layer", "source_mod", "source_mode", "source_history"]
                )
                .map_elements(_provenance_state, return_dtype=pl.String)
                .alias("provenance_state"),
                pl.when(pl.col("source_mod").is_not_null())
                .then(pl.col("source_mod"))
                .otherwise(pl.col("source_layer"))
                .fill_null("unknown")
                .alias("provenance_source"),
            ]
        )
        .select(
            [
                "name",
                "category",
                "input_method_count",
                "output_method_count",
                "default_market_price",
                "transport_cost",
                "food",
                "provenance_state",
                "provenance_source",
                "source_layer",
                "source_mod",
                "source_mode",
                "source_history",
            ]
        )
        .sort(["name"])
    )
    return summary


def _goods_count_table(
    production_methods: pl.DataFrame, list_column: str, output_column: str
) -> pl.DataFrame:
    return (
        production_methods.select(["name", list_column])
        .explode(list_column)
        .filter(pl.col(list_column).is_not_null())
        .group_by(list_column)
        .agg(pl.len().alias(output_column))
        .rename({list_column: "name"})
    )


def _provenance_state(source: dict[str, Any]) -> str:
    source_layer = source.get("source_layer")
    source_mod = source.get("source_mod")
    source_mode = source.get("source_mode")
    source_history = source.get("source_history")
    if not source_layer and not source_mod:
        return "unknown"
    history = _parse_source_history(source_history)
    modes = {str(record.get("mode") or "").upper() for record in history}
    if "INJECT" in modes or "TRY_INJECT" in modes:
        return "merged"
    if (source_layer == "vanilla" or source_mod is None) and len(history) <= 1:
        return "vanilla_exact"
    if source_mod is not None or source_layer != "vanilla":
        return "mod_exact"
    if source_mode == "CREATE":
        return "vanilla_exact"
    return "unknown"


def _parse_source_history(source_history: str | None) -> list[dict[str, Any]]:
    if not source_history:
        return []
    try:
        parsed = json.loads(source_history)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def load_goods_data(
    config: ParserConfig | None = None,
    *,
    profile: str | DataProfile | None = None,
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
) -> GoodsData:
    profile_config = _resolve_profile(config, profile, load_order_path)
    goods_dir = load_merged_directory(profile_config, "goods")
    goods = _load_goods(goods_dir)
    return GoodsData(
        goods=pl.DataFrame([_goods_row(good) for good in goods], schema=_goods_schema()),
        warnings=goods_dir.warnings,
    )


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


def _load_goods(directory: MergedDirectory) -> list[Good]:
    return [_good_from_entry(entry) for entry in directory.entries]


def _good_from_entry(entry: MergedEntry) -> Good:
    block = entry.value
    return Good(
        name=entry.key,
        method=_scalar_string(_last(block, "method")),
        category=_scalar_string(_last(block, "category")),
        color=_scalar_string(_last(block, "color")),
        default_market_price=_scalar_float(_last(block, "default_market_price")),
        transport_cost=_scalar_float(_last(block, "transport_cost"), DEFAULT_TRANSPORT_COST),
        food=_food_value(entry),
        block_rgo_upgrade=_scalar_bool(_last(block, "block_rgo_upgrade")),
        development_threshold=_scalar_float(_last(block, "development_threshold")),
        origin_in_old_world=_scalar_bool(_last(block, "origin_in_old_world")),
        origin_in_new_world=_scalar_bool(_last(block, "origin_in_new_world")),
        custom_tags=_scalar_list(_last(block, "custom_tags")),
        demand_add=_numeric_map(_last(block, "demand_add")),
        demand_multiply=_numeric_map(_last(block, "demand_multiply")),
        wealth_impact_threshold=_numeric_map(_last(block, "wealth_impact_threshold")),
        data=_to_python(block),
        source_file=entry.source_file,
        source_line=entry.source_line,
        source_layer=entry.source_layer,
        source_mod=entry.source_mod,
        source_mode=entry.source_mode,
        source_history=entry.source_history_json(),
    )


def _goods_row(good: Good) -> dict[str, Any]:
    return {
        "name": good.name,
        "method": good.method,
        "category": good.category,
        "color": good.color,
        "default_market_price": good.default_market_price,
        "transport_cost": good.transport_cost,
        "food": good.food,
        "block_rgo_upgrade": good.block_rgo_upgrade,
        "development_threshold": good.development_threshold,
        "origin_in_old_world": good.origin_in_old_world,
        "origin_in_new_world": good.origin_in_new_world,
        "custom_tags": good.custom_tags,
        "demand_add": _json(good.demand_add),
        "demand_multiply": _json(good.demand_multiply),
        "wealth_impact_threshold": _json(good.wealth_impact_threshold),
        "data": _json(good.data),
        "source_file": good.source_file,
        "source_line": good.source_line,
        "source_layer": good.source_layer,
        "source_mod": good.source_mod,
        "source_mode": good.source_mode,
        "source_history": good.source_history,
    }


def _goods_schema() -> dict[str, Any]:
    return {
        "name": pl.String,
        "method": pl.String,
        "category": pl.String,
        "color": pl.String,
        "default_market_price": pl.Float64,
        "transport_cost": pl.Float64,
        "food": pl.Float64,
        "block_rgo_upgrade": pl.Boolean,
        "development_threshold": pl.Float64,
        "origin_in_old_world": pl.Boolean,
        "origin_in_new_world": pl.Boolean,
        "custom_tags": pl.List(pl.String),
        "demand_add": pl.String,
        "demand_multiply": pl.String,
        "wealth_impact_threshold": pl.String,
        "data": pl.String,
        "source_file": pl.String,
        "source_line": pl.Int64,
        "source_layer": pl.String,
        "source_mod": pl.String,
        "source_mode": pl.String,
        "source_history": pl.String,
    }


def _numeric_map(value: Value | None) -> dict[str, float]:
    if not isinstance(value, CList):
        return {}
    result: dict[str, float] = {}
    for entry in value.entries:
        scalar = _scalar(entry.value)
        if isinstance(scalar, int | float):
            result[entry.key] = float(scalar)
    return result


def _last(block: CList, key: str, default: Value | None = None) -> Value | None:
    values = block.values(key)
    return values[-1] if values else default


def _scalar(value: Value | None) -> Scalar | None:
    if isinstance(value, CList):
        return None
    return value


def _scalar_bool(value: Value | None) -> bool | None:
    scalar = _scalar(value)
    return scalar if isinstance(scalar, bool) else None


def _scalar_float(value: Value | None, default: float | None = None) -> float | None:
    scalar = _scalar(value)
    if isinstance(scalar, int | float):
        return float(scalar)
    return default


def _food_value(entry: MergedEntry) -> float | None:
    values = entry.value.values("food")
    if not values:
        return None
    if _has_injected_history(entry):
        total = 0.0
        has_food = False
        for value in values:
            scalar = _scalar(value)
            if isinstance(scalar, int | float):
                total += float(scalar)
                has_food = True
        return total if has_food else None
    return _scalar_float(values[-1])


def _has_injected_history(entry: MergedEntry) -> bool:
    return any(record.mode in {"INJECT", "TRY_INJECT"} for record in entry.source_history)


def _scalar_list(value: Value | None) -> list[str]:
    if isinstance(value, CList):
        return [str(item) for item in value.items if not isinstance(item, CList)]
    scalar = _scalar(value)
    return [] if scalar is None else [str(scalar)]


def _scalar_string(value: Value | None) -> str | None:
    scalar = _scalar(value)
    return None if scalar is None else str(scalar)


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


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))
