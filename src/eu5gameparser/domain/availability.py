from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Any

import polars as pl

from eu5gameparser.domain.buildings import BuildingData
from eu5gameparser.domain.goods import build_goods_summary

if TYPE_CHECKING:
    from eu5gameparser.domain.eu5 import Eu5Data

AGE_ORDER = (
    "age_1_traditions",
    "age_2_renaissance",
    "age_3_discovery",
    "age_4_reformation",
    "age_5_absolutism",
    "age_6_revolutions",
)
AGE_INDEX = {age: index for index, age in enumerate(AGE_ORDER)}


def filter_eu5_data_by_age(
    data: Eu5Data,
    max_age: str,
    *,
    include_specific_unlocks: bool = False,
) -> Eu5Data:
    building_data = filter_building_data_by_age(
        data.building_data,
        data.advancements,
        max_age,
        include_specific_unlocks=include_specific_unlocks,
    )
    return replace(
        data,
        buildings=building_data.buildings,
        goods_summary=build_goods_summary(data.goods, building_data.production_methods),
        production_methods=building_data.production_methods,
        goods_flow_nodes=building_data.goods_flow_nodes,
        goods_flow_edges=building_data.goods_flow_edges,
        building_data=building_data,
    )


def filter_building_data_by_age(
    data: BuildingData,
    advancements: pl.DataFrame,
    max_age: str,
    *,
    include_specific_unlocks: bool = False,
) -> BuildingData:
    _validate_age(max_age)
    annotated = annotate_building_data_availability(
        data,
        advancements,
        include_specific_unlocks=include_specific_unlocks,
    )
    buildings = annotated.buildings.with_columns(
        _available_expr(max_age, include_specific_unlocks).alias("is_available_by_age")
    )
    production_methods = annotated.production_methods.with_columns(
        _effective_available_expr(max_age, include_specific_unlocks).alias(
            "is_available_by_age"
        )
    )

    available_buildings = set(
        buildings.filter(pl.col("is_available_by_age"))["name"].to_list()
    )
    buildings = buildings.filter(pl.col("is_available_by_age"))
    production_methods = production_methods.filter(pl.col("is_available_by_age"))
    nodes, edges = _filter_goods_flow_tables(
        data.goods_flow_nodes,
        data.goods_flow_edges,
        available_buildings,
        set(production_methods["name"].to_list()),
    )
    return replace(
        data,
        buildings=buildings,
        production_methods=production_methods,
        goods_flow_nodes=nodes,
        goods_flow_edges=edges,
    )


def annotate_building_data_availability(
    data: BuildingData,
    advancements: pl.DataFrame,
    *,
    include_specific_unlocks: bool = False,
) -> BuildingData:
    buildings = _annotate_availability(
        data.buildings,
        advancements,
        "unlock_building",
        include_specific_unlocks=include_specific_unlocks,
    )
    buildings = _annotate_effective_building_prices(buildings, data.baseline_prices)
    production_methods = _annotate_availability(
        data.production_methods,
        advancements,
        "unlock_production_method",
        include_specific_unlocks=include_specific_unlocks,
    )
    production_methods = _annotate_effective_method_availability(
        production_methods,
        buildings,
        include_specific_unlocks=include_specific_unlocks,
    )
    return replace(
        data,
        buildings=buildings,
        production_methods=production_methods,
    )


def _annotate_effective_building_prices(
    buildings: pl.DataFrame,
    baseline_prices: dict[str, Any],
) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in buildings.to_dicts():
        if row.get("price"):
            row["effective_price"] = row["price"]
            row["effective_price_gold"] = row.get("price_gold")
            row["effective_price_source"] = row.get("price_source")
            row["price_kind"] = "explicit"
            rows.append(row)
            continue

        age = _price_age_for_building(row)
        baseline = baseline_prices.get(age)
        if baseline is None:
            row["effective_price"] = None
            row["effective_price_gold"] = None
            row["effective_price_source"] = None
            row["price_kind"] = "unresolved"
        else:
            row["effective_price"] = baseline.key
            row["effective_price_gold"] = baseline.gold
            row["effective_price_source"] = baseline.source
            row["price_kind"] = "baseline_age"
        rows.append(row)
    return pl.DataFrame(rows, schema=dict(buildings.schema))


def _price_age_for_building(row: dict[str, Any]) -> str:
    for key in ("general_unlock_age", "unlock_age", "specific_unlock_age"):
        age = row.get(key)
        if isinstance(age, str) and age in AGE_INDEX:
            return age
    return AGE_ORDER[0]


def _annotate_effective_method_availability(
    production_methods: pl.DataFrame,
    buildings: pl.DataFrame,
    *,
    include_specific_unlocks: bool,
) -> pl.DataFrame:
    building_states = {
        row["name"]: {
            "unlock_age": row["unlock_age"],
            "general_unlock_age": row["general_unlock_age"],
            "specific_unlock_age": row["specific_unlock_age"],
            "availability_kind": row["availability_kind"],
            "is_specific_only": row["is_specific_only"],
        }
        for row in buildings.to_dicts()
    }
    default_building_state = {
        "unlock_age": None,
        "general_unlock_age": None,
        "specific_unlock_age": None,
        "availability_kind": "available_by_default",
        "is_specific_only": False,
    }

    rows: list[dict[str, Any]] = []
    for row in production_methods.to_dicts():
        building_state = building_states.get(row.get("building"), default_building_state)
        method_state = {
            "unlock_age": row["unlock_age"],
            "general_unlock_age": row["general_unlock_age"],
            "specific_unlock_age": row["specific_unlock_age"],
            "availability_kind": row["availability_kind"],
            "is_specific_only": row["is_specific_only"],
        }
        effective_state = _combine_availability_states(
            method_state,
            building_state,
            include_specific_unlocks=include_specific_unlocks,
        )
        row.update(
            {
                "building_unlock_age": building_state["unlock_age"],
                "building_general_unlock_age": building_state["general_unlock_age"],
                "building_specific_unlock_age": building_state["specific_unlock_age"],
                "building_availability_kind": building_state["availability_kind"],
                "building_is_specific_only": building_state["is_specific_only"],
                "effective_unlock_age": effective_state["unlock_age"],
                "effective_general_unlock_age": effective_state["general_unlock_age"],
                "effective_specific_unlock_age": effective_state["specific_unlock_age"],
                "effective_availability_kind": effective_state["availability_kind"],
                "effective_is_specific_only": effective_state["is_specific_only"],
            }
        )
        rows.append(row)

    schema = dict(production_methods.schema)
    schema.update(
        {
            "building_unlock_age": pl.String,
            "building_general_unlock_age": pl.String,
            "building_specific_unlock_age": pl.String,
            "building_availability_kind": pl.String,
            "building_is_specific_only": pl.Boolean,
            "effective_unlock_age": pl.String,
            "effective_general_unlock_age": pl.String,
            "effective_specific_unlock_age": pl.String,
            "effective_availability_kind": pl.String,
            "effective_is_specific_only": pl.Boolean,
        }
    )
    return pl.DataFrame(rows, schema=schema)


def _combine_availability_states(
    method_state: dict[str, str | bool | None],
    building_state: dict[str, str | bool | None],
    *,
    include_specific_unlocks: bool,
) -> dict[str, str | bool | None]:
    method_age = _state_unlock_age(method_state, include_specific_unlocks)
    building_age = _state_unlock_age(building_state, include_specific_unlocks)
    unlock_age = _latest_age(
        [age for age in (method_age, building_age) if isinstance(age, str)]
    )
    general_age = _latest_age(
        [
            age
            for age in (
                method_state["general_unlock_age"],
                building_state["general_unlock_age"],
            )
            if isinstance(age, str)
        ]
    )
    specific_age = _earliest_age(
        [
            age
            for age in (
                method_state["specific_unlock_age"],
                building_state["specific_unlock_age"],
            )
            if isinstance(age, str)
        ]
    )
    is_specific_only = _is_specific_only(method_state) or _is_specific_only(building_state)
    if is_specific_only and not include_specific_unlocks:
        return {
            "unlock_age": unlock_age or specific_age,
            "general_unlock_age": general_age,
            "specific_unlock_age": specific_age,
            "availability_kind": "specific_only",
            "is_specific_only": True,
        }

    if unlock_age is None:
        return {
            "unlock_age": None,
            "general_unlock_age": None,
            "specific_unlock_age": specific_age,
            "availability_kind": "available_by_default",
            "is_specific_only": False,
        }
    return {
        "unlock_age": unlock_age,
        "general_unlock_age": general_age,
        "specific_unlock_age": specific_age,
        "availability_kind": "specific_only" if is_specific_only else "unlocked",
        "is_specific_only": is_specific_only,
    }


def _is_specific_only(state: dict[str, str | bool | None]) -> bool:
    return bool(
        state["is_specific_only"] or state["availability_kind"] == "specific_only"
    )


def _state_unlock_age(
    state: dict[str, str | bool | None],
    include_specific_unlocks: bool,
) -> str | None:
    if include_specific_unlocks:
        return _earliest_age(
            [
                age
                for age in (state["general_unlock_age"], state["specific_unlock_age"])
                if isinstance(age, str)
            ]
        )
    return state["general_unlock_age"] if isinstance(state["general_unlock_age"], str) else None


def hidden_counts(original: BuildingData, filtered: BuildingData) -> dict[str, int]:
    return {
        "buildings": original.buildings.height - filtered.buildings.height,
        "production_methods": (
            original.production_methods.height - filtered.production_methods.height
        ),
    }


def _annotate_availability(
    table: pl.DataFrame,
    advancements: pl.DataFrame,
    unlock_column: str,
    *,
    include_specific_unlocks: bool,
) -> pl.DataFrame:
    availability = _availability_by_item(
        advancements, unlock_column, include_specific_unlocks=include_specific_unlocks
    )
    rows: list[dict[str, Any]] = []
    for row in table.to_dicts():
        state = availability.get(
            row["name"],
            {
                "unlock_age": None,
                "general_unlock_age": None,
                "specific_unlock_age": None,
                "availability_kind": "available_by_default",
                "is_specific_only": False,
                "is_available_by_age": True,
            },
        )
        row.update(
            {
                "unlock_age": state["unlock_age"],
                "general_unlock_age": state["general_unlock_age"],
                "specific_unlock_age": state["specific_unlock_age"],
                "availability_kind": state["availability_kind"],
                "is_specific_only": state["is_specific_only"],
                "is_available_by_age": True,
            }
        )
        rows.append(row)
    schema = dict(table.schema)
    schema.update(
        {
            "unlock_age": pl.String,
            "general_unlock_age": pl.String,
            "specific_unlock_age": pl.String,
            "availability_kind": pl.String,
            "is_specific_only": pl.Boolean,
            "is_available_by_age": pl.Boolean,
        }
    )
    return pl.DataFrame(rows, schema=schema)


def _availability_by_item(
    advancements: pl.DataFrame,
    unlock_column: str,
    *,
    include_specific_unlocks: bool,
) -> dict[str, dict[str, str | bool | None]]:
    unlocks: dict[str, dict[str, list[str]]] = {}
    for row in advancements.select(["age", "has_potential", unlock_column]).to_dicts():
        age = row["age"]
        if age not in AGE_INDEX:
            continue
        bucket = "specific" if row["has_potential"] else "general"
        for item in row[unlock_column] or []:
            unlocks.setdefault(item, {"general": [], "specific": []})[bucket].append(age)

    availability: dict[str, dict[str, str | bool | None]] = {}
    for item, ages in unlocks.items():
        candidate_ages = list(ages["general"])
        kind = "unlocked"
        if include_specific_unlocks:
            candidate_ages.extend(ages["specific"])
            if not ages["general"] and ages["specific"]:
                kind = "specific_only"
        elif not ages["general"] and ages["specific"]:
            availability[item] = {
                "unlock_age": _latest_age(ages["specific"]),
                "general_unlock_age": None,
                "specific_unlock_age": _earliest_age(ages["specific"]),
                "availability_kind": "specific_only",
                "is_specific_only": True,
                "is_available_by_age": False,
            }
            continue
        unlock_age = (
            _earliest_age(candidate_ages)
            if include_specific_unlocks
            else _latest_age(candidate_ages)
        )
        availability[item] = {
            "unlock_age": unlock_age,
            "general_unlock_age": _latest_age(ages["general"]),
            "specific_unlock_age": _earliest_age(ages["specific"]),
            "availability_kind": kind,
            "is_specific_only": False,
            "is_available_by_age": True,
        }
    return availability


def _available_expr(max_age: str, include_specific_unlocks: bool) -> pl.Expr:
    return _availability_expr(
        max_age,
        include_specific_unlocks,
        kind_column="availability_kind",
        age_column="unlock_age",
    )


def _effective_available_expr(max_age: str, include_specific_unlocks: bool) -> pl.Expr:
    return _availability_expr(
        max_age,
        include_specific_unlocks,
        kind_column="effective_availability_kind",
        age_column="effective_unlock_age",
    )


def _availability_expr(
    max_age: str,
    include_specific_unlocks: bool,
    *,
    kind_column: str,
    age_column: str,
) -> pl.Expr:
    locked_kind = (
        pl.lit(False)
        if include_specific_unlocks
        else (pl.col(kind_column) == "specific_only")
    )
    return (
        (pl.col(kind_column) == "available_by_default")
        | (
            ~locked_kind
            & pl.col(age_column).is_not_null()
            & (pl.col(age_column).replace_strict(AGE_INDEX) <= AGE_INDEX[max_age])
        )
    )


def _filter_goods_flow_tables(
    nodes: pl.DataFrame,
    edges: pl.DataFrame,
    available_buildings: set[str],
    available_methods: set[str],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    filtered_edges = edges.filter(
        (
            pl.col("building").is_null()
            | pl.col("building").is_in(sorted(available_buildings))
        )
        & (
            pl.col("production_method").is_null()
            | pl.col("production_method").is_in(sorted(available_methods))
        )
    )
    allowed_node_ids = set(filtered_edges["source"].to_list()) | set(
        filtered_edges["target"].to_list()
    )
    allowed_node_ids.update(f"building:{name}" for name in available_buildings)
    allowed_node_ids.update(f"production_method:{name}" for name in available_methods)
    return nodes.filter(pl.col("id").is_in(sorted(allowed_node_ids))), filtered_edges


def _latest_age(ages: list[str]) -> str | None:
    if not ages:
        return None
    return max(ages, key=lambda age: AGE_INDEX[age])


def _earliest_age(ages: list[str]) -> str | None:
    if not ages:
        return None
    return min(ages, key=lambda age: AGE_INDEX[age])


def _validate_age(age: str) -> None:
    if age not in AGE_INDEX:
        valid = ", ".join(AGE_ORDER)
        raise ValueError(f"Unknown age {age!r}; expected one of: {valid}")
