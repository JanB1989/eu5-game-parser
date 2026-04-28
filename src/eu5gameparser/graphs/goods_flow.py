from __future__ import annotations

import html
import json
import webbrowser
from collections import deque
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from eu5gameparser.config import ParserConfig
from eu5gameparser.domain.advancements import load_advancement_data
from eu5gameparser.domain.availability import (
    AGE_ORDER,
    annotate_building_data_availability,
    filter_building_data_by_age,
    filter_eu5_data_by_age,
)
from eu5gameparser.domain.buildings import BuildingData, load_building_data
from eu5gameparser.domain.goods import GoodsData, build_goods_summary, load_goods_data
from eu5gameparser.load_order import DEFAULT_LOAD_ORDER_PATH

NODE_X_SPACING = 320
NODE_Y_SPACING = 112
GROUP_Y_SPACING = 156
DEFAULT_MIN_ZOOM = 0.35
DEFAULT_MAX_ZOOM = 2.5
DEFAULT_WHEEL_SENSITIVITY = 0.001
DEFAULT_WIDGET_HEIGHT = "900px"
DEFAULT_WIDGET_WIDTH = "100%"
MIN_COLUMN_NODE_SPACING = 96
EXPLORER_METRIC_MODES = {
    "goods",
    "input_cost",
    "output_value",
    "profit",
    "profit_margin_percent",
}


@dataclass(frozen=True)
class _Method:
    name: str
    produced: str | None
    output: float | None
    input_goods: list[str]
    input_amounts: list[float]
    building: str | None
    source_layer: str | None = None
    source_mod: str | None = None
    source_mode: str | None = None
    source_history: str | None = None
    unlock_age: str | None = None
    general_unlock_age: str | None = None
    specific_unlock_age: str | None = None
    availability_kind: str | None = None
    is_specific_only: bool | None = None
    building_unlock_age: str | None = None
    building_general_unlock_age: str | None = None
    building_specific_unlock_age: str | None = None
    building_availability_kind: str | None = None
    building_is_specific_only: bool | None = None
    effective_unlock_age: str | None = None
    effective_general_unlock_age: str | None = None
    effective_specific_unlock_age: str | None = None
    effective_availability_kind: str | None = None
    effective_is_specific_only: bool | None = None
    input_cost: float | None = None
    output_value: float | None = None
    profit: float | None = None
    profit_margin_percent: float | None = None
    missing_price_goods: list[str] | None = None


def show_good_flow(
    good: str,
    *,
    depth: int = 1,
    config: ParserConfig | None = None,
    profile: str | None = None,
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
    data: BuildingData | None = None,
    goods_data: GoodsData | None = None,
    eu5_data: Any | None = None,
    max_age: str | None = None,
    include_specific_unlocks: bool = False,
    height: str = DEFAULT_WIDGET_HEIGHT,
    width: str = DEFAULT_WIDGET_WIDTH,
    enable_zoom: bool = False,
):
    import ipycytoscape

    graph = build_good_flow_graph(
        good,
        depth=depth,
        config=config,
        profile=profile,
        load_order_path=load_order_path,
        data=data,
        goods_data=goods_data,
        eu5_data=eu5_data,
        max_age=max_age,
        include_specific_unlocks=include_specific_unlocks,
    )
    widget = ipycytoscape.CytoscapeWidget()
    widget.layout.width = width
    widget.layout.height = height
    widget.cytoscape_layout = {"name": "preset", "fit": True, "padding": 72}
    widget.graph.add_graph_from_json(graph, directed=True)
    widget.set_layout(name="preset", fit=True, padding=72)
    widget.set_style(_CYTOSCAPE_STYLE)
    widget.autolock = True
    widget.auto_ungrabify = True
    widget.min_zoom = DEFAULT_MIN_ZOOM
    widget.max_zoom = DEFAULT_MAX_ZOOM
    widget.wheel_sensitivity = DEFAULT_WHEEL_SENSITIVITY
    widget.user_panning_enabled = True
    widget.user_zooming_enabled = enable_zoom
    widget.relayout()
    return widget


def write_good_flow_html(
    good: str,
    path: str | Path | None = None,
    *,
    depth: int = 1,
    config: ParserConfig | None = None,
    profile: str | None = None,
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
    data: BuildingData | None = None,
    goods_data: GoodsData | None = None,
    eu5_data: Any | None = None,
    max_age: str | None = None,
    include_specific_unlocks: bool = False,
) -> Path:
    output_path = Path(path or Path("out") / f"good_flow_{good}.html")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    graph = build_good_flow_graph(
        good,
        depth=depth,
        config=config,
        profile=profile,
        load_order_path=load_order_path,
        data=data,
        goods_data=goods_data,
        eu5_data=eu5_data,
        include_specific_unlocks=include_specific_unlocks,
        annotate_availability=True,
    )
    output_path.write_text(
        _standalone_html(
            good,
            graph,
            selected_age=max_age,
            include_specific_unlocks=include_specific_unlocks,
        ),
        encoding="utf-8",
    )
    return output_path


def open_good_flow(
    good: str,
    *,
    depth: int = 1,
    config: ParserConfig | None = None,
    profile: str | None = None,
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
    data: BuildingData | None = None,
    goods_data: GoodsData | None = None,
    eu5_data: Any | None = None,
    max_age: str | None = None,
    include_specific_unlocks: bool = False,
    path: str | Path | None = None,
) -> Path:
    output_path = write_good_flow_html(
        good,
        path,
        depth=depth,
        config=config,
        profile=profile,
        load_order_path=load_order_path,
        data=data,
        goods_data=goods_data,
        eu5_data=eu5_data,
        max_age=max_age,
        include_specific_unlocks=include_specific_unlocks,
    )
    webbrowser.open(output_path.resolve().as_uri())
    return output_path


def write_goods_flow_explorer_html(
    path: str | Path | None = None,
    *,
    good: str | None = None,
    building: str | None = None,
    depth: int = 1,
    config: ParserConfig | None = None,
    profile: str = "merged_default",
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
    data: BuildingData | None = None,
    goods_data: GoodsData | None = None,
    eu5_data: Any | None = None,
    max_age: str | None = None,
    include_specific_unlocks: bool = False,
    metric_mode: str = "goods",
) -> Path:
    if depth < 1:
        raise ValueError("depth must be at least 1")
    if metric_mode not in EXPLORER_METRIC_MODES:
        modes = ", ".join(sorted(EXPLORER_METRIC_MODES))
        raise ValueError(f"metric_mode must be one of: {modes}")

    output_path = Path(path or Path("out") / "goods_flow_explorer.html")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if eu5_data is not None:
        data = data or eu5_data.building_data
        goods_data = goods_data or eu5_data.goods_data
        advancements = eu5_data.advancements
    else:
        config = config or (None if profile else ParserConfig.from_env())
        goods_data = goods_data or load_goods_data(
            config, profile=profile, load_order_path=load_order_path
        )
        data = data or load_building_data(
            config, profile=profile, load_order_path=load_order_path, goods_data=goods_data
        )
        advancements = load_advancement_data(
            config, profile=profile, load_order_path=load_order_path
        ).advancements
    data = annotate_building_data_availability(
        data,
        advancements,
        include_specific_unlocks=include_specific_unlocks,
    )
    network = _explorer_network(data, goods_data, advancements)
    selected = _default_explorer_selection(network, good=good, building=building)
    output_path.write_text(
        _explorer_html(
            network,
            selected=selected,
            selected_age=max_age,
            depth=depth,
            include_specific_unlocks=include_specific_unlocks,
        ),
        encoding="utf-8",
    )
    return output_path


def open_goods_flow_explorer(
    path: str | Path | None = None,
    *,
    good: str | None = None,
    building: str | None = None,
    depth: int = 1,
    config: ParserConfig | None = None,
    profile: str = "merged_default",
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
    data: BuildingData | None = None,
    goods_data: GoodsData | None = None,
    eu5_data: Any | None = None,
    max_age: str | None = None,
    include_specific_unlocks: bool = False,
    metric_mode: str = "goods",
) -> Path:
    output_path = write_goods_flow_explorer_html(
        path,
        good=good,
        building=building,
        depth=depth,
        config=config,
        profile=profile,
        load_order_path=load_order_path,
        data=data,
        goods_data=goods_data,
        eu5_data=eu5_data,
        max_age=max_age,
        include_specific_unlocks=include_specific_unlocks,
        metric_mode=metric_mode,
    )
    webbrowser.open(output_path.resolve().as_uri())
    return output_path


def build_good_flow_graph(
    good: str,
    *,
    depth: int = 1,
    config: ParserConfig | None = None,
    profile: str | None = None,
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
    data: BuildingData | None = None,
    goods_data: GoodsData | None = None,
    eu5_data: Any | None = None,
    max_age: str | None = None,
    include_specific_unlocks: bool = False,
    annotate_availability: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    if depth < 1:
        raise ValueError("depth must be at least 1")

    if eu5_data is not None:
        if max_age is not None:
            eu5_data = filter_eu5_data_by_age(
                eu5_data,
                max_age,
                include_specific_unlocks=include_specific_unlocks,
            )
        else:
            building_data = annotate_building_data_availability(
                eu5_data.building_data,
                eu5_data.advancements,
                include_specific_unlocks=include_specific_unlocks,
            )
            eu5_data = replace(
                eu5_data,
                buildings=building_data.buildings,
                production_methods=building_data.production_methods,
                goods_flow_nodes=building_data.goods_flow_nodes,
                goods_flow_edges=building_data.goods_flow_edges,
                building_data=building_data,
            )
        data = data or eu5_data.building_data
        goods_data = goods_data or eu5_data.goods_data

    data = data or load_building_data(config, profile=profile, load_order_path=load_order_path)
    if max_age is not None and eu5_data is None:
        advancement_data = load_advancement_data(
            config, profile=profile, load_order_path=load_order_path
        )
        data = filter_building_data_by_age(
            data,
            advancement_data.advancements,
            max_age,
            include_specific_unlocks=include_specific_unlocks,
        )
    elif annotate_availability and eu5_data is None:
        advancement_data = load_advancement_data(
            config, profile=profile, load_order_path=load_order_path
        )
        data = annotate_building_data_availability(
            data,
            advancement_data.advancements,
            include_specific_unlocks=include_specific_unlocks,
        )
    good_sources = _good_sources_from_data(goods_data)
    methods = _methods_from_data(data)
    produced_by, consumed_by = _index_methods(methods)
    if good not in produced_by and good not in consumed_by:
        raise ValueError(f"Good {good!r} is not used by any parsed production method.")

    nodes: dict[str, dict[str, Any]] = {}
    edges: dict[str, dict[str, Any]] = {}
    layout_hints: dict[str, list[tuple[int, float]]] = {}
    queued: deque[tuple[str, int, int]] = deque([(good, 0, 0)])
    expanded: set[tuple[str, int]] = set()

    _add_good_node(nodes, good, level=0, selected=True, source=_good_source(good_sources, good))
    _add_layout_hint(layout_hints, _good_id(good), 0, 0)

    while queued:
        current_good, distance, level = queued.popleft()
        if (current_good, distance) in expanded or distance >= depth:
            continue
        expanded.add((current_good, distance))

        producer_methods = produced_by.get(current_good, [])
        consumer_methods = consumed_by.get(current_good, [])
        producer_start = -((len(producer_methods) - 1) * GROUP_Y_SPACING) / 2
        consumer_start = -((len(consumer_methods) - 1) * GROUP_Y_SPACING) / 2

        for method_index, method in enumerate(producer_methods):
            method_level = level - 1
            method_y = producer_start + method_index * GROUP_Y_SPACING
            _add_method_node(nodes, method, method_level)
            _add_layout_hint(layout_hints, _method_id(method.name), method_level, method_y)
            _add_edge(
                edges,
                source=_method_id(method.name),
                target=_good_id(current_good),
                kind="produces",
                label=_amount_label(method.output),
                amount=method.output,
                goods=current_good,
            )
            input_start = method_y - ((len(method.input_goods) - 1) * NODE_Y_SPACING) / 2
            for input_index, (input_good, amount) in enumerate(
                zip(method.input_goods, method.input_amounts, strict=False)
            ):
                input_level = method_level - 1
                input_y = input_start + input_index * NODE_Y_SPACING
                _add_good_node(
                    nodes,
                    input_good,
                    level=input_level,
                    source=_good_source(good_sources, input_good),
                )
                _add_layout_hint(layout_hints, _good_id(input_good), input_level, input_y)
                _add_edge(
                    edges,
                    source=_good_id(input_good),
                    target=_method_id(method.name),
                    kind="consumes",
                    label=_amount_label(amount),
                    amount=amount,
                    goods=input_good,
                )
                if distance + 1 < depth:
                    queued.append((input_good, distance + 1, input_level))

        for method_index, method in enumerate(consumer_methods):
            method_level = level + 1
            method_y = consumer_start + method_index * GROUP_Y_SPACING
            _add_method_node(nodes, method, method_level)
            _add_layout_hint(layout_hints, _method_id(method.name), method_level, method_y)
            input_amount = _input_amount(method, current_good)
            _add_edge(
                edges,
                source=_good_id(current_good),
                target=_method_id(method.name),
                kind="consumes",
                label=_amount_label(input_amount),
                amount=input_amount,
                goods=current_good,
            )
            if method.produced:
                output_level = method_level + 1
                output_y = method_y
                _add_good_node(
                    nodes,
                    method.produced,
                    level=output_level,
                    source=_good_source(good_sources, method.produced),
                )
                _add_layout_hint(layout_hints, _good_id(method.produced), output_level, output_y)
                _add_edge(
                    edges,
                    source=_method_id(method.name),
                    target=_good_id(method.produced),
                    kind="produces",
                    label=_amount_label(method.output),
                    amount=method.output,
                    goods=method.produced,
                )
                if distance + 1 < depth:
                    queued.append((method.produced, distance + 1, output_level))

    _assign_positions(nodes, layout_hints)
    return {"nodes": list(nodes.values()), "edges": list(edges.values())}


def _methods_from_data(data: BuildingData) -> list[_Method]:
    methods: list[_Method] = []
    for row in data.production_methods.to_dicts():
        methods.append(
            _Method(
                name=row["name"],
                produced=row["produced"],
                output=row["output"],
                input_goods=row["input_goods"] or [],
                input_amounts=row["input_amounts"] or [],
                building=row["building"],
                source_layer=row.get("source_layer"),
                source_mod=row.get("source_mod"),
                source_mode=row.get("source_mode"),
                source_history=row.get("source_history"),
                unlock_age=row.get("unlock_age"),
                general_unlock_age=row.get("general_unlock_age"),
                specific_unlock_age=row.get("specific_unlock_age"),
                availability_kind=row.get("availability_kind"),
                is_specific_only=row.get("is_specific_only"),
                building_unlock_age=row.get("building_unlock_age"),
                building_general_unlock_age=row.get("building_general_unlock_age"),
                building_specific_unlock_age=row.get("building_specific_unlock_age"),
                building_availability_kind=row.get("building_availability_kind"),
                building_is_specific_only=row.get("building_is_specific_only"),
                effective_unlock_age=row.get("effective_unlock_age"),
                effective_general_unlock_age=row.get("effective_general_unlock_age"),
                effective_specific_unlock_age=row.get("effective_specific_unlock_age"),
                effective_availability_kind=row.get("effective_availability_kind"),
                effective_is_specific_only=row.get("effective_is_specific_only"),
                input_cost=row.get("input_cost"),
                output_value=row.get("output_value"),
                profit=row.get("profit"),
                profit_margin_percent=row.get("profit_margin_percent"),
                missing_price_goods=row.get("missing_price_goods") or [],
            )
        )
    return methods


def _explorer_network(
    data: BuildingData,
    goods_data: GoodsData | None,
    advancements: Any | None = None,
) -> dict[str, Any]:
    methods = _methods_from_data(data)
    goods = _explorer_goods(goods_data, data, methods)

    buildings = []
    for row in data.buildings.to_dicts():
        production_methods = [
            *list(row.get("unique_production_methods") or []),
            *list(row.get("possible_production_methods") or []),
        ]
        buildings.append(
            {
                "name": row["name"],
                "category": row.get("category"),
                "production_methods": sorted(set(production_methods)),
                "unique_production_methods": row.get("unique_production_methods") or [],
                "possible_production_methods": row.get("possible_production_methods") or [],
                "source_layer": row.get("source_layer"),
                "source_mod": row.get("source_mod"),
                "source_mode": row.get("source_mode"),
                "source_history": row.get("source_history"),
                "provenance_state": _provenance_state(
                    row.get("source_layer"),
                    row.get("source_mod"),
                    row.get("source_mode"),
                    row.get("source_history"),
                ),
                "unlock_age": row.get("unlock_age"),
                "general_unlock_age": row.get("general_unlock_age"),
                "specific_unlock_age": row.get("specific_unlock_age"),
                "availability_kind": row.get("availability_kind"),
                "is_specific_only": row.get("is_specific_only"),
            }
        )

    return {
        "goods": goods,
        "buildings": sorted(buildings, key=lambda item: item["name"]),
        "output_modifiers": _advancement_output_modifiers(advancements),
        "methods": [
            {
                "name": method.name,
                "produced": method.produced,
                "output": method.output,
                "input_goods": method.input_goods,
                "input_amounts": method.input_amounts,
                "building": method.building,
                "source_layer": method.source_layer,
                "source_mod": method.source_mod,
                "source_mode": method.source_mode,
                "source_history": method.source_history,
                "provenance_state": _provenance_state(
                    method.source_layer,
                    method.source_mod,
                    method.source_mode,
                    method.source_history,
                ),
                "unlock_age": method.unlock_age,
                "general_unlock_age": method.general_unlock_age,
                "specific_unlock_age": method.specific_unlock_age,
                "availability_kind": method.availability_kind,
                "is_specific_only": method.is_specific_only,
                "building_unlock_age": method.building_unlock_age,
                "building_general_unlock_age": method.building_general_unlock_age,
                "building_specific_unlock_age": method.building_specific_unlock_age,
                "building_availability_kind": method.building_availability_kind,
                "building_is_specific_only": method.building_is_specific_only,
                "effective_unlock_age": method.effective_unlock_age,
                "effective_general_unlock_age": method.effective_general_unlock_age,
                "effective_specific_unlock_age": method.effective_specific_unlock_age,
                "effective_availability_kind": method.effective_availability_kind,
                "effective_is_specific_only": method.effective_is_specific_only,
                "input_cost": method.input_cost,
                "output_value": method.output_value,
                "profit": method.profit,
                "profit_margin_percent": method.profit_margin_percent,
                "missing_price_goods": method.missing_price_goods or [],
            }
            for method in methods
        ],
    }


def _explorer_goods(
    goods_data: GoodsData | None,
    data: BuildingData,
    methods: list[_Method],
) -> list[dict[str, Any]]:
    goods_sources = _good_sources_from_data(goods_data)
    goods_names = set(goods_sources)
    output_counts: dict[str, int] = {}
    input_counts: dict[str, int] = {}
    for method in methods:
        if method.produced:
            goods_names.add(method.produced)
            output_counts[method.produced] = output_counts.get(method.produced, 0) + 1
        for input_good in method.input_goods:
            goods_names.add(input_good)
            input_counts[input_good] = input_counts.get(input_good, 0) + 1

    summary_by_name: dict[str, dict[str, Any]] = {}
    if goods_data is not None:
        summary_by_name = {
            row["name"]: row
            for row in build_goods_summary(goods_data.goods, data.production_methods).to_dicts()
        }

    goods = []
    for good in sorted(goods_names):
        source = _good_source(goods_sources, good)
        summary = summary_by_name.get(good, {})
        goods.append(
            {
                "name": good,
                "price": summary.get("default_market_price"),
                "food": summary.get("food"),
                "type": summary.get("category"),
                "transport_cost": summary.get("transport_cost"),
                "pm_output": summary.get("output_method_count", output_counts.get(good, 0)),
                "pm_input": summary.get("input_method_count", input_counts.get(good, 0)),
                "source_layer": source["source_layer"],
                "source_mod": source["source_mod"],
                "source_mode": source["source_mode"],
                "source_history": source["source_history"],
                "provenance_state": _provenance_state(
                    source["source_layer"],
                    source["source_mod"],
                    source["source_mode"],
                    source["source_history"],
                ),
            }
        )
    return goods


def _advancement_output_modifiers(advancements: Any | None) -> list[dict[str, Any]]:
    if advancements is None:
        return []
    output_modifiers: list[dict[str, Any]] = []
    for row in advancements.to_dicts():
        age = row.get("age")
        if age not in AGE_ORDER:
            continue
        try:
            modifiers = json.loads(row.get("modifiers") or "{}")
        except json.JSONDecodeError:
            continue
        for key, value in modifiers.items():
            if not key.startswith("global_") or not key.endswith("_output_modifier"):
                continue
            if not isinstance(value, int | float) or isinstance(value, bool):
                continue
            good = key.removeprefix("global_").removesuffix("_output_modifier")
            output_modifiers.append(
                {
                    "good": good,
                    "advancement": row["name"],
                    "age": age,
                    "value": float(value),
                    "has_potential": bool(row.get("has_potential")),
                    "modifier_key": key,
                    "source_layer": row.get("source_layer"),
                    "source_mod": row.get("source_mod"),
                    "source_mode": row.get("source_mode"),
                    "source_history": row.get("source_history"),
                    "provenance_state": _provenance_state(
                        row.get("source_layer"),
                        row.get("source_mod"),
                        row.get("source_mode"),
                        row.get("source_history"),
                    ),
                }
            )
    return sorted(
        output_modifiers,
        key=lambda item: (AGE_ORDER.index(item["age"]), item["good"], item["advancement"]),
    )


def _default_explorer_selection(
    network: dict[str, Any], *, good: str | None, building: str | None
) -> dict[str, str]:
    goods = [item["name"] for item in network["goods"]]
    buildings = [item["name"] for item in network["buildings"]]
    if building is not None:
        if building not in buildings:
            raise ValueError(f"Building {building!r} is not available for the goods flow explorer.")
        return {"type": "building", "name": building}
    if good in goods:
        return {"type": "good", "name": str(good)}
    if "wheat" in goods:
        return {"type": "good", "name": "wheat"}
    if goods:
        return {"type": "good", "name": goods[0]}
    if buildings:
        return {"type": "building", "name": buildings[0]}
    raise ValueError("No goods or buildings are available for the goods flow explorer.")


def _good_sources_from_data(goods_data: GoodsData | None) -> dict[str, dict[str, str | None]]:
    if goods_data is None:
        return {}
    return {
        row["name"]: {
            "source_layer": row.get("source_layer"),
            "source_mod": row.get("source_mod"),
            "source_mode": row.get("source_mode"),
            "source_history": row.get("source_history"),
        }
        for row in goods_data.goods.to_dicts()
    }


def _good_source(
    good_sources: dict[str, dict[str, str | None]], good: str
) -> dict[str, str | None]:
    return good_sources.get(
        good,
        {
            "source_layer": None,
            "source_mod": None,
            "source_mode": None,
            "source_history": None,
        },
    )


def _index_methods(
    methods: list[_Method],
) -> tuple[dict[str, list[_Method]], dict[str, list[_Method]]]:
    produced_by: dict[str, list[_Method]] = {}
    consumed_by: dict[str, list[_Method]] = {}
    for method in methods:
        if method.produced:
            produced_by.setdefault(method.produced, []).append(method)
        for input_good in method.input_goods:
            consumed_by.setdefault(input_good, []).append(method)
    return produced_by, consumed_by


def _provenance_state(
    source_layer: str | None,
    source_mod: str | None,
    source_mode: str | None,
    source_history: str | None,
) -> str:
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


def _add_good_node(
    nodes: dict[str, dict[str, Any]],
    good: str,
    *,
    level: int,
    selected: bool = False,
    source: dict[str, str | None] | None = None,
) -> None:
    node_id = _good_id(good)
    classes = "good selected" if selected else "good"
    source = source or {
        "source_layer": None,
        "source_mod": None,
        "source_mode": None,
        "source_history": None,
    }
    provenance_state = _provenance_state(
        source.get("source_layer"),
        source.get("source_mod"),
        source.get("source_mode"),
        source.get("source_history"),
    )
    node = nodes.setdefault(
        node_id,
        {
            "data": {
                "id": node_id,
                "label": good,
                "kind": "good",
                "source_layer": source["source_layer"],
                "source_mod": source["source_mod"],
                "source_mode": source["source_mode"],
                "source_history": source["source_history"],
                "provenance_state": provenance_state,
            },
            "classes": classes,
        },
    )
    data = node["data"]
    if data.get("source_layer") is None and source.get("source_layer") is not None:
        data["source_layer"] = source["source_layer"]
        data["source_mod"] = source["source_mod"]
        data["source_mode"] = source["source_mode"]
        data["source_history"] = source["source_history"]
        data["provenance_state"] = provenance_state
    if selected:
        node["classes"] = "good selected"
    _set_level(node, level)


def _add_method_node(nodes: dict[str, dict[str, Any]], method: _Method, level: int) -> None:
    node_id = _method_id(method.name)
    label = method.name if method.building is None else f"{method.name}\n{method.building}"
    node = nodes.setdefault(
        node_id,
        {
            "data": {
                "id": node_id,
                "label": label,
                "kind": "production_method",
                "production_method": method.name,
                "building": method.building,
                "source_layer": method.source_layer,
                "source_mod": method.source_mod,
                "source_mode": method.source_mode,
                "source_history": method.source_history,
                "unlock_age": method.unlock_age,
                "general_unlock_age": method.general_unlock_age,
                "specific_unlock_age": method.specific_unlock_age,
                "availability_kind": method.availability_kind,
                "is_specific_only": method.is_specific_only,
                "building_unlock_age": method.building_unlock_age,
                "building_general_unlock_age": method.building_general_unlock_age,
                "building_specific_unlock_age": method.building_specific_unlock_age,
                "building_availability_kind": method.building_availability_kind,
                "building_is_specific_only": method.building_is_specific_only,
                "effective_unlock_age": method.effective_unlock_age,
                "effective_general_unlock_age": method.effective_general_unlock_age,
                "effective_specific_unlock_age": method.effective_specific_unlock_age,
                "effective_availability_kind": method.effective_availability_kind,
                "effective_is_specific_only": method.effective_is_specific_only,
                "provenance_state": _provenance_state(
                    method.source_layer,
                    method.source_mod,
                    method.source_mode,
                    method.source_history,
                ),
            },
            "classes": "production-method",
        },
    )
    _set_level(node, level)


def _set_level(node: dict[str, Any], level: int) -> None:
    data = node["data"]
    current = data.get("level")
    if current is None or abs(level) < abs(current):
        data["level"] = level


def _add_edge(
    edges: dict[str, dict[str, Any]],
    *,
    source: str,
    target: str,
    kind: str,
    label: str,
    amount: float | None,
    goods: str | None,
) -> None:
    edge_id = f"{source}->{target}:{kind}"
    edges.setdefault(
        edge_id,
        {
            "data": {
                "id": edge_id,
                "source": source,
                "target": target,
                "label": label,
                "kind": kind,
                "amount": amount,
                "goods": goods,
            },
            "classes": kind,
        },
    )


def _add_layout_hint(
    layout_hints: dict[str, list[tuple[int, float]]], node_id: str, level: int, y: float
) -> None:
    layout_hints.setdefault(node_id, []).append((level, y))


def _assign_positions(
    nodes: dict[str, dict[str, Any]], layout_hints: dict[str, list[tuple[int, float]]]
) -> None:
    by_level: dict[int, list[dict[str, Any]]] = {}
    for node in nodes.values():
        node_id = node["data"]["id"]
        hints = layout_hints.get(node_id, [])
        if hints:
            level = min((hint[0] for hint in hints), key=abs)
            y = sum(hint[1] for hint in hints) / len(hints)
            node["data"]["level"] = level
            node["position"] = {"x": level * NODE_X_SPACING, "y": y}
        else:
            by_level.setdefault(node["data"]["level"], []).append(node)

    for level, level_nodes in by_level.items():
        level_nodes.sort(key=lambda node: node["data"]["label"])
        y_offset = -((len(level_nodes) - 1) * NODE_Y_SPACING) / 2
        for index, node in enumerate(level_nodes):
            node["position"] = {
                "x": level * NODE_X_SPACING,
                "y": y_offset + index * NODE_Y_SPACING,
            }

    _spread_column_collisions(nodes)


def _spread_column_collisions(nodes: dict[str, dict[str, Any]]) -> None:
    by_level: dict[int, list[dict[str, Any]]] = {}
    for node in nodes.values():
        by_level.setdefault(node["data"]["level"], []).append(node)

    for level_nodes in by_level.values():
        level_nodes.sort(key=lambda node: (node["position"]["y"], node["data"]["label"]))
        for index in range(1, len(level_nodes)):
            previous_y = level_nodes[index - 1]["position"]["y"]
            current_y = level_nodes[index]["position"]["y"]
            if current_y - previous_y < MIN_COLUMN_NODE_SPACING:
                level_nodes[index]["position"]["y"] = previous_y + MIN_COLUMN_NODE_SPACING


def _input_amount(method: _Method, good: str) -> float | None:
    for input_good, amount in zip(method.input_goods, method.input_amounts, strict=False):
        if input_good == good:
            return amount
    return None


def _amount_label(amount: float | None) -> str:
    if amount is None:
        return ""
    return f"{amount:g}"


def _good_id(good: str) -> str:
    return f"good:{good}"


def _method_id(method: str) -> str:
    return f"production_method:{method}"


_CYTOSCAPE_STYLE = [
    {
        "selector": "node",
        "style": {
            "font-family": "Inter, Segoe UI, sans-serif",
            "font-size": "12px",
            "label": "data(label)",
            "text-halign": "center",
            "text-valign": "center",
            "text-wrap": "wrap",
            "text-max-width": "120px",
            "width": "label",
            "height": "label",
            "padding": "12px",
            "border-width": 4,
            "border-color": "data(provenance_color)",
            "border-style": "data(provenance_border_style)",
            "background-color": "#f8fafc",
            "color": "#172033",
            "shape": "round-rectangle",
        },
    },
    {
        "selector": ".good",
        "style": {
            "background-color": "data(goods_color)",
            "border-color": "data(provenance_color)",
            "border-width": 4,
            "color": "#ffffff",
            "font-weight": "700",
            "text-outline-color": "data(goods_color)",
            "text-outline-width": 1,
        },
    },
    {
        "selector": ".selected",
        "style": {
            "border-width": 6,
            "color": "#ffffff",
            "font-weight": "700",
        },
    },
    {
        "selector": ".production-method",
        "style": {
            "background-color": "#ecfdf5",
            "border-color": "data(provenance_color)",
            "border-width": 4,
            "padding": "12px",
            "shape": "round-rectangle",
            "text-max-width": "220px",
        },
    },
    {
        "selector": ".building",
        "style": {
            "background-color": "#f1f5f9",
            "text-max-width": "170px",
        },
    },
    {
        "selector": ".age-node",
        "style": {
            "background-color": "#e0f2fe",
            "border-color": "#0284c7",
            "border-width": 4,
            "font-weight": "700",
            "shape": "round-rectangle",
            "text-max-width": "190px",
        },
    },
    {
        "selector": ".advancement-node",
        "style": {
            "background-color": "#fff7ed",
            "border-color": "data(provenance_color)",
            "border-style": "data(provenance_border_style)",
            "border-width": 4,
            "padding": "10px",
            "shape": "round-rectangle",
            "text-max-width": "210px",
        },
    },
    {
        "selector": "edge",
        "style": {
            "curve-style": "bezier",
            "control-point-step-size": 56,
            "font-size": "11px",
            "label": "data(label)",
            "line-color": "data(goods_color)",
            "opacity": 0.7,
            "target-arrow-color": "data(goods_color)",
            "target-arrow-shape": "triangle",
            "text-background-color": "#ffffff",
            "text-background-opacity": 0.9,
            "text-background-padding": "3px",
            "width": "data(edge_width)",
        },
    },
    {
        "selector": ".produces",
        "style": {
            "target-arrow-shape": "triangle",
        },
    },
    {
        "selector": ".building-ranked-edge",
        "style": {
            "curve-style": "segments",
            "edge-distances": "endpoints",
            "segment-distances": "0px",
            "segment-weights": "0.5",
            "source-endpoint": "data(source_endpoint)",
            "target-endpoint": "data(target_endpoint)",
        },
    },
    {
        "selector": ".modifier-edge",
        "style": {
            "curve-style": "straight",
            "line-style": "dashed",
        },
    },
    {
        "selector": ".dimmed",
        "style": {
            "opacity": 0.15,
            "text-opacity": 0.08,
        },
    },
    {
        "selector": "edge.dimmed",
        "style": {
            "opacity": 0.12,
            "text-opacity": 0.06,
        },
    },
    {
        "selector": ".focus-neighbor",
        "style": {
            "opacity": 1,
            "text-opacity": 1,
        },
    },
    {
        "selector": ".focused",
        "style": {
            "opacity": 1,
            "text-opacity": 1,
            "z-index": 100,
        },
    },
    {
        "selector": "node.focused",
        "style": {
            "border-width": 7,
        },
    },
    {
        "selector": "edge.focused",
        "style": {
            "opacity": 1,
            "text-opacity": 1,
        },
    },
]


def _explorer_html(
    network: dict[str, Any],
    *,
    selected: dict[str, str],
    selected_age: str | None = None,
    depth: int = 1,
    include_specific_unlocks: bool = False,
) -> str:
    title = "EU5 Goods Flow Explorer"
    selection_options = "\n".join(
        (
            f"""          <option value="{html.escape(item["name"])}"></option>"""
        )
        for item in network["goods"]
    )
    building_options = "\n".join(
        (
            f"""          <option value="{html.escape(item["name"])}"></option>"""
        )
        for item in network["buildings"]
    )
    age_options = "\n".join(
        ["""          <option value="">All ages</option>"""]
        + [
            f"""          <option value="{html.escape(age)}">{html.escape(age)}</option>"""
            for age in AGE_ORDER
        ]
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <script src="https://unpkg.com/cytoscape@3.30.4/dist/cytoscape.min.js"></script>
  <script src="https://unpkg.com/layout-base@2.0.1/layout-base.js"></script>
  <script src="https://unpkg.com/cose-base@2.2.0/cose-base.js"></script>
  <script src="https://unpkg.com/cytoscape-fcose@2.2.0/cytoscape-fcose.js"></script>
  <script src="https://unpkg.com/dagre@0.8.5/dist/dagre.min.js"></script>
  <script src="https://unpkg.com/cytoscape-dagre@2.5.0/cytoscape-dagre.js"></script>
  <style>
    * {{ box-sizing: border-box; }}
    html, body {{ height: 100%; margin: 0; }}
    body {{
      font-family: Inter, Segoe UI, system-ui, sans-serif;
      background: #f8fafc;
      color: #172033;
      overflow: hidden;
    }}
    .shell {{
      display: grid;
      grid-template-rows: auto 1fr;
      height: 100vh;
      width: 100vw;
    }}
    header {{
      align-items: center;
      background: #ffffff;
      border-bottom: 1px solid #dbe4ef;
      display: flex;
      gap: 12px;
      min-height: 58px;
      padding: 10px 18px;
    }}
    h1 {{
      font-size: 16px;
      font-weight: 700;
      line-height: 1.2;
      margin: 0;
      white-space: nowrap;
    }}
    .meta {{
      color: #64748b;
      font-size: 13px;
      white-space: nowrap;
    }}
    .spacer {{ flex: 1; }}
    .tabs {{
      display: inline-flex;
      gap: 4px;
    }}
    .tab-button {{
      font-weight: 600;
    }}
    .tab-button.active {{
      background: #172033;
      border-color: #172033;
      color: #ffffff;
    }}
    .controls {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      justify-content: flex-end;
    }}
    button, select, input[type="number"], input[type="search"] {{
      background: #ffffff;
      border: 1px solid #cbd5e1;
      border-radius: 6px;
      color: #172033;
      font: inherit;
      font-size: 13px;
      min-height: 32px;
      padding: 6px 8px;
    }}
    button {{
      cursor: pointer;
    }}
    button:hover {{ background: #f1f5f9; }}
    #entityType {{
      width: 104px;
    }}
    #entitySelect {{
      min-width: 220px;
      max-width: 300px;
    }}
    #modifierGoodSelect {{
      min-width: 220px;
      max-width: 300px;
    }}
    #depthInput {{
      width: 70px;
    }}
    .toggle {{
      align-items: center;
      border: 1px solid #cbd5e1;
      border-radius: 6px;
      display: inline-flex;
      gap: 6px;
      min-height: 32px;
      padding: 6px 8px;
      white-space: nowrap;
    }}
    .toggle input {{
      margin: 0;
    }}
    #cy {{
      height: 100%;
      width: 100%;
    }}
    #goodsOverview {{
      background: #ffffff;
      display: none;
      height: 100%;
      overflow: auto;
      width: 100%;
    }}
    .overview-table {{
      border-collapse: collapse;
      font-size: 13px;
      min-width: 760px;
      width: 100%;
    }}
    .overview-table th {{
      background: #f8fafc;
      border-bottom: 1px solid #cbd5e1;
      color: #475569;
      font-size: 11px;
      font-weight: 700;
      padding: 9px 12px;
      position: sticky;
      text-align: left;
      text-transform: uppercase;
      top: 0;
      z-index: 2;
    }}
    .overview-table th.sortable {{
      cursor: pointer;
      user-select: none;
    }}
    .sort-header {{
      align-items: center;
      background: transparent;
      border: 0;
      color: inherit;
      cursor: pointer;
      display: inline-flex;
      font: inherit;
      font-size: inherit;
      font-weight: inherit;
      gap: 4px;
      justify-content: flex-start;
      min-height: 0;
      padding: 0;
      text-transform: inherit;
      width: 100%;
    }}
    .overview-table th.numeric .sort-header {{
      justify-content: flex-end;
    }}
    .sort-indicator {{
      color: #2563eb;
      display: inline-block;
      min-width: 0.8em;
    }}
    .overview-table td {{
      border-bottom: 1px solid #e2e8f0;
      padding: 8px 12px;
      white-space: nowrap;
    }}
    .overview-table .numeric {{
      text-align: right;
    }}
    .overview-table tbody tr:hover {{
      background: #f8fafc;
    }}
    .legend {{
      background: rgba(255, 255, 255, 0.96);
      border: 1px solid #cbd5e1;
      border-radius: 8px;
      box-shadow: 0 8px 24px rgba(15, 23, 42, 0.12);
      max-height: calc(100vh - 92px);
      max-width: 320px;
      overflow: auto;
      position: fixed;
      right: 16px;
      top: 74px;
      z-index: 4;
    }}
    .legend summary {{
      cursor: pointer;
      font-size: 13px;
      font-weight: 700;
      padding: 10px 12px;
      user-select: none;
    }}
    .legend-body {{
      border-top: 1px solid #e2e8f0;
      display: grid;
      gap: 12px;
      padding: 10px 12px 12px;
    }}
    .legend-section-title {{
      color: #475569;
      font-size: 11px;
      font-weight: 700;
      margin: 0 0 6px;
      text-transform: uppercase;
    }}
    .legend-list {{
      display: grid;
      gap: 5px;
    }}
    .legend-row {{
      align-items: center;
      display: grid;
      gap: 7px;
      grid-template-columns: 14px minmax(0, 1fr) auto;
      min-width: 0;
    }}
    .legend-swatch {{
      border: 2px solid #64748b;
      border-radius: 4px;
      height: 14px;
      width: 14px;
    }}
    .legend-label {{
      font-size: 12px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }}
    .legend-count {{
      color: #64748b;
      font-size: 11px;
    }}
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <h1>Goods Flow</h1>
      <div class="meta" id="graphMeta">0 nodes &middot; 0 edges</div>
      <div class="tabs" aria-label="Explorer view">
        <button class="tab-button" id="overviewTab" type="button">Overview</button>
        <button class="tab-button active" id="flowTab" type="button">Flow</button>
        <button class="tab-button" id="modifierTab" type="button">Output Modifiers</button>
      </div>
      <div class="spacer"></div>
      <div class="controls">
        <select class="flow-control" id="entityType" aria-label="Selection type">
          <option value="good">Goods</option>
          <option value="building">Buildings</option>
        </select>
        <input
          aria-label="Good or building"
          class="flow-control"
          id="entitySelect"
          list="entityOptions"
          type="search"
        >
        <datalist
          id="entityOptions"
          data-good-options="{html.escape(selection_options)}"
          data-building-options="{html.escape(building_options)}"
        >
{selection_options}
        </datalist>
        <input
          aria-label="Modifier good"
          class="modifier-control"
          id="modifierGoodSelect"
          list="modifierGoodOptions"
          type="search"
        >
        <datalist id="modifierGoodOptions">
{selection_options}
        </datalist>
        <input
          aria-label="Depth"
          class="flow-control"
          id="depthInput"
          max="5"
          min="1"
          type="number"
        >
        <select class="flow-control" id="ageFilter" aria-label="Maximum age">
{age_options}
        </select>
        <label
          class="toggle shared-graph-control"
          title="Include country, region, and religion-specific unlocks"
        >
          <input id="specificUnlocks" type="checkbox">
          Specific unlocks
        </label>
        <button class="flow-control" type="button" onclick="runSpreadLayout()">Spread</button>
        <button class="flow-control" type="button" onclick="runRankedLayout()">Ranked</button>
        <button
          class="shared-graph-control"
          type="button"
          onclick="cy.fit(undefined, 80)"
        >Fit</button>
        <button
          class="shared-graph-control"
          type="button"
          onclick="cy.zoom(cy.zoom() * 1.2)"
        >Zoom In</button>
        <button
          class="shared-graph-control"
          type="button"
          onclick="cy.zoom(cy.zoom() / 1.2)"
        >Zoom Out</button>
      </div>
    </header>
    <div id="cy"></div>
    <div id="goodsOverview"></div>
    <details class="legend" open>
      <summary>Legend</summary>
      <div class="legend-body">
        <section>
          <p class="legend-section-title">Provenance</p>
          <div class="legend-list" id="provenanceLegend"></div>
        </section>
      </div>
    </details>
  </div>
  <script>
    const network = {json.dumps(network, ensure_ascii=False)};
    const ageOrder = {json.dumps(list(AGE_ORDER), ensure_ascii=False)};
    const ageIndex = Object.fromEntries(ageOrder.map((age, index) => [age, index]));
    const initialSelection = {json.dumps(selected, ensure_ascii=False)};
    const initialAge = {json.dumps(selected_age)};
    const initialDepth = {json.dumps(depth)};
    const initialSpecificUnlocks = {json.dumps(include_specific_unlocks)};
    let currentLayout = "spread";
    let currentExplorerView = "flow";
    const cloneElement = element => JSON.parse(JSON.stringify(element));
    const goodsByName = new Map(network.goods.map(good => [good.name, good]));
    const buildingsByName = new Map(network.buildings.map(building => [building.name, building]));
    const goodsOverviewColumns = [
      ["name", "name", false],
      ["price", "price", true],
      ["food", "food", true],
      ["type", "type", false],
      ["transport_cost", "transport cost", true],
      ["pm_output", "pm_output", true],
      ["pm_input", "pm_input", true]
    ];
    let goodsOverviewSort = {{ key: "name", direction: "asc" }};
    const provenanceStyles = {{
      vanilla_exact: {{
        label: "Exact vanilla value",
        color: "#334155",
        borderStyle: "solid"
      }},
      mod_exact: {{
        borderStyle: "solid"
      }},
      merged: {{
        label: "Merged by load order",
        color: "#f59e0b",
        borderStyle: "dashed"
      }},
      unknown: {{
        label: "Unknown source",
        color: "#94a3b8",
        borderStyle: "dotted"
      }},
      timeline: {{
        label: "Timeline age",
        color: "#0284c7",
        borderStyle: "solid"
      }}
    }};
    function colorForGood(good) {{
      if (!good) return "#94a3b8";
      const palette = [
        "#2563eb", "#dc2626", "#16a34a", "#ca8a04", "#9333ea", "#0891b2",
        "#ea580c", "#4f46e5", "#be123c", "#0f766e", "#7c3aed", "#65a30d",
        "#c2410c", "#0284c7", "#a21caf", "#15803d", "#b45309", "#1d4ed8"
      ];
      let hash = 0;
      for (let index = 0; index < good.length; index += 1) {{
        hash = ((hash << 5) - hash + good.charCodeAt(index)) | 0;
      }}
      return palette[Math.abs(hash) % palette.length];
    }}
    function sourceKey(data) {{
      return data.source_mod || data.source_layer || "unknown";
    }}
    function sourceLabel(source) {{
      if (!source || source === "unknown") return "Unknown source";
      return source === "vanilla" ? "Vanilla" : source;
    }}
    function colorForSource(source) {{
      if (!source || source === "unknown") return "#94a3b8";
      if (source === "vanilla") return "#334155";
      const palette = [
        "#be123c", "#7c2d12", "#166534", "#0e7490", "#4338ca", "#86198f",
        "#a16207", "#047857", "#1d4ed8", "#c2410c", "#0f766e", "#6d28d9"
      ];
      let hash = 0;
      for (let index = 0; index < source.length; index += 1) {{
        hash = ((hash << 5) - hash + source.charCodeAt(index)) | 0;
      }}
      return palette[Math.abs(hash) % palette.length];
    }}
    function provenanceStyle(data) {{
      const provenance = data.provenance_state || "unknown";
      const source = sourceKey(data);
      if (provenance === "mod_exact") {{
        return {{
          key: `mod_exact:${{source}}`,
          label: `${{sourceLabel(source)}} value`,
          color: colorForSource(source),
          borderStyle: "solid"
        }};
      }}
      const style = provenanceStyles[provenance] || provenanceStyles.unknown;
      return {{
        key: provenance,
        label: style.label,
        color: style.color,
        borderStyle: style.borderStyle
      }};
    }}
    function selectionLabel(selection) {{
      return selection && selection.name ? selection.name : "";
    }}
    function updateEntityOptions(type) {{
      const options = document.getElementById("entityOptions");
      options.innerHTML = type === "building"
        ? options.dataset.buildingOptions
        : options.dataset.goodOptions;
    }}
    function resolveSelection(type, value) {{
      if (type === "building" && buildingsByName.has(value)) {{
        return {{ type: "building", name: value }};
      }}
      if (type === "good" && goodsByName.has(value)) {{
        return {{ type: "good", name: value }};
      }}
      return initialSelection;
    }}
    function ageAllowed(unlockAge, selectedAge) {{
      if (!selectedAge || !unlockAge) return true;
      return ageIndex[unlockAge] <= ageIndex[selectedAge];
    }}
    function minAge(left, right) {{
      if (!left) return right || null;
      if (!right) return left;
      return ageIndex[left] <= ageIndex[right] ? left : right;
    }}
    function latestAge(left, right) {{
      if (!left) return right || null;
      if (!right) return left;
      return ageIndex[left] >= ageIndex[right] ? left : right;
    }}
    function selectedUnlockAge(generalAge, specificAge, fallbackAge, includeSpecific) {{
      if (includeSpecific) return minAge(generalAge || fallbackAge, specificAge);
      return generalAge || fallbackAge || null;
    }}
    function methodVisible(method, selectedAge, includeSpecific) {{
      if (!selectedAge) return true;
      const kind = method.effective_availability_kind
        || method.availability_kind
        || "available_by_default";
      if (kind === "available_by_default") return true;
      if (kind === "specific_only" && !includeSpecific) return false;
      const methodAge = selectedUnlockAge(
        method.general_unlock_age,
        method.specific_unlock_age,
        method.unlock_age,
        includeSpecific
      );
      const buildingAge = selectedUnlockAge(
        method.building_general_unlock_age,
        method.building_specific_unlock_age,
        method.building_unlock_age,
        includeSpecific
      );
      const unlockAge = latestAge(methodAge, buildingAge);
      return ageAllowed(unlockAge, selectedAge);
    }}
    function buildingVisible(building, selectedAge, includeSpecific) {{
      if (!selectedAge) return true;
      const kind = building.availability_kind || "available_by_default";
      if (kind === "available_by_default") return true;
      if (kind === "specific_only" && !includeSpecific) return false;
      const unlockAge = selectedUnlockAge(
        building.general_unlock_age,
        building.specific_unlock_age,
        building.unlock_age,
        includeSpecific
      );
      return ageAllowed(unlockAge, selectedAge);
    }}
    function amountLabel(amount) {{
      if (amount === null || amount === undefined) return "";
      return Number(amount).toPrecision(12).replace(/\\.0+$|(?<=\\.[0-9]*?)0+$/g, "");
    }}
    function widthForAmount(amount) {{
      const numeric = Number(amount);
      if (!Number.isFinite(numeric) || numeric <= 0) return 2;
      return Math.max(2, Math.min(9, 1.6 + Math.sqrt(numeric) * 1.8));
    }}
    function formatMetricValue(value, signed = false) {{
      if (value === null || value === undefined) return "n/a";
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) return "n/a";
      const formatted = numeric.toLocaleString(undefined, {{
        maximumFractionDigits: 2,
        minimumFractionDigits: 0
      }});
      return signed && numeric > 0 ? `+${{formatted}}` : formatted;
    }}
    function formatPercentValue(value) {{
      const formatted = formatMetricValue(value, true);
      return formatted === "n/a" ? formatted : `${{formatted}}%`;
    }}
    function formatOverviewValue(value) {{
      if (value === null || value === undefined || value === "") return "n/a";
      if (typeof value === "number") {{
        return Number.isFinite(value)
          ? value.toLocaleString(undefined, {{ maximumFractionDigits: 2 }})
          : "n/a";
      }}
      return String(value);
    }}
    function formatOverviewCount(value) {{
      const numeric = Number(value);
      return Number.isFinite(numeric) ? String(Math.trunc(numeric)) : "0";
    }}
    function goodsOverviewSortableValue(row, key) {{
      const value = row[key];
      if (value === null || value === undefined || value === "") return null;
      if (typeof value === "number") return value;
      const numeric = Number(value);
      if (Number.isFinite(numeric)) return numeric;
      return String(value).toLocaleLowerCase();
    }}
    function compareGoodsOverviewRows(left, right) {{
      const leftValue = goodsOverviewSortableValue(left, goodsOverviewSort.key);
      const rightValue = goodsOverviewSortableValue(right, goodsOverviewSort.key);
      if (leftValue === null && rightValue === null) return left.name.localeCompare(right.name);
      if (leftValue === null) return 1;
      if (rightValue === null) return -1;
      let result = 0;
      if (typeof leftValue === "number" && typeof rightValue === "number") {{
        result = leftValue - rightValue;
      }} else {{
        result = String(leftValue).localeCompare(String(rightValue));
      }}
      if (result === 0) return left.name.localeCompare(right.name);
      return goodsOverviewSort.direction === "asc" ? result : -result;
    }}
    function setGoodsOverviewSort(key) {{
      if (goodsOverviewSort.key === key) {{
        goodsOverviewSort = {{
          key,
          direction: goodsOverviewSort.direction === "asc" ? "desc" : "asc"
        }};
      }} else {{
        const column = goodsOverviewColumns.find(([columnKey]) => columnKey === key);
        goodsOverviewSort = {{
          key,
          direction: column && column[2] ? "desc" : "asc"
        }};
      }}
      renderGoodsOverview();
    }}
    function renderGoodsOverview() {{
      const container = document.getElementById("goodsOverview");
      container.replaceChildren();
      const table = document.createElement("table");
      table.className = "overview-table";
      const thead = document.createElement("thead");
      const headerRow = document.createElement("tr");
      for (const [key, label, numeric] of goodsOverviewColumns) {{
        const cell = document.createElement("th");
        cell.className = numeric ? "numeric sortable" : "sortable";
        const button = document.createElement("button");
        button.className = "sort-header";
        button.type = "button";
        button.addEventListener("click", () => setGoodsOverviewSort(key));
        const labelSpan = document.createElement("span");
        labelSpan.textContent = label;
        const indicator = document.createElement("span");
        indicator.className = "sort-indicator";
        indicator.textContent = goodsOverviewSort.key === key
          ? (goodsOverviewSort.direction === "asc" ? "^" : "v")
          : "";
        button.append(labelSpan, indicator);
        cell.append(button);
        headerRow.append(cell);
      }}
      thead.append(headerRow);
      const tbody = document.createElement("tbody");
      for (const good of [...network.goods].sort(compareGoodsOverviewRows)) {{
        const row = document.createElement("tr");
        for (const [key, , numeric] of goodsOverviewColumns) {{
          const cell = document.createElement("td");
          if (numeric) cell.className = "numeric";
          cell.textContent = key === "pm_output" || key === "pm_input"
            ? formatOverviewCount(good[key])
            : formatOverviewValue(good[key]);
          row.append(cell);
        }}
        tbody.append(row);
      }}
      table.append(thead, tbody);
      container.append(table);
      document.getElementById("graphMeta").textContent = `${{network.goods.length}} goods`;
    }}
    function methodMetricLines(method) {{
      return [
        `Input: ${{formatMetricValue(method.input_cost)}}`,
        `Output: ${{formatMetricValue(method.output_value)}}`,
        `Profit: ${{formatMetricValue(method.profit, true)}}`,
        `Profit %: ${{formatPercentValue(method.profit_margin_percent)}}`
      ];
    }}
    function referencedBuildings(method) {{
      const buildingNames = [];
      for (const building of network.buildings) {{
        if ((building.production_methods || []).includes(method.name)) {{
          buildingNames.push(building.name);
        }}
      }}
      return buildingNames;
    }}
    function methodBuildingContext(method, selectedBuilding = null) {{
      if (selectedBuilding) return selectedBuilding;
      if (method.building) return method.building;
      const buildings = referencedBuildings(method);
      if (buildings.length === 1) return buildings[0];
      if (buildings.length > 1) return `${{buildings[0]}} +${{buildings.length - 1}}`;
      return null;
    }}
    function methodLabel(method, selectedBuilding = null) {{
      const lines = [method.name];
      const buildingContext = methodBuildingContext(method, selectedBuilding);
      if (buildingContext) lines.push(buildingContext);
      lines.push(...methodMetricLines(method));
      return lines.join("\\n");
    }}
    function formatModifierValue(value) {{
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) return "0.00";
      const formatted = Math.abs(numeric).toFixed(2);
      if (numeric > 0) return `+${{formatted}}`;
      if (numeric < 0) return `-${{formatted}}`;
      return formatted;
    }}
    function goodId(good) {{
      return `good:${{good}}`;
    }}
    function modifierAgeId(age) {{
      return `modifier_age:${{age}}`;
    }}
    function modifierAdvancementId(modifier, index) {{
      return `modifier_advancement:${{modifier.good}}:${{modifier.advancement}}:${{index}}`;
    }}
    function buildingInputGoodId(good) {{
      return `building_input_good:${{good}}`;
    }}
    function buildingOutputGoodId(good) {{
      return `building_output_good:${{good}}`;
    }}
    function methodId(method) {{
      return `production_method:${{method}}`;
    }}
    function addGoodNodeWithId(nodes, id, good, selected = false, rankedRole = null) {{
      if (nodes.has(id)) {{
        if (selected) nodes.get(id).classes = "good selected";
        if (rankedRole && !nodes.get(id).data.ranked_role) {{
          nodes.get(id).data.ranked_role = rankedRole;
        }}
        return;
      }}
      const source = goodsByName.get(good) || {{ name: good, provenance_state: "unknown" }};
      const style = provenanceStyle(source);
      nodes.set(id, {{
        data: {{
          id,
          label: good,
          kind: "good",
          source_layer: source.source_layer,
          source_mod: source.source_mod,
          source_mode: source.source_mode,
          source_history: source.source_history,
          provenance_state: source.provenance_state || "unknown",
          provenance_color: style.color,
          provenance_border_style: style.borderStyle,
          goods_color: colorForGood(good),
          ranked_role: rankedRole
        }},
        classes: selected ? "good selected" : "good"
      }});
    }}
    function addGoodNode(nodes, good, selected = false) {{
      addGoodNodeWithId(nodes, goodId(good), good, selected);
    }}
    function addBuildingInputGoodNode(nodes, good) {{
      addGoodNodeWithId(nodes, buildingInputGoodId(good), good, false, "input_good");
    }}
    function addBuildingOutputGoodNode(nodes, good) {{
      addGoodNodeWithId(nodes, buildingOutputGoodId(good), good, false, "output_good");
    }}
    function addMethodNode(nodes, method, selectedBuilding = null) {{
      const id = methodId(method.name);
      if (nodes.has(id)) return;
      const style = provenanceStyle(method);
      nodes.set(id, {{
        data: {{
          id,
          label: methodLabel(method, selectedBuilding),
          kind: "production_method",
          production_method: method.name,
          building: method.building,
          input_cost: method.input_cost,
          output_value: method.output_value,
          profit: method.profit,
          profit_margin_percent: method.profit_margin_percent,
          missing_price_goods: method.missing_price_goods || [],
          source_layer: method.source_layer,
          source_mod: method.source_mod,
          source_mode: method.source_mode,
          source_history: method.source_history,
          unlock_age: method.unlock_age,
          general_unlock_age: method.general_unlock_age,
          specific_unlock_age: method.specific_unlock_age,
          availability_kind: method.availability_kind,
          is_specific_only: method.is_specific_only,
          building_unlock_age: method.building_unlock_age,
          building_general_unlock_age: method.building_general_unlock_age,
          building_specific_unlock_age: method.building_specific_unlock_age,
          building_availability_kind: method.building_availability_kind,
          building_is_specific_only: method.building_is_specific_only,
          effective_unlock_age: method.effective_unlock_age,
          effective_general_unlock_age: method.effective_general_unlock_age,
          effective_specific_unlock_age: method.effective_specific_unlock_age,
          effective_availability_kind: method.effective_availability_kind,
          effective_is_specific_only: method.effective_is_specific_only,
          provenance_state: method.provenance_state || "unknown",
          provenance_color: style.color,
          provenance_border_style: style.borderStyle
        }},
        classes: "production-method"
      }});
    }}
    function addEdge(edges, source, target, kind, amount, goods, extraData = {{}}) {{
      const id = `${{source}}->${{target}}:${{kind}}`;
      if (edges.has(id)) return;
      edges.set(id, {{
        data: {{
          id,
          source,
          target,
          label: amountLabel(amount),
          kind,
          amount,
          goods,
          goods_color: colorForGood(goods),
          edge_width: widthForAmount(amount),
          ...extraData
        }},
        classes: kind
      }});
    }}
    function visibleMethods(selectedAge, includeSpecific) {{
      return network.methods.filter(method => methodVisible(method, selectedAge, includeSpecific));
    }}
    function methodsForBuilding(building, methods) {{
      const references = new Set(building.production_methods || []);
      return methods.filter(
        method => method.building === building.name || references.has(method.name)
      );
    }}
    function indexMethods(methods) {{
      const producedBy = new Map();
      const consumedBy = new Map();
      for (const method of methods) {{
        if (method.produced) {{
          if (!producedBy.has(method.produced)) producedBy.set(method.produced, []);
          producedBy.get(method.produced).push(method);
        }}
        for (const input of method.input_goods || []) {{
          if (!consumedBy.has(input)) consumedBy.set(input, []);
          consumedBy.get(input).push(method);
        }}
      }}
      return {{ producedBy, consumedBy }};
    }}
    function addMethodGoods(nodes, edges, method, selectedBuilding = null) {{
      addMethodNode(nodes, method, selectedBuilding);
      const methodNode = nodes.get(methodId(method.name));
      if (methodNode) methodNode.data.ranked_role = "production_method";
      for (let index = 0; index < (method.input_goods || []).length; index += 1) {{
        const inputGood = method.input_goods[index];
        const amount = (method.input_amounts || [])[index];
        addBuildingInputGoodNode(nodes, inputGood);
        addEdge(
          edges,
          buildingInputGoodId(inputGood),
          methodId(method.name),
          "consumes",
          amount,
          inputGood,
          {{ ranked_edge_role: "input" }},
        );
      }}
      if (method.produced) {{
        addBuildingOutputGoodNode(nodes, method.produced);
        addEdge(
          edges,
          methodId(method.name),
          buildingOutputGoodId(method.produced),
          "produces",
          method.output,
          method.produced,
          {{ ranked_edge_role: "output" }},
        );
      }}
    }}
    function expandGoods(nodes, edges, startGoods, methods, depth, selectedGood = null) {{
      const {{ producedBy, consumedBy }} = indexMethods(methods);
      const queued = startGoods.map(good => [good, 0]);
      const expanded = new Set();
      while (queued.length) {{
        const [currentGood, distance] = queued.shift();
        const expansionKey = `${{currentGood}}:${{distance}}`;
        if (expanded.has(expansionKey) || distance >= depth) continue;
        expanded.add(expansionKey);
        addGoodNode(nodes, currentGood, currentGood === selectedGood);
        for (const method of producedBy.get(currentGood) || []) {{
          addMethodNode(nodes, method);
          addEdge(
            edges,
            methodId(method.name),
            goodId(currentGood),
            "produces",
            method.output,
            currentGood,
          );
          for (let index = 0; index < (method.input_goods || []).length; index += 1) {{
            const inputGood = method.input_goods[index];
            const amount = (method.input_amounts || [])[index];
            addGoodNode(nodes, inputGood);
            addEdge(edges, goodId(inputGood), methodId(method.name), "consumes", amount, inputGood);
            if (distance + 1 < depth) queued.push([inputGood, distance + 1]);
          }}
        }}
        for (const method of consumedBy.get(currentGood) || []) {{
          addMethodNode(nodes, method);
          const inputIndex = (method.input_goods || []).indexOf(currentGood);
          const amount = inputIndex < 0 ? null : (method.input_amounts || [])[inputIndex];
          addEdge(
            edges,
            goodId(currentGood),
            methodId(method.name),
            "consumes",
            amount,
            currentGood,
          );
          if (method.produced) {{
            addGoodNode(nodes, method.produced);
            addEdge(
              edges,
              methodId(method.name),
              goodId(method.produced),
              "produces",
              method.output,
              method.produced,
            );
            if (distance + 1 < depth) queued.push([method.produced, distance + 1]);
          }}
        }}
      }}
      return {{
        nodes: [...nodes.values()].map(cloneElement),
        edges: [...edges.values()].map(cloneElement)
      }};
    }}
    function buildGoodGraph(selectedGood, selectedAge, includeSpecific, depth) {{
      const methods = visibleMethods(selectedAge, includeSpecific);
      const nodes = new Map();
      const edges = new Map();
      expandGoods(nodes, edges, [selectedGood], methods, depth, selectedGood);
      return {{
        nodes: [...nodes.values()].map(cloneElement),
        edges: [...edges.values()].map(cloneElement)
      }};
    }}
    function buildBuildingGraph(selectedBuilding, selectedAge, includeSpecific, depth) {{
      const building = buildingsByName.get(selectedBuilding);
      const nodes = new Map();
      const edges = new Map();
      if (!building) {{
        return {{ nodes: [], edges: [] }};
      }}
      if (!buildingVisible(building, selectedAge, includeSpecific)) {{
        return {{ nodes: [], edges: [] }};
      }}
      const methods = visibleMethods(selectedAge, includeSpecific);
      const buildingMethods = methodsForBuilding(building, methods);
      const connectedGoods = new Set();
      for (const method of buildingMethods) {{
        addMethodGoods(nodes, edges, method, building.name);
        for (const inputGood of method.input_goods || []) connectedGoods.add(inputGood);
        if (method.produced) connectedGoods.add(method.produced);
      }}
      if (depth > 1 && connectedGoods.size) {{
        expandGoods(nodes, edges, [...connectedGoods], methods, depth - 1);
      }}
      return {{
        nodes: [...nodes.values()].map(cloneElement),
        edges: [...edges.values()].map(cloneElement)
      }};
    }}
    function buildLocalGraph(selection, selectedAge, includeSpecific, depth) {{
      if (selection.type === "building") {{
        return buildBuildingGraph(selection.name, selectedAge, includeSpecific, depth);
      }}
      return buildGoodGraph(selection.name, selectedAge, includeSpecific, depth);
    }}
    function buildModifierTimeline(selectedGood, includeSpecific) {{
      const nodes = new Map();
      const edges = new Map();
      const modifiers = network.output_modifiers.filter(modifier =>
        modifier.good === selectedGood
          && ageIndex[modifier.age] !== undefined
          && (includeSpecific || !modifier.has_potential)
      );
      const modifiersByAge = new Map(ageOrder.map(age => [age, []]));
      for (const modifier of modifiers) {{
        modifiersByAge.get(modifier.age).push(modifier);
      }}
      let cumulative = 0;
      for (const age of ageOrder) {{
        const ageModifiers = modifiersByAge.get(age).sort(
          (left, right) => left.advancement.localeCompare(right.advancement)
        );
        for (const modifier of ageModifiers) {{
          cumulative += Number(modifier.value) || 0;
        }}
        nodes.set(modifierAgeId(age), {{
          data: {{
            id: modifierAgeId(age),
            label: `${{age}}\\nTotal: ${{formatModifierValue(cumulative)}}`,
            kind: "modifier_age",
            age,
            cumulative_modifier: cumulative,
            provenance_state: "timeline",
            provenance_color: "#0284c7",
            provenance_border_style: "solid"
          }},
          classes: "age-node"
        }});
      }}
      modifiers.forEach((modifier, index) => {{
        const style = provenanceStyle(modifier);
        const advancementId = modifierAdvancementId(modifier, index);
        nodes.set(advancementId, {{
          data: {{
            id: advancementId,
            label: `${{modifier.advancement}}\\n${{formatModifierValue(modifier.value)}}`,
            kind: "advancement_modifier",
            good: modifier.good,
            age: modifier.age,
            modifier_key: modifier.modifier_key,
            modifier_value: modifier.value,
            has_potential: modifier.has_potential,
            source_layer: modifier.source_layer,
            source_mod: modifier.source_mod,
            source_mode: modifier.source_mode,
            source_history: modifier.source_history,
            provenance_state: modifier.provenance_state || "unknown",
            provenance_color: style.color,
            provenance_border_style: style.borderStyle
          }},
          classes: "advancement-node"
        }});
        const edgeId = `${{advancementId}}->${{modifierAgeId(modifier.age)}}`;
        edges.set(edgeId, {{
          data: {{
            id: edgeId,
            source: advancementId,
            target: modifierAgeId(modifier.age),
            label: formatModifierValue(modifier.value),
            kind: "modifier_contribution",
            amount: Math.abs(Number(modifier.value) || 0),
            goods: modifier.good,
            goods_color: style.color,
            edge_width: Math.max(2, Math.min(8, 2 + Math.abs(Number(modifier.value) || 0) * 12))
          }},
          classes: "modifier-edge"
        }});
      }});
      return {{
        nodes: [...nodes.values()].map(cloneElement),
        edges: [...edges.values()].map(cloneElement)
      }};
    }}
    function buildLegend(nodes) {{
      const provenanceRows = new Map();
      for (const node of nodes) {{
        const style = provenanceStyle(node.data);
        const row = provenanceRows.get(style.key) || {{...style, count: 0}};
        row.count += 1;
        provenanceRows.set(style.key, row);
      }}
      renderProvenanceLegend(provenanceRows);
    }}
    function renderProvenanceLegend(provenanceRows) {{
      const container = document.getElementById("provenanceLegend");
      container.replaceChildren();
      const rowOrder = row => {{
        if (row.key === "vanilla_exact") return 0;
        if (row.key.startsWith("mod_exact:")) return 1;
        if (row.key === "merged") return 2;
        return 3;
      }};
      for (const style of [...provenanceRows.values()].sort(
        (left, right) => rowOrder(left) - rowOrder(right) || left.label.localeCompare(right.label)
      )) {{
        const item = document.createElement("div");
        item.className = "legend-row";
        const swatch = document.createElement("span");
        swatch.className = "legend-swatch";
        swatch.style.backgroundColor = "#ffffff";
        swatch.style.borderColor = style.color;
        swatch.style.borderStyle = style.borderStyle;
        const label = document.createElement("span");
        label.className = "legend-label";
        label.textContent = style.label;
        label.title = style.label;
        const count = document.createElement("span");
        count.className = "legend-count";
        count.textContent = style.count;
        item.append(swatch, label, count);
        container.append(item);
      }}
    }}
    const spreadLayout = {{
      name: "fcose",
      quality: "proof",
      randomize: true,
      animate: false,
      fit: true,
      padding: 100,
      nodeDimensionsIncludeLabels: true,
      nodeSeparation: 120,
      idealEdgeLength: edge => edge.data("kind") === "produces" ? 220 : 260,
      nodeRepulsion: 18000,
      gravity: 0.08,
      gravityRangeCompound: 1.5,
      gravityCompound: 0.2,
      nestingFactor: 0.1,
      numIter: 8000,
      tile: true,
      tilingPaddingVertical: 40,
      tilingPaddingHorizontal: 40
    }};
    const rankedLayout = {{
      name: "dagre",
      rankDir: "LR",
      ranker: "network-simplex",
      nodeSep: 130,
      edgeSep: 48,
      rankSep: 260,
      fit: true,
      padding: 80,
      animate: false
    }};
    const buildingRankedColumnX = {{
      input_good: -360,
      production_method: 0,
      output_good: 360
    }};
    function graphSpacingFactor() {{
      const nodeCount = Math.max(1, cy.nodes().length);
      const edgeCount = cy.edges().length;
      const density = edgeCount / nodeCount;
      return Math.min(1.9, Math.max(1, 0.85 + density * 0.28));
    }}
    function scaledLayoutValue(value) {{
      return Math.round(value * graphSpacingFactor());
    }}
    function currentSelectionType() {{
      return document.getElementById("entityType").value;
    }}
    function rankedRole(node) {{
      return node.data("ranked_role") || node.data("kind") || "unknown";
    }}
    function buildingRankedPositions() {{
      const columns = {{
        input_good: [],
        production_method: [],
        output_good: [],
        unknown: []
      }};
      cy.nodes().forEach(node => {{
        const role = rankedRole(node);
        const column = columns[role] ? role : "unknown";
        columns[column].push(node);
      }});
      const positions = {{}};
      for (const [role, nodes] of Object.entries(columns)) {{
        nodes.sort((left, right) => {{
          const leftLabel = left.data("label") || left.id();
          const rightLabel = right.data("label") || right.id();
          return leftLabel.localeCompare(rightLabel);
        }});
        const spacing = scaledLayoutValue(role === "production_method" ? 170 : 120);
        const startY = -((nodes.length - 1) * spacing) / 2;
        const x = scaledLayoutValue(
          buildingRankedColumnX[role] ?? buildingRankedColumnX.production_method
        );
        nodes.forEach((node, index) => {{
          positions[node.id()] = {{ x, y: startY + index * spacing }};
        }});
      }}
      return positions;
    }}
    function edgeSortLabel(edge) {{
      const source = edge.source().data("label") || edge.source().id();
      const target = edge.target().data("label") || edge.target().id();
      return `${{source}} -> ${{target}} -> ${{edge.id()}}`;
    }}
    function anchorOffset(index, count) {{
      return Math.round((index - (count - 1) / 2) * 18);
    }}
    function assignBuildingRankedEdgeAnchors() {{
      const inputEdgesByMethod = new Map();
      const outputEdgesByMethod = new Map();
      cy.edges().forEach(edge => {{
        const role = edge.data("ranked_edge_role");
        edge.data("source_endpoint", "outside-to-node");
        edge.data("target_endpoint", "outside-to-node");
        if (role === "input") {{
          const methodId = edge.target().id();
          if (!inputEdgesByMethod.has(methodId)) inputEdgesByMethod.set(methodId, []);
          inputEdgesByMethod.get(methodId).push(edge);
        }} else if (role === "output") {{
          const methodId = edge.source().id();
          if (!outputEdgesByMethod.has(methodId)) outputEdgesByMethod.set(methodId, []);
          outputEdgesByMethod.get(methodId).push(edge);
        }}
      }});
      const assignAnchors = (edgeGroups, role) => {{
        for (const edges of edgeGroups.values()) {{
          edges.sort((left, right) => edgeSortLabel(left).localeCompare(edgeSortLabel(right)));
          edges.forEach((edge, index) => {{
            const offset = `${{anchorOffset(index, edges.length)}}px`;
            if (role === "input") {{
              edge.data("source_endpoint", `50% ${{offset}}`);
              edge.data("target_endpoint", `-50% ${{offset}}`);
            }} else {{
              edge.data("source_endpoint", `50% ${{offset}}`);
              edge.data("target_endpoint", `-50% ${{offset}}`);
            }}
          }});
        }}
      }};
      assignAnchors(inputEdgesByMethod, "input");
      assignAnchors(outputEdgesByMethod, "output");
    }}
    function modifierTimelinePositions() {{
      const positions = {{}};
      const ageSpacing = 280;
      const startX = -((ageOrder.length - 1) * ageSpacing) / 2;
      for (const [index, age] of ageOrder.entries()) {{
        positions[modifierAgeId(age)] = {{ x: startX + index * ageSpacing, y: 120 }};
      }}
      const advancementGroups = new Map(ageOrder.map(age => [age, []]));
      cy.nodes(".advancement-node").forEach(node => {{
        const age = node.data("age");
        if (advancementGroups.has(age)) advancementGroups.get(age).push(node);
      }});
      for (const [age, nodes] of advancementGroups.entries()) {{
        nodes.sort((left, right) => {{
          const leftLabel = left.data("label") || left.id();
          const rightLabel = right.data("label") || right.id();
          return leftLabel.localeCompare(rightLabel);
        }});
        const ageX = positions[modifierAgeId(age)].x;
        nodes.forEach((node, index) => {{
          const side = index % 2 === 0 ? -1 : 1;
          const lane = Math.floor(index / 2);
          positions[node.id()] = {{
            x: ageX + (index - (nodes.length - 1) / 2) * 72,
            y: 120 + side * (150 + lane * 94)
          }};
        }});
      }}
      return positions;
    }}
    function runModifierTimelineLayout() {{
      cy.edges().removeClass("building-ranked-edge");
      const positions = modifierTimelinePositions();
      cy.layout({{
        name: "preset",
        fit: true,
        padding: 90,
        animate: false,
        positions: node => positions[node.id()] || {{ x: 0, y: 0 }}
      }}).run();
    }}
    function runBuildingRankedLayout() {{
      const positions = buildingRankedPositions();
      assignBuildingRankedEdgeAnchors();
      cy.edges().addClass("building-ranked-edge");
      cy.layout({{
        name: "preset",
        fit: true,
        padding: 100,
        animate: false,
        positions: node => positions[node.id()] || {{ x: 0, y: 0 }}
      }}).run();
    }}
    const cy = cytoscape({{
      container: document.getElementById("cy"),
      elements: [],
      layout: spreadLayout,
      minZoom: {DEFAULT_MIN_ZOOM},
      maxZoom: {DEFAULT_MAX_ZOOM},
      wheelSensitivity: 0.08,
      style: {json.dumps(_CYTOSCAPE_STYLE, ensure_ascii=False)}
    }});
    window.cy = cy;
    function updateMeta(elements) {{
      document.getElementById("graphMeta").textContent =
        `${{elements.nodes.length}} nodes \\u00b7 ${{elements.edges.length}} edges`;
    }}
    function clearFocus() {{
      cy.elements()
        .removeClass("dimmed")
        .removeClass("focused")
        .removeClass("focus-neighbor");
    }}
    function focusElement(element) {{
      clearFocus();
      let visible;
      if (element.isNode()) {{
        const connectedEdges = element.connectedEdges();
        visible = element.union(connectedEdges).union(connectedEdges.connectedNodes());
      }} else {{
        visible = element.union(element.connectedNodes());
      }}
      cy.elements().addClass("dimmed");
      visible.removeClass("dimmed").addClass("focus-neighbor");
      element.removeClass("focus-neighbor").addClass("focused");
    }}
    function defaultModifierGood() {{
      if (initialSelection.type === "good" && goodsByName.has(initialSelection.name)) {{
        return initialSelection.name;
      }}
      if (goodsByName.has("wheat")) return "wheat";
      return network.goods[0] ? network.goods[0].name : "";
    }}
    function validModifierGood(value) {{
      return goodsByName.has(value) ? value : defaultModifierGood();
    }}
    function applyExplorerGraph() {{
      if (currentExplorerView === "overview") {{
        cy.elements().remove();
        clearFocus();
        renderGoodsOverview();
        buildLegend([]);
        return;
      }}
      const includeSpecific = document.getElementById("specificUnlocks").checked;
      let elements;
      if (currentExplorerView === "modifiers") {{
        const modifierInput = document.getElementById("modifierGoodSelect");
        const selectedGood = validModifierGood(modifierInput.value);
        modifierInput.value = selectedGood;
        elements = buildModifierTimeline(selectedGood, includeSpecific);
      }} else {{
        const selectedType = document.getElementById("entityType").value;
        const selectedInput = document.getElementById("entitySelect");
        const selection = resolveSelection(selectedType, selectedInput.value);
        selectedInput.value = selectionLabel(selection);
        document.getElementById("entityType").value = selection.type;
        updateEntityOptions(selection.type);
        const selectedAge = document.getElementById("ageFilter").value;
        const depth = Math.max(1, Number(document.getElementById("depthInput").value || 1));
        elements = buildLocalGraph(
          selection,
          selectedAge,
          includeSpecific,
          depth,
        );
      }}
      cy.elements().remove();
      cy.add([...elements.nodes, ...elements.edges]);
      clearFocus();
      updateMeta(elements);
      buildLegend(elements.nodes);
      runCurrentLayout();
    }}
    function runCurrentLayout() {{
      if (currentExplorerView === "overview") return;
      if (currentExplorerView === "modifiers") runModifierTimelineLayout();
      else if (currentLayout === "ranked") runRankedLayout();
      else runSpreadLayout();
    }}
    function runSpreadLayout() {{
      currentLayout = "spread";
      cy.edges().removeClass("building-ranked-edge");
      cy.layout({{
        ...spreadLayout,
        nodeSeparation: scaledLayoutValue(120),
        idealEdgeLength: edge => scaledLayoutValue(edge.data("kind") === "produces" ? 220 : 260),
        nodeRepulsion: Math.round(18000 * graphSpacingFactor())
      }}).run();
    }}
    function runRankedLayout() {{
      currentLayout = "ranked";
      if (currentSelectionType() === "building") {{
        runBuildingRankedLayout();
        return;
      }}
      cy.edges().removeClass("building-ranked-edge");
      cy.layout({{
        ...rankedLayout,
        nodeSep: scaledLayoutValue(130),
        edgeSep: scaledLayoutValue(48),
        rankSep: scaledLayoutValue(260)
      }}).run();
    }}
    function updateExplorerControls() {{
      const overviewActive = currentExplorerView === "overview";
      const flowActive = currentExplorerView === "flow";
      document.getElementById("overviewTab").classList.toggle("active", overviewActive);
      document.getElementById("flowTab").classList.toggle("active", flowActive);
      document.getElementById("modifierTab").classList.toggle(
        "active",
        currentExplorerView === "modifiers"
      );
      document.getElementById("cy").style.display = overviewActive ? "none" : "";
      document.getElementById("goodsOverview").style.display = overviewActive ? "block" : "none";
      document.querySelector(".legend").style.display = overviewActive ? "none" : "";
      for (const element of document.querySelectorAll(".flow-control")) {{
        element.style.display = flowActive ? "" : "none";
      }}
      for (const element of document.querySelectorAll(".modifier-control")) {{
        element.style.display = currentExplorerView === "modifiers" ? "" : "none";
      }}
      for (const element of document.querySelectorAll(".shared-graph-control")) {{
        element.style.display = overviewActive ? "none" : "";
      }}
    }}
    function setExplorerView(view) {{
      currentExplorerView = view;
      updateExplorerControls();
      applyExplorerGraph();
    }}
    window.runSpreadLayout = runSpreadLayout;
    window.runRankedLayout = runRankedLayout;
    window.applyExplorerGraph = applyExplorerGraph;
    window.setExplorerView = setExplorerView;
    cy.on("tap", "node", event => focusElement(event.target));
    cy.on("tap", "edge", event => focusElement(event.target));
    cy.on("tap", event => {{
      if (event.target === cy) clearFocus();
    }});
    cy.ready(() => {{
      document.getElementById("entityType").value = initialSelection.type;
      updateEntityOptions(initialSelection.type);
      document.getElementById("entitySelect").value = selectionLabel(initialSelection);
      document.getElementById("modifierGoodSelect").value = defaultModifierGood();
      document.getElementById("ageFilter").value = initialAge || "";
      document.getElementById("depthInput").value = initialDepth;
      document.getElementById("specificUnlocks").checked = initialSpecificUnlocks;
      const controlIds = [
        "entitySelect",
        "modifierGoodSelect",
        "ageFilter",
        "depthInput",
        "specificUnlocks"
      ];
      for (const id of controlIds) {{
        document.getElementById(id).addEventListener("change", applyExplorerGraph);
      }}
      document.getElementById("entityType").addEventListener("change", event => {{
        updateEntityOptions(event.target.value);
        const firstItem = event.target.value === "building"
          ? network.buildings[0]
          : network.goods[0];
        document.getElementById("entitySelect").value = firstItem ? firstItem.name : "";
        applyExplorerGraph();
      }});
      document.getElementById("overviewTab").addEventListener(
        "click",
        () => setExplorerView("overview")
      );
      document.getElementById("flowTab").addEventListener("click", () => setExplorerView("flow"));
      document.getElementById("modifierTab").addEventListener(
        "click",
        () => setExplorerView("modifiers")
      );
      updateExplorerControls();
      applyExplorerGraph();
      cy.fit(undefined, 80);
    }});
  </script>
</body>
</html>
"""


def _standalone_html(
    good: str,
    graph: dict[str, list[dict[str, Any]]],
    *,
    selected_age: str | None = None,
    include_specific_unlocks: bool = False,
) -> str:
    title = f"EU5 Goods Flow: {good}"
    meta_text = f'{len(graph["nodes"])} nodes &middot; {len(graph["edges"])} edges'
    age_options = "\n".join(
        ["""          <option value="">All ages</option>"""]
        + [
            f"""          <option value="{html.escape(age)}">{html.escape(age)}</option>"""
            for age in AGE_ORDER
        ]
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <script src="https://unpkg.com/cytoscape@3.30.4/dist/cytoscape.min.js"></script>
  <script src="https://unpkg.com/layout-base@2.0.1/layout-base.js"></script>
  <script src="https://unpkg.com/cose-base@2.2.0/cose-base.js"></script>
  <script src="https://unpkg.com/cytoscape-fcose@2.2.0/cytoscape-fcose.js"></script>
  <script src="https://unpkg.com/dagre@0.8.5/dist/dagre.min.js"></script>
  <script src="https://unpkg.com/cytoscape-dagre@2.5.0/cytoscape-dagre.js"></script>
  <style>
    * {{ box-sizing: border-box; }}
    html, body {{ height: 100%; margin: 0; }}
    body {{
      font-family: Inter, Segoe UI, system-ui, sans-serif;
      background: #f8fafc;
      color: #172033;
      overflow: hidden;
    }}
    .shell {{
      display: grid;
      grid-template-rows: auto 1fr;
      height: 100vh;
      width: 100vw;
    }}
    header {{
      align-items: center;
      background: #ffffff;
      border-bottom: 1px solid #dbe4ef;
      display: flex;
      gap: 16px;
      min-height: 58px;
      padding: 10px 18px;
    }}
    h1 {{
      font-size: 16px;
      font-weight: 700;
      line-height: 1.2;
      margin: 0;
    }}
    .meta {{
      color: #64748b;
      font-size: 13px;
    }}
    .spacer {{ flex: 1; }}
    .controls {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      justify-content: flex-end;
    }}
    button {{
      background: #ffffff;
      border: 1px solid #cbd5e1;
      border-radius: 6px;
      color: #172033;
      cursor: pointer;
      font: inherit;
      font-size: 13px;
      padding: 7px 10px;
    }}
    button:hover {{ background: #f1f5f9; }}
    select {{
      background: #ffffff;
      border: 1px solid #cbd5e1;
      border-radius: 6px;
      color: #172033;
      font: inherit;
      font-size: 13px;
      min-height: 32px;
      padding: 6px 8px;
    }}
    .toggle {{
      align-items: center;
      border: 1px solid #cbd5e1;
      border-radius: 6px;
      display: inline-flex;
      gap: 6px;
      min-height: 32px;
      padding: 6px 8px;
      white-space: nowrap;
    }}
    .toggle input {{
      margin: 0;
    }}
    #cy {{
      height: 100%;
      width: 100%;
    }}
    .legend {{
      background: rgba(255, 255, 255, 0.96);
      border: 1px solid #cbd5e1;
      border-radius: 8px;
      box-shadow: 0 8px 24px rgba(15, 23, 42, 0.12);
      max-height: calc(100vh - 92px);
      max-width: 320px;
      overflow: auto;
      position: fixed;
      right: 16px;
      top: 74px;
      z-index: 4;
    }}
    .legend summary {{
      cursor: pointer;
      font-size: 13px;
      font-weight: 700;
      padding: 10px 12px;
      user-select: none;
    }}
    .legend-body {{
      border-top: 1px solid #e2e8f0;
      display: grid;
      gap: 12px;
      padding: 10px 12px 12px;
    }}
    .legend-section-title {{
      color: #475569;
      font-size: 11px;
      font-weight: 700;
      margin: 0 0 6px;
      text-transform: uppercase;
    }}
    .legend-list {{
      display: grid;
      gap: 5px;
    }}
    .legend-row {{
      align-items: center;
      display: grid;
      gap: 7px;
      grid-template-columns: 14px minmax(0, 1fr) auto;
      min-width: 0;
    }}
    .legend-swatch {{
      border: 2px solid #64748b;
      border-radius: 4px;
      height: 14px;
      width: 14px;
    }}
    .legend-label {{
      font-size: 12px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }}
    .legend-count {{
      color: #64748b;
      font-size: 11px;
    }}
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <h1>{html.escape(good)}</h1>
      <div class="meta" id="graphMeta">{meta_text}</div>
      <div class="spacer"></div>
      <div class="controls">
        <select id="ageFilter" aria-label="Maximum age">
{age_options}
        </select>
        <label class="toggle" title="Include country, region, and religion-specific unlocks">
          <input id="specificUnlocks" type="checkbox">
          Specific unlocks
        </label>
        <button type="button" onclick="runSpreadLayout()">Spread</button>
        <button type="button" onclick="runRankedLayout()">Ranked</button>
        <button type="button" onclick="runColumnLayout()">Columns</button>
        <button type="button" onclick="cy.fit(undefined, 80)">Fit</button>
        <button type="button" onclick="cy.zoom(cy.zoom() * 1.2)">Zoom In</button>
        <button type="button" onclick="cy.zoom(cy.zoom() / 1.2)">Zoom Out</button>
      </div>
    </header>
    <div id="cy"></div>
    <details class="legend" open>
      <summary>Legend</summary>
      <div class="legend-body">
        <section>
          <p class="legend-section-title">Provenance</p>
          <div class="legend-list" id="provenanceLegend"></div>
        </section>
      </div>
    </details>
  </div>
  <script>
    const graph = {json.dumps(graph, ensure_ascii=False)};
    const ageOrder = {json.dumps(list(AGE_ORDER), ensure_ascii=False)};
    const ageIndex = Object.fromEntries(ageOrder.map((age, index) => [age, index]));
    const initialAge = {json.dumps(selected_age)};
    const initialSpecificUnlocks = {json.dumps(include_specific_unlocks)};
    const rootGoodId = {json.dumps(_good_id(good))};
    let currentLayout = "spread";
    const cloneElement = element => JSON.parse(JSON.stringify(element));
    const provenanceStyles = {{
      vanilla_exact: {{
        label: "Exact vanilla value",
        color: "#334155",
        borderStyle: "solid"
      }},
      mod_exact: {{
        borderStyle: "solid"
      }},
      merged: {{
        label: "Merged by load order",
        color: "#f59e0b",
        borderStyle: "dashed"
      }},
      unknown: {{
        label: "Unknown source",
        color: "#94a3b8",
        borderStyle: "dotted"
      }}
    }};
    function colorForGood(good) {{
      if (!good) return "#94a3b8";
      const palette = [
        "#2563eb", "#dc2626", "#16a34a", "#ca8a04", "#9333ea", "#0891b2",
        "#ea580c", "#4f46e5", "#be123c", "#0f766e", "#7c3aed", "#65a30d",
        "#c2410c", "#0284c7", "#a21caf", "#15803d", "#b45309", "#1d4ed8"
      ];
      let hash = 0;
      for (let index = 0; index < good.length; index += 1) {{
        hash = ((hash << 5) - hash + good.charCodeAt(index)) | 0;
      }}
      return palette[Math.abs(hash) % palette.length];
    }}
    function sourceKey(data) {{
      return data.source_mod || data.source_layer || "unknown";
    }}
    function sourceLabel(source) {{
      if (!source || source === "unknown") return "Unknown source";
      return source === "vanilla" ? "Vanilla" : source;
    }}
    function colorForSource(source) {{
      if (!source || source === "unknown") return "#94a3b8";
      if (source === "vanilla") return "#334155";
      const palette = [
        "#be123c", "#7c2d12", "#166534", "#0e7490", "#4338ca", "#86198f",
        "#a16207", "#047857", "#1d4ed8", "#c2410c", "#0f766e", "#6d28d9"
      ];
      let hash = 0;
      for (let index = 0; index < source.length; index += 1) {{
        hash = ((hash << 5) - hash + source.charCodeAt(index)) | 0;
      }}
      return palette[Math.abs(hash) % palette.length];
    }}
    function provenanceStyle(data) {{
      const provenance = data.provenance_state || "unknown";
      const source = sourceKey(data);
      if (provenance === "mod_exact") {{
        return {{
          key: `mod_exact:${{source}}`,
          label: `${{sourceLabel(source)}} value`,
          color: colorForSource(source),
          borderStyle: "solid"
        }};
      }}
      const style = provenanceStyles[provenance] || provenanceStyles.unknown;
      return {{
        key: provenance,
        label: style.label,
        color: style.color,
        borderStyle: style.borderStyle
      }};
    }}
    for (const node of graph.nodes) {{
      const style = provenanceStyle(node.data);
      node.data.provenance_color = style.color;
      node.data.provenance_border_style = style.borderStyle;
      if (node.data.kind === "good") {{
        node.data.goods_color = colorForGood(node.data.label);
      }}
    }}
    for (const edge of graph.edges) {{
      edge.data.goods_color = colorForGood(edge.data.goods);
      edge.data.edge_width = widthForAmount(edge.data.amount);
    }}
    function widthForAmount(amount) {{
      const numeric = Number(amount);
      if (!Number.isFinite(numeric) || numeric <= 0) return 2;
      return Math.max(2, Math.min(9, 1.6 + Math.sqrt(numeric) * 1.8));
    }}
    function buildLegend(nodes = graph.nodes) {{
      const provenanceRows = new Map();
      for (const node of nodes) {{
        const style = provenanceStyle(node.data);
        const row = provenanceRows.get(style.key) || {{...style, count: 0}};
        row.count += 1;
        provenanceRows.set(style.key, row);
      }}
      renderProvenanceLegend(provenanceRows);
    }}
    function renderProvenanceLegend(provenanceRows) {{
      const container = document.getElementById("provenanceLegend");
      container.replaceChildren();
      const rowOrder = row => {{
        if (row.key === "vanilla_exact") return 0;
        if (row.key.startsWith("mod_exact:")) return 1;
        if (row.key === "merged") return 2;
        return 3;
      }};
      for (const style of [...provenanceRows.values()].sort(
        (left, right) => rowOrder(left) - rowOrder(right) || left.label.localeCompare(right.label)
      )) {{
        if (style.key === "unknown" && style.count === 0) continue;
          const item = document.createElement("div");
          item.className = "legend-row";
          const swatch = document.createElement("span");
          swatch.className = "legend-swatch";
          swatch.style.backgroundColor = "#ffffff";
          swatch.style.borderColor = style.color;
          swatch.style.borderStyle = style.borderStyle;
          const label = document.createElement("span");
          label.className = "legend-label";
          label.textContent = style.label;
          label.title = style.label;
          const count = document.createElement("span");
          count.className = "legend-count";
          count.textContent = style.count;
          item.append(swatch, label, count);
          container.append(item);
      }}
    }}
    const spreadLayout = {{
      name: "fcose",
      quality: "proof",
      randomize: true,
      animate: false,
      fit: true,
      padding: 100,
      nodeDimensionsIncludeLabels: true,
      nodeSeparation: 120,
      idealEdgeLength: edge => edge.data("kind") === "produces" ? 220 : 260,
      nodeRepulsion: 18000,
      gravity: 0.08,
      gravityRangeCompound: 1.5,
      gravityCompound: 0.2,
      nestingFactor: 0.1,
      numIter: 8000,
      tile: true,
      tilingPaddingVertical: 40,
      tilingPaddingHorizontal: 40
    }};
    const rankedLayout = {{
      name: "dagre",
      rankDir: "LR",
      ranker: "network-simplex",
      nodeSep: 130,
      edgeSep: 48,
      rankSep: 260,
      fit: true,
      padding: 80,
      animate: false
    }};
    const columnLayout = {{ name: "preset", fit: true, padding: 80 }};
    const cy = cytoscape({{
      container: document.getElementById("cy"),
      elements: [],
      layout: spreadLayout,
      minZoom: {DEFAULT_MIN_ZOOM},
      maxZoom: {DEFAULT_MAX_ZOOM},
      wheelSensitivity: 0.08,
      style: {json.dumps(_CYTOSCAPE_STYLE, ensure_ascii=False)}
    }});
    window.cy = cy;
    function ageAllowed(unlockAge, selectedAge) {{
      if (!selectedAge || !unlockAge) return true;
      return ageIndex[unlockAge] <= ageIndex[selectedAge];
    }}
    function minAge(left, right) {{
      if (!left) return right || null;
      if (!right) return left;
      return ageIndex[left] <= ageIndex[right] ? left : right;
    }}
    function latestAge(left, right) {{
      if (!left) return right || null;
      if (!right) return left;
      return ageIndex[left] >= ageIndex[right] ? left : right;
    }}
    function selectedUnlockAge(generalAge, specificAge, fallbackAge, includeSpecific) {{
      if (includeSpecific) return minAge(generalAge || fallbackAge, specificAge);
      return generalAge || fallbackAge || null;
    }}
    function methodVisible(data, selectedAge, includeSpecific) {{
      if (!selectedAge) return true;
      const kind = data.effective_availability_kind
        || data.availability_kind
        || "available_by_default";
      if (kind === "available_by_default") return true;
      if (kind === "specific_only" && !includeSpecific) return false;
      const methodAge = selectedUnlockAge(
        data.general_unlock_age,
        data.specific_unlock_age,
        data.unlock_age,
        includeSpecific
      );
      const buildingAge = selectedUnlockAge(
        data.building_general_unlock_age,
        data.building_specific_unlock_age,
        data.building_unlock_age,
        includeSpecific
      );
      const unlockAge = latestAge(methodAge, buildingAge);
      return ageAllowed(unlockAge, selectedAge);
    }}
    function filteredGraph(selectedAge, includeSpecific) {{
      if (!selectedAge) {{
        return {{
          nodes: graph.nodes.map(cloneElement),
          edges: graph.edges.map(cloneElement)
        }};
      }}
      const visibleMethodIds = new Set();
      for (const node of graph.nodes) {{
        const visibleMethod = node.data.kind === "production_method"
          && methodVisible(node.data, selectedAge, includeSpecific);
        if (visibleMethod) {{
          visibleMethodIds.add(node.data.id);
        }}
      }}
      const candidateEdges = graph.edges.filter(edge => {{
        const sourceIsMethod = edge.data.source.startsWith("production_method:");
        const targetIsMethod = edge.data.target.startsWith("production_method:");
        return (!sourceIsMethod || visibleMethodIds.has(edge.data.source))
          && (!targetIsMethod || visibleMethodIds.has(edge.data.target));
      }});
      const connectedNodeIds = new Set([rootGoodId]);
      for (const edge of candidateEdges) {{
        connectedNodeIds.add(edge.data.source);
        connectedNodeIds.add(edge.data.target);
      }}
      const visibleNodes = graph.nodes.filter(node => {{
        if (node.data.kind === "production_method") return visibleMethodIds.has(node.data.id);
        return connectedNodeIds.has(node.data.id);
      }});
      const visibleNodeIds = new Set(visibleNodes.map(node => node.data.id));
      const visibleEdges = candidateEdges.filter(
        edge => visibleNodeIds.has(edge.data.source) && visibleNodeIds.has(edge.data.target)
      );
      return {{
        nodes: visibleNodes.map(cloneElement),
        edges: visibleEdges.map(cloneElement)
      }};
    }}
    function updateMeta(elements) {{
      document.getElementById("graphMeta").textContent =
        `${{elements.nodes.length}} nodes \\u00b7 ${{elements.edges.length}} edges`;
    }}
    function applyAgeFilter() {{
      const selectedAge = document.getElementById("ageFilter").value;
      const includeSpecific = document.getElementById("specificUnlocks").checked;
      const elements = filteredGraph(selectedAge, includeSpecific);
      cy.elements().remove();
      cy.add([...elements.nodes, ...elements.edges]);
      updateMeta(elements);
      buildLegend(elements.nodes);
      runCurrentLayout();
    }}
    function runCurrentLayout() {{
      if (currentLayout === "ranked") runRankedLayout();
      else if (currentLayout === "column") runColumnLayout();
      else runSpreadLayout();
    }}
    function runSpreadLayout() {{
      currentLayout = "spread";
      cy.layout(spreadLayout).run();
    }}
    function runRankedLayout() {{
      currentLayout = "ranked";
      cy.layout(rankedLayout).run();
    }}
    function runColumnLayout() {{
      currentLayout = "column";
      cy.layout(columnLayout).run();
    }}
    window.runSpreadLayout = runSpreadLayout;
    window.runRankedLayout = runRankedLayout;
    window.runColumnLayout = runColumnLayout;
    window.applyAgeFilter = applyAgeFilter;
    cy.ready(() => {{
      document.getElementById("ageFilter").value = initialAge || "";
      document.getElementById("specificUnlocks").checked = initialSpecificUnlocks;
      document.getElementById("ageFilter").addEventListener("change", applyAgeFilter);
      document.getElementById("specificUnlocks").addEventListener("change", applyAgeFilter);
      applyAgeFilter();
      cy.fit(undefined, 80);
    }});
  </script>
</body>
</html>
"""
