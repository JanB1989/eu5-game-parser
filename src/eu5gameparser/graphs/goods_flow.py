from __future__ import annotations

import html
import json
import webbrowser
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from eu5gameparser.config import ParserConfig
from eu5gameparser.domain.buildings import BuildingData, load_building_data
from eu5gameparser.domain.goods import GoodsData
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
    )
    output_path.write_text(_standalone_html(good, graph), encoding="utf-8")
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
) -> dict[str, list[dict[str, Any]]]:
    if depth < 1:
        raise ValueError("depth must be at least 1")

    if eu5_data is not None:
        data = data or eu5_data.building_data
        goods_data = goods_data or eu5_data.goods_data

    data = data or load_building_data(config, profile=profile, load_order_path=load_order_path)
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
            )
        )
    return methods


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
]


def _standalone_html(good: str, graph: dict[str, list[dict[str, Any]]]) -> str:
    title = f"EU5 Goods Flow: {good}"
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
      <div class="meta">{len(graph["nodes"])} nodes &middot; {len(graph["edges"])} edges</div>
      <div class="spacer"></div>
      <div class="controls">
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
    function buildLegend() {{
      const provenanceRows = new Map();
      for (const node of graph.nodes) {{
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
    buildLegend();
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
      elements: [...graph.nodes, ...graph.edges],
      layout: spreadLayout,
      minZoom: {DEFAULT_MIN_ZOOM},
      maxZoom: {DEFAULT_MAX_ZOOM},
      wheelSensitivity: 0.08,
      style: {json.dumps(_CYTOSCAPE_STYLE, ensure_ascii=False)}
    }});
    window.cy = cy;
    function runSpreadLayout() {{
      cy.layout(spreadLayout).run();
    }}
    function runRankedLayout() {{
      cy.layout(rankedLayout).run();
    }}
    function runColumnLayout() {{
      cy.layout(columnLayout).run();
    }}
    window.runSpreadLayout = runSpreadLayout;
    window.runRankedLayout = runRankedLayout;
    window.runColumnLayout = runColumnLayout;
    cy.ready(() => {{
      runSpreadLayout();
      cy.fit(undefined, 80);
    }});
  </script>
</body>
</html>
"""
