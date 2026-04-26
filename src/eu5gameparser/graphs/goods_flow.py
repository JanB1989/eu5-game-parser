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


def show_good_flow(
    good: str,
    *,
    depth: int = 1,
    config: ParserConfig | None = None,
    profile: str | None = None,
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
    data: BuildingData | None = None,
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
) -> dict[str, list[dict[str, Any]]]:
    if depth < 1:
        raise ValueError("depth must be at least 1")

    data = data or load_building_data(config, profile=profile, load_order_path=load_order_path)
    methods = _methods_from_data(data)
    produced_by, consumed_by = _index_methods(methods)
    if good not in produced_by and good not in consumed_by:
        raise ValueError(f"Good {good!r} is not used by any parsed production method.")

    nodes: dict[str, dict[str, Any]] = {}
    edges: dict[str, dict[str, Any]] = {}
    layout_hints: dict[str, list[tuple[int, float]]] = {}
    queued: deque[tuple[str, int, int]] = deque([(good, 0, 0)])
    expanded: set[tuple[str, int]] = set()

    _add_good_node(nodes, good, level=0, selected=True)
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
            )
            input_start = method_y - ((len(method.input_goods) - 1) * NODE_Y_SPACING) / 2
            for input_index, (input_good, amount) in enumerate(
                zip(method.input_goods, method.input_amounts, strict=False)
            ):
                input_level = method_level - 1
                input_y = input_start + input_index * NODE_Y_SPACING
                _add_good_node(nodes, input_good, level=input_level)
                _add_layout_hint(layout_hints, _good_id(input_good), input_level, input_y)
                _add_edge(
                    edges,
                    source=_good_id(input_good),
                    target=_method_id(method.name),
                    kind="consumes",
                    label=_amount_label(amount),
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
            )
            if method.produced:
                output_level = method_level + 1
                output_y = method_y
                _add_good_node(nodes, method.produced, level=output_level)
                _add_layout_hint(layout_hints, _good_id(method.produced), output_level, output_y)
                _add_edge(
                    edges,
                    source=_method_id(method.name),
                    target=_good_id(method.produced),
                    kind="produces",
                    label=_amount_label(method.output),
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
            )
        )
    return methods


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


def _add_good_node(
    nodes: dict[str, dict[str, Any]], good: str, *, level: int, selected: bool = False
) -> None:
    node_id = _good_id(good)
    classes = "good selected" if selected else "good"
    node = nodes.setdefault(
        node_id,
        {"data": {"id": node_id, "label": good, "kind": "good"}, "classes": classes},
    )
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
    edges: dict[str, dict[str, Any]], *, source: str, target: str, kind: str, label: str
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
            "border-width": 1,
            "border-color": "#ccd5df",
            "background-color": "#f8fafc",
            "color": "#172033",
            "shape": "round-rectangle",
        },
    },
    {
        "selector": ".selected",
        "style": {
            "background-color": "#2563eb",
            "border-color": "#1d4ed8",
            "border-width": 2,
            "color": "#ffffff",
            "font-weight": "700",
        },
    },
    {
        "selector": ".production-method",
        "style": {
            "background-color": "#ecfdf5",
            "border-color": "#10b981",
            "padding": "16px",
            "shape": "round-diamond",
            "text-max-width": "156px",
        },
    },
    {
        "selector": "edge",
        "style": {
            "curve-style": "bezier",
            "font-size": "11px",
            "label": "data(label)",
            "line-color": "#94a3b8",
            "target-arrow-color": "#94a3b8",
            "target-arrow-shape": "triangle",
            "text-background-color": "#ffffff",
            "text-background-opacity": 0.9,
            "text-background-padding": "3px",
            "width": 2,
        },
    },
    {
        "selector": ".produces",
        "style": {
            "line-color": "#2563eb",
            "target-arrow-color": "#2563eb",
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
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <h1>{html.escape(good)}</h1>
      <div class="meta">{len(graph["nodes"])} nodes · {len(graph["edges"])} edges</div>
      <div class="spacer"></div>
      <button type="button" onclick="cy.fit(undefined, 80)">Fit</button>
      <button type="button" onclick="cy.zoom(cy.zoom() * 1.2)">Zoom In</button>
      <button type="button" onclick="cy.zoom(cy.zoom() / 1.2)">Zoom Out</button>
    </header>
    <div id="cy"></div>
  </div>
  <script>
    const graph = {json.dumps(graph, ensure_ascii=False)};
    const cy = cytoscape({{
      container: document.getElementById("cy"),
      elements: [...graph.nodes, ...graph.edges],
      layout: {{ name: "preset", fit: true, padding: 80 }},
      minZoom: {DEFAULT_MIN_ZOOM},
      maxZoom: {DEFAULT_MAX_ZOOM},
      wheelSensitivity: 0.08,
      style: {json.dumps(_CYTOSCAPE_STYLE, ensure_ascii=False)}
    }});
    window.cy = cy;
    cy.ready(() => cy.fit(undefined, 80));
  </script>
</body>
</html>
"""
