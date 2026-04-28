import json
from pathlib import Path

import pytest

from eu5gameparser.config import ParserConfig
from eu5gameparser.domain.buildings import load_building_data
from eu5gameparser.domain.eu5 import load_eu5_data
from eu5gameparser.domain.goods import load_goods_data
from eu5gameparser.graphs import (
    build_good_flow_graph,
    show_good_flow,
    write_good_flow_html,
    write_goods_flow_explorer_html,
)

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "eu5"
MOD_ROOT = Path(__file__).parent / "fixtures" / "eu5_mod"


def test_selected_good_upstream_producer_includes_inputs_and_amounts() -> None:
    data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))

    graph = build_good_flow_graph("masonry", data=data)

    assert _node_ids(graph) >= {
        "good:masonry",
        "production_method:stone_bricks",
        "good:stone",
    }
    assert _edge(graph, "good:stone", "production_method:stone_bricks")["data"]["label"] == "0.4"
    assert _edge(graph, "good:stone", "production_method:stone_bricks")["data"]["amount"] == 0.4
    assert _edge(graph, "production_method:stone_bricks", "good:masonry")["data"]["label"] == "0.5"
    assert _edge(graph, "production_method:stone_bricks", "good:masonry")["data"]["amount"] == 0.5


def test_selected_good_downstream_consumer_includes_outputs_and_amounts() -> None:
    data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))

    graph = build_good_flow_graph("stone", data=data)

    assert _node_ids(graph) >= {
        "good:stone",
        "production_method:stone_bricks",
        "good:masonry",
    }
    assert _edge(graph, "good:stone", "production_method:stone_bricks")["data"]["label"] == "0.4"
    assert _edge(graph, "production_method:stone_bricks", "good:masonry")["data"]["label"] == "0.5"


def test_raw_material_good_includes_generated_rgo_producer() -> None:
    data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))

    graph = build_good_flow_graph("cotton", data=data)

    assert _node_ids(graph) >= {
        "good:cotton",
        "production_method:rgo_cotton",
    }
    assert _edge(graph, "production_method:rgo_cotton", "good:cotton")["data"]["label"] == "1"
    assert _edge(graph, "production_method:rgo_cotton", "good:cotton")["data"]["amount"] == 1.0


def test_depth_one_does_not_expand_newly_found_goods() -> None:
    data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))

    graph = build_good_flow_graph("stone", depth=1, data=data)

    assert "good:masonry" in _node_ids(graph)
    assert "production_method:monument_work" not in _node_ids(graph)
    assert "good:prestige" not in _node_ids(graph)


def test_depth_two_expands_newly_found_goods() -> None:
    data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))

    graph = build_good_flow_graph("stone", depth=2, data=data)

    assert _node_ids(graph) >= {
        "good:masonry",
        "production_method:monument_work",
        "good:prestige",
    }
    assert _edge(graph, "good:masonry", "production_method:monument_work")["data"]["label"] == "0.3"
    assert _edge(graph, "production_method:monument_work", "good:prestige")["data"]["label"] == "2"


def test_unknown_good_raises_clear_error() -> None:
    data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))

    with pytest.raises(ValueError, match="not used by any parsed production method"):
        build_good_flow_graph("missing_good", data=data)


def test_show_good_flow_uses_sensible_interaction_defaults() -> None:
    data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))

    widget = show_good_flow("stone", data=data)

    assert widget.min_zoom == 0.35
    assert widget.max_zoom == 2.5
    assert widget.wheel_sensitivity == 0.001
    assert widget.layout.width == "100%"
    assert widget.layout.height == "900px"
    assert widget.cytoscape_layout["padding"] == 72
    assert widget.autolock is True
    assert widget.auto_ungrabify is True
    assert widget.user_panning_enabled is True
    assert widget.user_zooming_enabled is False


def test_write_good_flow_html_creates_standalone_graph_file(tmp_path: Path) -> None:
    data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))

    path = write_good_flow_html("stone", tmp_path / "stone.html", data=data)

    html = path.read_text(encoding="utf-8")
    assert path.exists()
    assert "cytoscape.min.js" in html
    assert "cytoscape-fcose" in html
    assert 'name: "fcose"' in html
    assert "runSpreadLayout" in html
    assert "colorForGood" in html
    assert "widthForAmount" in html
    assert "data(edge_width)" in html
    assert "data(goods_color)" in html
    assert "provenanceStyles" in html
    assert "colorForSource" in html
    assert "${sourceLabel(source)} value" in html
    assert "Exact mod value" not in html
    assert "data(provenance_color)" in html
    assert 'id="provenanceLegend"' in html
    assert 'id="sourceLegend"' not in html
    assert 'id="goodsLegend"' not in html
    assert '"selector": ".good"' in html
    assert "cytoscape-dagre" in html
    assert 'name: "dagre"' in html
    assert "runColumnLayout" in html
    assert "good:stone" in html
    assert "production_method:stone_bricks" in html
    assert '"shape": "round-rectangle"' in html
    assert '"shape": "round-diamond"' not in html
    assert '"padding": "12px"' in html
    assert "debug_max_profit" not in html
    assert "height: 100vh" in html
    assert 'id="ageFilter"' in html
    assert 'value="age_3_discovery"' in html
    assert "const ageOrder" in html
    assert "function applyAgeFilter" in html
    assert "function filteredGraph" in html
    assert "Specific unlocks" in html
    assert "show_good_flow" not in html


def test_graph_nodes_include_source_metadata() -> None:
    data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))

    graph = build_good_flow_graph("stone", data=data)
    method = _node(graph, "production_method:stone_bricks")

    assert method["data"]["source_layer"] == "vanilla"
    assert "source_mod" in method["data"]
    assert method["data"]["provenance_state"] == "vanilla_exact"
    assert "building:mason" not in _node_ids(graph)


def test_unfiltered_graph_from_eu5_data_includes_availability_metadata(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path)
    eu5_data = load_eu5_data(profile="merged_default", load_order_path=load_order)

    graph = build_good_flow_graph("masonry", eu5_data=eu5_data)
    method = _node(graph, "production_method:stone_bricks")

    assert method["data"]["unlock_age"] == "age_3_discovery"
    assert method["data"]["general_unlock_age"] == "age_3_discovery"
    assert "specific_unlock_age" in method["data"]
    assert method["data"]["availability_kind"] == "unlocked"
    assert method["data"]["is_specific_only"] is False


def test_graph_nodes_classify_mod_exact_and_merged_provenance(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path)
    data = load_building_data(profile="merged_default", load_order_path=load_order)

    injected_graph = build_good_flow_graph("luxury_masonry", data=data)
    injected_method = _node(injected_graph, "production_method:polished_stone")
    assert injected_method["data"]["provenance_state"] == "merged"

    created_graph = build_good_flow_graph("cloth", data=data)
    created_method = _node(created_graph, "production_method:mod_global_method")
    assert created_method["data"]["provenance_state"] == "mod_exact"


def test_graph_age_filter_hides_future_locked_methods(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path)
    eu5_data = load_eu5_data(profile="merged_default", load_order_path=load_order)

    graph = build_good_flow_graph(
        "masonry",
        eu5_data=eu5_data,
        max_age="age_3_discovery",
    )

    node_ids = _node_ids(graph)
    assert "production_method:stone_bricks" in node_ids
    assert "production_method:clay_bricks" not in node_ids
    assert "production_method:polished_stone" not in node_ids
    assert "production_method:default_late_building_method" not in node_ids
    assert "production_method:early_method_late_building" not in node_ids
    assert "production_method:late_method_early_building" not in node_ids


def test_graph_age_filter_allows_methods_after_method_and_building_are_unlocked(
    tmp_path: Path,
) -> None:
    load_order = _load_order_file(tmp_path)
    eu5_data = load_eu5_data(profile="vanilla", load_order_path=load_order)

    graph = build_good_flow_graph(
        "stone",
        eu5_data=eu5_data,
        max_age="age_4_reformation",
    )

    node_ids = _node_ids(graph)
    assert "production_method:default_late_building_method" in node_ids
    assert "production_method:early_method_late_building" in node_ids
    assert "production_method:late_method_early_building" in node_ids


def test_graph_age_filter_can_include_specific_unlocks(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path)
    eu5_data = load_eu5_data(profile="merged_default", load_order_path=load_order)

    graph = build_good_flow_graph(
        "luxury_masonry",
        eu5_data=eu5_data,
        max_age="age_3_discovery",
        include_specific_unlocks=True,
    )

    method = _node(graph, "production_method:polished_stone")
    assert method["data"]["unlock_age"] == "age_2_renaissance"
    assert method["data"]["availability_kind"] == "unlocked"


def test_write_good_flow_html_preselects_requested_age(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path)
    eu5_data = load_eu5_data(profile="merged_default", load_order_path=load_order)

    path = write_good_flow_html(
        "masonry",
        tmp_path / "masonry.html",
        eu5_data=eu5_data,
        max_age="age_3_discovery",
    )
    html = path.read_text(encoding="utf-8")

    assert 'const initialAge = "age_3_discovery";' in html
    assert "applyAgeFilter()" in html
    assert "production_method:clay_bricks" in html


def test_write_goods_flow_explorer_html_creates_single_popout_explorer(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path)
    eu5_data = load_eu5_data(profile="merged_default", load_order_path=load_order)

    path = write_goods_flow_explorer_html(
        tmp_path / "goods_flow_explorer.html",
        eu5_data=eu5_data,
        good="masonry",
        max_age="age_3_discovery",
    )
    html = path.read_text(encoding="utf-8")

    assert path.exists()
    assert 'id="goodSelect"' not in html
    assert 'id="entityType"' in html
    assert 'id="entitySelect"' in html
    assert 'list="entityOptions"' in html
    assert 'id="entityOptions"' in html
    assert '<option value="good">Goods</option>' in html
    assert '<option value="building">Buildings</option>' in html
    assert 'id="overviewTab"' in html
    assert 'id="flowTab"' in html
    assert 'id="modifierTab"' in html
    assert "function renderGoodsOverview" in html
    assert 'id="goodsOverview"' in html
    assert "const goodsOverviewColumns" in html
    assert "let goodsOverviewSort = { key: \"name\", direction: \"asc\" }" in html
    assert "function compareGoodsOverviewRows" in html
    assert "function setGoodsOverviewSort" in html
    assert "sort-header" in html
    assert "sort-indicator" in html
    assert "maximumFractionDigits: 0" in html
    assert "font-variant-numeric: tabular-nums" in html
    assert ".overview-table td:first-child" in html
    assert "cell.title = overviewExactTitle(good[key])" in html
    assert "transport cost" in html
    assert "Output Modifiers" in html
    assert 'id="modifierGoodSelect"' in html
    assert 'id="modifierGoodOptions"' in html
    assert 'value="masonry"' in html
    assert "&quot;mason&quot;" in html
    assert "function updateEntityOptions" in html
    assert '"selector": ".building.selected"' not in html
    assert 'id="ageFilter"' in html
    assert 'id="specificUnlocks"' in html
    assert 'id="depthInput"' in html
    assert 'id="metricMode"' not in html
    assert '<option value="goods">Goods</option>' not in html
    assert '<option value="input_cost">Input cost</option>' not in html
    assert '<option value="output_value">Output value</option>' not in html
    assert '<option value="profit">Profit</option>' not in html
    assert '<option value="profit_margin_percent">Profit %</option>' not in html
    assert "function buildLocalGraph" in html
    assert "function buildGoodGraph" in html
    assert "function buildBuildingGraph" in html
    assert "function buildModifierTimeline" in html
    assert "function runModifierTimelineLayout" in html
    assert "function modifierTimelinePositions" in html
    assert "function setExplorerView" in html
    assert "function formatModifierValue" in html
    assert "function buildingVisible" in html
    assert "function buildingRankedPositions" in html
    assert "function runBuildingRankedLayout" in html
    assert "function graphSpacingFactor" in html
    assert "function scaledLayoutValue" in html
    assert "function buildingInputGoodId" in html
    assert "function buildingOutputGoodId" in html
    assert "function addBuildingInputGoodNode" in html
    assert "function addBuildingOutputGoodNode" in html
    assert 'currentSelectionType() === "building"' in html
    assert "nodeSep: scaledLayoutValue(130)" in html
    assert "rankSep: scaledLayoutValue(260)" in html
    assert "nodeSeparation: scaledLayoutValue(120)" in html
    assert '"selector": ".building-ranked-edge"' in html
    assert '"curve-style": "segments"' in html
    assert '"edge-distances": "endpoints"' in html
    assert '"source-endpoint": "data(source_endpoint)"' in html
    assert '"target-endpoint": "data(target_endpoint)"' in html
    assert '"curve-style": "round-taxi"' not in html
    assert '"taxi-turn": "data(taxi_turn)"' not in html
    assert 'cy.edges().addClass("building-ranked-edge")' in html
    assert 'cy.edges().removeClass("building-ranked-edge")' in html
    assert "function assignBuildingRankedEdgeAnchors" in html
    assert "function anchorOffset" in html
    assert "function edgeSortLabel" in html
    assert "function applyExplorerGraph" in html
    assert "function clearFocus" in html
    assert "function focusElement" in html
    assert 'cy.on("tap", "node", event => focusElement(event.target))' in html
    assert 'cy.on("tap", "edge", event => focusElement(event.target))' in html
    assert 'cy.on("tap", event =>' in html
    assert "if (event.target === cy) clearFocus();" in html
    assert 'removeClass("dimmed")' in html
    assert 'addClass("focus-neighbor")' in html
    assert 'addClass("focused")' in html
    assert '"selector": ".dimmed"' in html
    assert '"selector": ".focused"' in html
    assert '"selector": ".focus-neighbor"' in html
    assert '"selector": ".age-node"' in html
    assert '"selector": ".advancement-node"' in html
    assert '"selector": ".modifier-edge"' in html
    assert '"opacity": 0.15' in html
    assert '"opacity": 0.12' in html
    assert "function metricLabel" not in html
    assert "function methodMetricLines" in html
    assert "function methodLabel" in html
    assert "function methodBuildingContext" in html
    assert "function referencedBuildings" in html
    assert "function formatPercentValue" in html
    assert 'if (value === null || value === undefined) return "n/a";' in html
    assert "const network =" in html
    assert '"price":' in html
    assert '"food":' in html
    assert '"type":' in html
    assert '"transport_cost":' in html
    assert '"pm_output":' in html
    assert '"pm_input":' in html
    assert '"output_modifiers":' in html
    assert '"modifier_key": "global_wheat_output_modifier"' in html
    assert '"modifier_key": "global_rice_output_modifier"' in html
    assert '"has_potential": true' in html
    assert 'modifier.good === selectedGood' in html
    assert "&& (includeSpecific || !modifier.has_potential)" in html
    assert 'label: `${age}\\nTotal: ${formatModifierValue(cumulative)}`' in html
    assert 'source: advancementId' in html
    assert 'target: modifierAgeId(modifier.age)' in html
    assert 'const initialSelection = {"type": "good", "name": "masonry"};' in html
    assert "const initialAge = \"age_3_discovery\";" in html
    assert "const initialMetricMode" not in html
    assert "Input: ${formatMetricValue(method.input_cost)}" in html
    assert "Output: ${formatMetricValue(method.output_value)}" in html
    assert "Profit: ${formatMetricValue(method.profit, true)}" in html
    assert "Profit %: ${formatPercentValue(method.profit_margin_percent)}" in html
    assert '"input_cost":' in html
    assert '"output_value":' in html
    assert '"profit":' in html
    assert '"profit_margin_percent":' in html
    assert '"buildings":' in html
    assert '"effective_unlock_age":' in html
    assert '"building_unlock_age":' in html
    assert "method.effective_availability_kind" in html
    assert "method.building_general_unlock_age" in html
    assert "stone_bricks" in html
    assert "clay_bricks" in html
    assert "masonry" in html
    assert "show_good_flow" not in html


def test_goods_flow_explorer_embeds_multiple_goods_and_methods(tmp_path: Path) -> None:
    data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))
    goods_data = load_goods_data(ParserConfig(game_root=FIXTURE_ROOT))

    path = write_goods_flow_explorer_html(
        tmp_path / "explorer.html",
        data=data,
        goods_data=goods_data,
    )
    html = path.read_text(encoding="utf-8")
    network = _embedded_network(html)

    assert '"goods"' in html
    assert '"buildings"' in html
    assert '"methods"' in html
    assert '"name": "stone"' in html
    assert '"name": "masonry"' in html
    assert '"name": "mason"' in html
    assert '"name": "stone_bricks"' in html
    assert '"name": "monument_work"' in html
    cotton = _network_good(network, "cotton")
    assert cotton["price"] == 3.0
    assert cotton["food"] == 8.0
    assert cotton["type"] == "raw_material"
    assert cotton["transport_cost"] == 1.0
    assert cotton["pm_output"] == 1
    assert cotton["pm_input"] == 0

    masonry = _network_good(network, "masonry")
    assert masonry["price"] == 8.0
    assert masonry["food"] is None
    assert masonry["type"] == "produced"
    assert masonry["pm_output"] == 4
    assert masonry["pm_input"] == 2


def test_goods_flow_explorer_can_preselect_building(tmp_path: Path) -> None:
    data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))

    path = write_goods_flow_explorer_html(
        tmp_path / "building_explorer.html",
        data=data,
        building="mason",
    )
    html = path.read_text(encoding="utf-8")

    assert 'const initialSelection = {"type": "building", "name": "mason"};' in html
    assert "&quot;mason&quot;" in html
    assert '"name": "mason"' in html
    assert '"production_methods":' in html
    assert "stone_bricks" in html
    assert "stone" in html
    assert "masonry" in html
    assert "function buildBuildingGraph" in html
    assert "addBuildingNode(nodes, building, true)" not in html
    assert "buildingId(building.name)" not in html
    assert "uses_production_method" not in html
    assert "dummy" not in html
    assert "invisible" not in html
    assert "addMethodGoods(nodes, edges, method, building.name)" in html
    assert "function methodBuildingContext" in html
    assert "shared_maintenance" in html
    assert 'methodNode.data.ranked_role = "production_method"' in html
    assert "buildingInputGoodId(inputGood)" in html
    assert "buildingOutputGoodId(method.produced)" in html
    assert 'ranked_edge_role: "input"' in html
    assert 'ranked_edge_role: "output"' in html
    assert 'edge.data("source_endpoint"' in html
    assert 'edge.data("target_endpoint"' in html
    assert 'edge.data("target_endpoint", `-50% ${offset}`)' in html
    assert 'edge.data("source_endpoint", `50% ${offset}`)' in html
    assert '"input_good"' in html
    assert '"output_good"' in html
    assert "building_input_good:" in html
    assert "building_output_good:" in html
    assert "masonry_rework" in html


def test_goods_flow_explorer_building_age_filter_keeps_selected_building(
    tmp_path: Path,
) -> None:
    load_order = _load_order_file(tmp_path)
    eu5_data = load_eu5_data(profile="vanilla", load_order_path=load_order)

    path = write_goods_flow_explorer_html(
        tmp_path / "late_building_explorer.html",
        eu5_data=eu5_data,
        building="late_workshop",
        max_age="age_3_discovery",
    )
    html = path.read_text(encoding="utf-8")

    assert 'const initialSelection = {"type": "building", "name": "late_workshop"};' in html
    assert "if (!buildingVisible(building, selectedAge, includeSpecific))" in html
    assert "return { nodes: [], edges: [] };" in html
    assert "&quot;late_workshop&quot;" in html
    assert "default_late_building_method" in html


def test_goods_flow_explorer_keeps_metric_mode_api_compatibility(tmp_path: Path) -> None:
    data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))

    path = write_goods_flow_explorer_html(
        tmp_path / "compat_explorer.html",
        data=data,
        metric_mode="profit",
    )
    html = path.read_text(encoding="utf-8")

    assert 'id="metricMode"' not in html
    assert "const initialMetricMode" not in html
    assert '"profit": 3.2' in html
    assert '"profit_margin_percent": 400.0' in html
    assert '"input_cost": 0.8' in html
    assert '"output_value": 4.0' in html


def test_goods_flow_explorer_renders_missing_metric_values_as_na(tmp_path: Path) -> None:
    data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))

    path = write_goods_flow_explorer_html(
        tmp_path / "missing_metric_explorer.html",
        data=data,
        metric_mode="profit",
    )
    html = path.read_text(encoding="utf-8")

    assert '"name": "shared_maintenance"' in html
    assert '"profit": null' in html
    assert '"profit_margin_percent": null' in html
    assert 'if (value === null || value === undefined) return "n/a";' in html


def test_goods_flow_explorer_rejects_unknown_metric_mode(tmp_path: Path) -> None:
    data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))

    with pytest.raises(ValueError, match="metric_mode must be one of"):
        write_goods_flow_explorer_html(
            tmp_path / "bad_metric.html",
            data=data,
            metric_mode="margin",
        )


def test_trade_goods_notebook_uses_popout_explorer() -> None:
    notebook = (Path(__file__).parents[1] / "notebooks" / "trade_goods.ipynb").read_text(
        encoding="utf-8"
    )

    assert "write_goods_flow_explorer_html" in notebook or "open_goods_flow_explorer" in notebook
    assert "show_good_flow" not in notebook


def _load_order_file(tmp_path: Path) -> Path:
    path = tmp_path / "load_order.toml"
    path.write_text(
        f"""
[paths]
vanilla_root = "{FIXTURE_ROOT.as_posix()}"

[[mods]]
id = "test_mod"
name = "Test Mod"
root = "{MOD_ROOT.as_posix()}"

[profiles]
vanilla = ["vanilla"]
merged_default = ["vanilla", "test_mod"]
""".strip(),
        encoding="utf-8",
    )
    return path


def _node_ids(graph: dict) -> set[str]:
    return {node["data"]["id"] for node in graph["nodes"]}


def _edge(graph: dict, source: str, target: str) -> dict:
    for edge in graph["edges"]:
        if edge["data"]["source"] == source and edge["data"]["target"] == target:
            return edge
    raise AssertionError(f"Missing edge {source} -> {target}")


def _node(graph: dict, node_id: str) -> dict:
    for node in graph["nodes"]:
        if node["data"]["id"] == node_id:
            return node
    raise AssertionError(f"Missing node {node_id}")


def _embedded_network(html: str) -> dict:
    marker = "    const network = "
    start = html.index(marker) + len(marker)
    end = html.index(";\n    const ageOrder", start)
    return json.loads(html[start:end])


def _network_good(network: dict, name: str) -> dict:
    for good in network["goods"]:
        if good["name"] == name:
            return good
    raise AssertionError(f"Missing network good {name}")
