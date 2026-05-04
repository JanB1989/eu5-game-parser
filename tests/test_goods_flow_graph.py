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
from eu5gameparser.graphs.goods_flow import _building_icon_source

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
    assert "function renderDesignationTally" in html
    assert "function designationTallyRows" in html
    assert 'id="goodsOverview"' in html
    assert "const goodsOverviewColumns" in html
    assert "let goodsOverviewSort = { key: \"name\", direction: \"asc\" }" in html
    assert "function compareGoodsOverviewRows" in html
    assert "function setGoodsOverviewSort" in html
    assert "sort-header" in html
    assert "sort-indicator" in html
    assert "designation-column" in html
    assert "designation-tally" in html
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
    assert "function enhanceSearchInput" in html
    assert "suppressNextSelect" in html
    assert "pickerClick" in html
    assert 'event.key === "Enter"' in html
    assert 'event.key === "Escape"' in html
    assert "input.select()" in html
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
    assert "function goodRankedColumnMap" in html
    assert "function goodRankedPositions" in html
    assert "function runGoodRankedLayout" in html
    assert 'let currentLayout = "ranked";' in html
    assert 'currentGood === selectedGood ? "selected_good" : null' in html
    assert 'setRankedRole(nodes, methodId(method.name), "producer_method")' in html
    assert 'currentGood === selectedGood && method.produced === selectedGood' in html
    assert "function directGoodRankedColumn" in html
    assert "const nextPriority = rankedRolePriority[rankedRole] || 0" in html
    assert "const sourceColumn = columns.get(sourceId)" in html
    assert "columns.set(sourceId, targetColumn)" in html
    assert "sourceId, column - 1" not in html
    assert "function graphSpacingFactor" in html
    assert "function scaledLayoutValue" in html
    assert "function buildingInputGoodId" in html
    assert "function buildingOutputGoodId" in html
    assert "function addBuildingInputGoodNode" in html
    assert "function addBuildingOutputGoodNode" in html
    assert 'currentSelectionType() === "building"' in html
    assert "runGoodRankedLayout()" in html
    assert "const goodRankedFallbackColumn" in html
    assert "const x = scaledLayoutValue(column * 260)" in html
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
    assert '"designation":' in html
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
    assert '"price_gold":' in html
    assert '"effective_price_gold":' in html
    assert '"price_kind":' in html
    assert '"pop_type":' in html
    assert '"employment_size":' in html
    assert '"production_method_group_index":' in html
    assert '"slot_label":' in html
    assert '"progressionByGood":' in html
    assert '"icon": "mason"' in html
    assert '"icon_source":' in html
    assert '"icon_url":' in html
    assert '"icon_panel_url":' in html
    assert '"building_icon_panel_url":' in html
    assert '"selector": ".has-building-icon"' in html
    assert '"background-image": "data(building_icon_panel_url)"' in html
    assert '"background-fit": "contain"' in html
    assert '"background-width": "100%"' in html
    assert '"background-height": "100%"' in html
    assert '"background-position-x": "50%"' in html
    assert '"background-position-y": "50%"' in html
    assert '"background-image-crossorigin": "null"' in html
    assert '"background-image-opacity": 1' in html
    assert '"background-image-containment": "over"' in html
    assert '"background-repeat": "no-repeat"' in html
    assert '"background-opacity": 0.18' in html
    assert '"width": 292' in html
    assert '"height": 116' in html
    assert '"text-halign": "center"' in html
    assert '"text-valign": "center"' in html
    assert '"text-justification": "center"' in html
    assert '"text-max-width": "178px"' in html
    assert '"text-margin-x": "48px"' in html
    assert '"text-margin-y": "0px"' in html
    assert '"text-background-color": "#ffffff"' in html
    assert '"text-background-opacity": 0.92' in html
    assert '"text-background-padding": "5px"' in html
    assert '"text-background-shape": "round-rectangle"' in html
    assert '"background-fit": "none"' not in html
    assert '"height": 156' not in html
    assert '"text-margin-y": "-14px"' not in html
    assert '"text-background-shape": "roundrectangle"' not in html
    assert "nodeDimensionsIncludeLabels: true" in html
    assert 'id="progressionTab"' in html
    assert 'id="progressionView"' in html
    assert "function renderProgression" in html
    assert "function progressionSelectedAge" in html
    assert "progression-grid" in html
    assert "progression-legend" in html
    assert ".progression-swatch.building" in html
    assert ".progression-swatch.slot" in html
    assert "function appendProgressionMethodGroups" in html
    assert "progression-slot-group" in html
    assert "Cost:" in html
    assert "Baseline age price:" in html
    assert "overflow-wrap: anywhere" in html
    assert "Pop:" in html
    assert "Employment:" in html
    assert "Specific/regional" in html
    assert "#6366f1" in html
    assert "progression-card" in html
    assert '"type": "building_unlock"' in html
    assert '"type": "method_unlock"' in html
    assert '"effective_unlock_age":' in html
    assert '"building_unlock_age":' in html
    assert "method.effective_availability_kind" in html
    assert "event.effective_general_unlock_age" in html
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
    masonry_line = network["progressionByGood"]["masonry"][0]
    assert masonry_line["family"] == "mason"
    assert masonry_line["buildings"][0]["building"]["name"] == "mason"
    assert masonry_line["buildings"][0]["building"]["icon"] == "mason"
    assert masonry_line["buildings"][0]["building"]["price_gold"] == 100.0
    assert masonry_line["buildings"][0]["building"]["pop_type"] == "laborers"
    assert masonry_line["buildings"][0]["building"]["employment_size"] == 2.0
    assert masonry_line["buildings"][0]["methods"][0]["produced"] == "masonry"
    slot_by_method = {
        method["name"]: method["slot_label"]
        for method in masonry_line["buildings"][0]["methods"]
    }
    assert slot_by_method == {
        "clay_bricks": "Slot 1",
        "stone_bricks": "Slot 1",
        "gem_inlay": "Slot 2",
        "masonry_rework": "Slot 2",
    }
    assert '"name": "stone_bricks"' in html
    assert '"name": "monument_work"' in html
    cotton = _network_good(network, "cotton")
    assert cotton["designation"] == "farming"
    assert cotton["price"] == 3.0
    assert cotton["food"] == 8.0
    assert cotton["type"] == "raw_material"
    assert cotton["transport_cost"] == 1.0
    assert cotton["pm_output"] == 1
    assert cotton["pm_input"] == 0

    masonry = _network_good(network, "masonry")
    assert masonry["price"] == 8.0
    assert masonry["food"] is None
    assert masonry["designation"] == "produced"
    assert masonry["type"] == "produced"
    assert masonry["pm_output"] == 4
    assert masonry["pm_input"] == 2

    designation_counts = _designation_counts(network)
    assert designation_counts["farming"] == 1
    assert designation_counts["produced"] == 13


def test_progression_payload_separates_building_and_method_unlock_events(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path)
    eu5_data = load_eu5_data(profile="merged_default", load_order_path=load_order)

    path = write_goods_flow_explorer_html(
        tmp_path / "explorer.html",
        eu5_data=eu5_data,
        good="early_goods",
        max_age="age_3_discovery",
    )
    network = _embedded_network(path.read_text(encoding="utf-8"))

    early_line = network["progressionByGood"]["early_goods"][0]
    early_stage = early_line["buildings"][0]
    building_event = _progression_event(early_stage, "building_unlock")
    method_event = _progression_event(early_stage, "method_unlock", "late_method_early_building")

    assert early_stage["building"]["name"] == "early_workshop"
    assert early_stage["building"]["effective_price"] == "p_building_age_3_discovery"
    assert early_stage["building"]["effective_price_gold"] == 200.0
    assert early_stage["building"]["price_kind"] == "baseline_age"
    assert building_event["general_unlock_age"] == "age_3_discovery"
    assert method_event["general_unlock_age"] == "age_4_reformation"
    assert method_event["effective_general_unlock_age"] == "age_4_reformation"

    late_line = network["progressionByGood"]["late_goods"][0]
    late_stage = late_line["buildings"][0]
    late_building_event = _progression_event(late_stage, "building_unlock")
    late_method_event = _progression_event(
        late_stage,
        "method_unlock",
        "early_method_late_building",
    )

    assert late_stage["building"]["name"] == "late_workshop"
    assert late_stage["building"]["effective_price"] == "non_gold_price"
    assert late_stage["building"]["price_kind"] == "explicit"
    assert late_building_event["general_unlock_age"] == "age_4_reformation"
    assert late_method_event["general_unlock_age"] == "age_3_discovery"
    assert late_method_event["effective_general_unlock_age"] == "age_4_reformation"


def test_progression_payload_groups_obsolete_upgrade_chains(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path)
    eu5_data = load_eu5_data(profile="merged_default", load_order_path=load_order)

    path = write_goods_flow_explorer_html(
        tmp_path / "explorer.html",
        eu5_data=eu5_data,
        good="tool_fixture",
    )
    network = _embedded_network(path.read_text(encoding="utf-8"))
    rows = network["progressionByGood"]["tool_fixture"]
    rows_by_family = {row["family"]: row for row in rows}

    assert [
        stage["building"]["name"]
        for stage in rows_by_family["tool_guild"]["buildings"]
    ] == ["tool_guild", "tool_workshop", "tool_foundry", "tool_mill"]
    assert rows_by_family["tool_guild"]["buildings"][1]["building"][
        "upgrade_source"
    ] == "obsolete"
    assert rows_by_family["tool_guild"]["buildings"][1]["building"][
        "upgrade_previous"
    ] == "tool_guild"
    assert rows_by_family["tool_guild"]["buildings"][2]["building"][
        "upgrade_tier"
    ] == 2
    assert rows_by_family["tool_market"]["buildings"][0]["building"][
        "upgrade_source"
    ] == "none"


def test_progression_keeps_specific_method_events_visible_as_muted(
    tmp_path: Path,
) -> None:
    load_order = _load_order_file(tmp_path)
    eu5_data = load_eu5_data(profile="merged_default", load_order_path=load_order)

    path = write_goods_flow_explorer_html(
        tmp_path / "explorer.html",
        eu5_data=eu5_data,
        good="masonry",
        max_age="age_3_discovery",
    )
    html = path.read_text(encoding="utf-8")
    network = _embedded_network(html)

    mason_stage = network["progressionByGood"]["masonry"][0]["buildings"][0]
    methods = {method["name"]: method for method in mason_stage["methods"]}

    assert methods["gem_inlay"]["effective_availability_kind"] == "specific_only"
    assert 'methodItem.classList.add("specific-only")' in html
    assert "methods: (stage.methods || []).filter" not in html
    assert "!progressionEventAllowedBySpecific(buildingEvent, includeSpecific)" not in html


def test_building_icon_source_prefers_explicit_png(tmp_path: Path) -> None:
    root = tmp_path / "mod"
    icon_dir = root / "in_game" / "gfx" / "interface" / "icons" / "buildings"
    icon_dir.mkdir(parents=True)
    source_file = root / "in_game" / "common" / "building_types" / "buildings.txt"
    source_file.parent.mkdir(parents=True)
    source_file.write_text("test_building = {}", encoding="utf-8")
    explicit_icon = icon_dir / "explicit_icon.png"
    fallback_icon = icon_dir / "test_building.png"
    explicit_icon.write_text("png", encoding="utf-8")
    fallback_icon.write_text("png", encoding="utf-8")

    source = _building_icon_source(
        {"name": "test_building", "icon": "explicit_icon", "source_file": str(source_file)}
    )

    assert source == explicit_icon


def test_building_icon_source_falls_back_to_building_key_dds_from_profile_roots(
    tmp_path: Path,
) -> None:
    mod_root = tmp_path / "mod"
    vanilla_root = tmp_path / "vanilla"
    source_file = mod_root / "in_game" / "common" / "building_types" / "pp_farming_village.txt"
    source_file.parent.mkdir(parents=True)
    source_file.write_text("REPLACE:farming_village = {}", encoding="utf-8")
    icon_dir = vanilla_root / "game" / "main_menu" / "gfx" / "interface" / "icons" / "buildings"
    icon_dir.mkdir(parents=True)
    farming_icon = icon_dir / "farming_village.dds"
    farming_icon.write_text("dds", encoding="utf-8")

    source = _building_icon_source(
        {"name": "farming_village", "icon": None, "source_file": str(source_file)},
        profile_roots=[vanilla_root],
    )

    assert source == farming_icon


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


def _progression_event(stage: dict, event_type: str, name: str | None = None) -> dict:
    for event in stage["events"]:
        if event["type"] != event_type:
            continue
        if name is not None and event.get("method") != name:
            continue
        return event
    raise AssertionError(f"Missing progression event {event_type}:{name}")


def _designation_counts(network: dict) -> dict[str, int]:
    counts: dict[str, int] = {}
    for good in network["goods"]:
        designation = good["designation"] or "n/a"
        counts[designation] = counts.get(designation, 0) + 1
    return counts
