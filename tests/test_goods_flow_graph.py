from pathlib import Path

import pytest

from eu5gameparser.config import ParserConfig
from eu5gameparser.domain.buildings import load_building_data
from eu5gameparser.graphs import build_good_flow_graph, show_good_flow, write_good_flow_html

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


def test_graph_nodes_include_source_metadata() -> None:
    data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))

    graph = build_good_flow_graph("stone", data=data)
    method = _node(graph, "production_method:stone_bricks")

    assert method["data"]["source_layer"] == "vanilla"
    assert "source_mod" in method["data"]
    assert method["data"]["provenance_state"] == "vanilla_exact"
    assert "building:mason" not in _node_ids(graph)


def test_graph_nodes_classify_mod_exact_and_merged_provenance(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path)
    data = load_building_data(profile="merged_default", load_order_path=load_order)

    injected_graph = build_good_flow_graph("luxury_masonry", data=data)
    injected_method = _node(injected_graph, "production_method:polished_stone")
    assert injected_method["data"]["provenance_state"] == "merged"

    created_graph = build_good_flow_graph("cloth", data=data)
    created_method = _node(created_graph, "production_method:mod_global_method")
    assert created_method["data"]["provenance_state"] == "mod_exact"


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
