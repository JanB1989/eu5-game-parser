import json
import os

import pytest

from eu5gameparser.clausewitz.parser import parse_file
from eu5gameparser.config import ParserConfig
from eu5gameparser.domain.advancements import load_advancement_data
from eu5gameparser.domain.buildings import load_building_data
from eu5gameparser.domain.goods import load_goods_data
from eu5gameparser.graphs import build_good_flow_graph
from eu5gameparser.scanner import iter_text_files


@pytest.mark.integration
def test_parse_real_install_when_enabled() -> None:
    if os.environ.get("EU5_RUN_INTEGRATION") != "1":
        pytest.skip("Set EU5_RUN_INTEGRATION=1 to parse the local EU5 install.")

    config = ParserConfig.from_env()
    if not config.game_root.exists():
        pytest.skip(f"EU5 game root not found: {config.game_root}")

    data = load_building_data(config)
    advancement_data = load_advancement_data(config)
    goods_data = load_goods_data(config)

    assert advancement_data.advancements.height > 0
    assert data.categories.height > 0
    assert data.buildings.height > 0
    assert data.production_methods.height > 0
    assert data.goods_flow_nodes.height > 0
    assert data.goods_flow_edges.height > 0
    assert goods_data.goods.height > 0

    game_goods = [
        entry.key
        for path in iter_text_files(config.goods_dir)
        for entry in parse_file(path).entries
    ]
    assert set(goods_data.goods["name"].to_list()) == set(game_goods)
    assert goods_data.goods["data"].str.len_chars().min() > 0


@pytest.mark.integration
def test_parse_prosper_or_perish_when_enabled() -> None:
    if os.environ.get("EU5_RUN_INTEGRATION") != "1":
        pytest.skip("Set EU5_RUN_INTEGRATION=1 to parse the local EU5 install and mod.")

    data = load_building_data(profile="merged_default")
    advancement_data = load_advancement_data(profile="merged_default")
    goods_data = load_goods_data(profile="merged_default")

    assert {"cookery", "victuals_market", "mining_village"}.issubset(
        set(data.buildings["name"].to_list())
    )
    assert any(name.startswith("pp_") for name in data.production_methods["name"].to_list())
    assert "pp_herring_buss_north_sea" in set(advancement_data.advancements["name"].to_list())

    food_advance = advancement_data.advancements.filter(
        advancement_data.advancements["name"] == "food_advance_renaissance"
    ).row(0, named=True)
    food_modifiers = json.loads(food_advance["modifiers"])
    assert food_modifiers["global_monthly_food_modifier"] == 0.0
    assert food_modifiers["global_wheat_output_modifier"] == 0.1

    farming_village = data.buildings.filter(data.buildings["name"] == "farming_village").row(
        0, named=True
    )
    assert any(method.startswith("pp_") for method in farming_village["unique_production_methods"])

    goods = {row["name"]: row for row in goods_data.goods.to_dicts()}
    assert goods["victuals"]["food"] is not None
    assert goods["victuals"]["source_layer"] == "prosper_or_perish"
    for name in ("wheat", "fish", "livestock"):
        assert goods[name]["food"] is not None
        history_modes = {record["mode"] for record in json.loads(goods[name]["source_history"])}
        assert "TRY_INJECT" in history_modes

    graph = build_good_flow_graph("victuals", data=data)
    node_ids = {node["data"]["id"] for node in graph["nodes"]}
    assert "production_method:victuals_market_maintenance" in node_ids
    assert any("pp_cookery" in node_id for node_id in node_ids)
