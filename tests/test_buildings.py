from pathlib import Path

import pytest

from eu5gameparser.config import ParserConfig
from eu5gameparser.domain.advancements import load_advancement_data
from eu5gameparser.domain.availability import annotate_building_data_availability
from eu5gameparser.domain.buildings import load_building_data

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "eu5"


def test_load_building_tables_from_synthetic_fixture() -> None:
    data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))

    assert data.categories["name"].to_list() == [
        "basic_industry_category",
        "infrastructure_category",
    ]
    assert set(data.buildings["name"].to_list()) == {
        "mason",
        "bridge_infrastructure",
        "late_workshop",
        "early_workshop",
        "tool_guild",
        "tool_workshop",
        "tool_foundry",
        "tool_mill",
        "tool_market",
        "regional_workshop",
    }
    assert set(data.production_methods["name"].to_list()) == {
        "shared_maintenance",
        "bridge_maintenance",
        "monument_work",
        "stone_bricks",
            "clay_bricks",
            "plain_finish",
            "stone_upkeep",
            "gem_inlay",
            "masonry_rework",
        "default_late_building_method",
        "early_method_late_building",
        "late_method_early_building",
        "tool_guild_method",
        "tool_workshop_method",
        "tool_foundry_method",
        "tool_mill_method",
        "tool_market_method",
        "regional_default_method",
        "rgo_cotton",
    }

    mason = data.buildings.filter(data.buildings["name"] == "mason").row(0, named=True)
    assert mason["employment_size"] == 2.0
    assert mason["icon"] == "mason"
    assert mason["price"] == "mason_price"
    assert mason["price_gold"] == 100.0
    assert mason["price_source"].endswith("00_prices.txt")
    assert mason["effective_price"] == "mason_price"
    assert mason["effective_price_gold"] == 100.0
    assert mason["price_kind"] == "explicit"
    assert mason["unique_production_methods"] == [
        "stone_bricks",
        "clay_bricks",
        "plain_finish",
        "stone_upkeep",
        "gem_inlay",
        "masonry_rework",
    ]
    assert mason["unique_production_method_groups"] == [
        ["stone_bricks", "clay_bricks"],
        ["plain_finish", "stone_upkeep", "gem_inlay", "masonry_rework"],
    ]
    tool_workshop = data.buildings.filter(
        data.buildings["name"] == "tool_workshop"
    ).row(0, named=True)
    assert tool_workshop["obsolete_buildings"] == ["tool_guild"]
    tool_guild = data.buildings.filter(data.buildings["name"] == "tool_guild").row(
        0, named=True
    )
    assert tool_guild["price"] == "scripted_price"
    assert tool_guild["price_gold"] == 125.0
    assert tool_guild["effective_price"] == "scripted_price"
    assert tool_guild["effective_price_gold"] == 125.0
    assert tool_guild["price_kind"] == "explicit"
    bridge = data.buildings.filter(
        data.buildings["name"] == "bridge_infrastructure"
    ).row(0, named=True)
    assert bridge["price"] == "missing_price"
    assert bridge["price_gold"] is None
    assert bridge["price_source"] is None
    late_workshop = data.buildings.filter(
        data.buildings["name"] == "late_workshop"
    ).row(0, named=True)
    assert late_workshop["price"] == "non_gold_price"
    assert late_workshop["price_gold"] is None
    assert late_workshop["price_source"].endswith("00_prices.txt")
    assert late_workshop["effective_price"] == "non_gold_price"
    assert late_workshop["effective_price_gold"] is None
    assert late_workshop["price_kind"] == "explicit"

    stone_bricks = data.production_methods.filter(
        data.production_methods["name"] == "stone_bricks"
    ).row(0, named=True)
    assert stone_bricks["source_kind"] == "inline"
    assert stone_bricks["building"] == "mason"
    assert stone_bricks["production_method_group"] == "mason:unique_production_methods:0"
    assert stone_bricks["production_method_group_index"] == 0
    assert stone_bricks["produced"] == "masonry"
    assert stone_bricks["input_goods"] == ["stone"]
    assert stone_bricks["input_amounts"] == [0.4]
    assert stone_bricks["required_pop_type"] is None
    assert stone_bricks["required_pop_amount"] is None
    assert stone_bricks["production_efficiency_modifier"] == 0.0
    assert stone_bricks["adjusted_output"] == 0.5
    assert stone_bricks["output_value"] == 4.0
    assert stone_bricks["input_cost"] == 0.8
    assert stone_bricks["profit"] == pytest.approx(3.2)
    assert stone_bricks["profit_margin_percent"] == pytest.approx(400.0)
    assert stone_bricks["missing_price_goods"] == []
    assert stone_bricks["population_basis"] == 2.0
    assert stone_bricks["output_per_population"] == 0.25
    assert "debug_max_profit" not in stone_bricks["input_goods"]

    clay_bricks = data.production_methods.filter(
        data.production_methods["name"] == "clay_bricks"
    ).row(0, named=True)
    gem_inlay = data.production_methods.filter(
        data.production_methods["name"] == "gem_inlay"
    ).row(0, named=True)
    assert clay_bricks["category"] == "guild_input"
    assert gem_inlay["category"] == "guild_input"
    assert clay_bricks["production_method_group_index"] == 0
    assert gem_inlay["production_method_group_index"] == 1

    plain_finish = data.production_methods.filter(
        data.production_methods["name"] == "plain_finish"
    ).row(0, named=True)
    assert plain_finish["output_per_population"] is None
    assert plain_finish["output_value"] == 0.0
    assert plain_finish["input_cost"] == 0.0
    assert plain_finish["profit"] == 0.0
    assert plain_finish["profit_margin_percent"] is None

    stone_upkeep = data.production_methods.filter(
        data.production_methods["name"] == "stone_upkeep"
    ).row(0, named=True)
    assert stone_upkeep["output_value"] == 0.0
    assert stone_upkeep["input_cost"] == 1.0
    assert stone_upkeep["profit"] == -1.0
    assert stone_upkeep["profit_margin_percent"] == -100.0

    masonry_rework = data.production_methods.filter(
        data.production_methods["name"] == "masonry_rework"
    ).row(0, named=True)
    assert masonry_rework["input_goods"] == ["masonry"]
    assert masonry_rework["produced"] == "masonry"

    shared_maintenance = data.production_methods.filter(
        data.production_methods["name"] == "shared_maintenance"
    ).row(0, named=True)
    assert shared_maintenance["output_value"] == 0.0
    assert shared_maintenance["input_cost"] is None
    assert shared_maintenance["profit"] is None
    assert shared_maintenance["profit_margin_percent"] is None

    rgo_cotton = data.production_methods.filter(
        data.production_methods["name"] == "rgo_cotton"
    ).row(0, named=True)
    assert rgo_cotton["source_kind"] == "generated_rgo"
    assert rgo_cotton["building"] is None
    assert rgo_cotton["produced"] == "cotton"
    assert rgo_cotton["output"] == 1.0
    assert rgo_cotton["input_goods"] == []
    assert rgo_cotton["input_amounts"] == []
    assert rgo_cotton["required_pop_type"] == "laborers"
    assert rgo_cotton["required_pop_amount"] == 1.0
    assert rgo_cotton["production_method_group"] is None
    assert rgo_cotton["production_method_group_index"] is None
    assert rgo_cotton["population_basis"] == 1.0
    assert rgo_cotton["output_per_population"] == 1.0
    assert rgo_cotton["profit"] == 3.0
    assert rgo_cotton["profit_margin_percent"] is None
    assert rgo_cotton["source_layer"] == "vanilla"
    assert rgo_cotton["source_mod"] is None
    assert rgo_cotton["source_mode"] == "CREATE"


def test_building_prices_fall_back_to_baseline_age_price() -> None:
    config = ParserConfig(game_root=FIXTURE_ROOT)
    data = load_building_data(config)
    advancements = load_advancement_data(config).advancements

    annotated = annotate_building_data_availability(data, advancements)
    buildings = {row["name"]: row for row in annotated.buildings.to_dicts()}

    early_workshop = buildings["early_workshop"]
    assert early_workshop["price"] is None
    assert early_workshop["unlock_age"] == "age_3_discovery"
    assert early_workshop["effective_price"] == "p_building_age_3_discovery"
    assert early_workshop["effective_price_gold"] == 200.0
    assert early_workshop["price_kind"] == "baseline_age"

    mason = buildings["mason"]
    assert mason["effective_price"] == "mason_price"
    assert mason["effective_price_gold"] == 100.0
    assert mason["price_kind"] == "explicit"


def test_reports_unresolved_production_method_references() -> None:
    data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))

    assert data.unresolved_production_methods.to_dicts() == [
        {"building": "bridge_infrastructure", "production_method": "missing_method"}
    ]
    assert (
        "bridge_infrastructure references missing production method missing_method"
        in data.warnings
    )


def test_goods_flow_tables_include_goods_buildings_methods_and_edges() -> None:
    data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))

    node_ids = set(data.goods_flow_nodes["id"].to_list())
    assert {
        "building:mason",
        "production_method:stone_bricks",
        "production_method:rgo_cotton",
        "goods:stone",
        "goods:masonry",
        "goods:cotton",
    }.issubset(node_ids)

    edges = data.goods_flow_edges.to_dicts()
    assert any(
        edge["source"] == "building:mason"
        and edge["target"] == "production_method:stone_bricks"
        and edge["kind"] == "uses_production_method"
        and edge["amount"] is None
        and edge["building"] == "mason"
        and edge["production_method"] == "stone_bricks"
        and edge["goods"] is None
        for edge in edges
    )
    assert any(
        edge["source"] == "goods:stone"
        and edge["target"] == "production_method:stone_bricks"
        and edge["kind"] == "consumes"
        for edge in edges
    )
    assert any(
        edge["source"] == "production_method:stone_bricks"
        and edge["target"] == "goods:masonry"
        and edge["kind"] == "produces"
        for edge in edges
    )
