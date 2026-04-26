from pathlib import Path

import pytest

from eu5gameparser.config import ParserConfig
from eu5gameparser.domain.buildings import load_building_data

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "eu5"


def test_load_building_tables_from_synthetic_fixture() -> None:
    data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))

    assert data.categories["name"].to_list() == [
        "basic_industry_category",
        "infrastructure_category",
    ]
    assert set(data.buildings["name"].to_list()) == {"mason", "bridge_infrastructure"}
    assert set(data.production_methods["name"].to_list()) == {
        "shared_maintenance",
        "bridge_maintenance",
        "monument_work",
        "stone_bricks",
        "clay_bricks",
        "plain_finish",
        "gem_inlay",
        "rgo_cotton",
    }

    mason = data.buildings.filter(data.buildings["name"] == "mason").row(0, named=True)
    assert mason["employment_size"] == 2.0
    assert mason["unique_production_methods"] == [
        "stone_bricks",
        "clay_bricks",
        "plain_finish",
        "gem_inlay",
    ]
    assert mason["unique_production_method_groups"] == [
        ["stone_bricks", "clay_bricks"],
        ["plain_finish", "gem_inlay"],
    ]

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
    assert plain_finish["input_cost"] == 0.0
    assert plain_finish["profit"] is None

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
    assert rgo_cotton["source_layer"] == "vanilla"
    assert rgo_cotton["source_mod"] is None
    assert rgo_cotton["source_mode"] == "CREATE"


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
