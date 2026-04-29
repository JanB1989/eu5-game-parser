import json
from pathlib import Path

from eu5gameparser import build_goods_summary
from eu5gameparser.config import ParserConfig
from eu5gameparser.domain.buildings import load_building_data
from eu5gameparser.domain.goods import load_goods_data

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "eu5"


def test_load_goods_from_synthetic_fixture() -> None:
    data = load_goods_data(ParserConfig(game_root=FIXTURE_ROOT))

    assert data.goods["name"].to_list() == [
        "cotton",
        "stone",
        "clay",
        "masonry",
        "gems",
        "porcelain",
        "tools",
        "lumber",
        "infrastructure",
        "prestige",
        "early_goods",
        "late_goods",
        "regional_goods",
    ]

    cotton = data.goods.filter(data.goods["name"] == "cotton").row(0, named=True)
    assert cotton["method"] == "farming"
    assert cotton["category"] == "raw_material"
    assert cotton["color"] == "goods_cotton"
    assert cotton["default_market_price"] == 3.0
    assert cotton["transport_cost"] == 1.0
    assert cotton["origin_in_old_world"] is True
    assert cotton["origin_in_new_world"] is None
    assert cotton["custom_tags"] == ["old_world_goods", "textile_goods"]


def test_goods_preserve_nested_maps_and_full_raw_data() -> None:
    data = load_goods_data(ParserConfig(game_root=FIXTURE_ROOT))

    porcelain = data.goods.filter(data.goods["name"] == "porcelain").row(0, named=True)
    assert porcelain["transport_cost"] == 0.5
    assert porcelain["development_threshold"] == 30.0
    assert json.loads(porcelain["demand_add"]) == {"upper": 0.0005}
    assert json.loads(porcelain["demand_multiply"]) == {"nobles": 20.0}
    assert json.loads(porcelain["wealth_impact_threshold"]) == {"all": 1.1}

    raw_data = json.loads(porcelain["data"])
    raw_keys = {entry["key"] for entry in raw_data["entries"]}
    assert {
        "category",
        "color",
        "default_market_price",
        "transport_cost",
        "demand_add",
        "demand_multiply",
        "development_threshold",
        "wealth_impact_threshold",
    } == raw_keys


def test_build_goods_summary_counts_inputs_outputs_and_provenance() -> None:
    goods_data = load_goods_data(ParserConfig(game_root=FIXTURE_ROOT))
    building_data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))

    summary = build_goods_summary(goods_data.goods, building_data.production_methods)

    cotton = summary.filter(summary["name"] == "cotton").row(0, named=True)
    assert cotton["input_method_count"] == 0
    assert cotton["output_method_count"] == 1
    assert cotton["provenance_state"] == "vanilla_exact"
    assert cotton["provenance_source"] == "vanilla"

    porcelain = summary.filter(summary["name"] == "porcelain").row(0, named=True)
    assert porcelain["input_method_count"] == 0
    assert porcelain["output_method_count"] == 0


def test_parsed_goods_have_categories_and_cover_production_method_references() -> None:
    goods_data = load_goods_data(ParserConfig(game_root=FIXTURE_ROOT))
    building_data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))

    goods_by_name = {row["name"]: row for row in goods_data.goods.to_dicts()}
    missing_categories = [
        name for name, row in goods_by_name.items() if row.get("category") is None
    ]
    referenced_goods: set[str] = set()
    for method in building_data.production_methods.to_dicts():
        if method.get("produced"):
            referenced_goods.add(method["produced"])
        referenced_goods.update(method.get("input_goods") or [])

    assert missing_categories == []
    assert referenced_goods - goods_by_name.keys() == set()
