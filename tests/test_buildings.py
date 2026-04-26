from pathlib import Path

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
        "stone_bricks",
    }

    stone_bricks = data.production_methods.filter(
        data.production_methods["name"] == "stone_bricks"
    ).row(0, named=True)
    assert stone_bricks["source_kind"] == "inline"
    assert stone_bricks["building"] == "mason"
    assert stone_bricks["produced"] == "masonry"
    assert stone_bricks["input_goods"] == ["stone"]
    assert stone_bricks["input_amounts"] == [0.4]


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
        "goods:stone",
        "goods:masonry",
    }.issubset(node_ids)

    edges = data.goods_flow_edges.to_dicts()
    assert {
        "source": "building:mason",
        "target": "production_method:stone_bricks",
        "kind": "uses_production_method",
        "amount": None,
        "building": "mason",
        "production_method": "stone_bricks",
        "goods": None,
    } in edges
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
