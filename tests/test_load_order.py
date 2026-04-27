import json
from pathlib import Path

from eu5gameparser.domain.buildings import load_building_data
from eu5gameparser.domain.goods import load_goods_data
from eu5gameparser.load_order import LoadOrderConfig

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "eu5"
MOD_ROOT = Path(__file__).parent / "fixtures" / "eu5_mod"


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
test_mod = ["test_mod"]
merged_default = ["vanilla", "test_mod"]
""".strip(),
        encoding="utf-8",
    )
    return path


def test_profile_resolves_vanilla_mod_and_merged_layers(tmp_path: Path) -> None:
    config = LoadOrderConfig.load(_load_order_file(tmp_path))

    assert [layer.id for layer in config.profile("vanilla").layers] == ["vanilla"]
    assert [layer.id for layer in config.profile("test_mod").layers] == ["test_mod"]
    assert [layer.id for layer in config.profile("merged_default").layers] == [
        "vanilla",
        "test_mod",
    ]


def test_mod_file_override_and_database_entry_modes(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path)
    data = load_building_data(profile="merged_default", load_order_path=load_order)

    assert data.categories["name"].to_list() == ["mod_category"]

    mason = data.buildings.filter(data.buildings["name"] == "mason").row(0, named=True)
    assert mason["pop_type"] == "artisans"
    assert mason["unique_production_methods"] == [
        "stone_bricks",
        "clay_bricks",
        "plain_finish",
        "stone_upkeep",
        "gem_inlay",
        "masonry_rework",
        "polished_stone",
    ]
    assert mason["unique_production_method_groups"] == [
        ["stone_bricks", "clay_bricks"],
        ["plain_finish", "stone_upkeep", "gem_inlay", "masonry_rework"],
        ["polished_stone"],
    ]
    assert mason["source_layer"] == "test_mod"
    assert [record["mode"] for record in json.loads(mason["source_history"])] == [
        "CREATE",
        "INJECT",
    ]

    bridge = data.buildings.filter(data.buildings["name"] == "bridge_infrastructure").row(
        0, named=True
    )
    assert bridge["max_levels"] == "2"
    assert bridge["source_mode"] == "REPLACE"

    assert {"created_by_inject", "created_by_replace"}.issubset(
        set(data.buildings["name"].to_list())
    )
    assert "missing_try_replace" not in data.buildings["name"].to_list()


def test_mod_injection_updates_goods_and_adds_food_values(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path)
    data = load_goods_data(profile="merged_default", load_order_path=load_order)

    cotton = data.goods.filter(data.goods["name"] == "cotton").row(0, named=True)
    assert cotton["transport_cost"] == 0.2
    assert cotton["food"] == 3.0
    assert cotton["block_rgo_upgrade"] is False
    assert json.loads(cotton["demand_add"]) == {"peasants": 1.0}
    assert cotton["source_layer"] == "test_mod"

    victuals = data.goods.filter(data.goods["name"] == "victuals").row(0, named=True)
    assert victuals["food"] == 30.0
    assert victuals["source_mode"] == "CREATE"

    building_data = load_building_data(
        profile="merged_default", load_order_path=load_order, goods_data=data
    )
    rgo_cotton = building_data.production_methods.filter(
        building_data.production_methods["name"] == "rgo_cotton"
    ).row(0, named=True)
    assert rgo_cotton["source_kind"] == "generated_rgo"
    assert rgo_cotton["produced"] == "cotton"
    assert rgo_cotton["required_pop_type"] == "laborers"
    assert rgo_cotton["required_pop_amount"] == 1.0
    assert rgo_cotton["source_file"] == cotton["source_file"]
    assert rgo_cotton["source_line"] == cotton["source_line"]
    assert rgo_cotton["source_layer"] == cotton["source_layer"]
    assert rgo_cotton["source_mod"] == cotton["source_mod"]
    assert rgo_cotton["source_mode"] == cotton["source_mode"]
    assert rgo_cotton["source_history"] == cotton["source_history"]


def test_mod_only_profile_ignores_missing_try_modes(tmp_path: Path) -> None:
    data = load_building_data(profile="test_mod", load_order_path=_load_order_file(tmp_path))

    assert "mason" not in data.buildings["name"].to_list()
    assert {"created_by_inject", "created_by_replace"}.issubset(
        set(data.buildings["name"].to_list())
    )
