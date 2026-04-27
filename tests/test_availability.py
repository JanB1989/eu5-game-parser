from pathlib import Path

import pytest

from eu5gameparser.config import ParserConfig
from eu5gameparser.domain.advancements import load_advancement_data
from eu5gameparser.domain.availability import filter_building_data_by_age
from eu5gameparser.domain.buildings import load_building_data

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


def test_age_filter_keeps_available_defaults_and_hides_future_unlocks() -> None:
    building_data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))
    advancement_data = load_advancement_data(ParserConfig(game_root=FIXTURE_ROOT))

    filtered = filter_building_data_by_age(
        building_data, advancement_data.advancements, "age_3_discovery"
    )

    assert "mason" in set(filtered.buildings["name"].to_list())
    assert "bridge_infrastructure" not in set(filtered.buildings["name"].to_list())

    methods = set(filtered.production_methods["name"].to_list())
    assert "stone_bricks" in methods
    assert "plain_finish" in methods
    assert "rgo_cotton" in methods
    assert "clay_bricks" not in methods
    assert "gem_inlay" not in methods
    assert "default_late_building_method" not in methods
    assert "early_method_late_building" not in methods
    assert "late_method_early_building" not in methods
    assert "regional_default_method" not in methods

    stone = filtered.production_methods.filter(
        filtered.production_methods["name"] == "stone_bricks"
    ).row(0, named=True)
    assert stone["unlock_age"] == "age_3_discovery"
    assert stone["availability_kind"] == "unlocked"

    plain = filtered.production_methods.filter(
        filtered.production_methods["name"] == "plain_finish"
    ).row(0, named=True)
    assert plain["unlock_age"] is None
    assert plain["availability_kind"] == "available_by_default"


def test_age_filter_combines_method_and_building_unlock_ages() -> None:
    building_data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))
    advancement_data = load_advancement_data(ParserConfig(game_root=FIXTURE_ROOT))

    age_3 = filter_building_data_by_age(
        building_data, advancement_data.advancements, "age_3_discovery"
    )
    assert "late_workshop" not in set(age_3.buildings["name"].to_list())
    assert "early_workshop" in set(age_3.buildings["name"].to_list())
    assert "default_late_building_method" not in set(
        age_3.production_methods["name"].to_list()
    )
    assert "early_method_late_building" not in set(
        age_3.production_methods["name"].to_list()
    )
    assert "late_method_early_building" not in set(
        age_3.production_methods["name"].to_list()
    )

    age_4 = filter_building_data_by_age(
        building_data, advancement_data.advancements, "age_4_reformation"
    )
    methods = set(age_4.production_methods["name"].to_list())
    assert "default_late_building_method" in methods
    assert "early_method_late_building" in methods
    assert "late_method_early_building" in methods

    early_method = age_4.production_methods.filter(
        age_4.production_methods["name"] == "early_method_late_building"
    ).row(0, named=True)
    assert early_method["unlock_age"] == "age_3_discovery"
    assert early_method["building_unlock_age"] == "age_4_reformation"
    assert early_method["effective_unlock_age"] == "age_4_reformation"

    late_method = age_4.production_methods.filter(
        age_4.production_methods["name"] == "late_method_early_building"
    ).row(0, named=True)
    assert late_method["unlock_age"] == "age_4_reformation"
    assert late_method["building_unlock_age"] == "age_3_discovery"
    assert late_method["effective_unlock_age"] == "age_4_reformation"


def test_age_filter_can_include_specific_only_unlocks() -> None:
    building_data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))
    advancement_data = load_advancement_data(ParserConfig(game_root=FIXTURE_ROOT))

    filtered = filter_building_data_by_age(
        building_data,
        advancement_data.advancements,
        "age_1_traditions",
        include_specific_unlocks=True,
    )

    gem = filtered.production_methods.filter(
        filtered.production_methods["name"] == "gem_inlay"
    ).row(0, named=True)
    assert gem["unlock_age"] == "age_1_traditions"
    assert gem["availability_kind"] == "specific_only"

    regional = filtered.production_methods.filter(
        filtered.production_methods["name"] == "regional_default_method"
    ).row(0, named=True)
    assert regional["building_unlock_age"] == "age_1_traditions"
    assert regional["effective_availability_kind"] == "specific_only"


def test_age_filter_uses_worst_case_general_age_by_default(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path)
    building_data = load_building_data(profile="merged_default", load_order_path=load_order)
    advancement_data = load_advancement_data(profile="merged_default", load_order_path=load_order)

    conservative = filter_building_data_by_age(
        building_data, advancement_data.advancements, "age_3_discovery"
    )
    assert "polished_stone" not in set(conservative.production_methods["name"].to_list())

    specific = filter_building_data_by_age(
        building_data,
        advancement_data.advancements,
        "age_3_discovery",
        include_specific_unlocks=True,
    )
    polished = specific.production_methods.filter(
        specific.production_methods["name"] == "polished_stone"
    ).row(0, named=True)
    assert polished["unlock_age"] == "age_2_renaissance"
    assert polished["availability_kind"] == "unlocked"


def test_age_filter_rejects_unknown_age() -> None:
    building_data = load_building_data(ParserConfig(game_root=FIXTURE_ROOT))
    advancement_data = load_advancement_data(ParserConfig(game_root=FIXTURE_ROOT))

    with pytest.raises(ValueError, match="Unknown age"):
        filter_building_data_by_age(
            building_data, advancement_data.advancements, "age_99_future"
        )
