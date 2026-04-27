import json
from pathlib import Path

from eu5gameparser.config import ParserConfig
from eu5gameparser.domain.advancements import load_advancement_data

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


def test_load_advancements_from_synthetic_fixture() -> None:
    data = load_advancement_data(ParserConfig(game_root=FIXTURE_ROOT))

    assert data.advancements["name"].to_list() == [
        "test_farming_advance",
        "test_craft_advance",
        "age_3_method_unlock",
        "age_4_method_unlock",
        "age_4_building_unlock",
        "age_3_building_unlock",
        "specific_only_method_unlock",
        "specific_only_building_unlock",
    ]

    farming = data.advancements.filter(
        data.advancements["name"] == "test_farming_advance"
    ).row(0, named=True)
    assert farming["age"] == "age_1_traditions"
    assert farming["icon"] == "farming_icon"
    assert farming["requires"] == ["root_advance", "agriculture_advance"]
    assert farming["unlock_production_method"] == ["test_crop_rotation"]
    assert farming["unlock_building"] == ["test_farm"]
    assert json.loads(farming["modifiers"]) == {
        "global_monthly_food_modifier": 0.1,
        "global_wheat_output_modifier": 0.2,
    }

    raw_data = json.loads(farming["data"])
    raw_keys = [entry["key"] for entry in raw_data["entries"]]
    assert "ai_weight" in raw_keys
    assert "depth" in raw_keys
    assert "depth" not in json.loads(farming["modifiers"])
    assert "ai_weight" not in json.loads(farming["modifiers"])


def test_mod_injection_adds_numeric_advancement_modifiers(tmp_path: Path) -> None:
    data = load_advancement_data(
        profile="merged_default", load_order_path=_load_order_file(tmp_path)
    )

    farming = data.advancements.filter(
        data.advancements["name"] == "test_farming_advance"
    ).row(0, named=True)
    modifiers = json.loads(farming["modifiers"])
    assert modifiers["global_monthly_food_modifier"] == 0.05
    assert modifiers["global_wheat_output_modifier"] == 0.2
    assert modifiers["global_rice_output_modifier"] == 0.15
    assert "ai_weight" not in modifiers
    assert [record["mode"] for record in json.loads(farming["source_history"])] == [
        "CREATE",
        "TRY_INJECT",
    ]
    assert farming["source_layer"] == "test_mod"
    assert farming["source_mode"] == "TRY_INJECT"

    raw_entries = json.loads(farming["data"])["entries"]
    assert sum(1 for entry in raw_entries if entry["key"] == "ai_weight") == 1

    created = data.advancements.filter(
        data.advancements["name"] == "mod_created_advance"
    ).row(0, named=True)
    assert created["source_mode"] == "CREATE"
    assert created["source_layer"] == "test_mod"
    assert created["icon"] == "mod_icon"
    assert created["unlock_unit"] == ["mod_unit"]
    assert json.loads(created["modifiers"]) == {"global_fish_output_modifier": 0.25}
