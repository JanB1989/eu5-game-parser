from pathlib import Path

from typer.testing import CliRunner

from eu5gameparser.cli import app

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


def test_goods_command_writes_table_and_prints_sources(tmp_path: Path) -> None:
    output = tmp_path / "out"
    result = CliRunner().invoke(
        app,
        [
            "goods",
            "--load-order",
            str(_load_order_file(tmp_path)),
            "--output",
            str(output),
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0
    assert (output / "goods.csv").exists()
    assert "profile: merged_default" in result.stdout
    assert "goods:" in result.stdout
    assert "vanilla:" in result.stdout
    assert "Test Mod:" in result.stdout
    assert "modes:" in result.stdout


def test_buildings_command_writes_related_tables_and_prints_sources(tmp_path: Path) -> None:
    output = tmp_path / "out"
    result = CliRunner().invoke(
        app,
        [
            "buildings",
            "--load-order",
            str(_load_order_file(tmp_path)),
            "--output",
            str(output),
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0
    for name in (
        "building_categories",
        "buildings",
        "production_methods",
        "unresolved_production_methods",
        "duplicate_production_methods",
    ):
        assert (output / f"{name}.csv").exists()
    assert "profile: merged_default" in result.stdout
    assert "buildings:" in result.stdout
    assert "production_methods:" in result.stdout
    assert "vanilla:" in result.stdout
    assert "Test Mod:" in result.stdout
    assert "generated_rgo:" in result.stdout


def test_buildings_command_prints_warnings_when_applicable(tmp_path: Path) -> None:
    output = tmp_path / "out"
    result = CliRunner().invoke(
        app,
        [
            "buildings",
            "--load-order",
            str(_load_order_file(tmp_path)),
            "--profile",
            "vanilla",
            "--output",
            str(output),
        ],
    )

    assert result.exit_code == 0
    assert "profile: vanilla" in result.stdout
    assert "warnings:" in result.stdout


def test_buildings_command_accepts_age_filter(tmp_path: Path) -> None:
    output = tmp_path / "out"
    result = CliRunner().invoke(
        app,
        [
            "buildings",
            "--load-order",
            str(_load_order_file(tmp_path)),
            "--output",
            str(output),
            "--age",
            "age_3_discovery",
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0
    assert (output / "buildings.csv").exists()
    assert (output / "production_methods.csv").exists()
    assert "age: age_3_discovery" in result.stdout
    assert "specific_unlocks: excluded" in result.stdout
    assert "hidden: buildings" in result.stdout


def test_advancements_command_writes_table_and_prints_sources(tmp_path: Path) -> None:
    output = tmp_path / "out"
    result = CliRunner().invoke(
        app,
        [
            "advancements",
            "--load-order",
            str(_load_order_file(tmp_path)),
            "--output",
            str(output),
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0
    assert (output / "advancements.csv").exists()
    assert "profile: merged_default" in result.stdout
    assert "advancements:" in result.stdout
    assert "vanilla:" in result.stdout
    assert "Test Mod:" in result.stdout
    assert "modes:" in result.stdout


def test_all_command_writes_everything_and_groups_feedback(tmp_path: Path) -> None:
    output = tmp_path / "out"
    result = CliRunner().invoke(
        app,
        [
            "all",
            "--load-order",
            str(_load_order_file(tmp_path)),
            "--output",
            str(output),
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0
    for name in (
        "advancements",
        "goods",
        "goods_summary",
        "building_categories",
        "buildings",
        "production_methods",
        "unresolved_production_methods",
        "duplicate_production_methods",
        "goods_flow_nodes",
        "goods_flow_edges",
    ):
        assert (output / f"{name}.csv").exists()
    assert "profile: merged_default" in result.stdout
    assert "Goods" in result.stdout
    assert "Buildings" in result.stdout
    assert "Advancements" in result.stdout
    assert "Graphs" in result.stdout
    assert "Test Mod:" in result.stdout


def test_all_command_accepts_age_filter(tmp_path: Path) -> None:
    output = tmp_path / "out"
    result = CliRunner().invoke(
        app,
        [
            "all",
            "--load-order",
            str(_load_order_file(tmp_path)),
            "--output",
            str(output),
            "--age",
            "age_3_discovery",
            "--include-specific-unlocks",
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0
    assert (output / "goods_flow_nodes.csv").exists()
    assert "age: age_3_discovery" in result.stdout
    assert "specific_unlocks: included" in result.stdout
    assert "hidden: buildings" in result.stdout
