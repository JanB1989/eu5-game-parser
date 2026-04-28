import json
import os
import time
from pathlib import Path

import polars as pl
import pytest
from typer.testing import CliRunner

from eu5gameparser.cli import app
from eu5gameparser.domain.buildings import BuildingData
from eu5gameparser.domain.eu5 import load_eu5_data
from eu5gameparser.savegame import (
    is_text_save,
    latest_save_path,
    load_savegame_tables,
    write_savegame_explorer_html,
    write_savegame_parquet,
)
from eu5gameparser.savegame.exporter import _population_flow_table

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "eu5"
SAVE_FIXTURE = Path(__file__).parent / "fixtures" / "savegames" / "minimal_text_save.eu5"


def test_latest_save_path_returns_newest_file(tmp_path: Path) -> None:
    older = tmp_path / "older.eu5"
    newer = tmp_path / "newer.eu5"
    older.write_bytes(b"SAV\nmetadata={}")
    newer.write_bytes(b"SAV\nmetadata={}")
    now = time.time()
    os.utime(older, (now - 10, now - 10))
    os.utime(newer, (now, now))

    assert latest_save_path(tmp_path) == newer.resolve()


def test_latest_save_path_returns_none_for_missing_or_empty_dir(tmp_path: Path) -> None:
    assert latest_save_path(tmp_path / "missing") is None
    assert latest_save_path(tmp_path) is None


def test_is_text_save_detects_sav_header_and_metadata() -> None:
    assert is_text_save(SAVE_FIXTURE)


def test_load_savegame_tables_from_text_fixture(tmp_path: Path) -> None:
    data = _fixture_eu5_data(tmp_path)

    tables = load_savegame_tables(save_path=SAVE_FIXTURE, eu5_data=data)

    assert tables.save_metadata.item(0, "date") == "1337.1.1"
    assert tables.markets.height == 1
    assert tables.market_goods.height == 3
    assert tables.market_good_bucket_flows.height == 11
    assert tables.locations.height == 3
    assert tables.buildings.height == 1
    assert tables.building_methods.height == 2
    assert tables.rgo_flows.height == 2
    assert tables.production_method_population_flows.height == 4
    building = tables.buildings.row(0, named=True)
    assert building["building_type"] == "mason"
    assert building["market_id"] == 1
    assert building["active_method_ids"] == ["masonry_rework", "stone_bricks"]

    masonry = tables.market_goods.filter(pl.col("good_id") == "masonry").row(0, named=True)
    assert masonry["default_price"] == 8.0
    assert masonry["net"] == 8.0
    assert masonry["supplied_Production"] == 10.0
    assert masonry["supplied_Trade"] == 1.0
    assert masonry["demanded_Pops"] == 2.0
    assert masonry["demanded_Trade"] == 1.0


def test_production_method_flows_reconcile_to_save_buckets(tmp_path: Path) -> None:
    data = _fixture_eu5_data(tmp_path)

    tables = load_savegame_tables(save_path=SAVE_FIXTURE, eu5_data=data)

    checks = tables.accounting_checks
    assert checks.height == 3
    assert checks["status"].to_list() == ["ok"] * checks.height
    assert checks.select(pl.col("delta").abs().max()).item() < 1e-6
    assert tables.production_method_good_flows.height == 6
    assert tables.rgo_flows.height == 2

    masonry_output = tables.production_method_good_flows.filter(
        (pl.col("good_id") == "masonry")
        & (pl.col("direction") == "output")
        & (pl.col("production_method") == "stone_bricks")
    ).row(0, named=True)
    assert masonry_output["production_method"] == "stone_bricks"
    assert masonry_output["nominal_amount"] == 1.0
    assert masonry_output["allocated_amount"] == pytest.approx(6.25)
    assert masonry_output["allocation_factor"] == pytest.approx(6.25)
    assert masonry_output["building_count"] == 1
    assert masonry_output["level_sum"] == 2.0

    masonry_rework_output = tables.production_method_good_flows.filter(
        (pl.col("good_id") == "masonry")
        & (pl.col("direction") == "output")
        & (pl.col("production_method") == "masonry_rework")
    ).row(0, named=True)
    assert masonry_rework_output["nominal_amount"] == 0.6
    assert masonry_rework_output["allocated_amount"] == pytest.approx(3.75)

    stone_input = tables.production_method_good_flows.filter(
        (pl.col("good_id") == "stone") & (pl.col("direction") == "input")
    ).row(0, named=True)
    assert stone_input["nominal_amount"] == 0.8
    assert stone_input["allocated_amount"] == 8.0

    clay_input = tables.production_method_good_flows.filter(
        (pl.col("good_id") == "clay") & (pl.col("direction") == "input")
    ).row(0, named=True)
    assert clay_input["production_method"] == "unattributed building demand"
    assert clay_input["allocated_amount"] == 2.0

    clay_rgos = tables.rgo_flows.filter(pl.col("good_id") == "clay")
    assert set(clay_rgos["location_slug"].to_list()) == {"norrtalje", "uppsala"}
    assert clay_rgos.select(pl.col("allocated_amount").sum()).item() == 6.0


def test_population_flows_split_building_employment_by_method_value(tmp_path: Path) -> None:
    data = _fixture_eu5_data(tmp_path)

    tables = load_savegame_tables(save_path=SAVE_FIXTURE, eu5_data=data)

    population = tables.production_method_population_flows
    building_population = population.filter(pl.col("source_kind") == "building")
    assert building_population.select(pl.col("employed_total").sum()).item() == pytest.approx(2.0)

    stone_bricks = building_population.filter(
        pl.col("production_method") == "stone_bricks"
    ).row(0, named=True)
    masonry_rework = building_population.filter(
        pl.col("production_method") == "masonry_rework"
    ).row(0, named=True)

    assert stone_bricks["good_id"] == "masonry"
    assert stone_bricks["allocation_basis"] == "output_value"
    assert stone_bricks["employment_share"] == pytest.approx(0.625)
    assert stone_bricks["employed_total"] == pytest.approx(1.25)
    assert stone_bricks["employed_laborers"] == pytest.approx(1.25)
    assert stone_bricks["employed_nobles"] == 0.0

    assert masonry_rework["employment_share"] == pytest.approx(0.375)
    assert masonry_rework["employed_total"] == pytest.approx(0.75)
    assert masonry_rework["employed_laborers"] == pytest.approx(0.75)

    rgo_population = population.filter(pl.col("source_kind") == "rgo")
    assert rgo_population.select(pl.col("employed_total").sum()).item() == pytest.approx(2.0)
    assert rgo_population.select(pl.col("employed_laborers").sum()).item() == pytest.approx(2.0)


def test_population_flow_value_split_uses_one_third_two_thirds() -> None:
    building_data = BuildingData(
        categories=pl.DataFrame(),
        buildings=pl.DataFrame(
            [{"name": "test_workshop", "pop_type": "laborers"}],
            schema={"name": pl.String, "pop_type": pl.String},
        ),
        production_methods=pl.DataFrame(
            [
                {"name": "low_value", "produced": "widgets", "output": 1.0},
                {"name": "high_value", "produced": "widgets", "output": 2.0},
            ],
            schema={"name": pl.String, "produced": pl.String, "output": pl.Float64},
        ),
        goods_flow_nodes=pl.DataFrame(),
        goods_flow_edges=pl.DataFrame(),
        unresolved_production_methods=pl.DataFrame(),
        duplicate_production_methods=pl.DataFrame(),
    )
    buildings = pl.DataFrame(
        [
            {
                "building_id": 1,
                "building_type": "test_workshop",
                "location_id": 10,
                "location_slug": "test_location",
                "market_id": 7,
                "level": 1.0,
                "employed": 1.0,
            }
        ]
    )
    building_methods = pl.DataFrame(
        [
            {
                "building_id": 1,
                "building_type": "test_workshop",
                "location_id": 10,
                "location_slug": "test_location",
                "market_id": 7,
                "production_method": "low_value",
            },
            {
                "building_id": 1,
                "building_type": "test_workshop",
                "location_id": 10,
                "location_slug": "test_location",
                "market_id": 7,
                "production_method": "high_value",
            },
        ]
    )
    market_goods = pl.DataFrame(
        [{"market_id": 7, "good_id": "widgets", "price": 3.0, "default_price": 1.0}]
    )

    population = _population_flow_table(
        building_data,
        pl.DataFrame(),
        buildings,
        building_methods,
        market_goods,
    )

    low = population.filter(pl.col("production_method") == "low_value").row(0, named=True)
    high = population.filter(pl.col("production_method") == "high_value").row(0, named=True)
    assert low["employment_share"] == pytest.approx(1.0 / 3.0)
    assert high["employment_share"] == pytest.approx(2.0 / 3.0)
    assert low["employed_laborers"] == pytest.approx(1.0 / 3.0)
    assert high["employed_laborers"] == pytest.approx(2.0 / 3.0)


def test_write_savegame_parquet_writes_all_tables(tmp_path: Path) -> None:
    data = _fixture_eu5_data(tmp_path)
    output = tmp_path / "savegame"

    write_savegame_parquet(output, save_path=SAVE_FIXTURE, eu5_data=data)

    expected = {
        "save_metadata",
        "markets",
        "market_goods",
        "market_good_bucket_flows",
        "locations",
        "buildings",
        "building_methods",
        "rgo_flows",
        "production_method_good_flows",
        "production_method_population_flows",
        "accounting_checks",
    }
    assert {path.stem for path in output.glob("*.parquet")} == expected
    for name in expected:
        assert pl.read_parquet(output / f"{name}.parquet").height >= 1


def test_write_savegame_explorer_html_embeds_market_graph_data(tmp_path: Path) -> None:
    data = _fixture_eu5_data(tmp_path)
    tables = load_savegame_tables(save_path=SAVE_FIXTURE, eu5_data=data)

    path = write_savegame_explorer_html(tables, tmp_path / "savegame_explorer.html")
    html = path.read_text(encoding="utf-8")

    assert path.exists()
    assert "EU5 Savegame Market Explorer" in html
    assert "cytoscape.min.js" in html
    assert "Overview" in html
    assert "Good Flow" in html
    assert "const payload =" in html
    assert '"good_id": "masonry"' in html
    assert '"production_method": "stone_bricks"' in html
    assert "function graphElements" in html
    assert "function renderOverviewGraph" in html
    assert 'id="goodsHeaderRow"' in html
    assert "let overviewSort = { key: \"net\", direction: \"desc\", absolute: true }" in html
    assert "function formatOverviewNumber" in html
    assert "maximumFractionDigits: 0" in html
    assert "td.title = exactNumberTitle(row[column.key])" in html
    assert "font-variant-numeric: tabular-nums" in html
    assert "td:first-child" in html
    assert "function compareOverviewRows" in html
    assert "function sortedOverviewRows" in html
    assert "function setOverviewSort" in html
    assert "function renderTableHeader" in html
    assert "sort-header" in html
    assert "sort-indicator" in html
    assert "location_count" in html
    payload = _embedded_payload(html)
    assert "bucketFlows" in payload
    assert "rgoFlows" in payload
    assert "employed_laborers" in payload["goods"][0]
    assert any(row.get("employed_laborers", 0) > 0 for row in payload["marketGoods"])
    assert any(row.get("employed_total", 0) > 0 for row in payload["flows"])
    assert _graph_supply(payload, "masonry") == 11.0
    assert _graph_demand(payload, "masonry") == 3.0
    assert any(row.get("building_count") == 1 for row in payload["flows"])
    assert any(row.get("location_slug") == "norrtalje" for row in payload["rgoFlows"])
    assert any(row.get("location_slug") == "uppsala" for row in payload["rgoFlows"])
    clay_rgos = _rgo_graph_rows(payload, "clay")
    assert len(clay_rgos) == 1
    assert clay_rgos[0]["allocated_amount"] == 6.0
    assert clay_rgos[0]["location_count"] == 2
    assert clay_rgos[0]["rgo_employed"] == 2.0
    assert clay_rgos[0]["employed_laborers"] == 2.0
    assert clay_rgos[0]["max_raw_material_workers"] == 2.0


def test_savegame_cli_writes_parquet_tables(tmp_path: Path) -> None:
    output = tmp_path / "cli_savegame"
    result = CliRunner().invoke(
        app,
        [
            "savegame",
            "--save",
            str(SAVE_FIXTURE),
            "--output",
            str(output),
            "--load-order",
            str(_load_order_file(tmp_path)),
            "--profile",
            "vanilla",
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert "Savegame" in result.stdout
    assert "explorer:" in result.stdout
    assert "market_goods: 3" in result.stdout
    assert (output / "production_method_good_flows.parquet").exists()
    assert (output / "savegame_explorer.html").exists()
    assert (output / "market_good_bucket_flows.parquet").exists()
    assert (output / "rgo_flows.parquet").exists()
    assert (output / "production_method_population_flows.parquet").exists()
    assert pl.read_parquet(output / "accounting_checks.parquet")["status"].to_list() == ["ok"] * 3


@pytest.mark.integration
def test_latest_real_save_integration_when_available(tmp_path: Path) -> None:
    save = latest_save_path()
    if save is None:
        pytest.skip("No local EU5 saves available.")
    if not is_text_save(save):
        pytest.skip("Latest local save is not text-format.")

    output = tmp_path / "real_save"
    result = CliRunner().invoke(
        app,
        [
            "savegame",
            "--save",
            str(save),
            "--output",
            str(output),
        ],
    )

    assert result.exit_code == 0, result.stdout
    checks = pl.read_parquet(output / "accounting_checks.parquet")
    assert checks.height > 0
    assert set(checks["status"].unique().to_list()) == {"ok"}
    assert pl.read_parquet(output / "market_goods.parquet").height > 0
    assert pl.read_parquet(output / "buildings.parquet").height > 0


def _load_order_file(tmp_path: Path) -> Path:
    path = tmp_path / "load_order.toml"
    path.write_text(
        f"""
[paths]
vanilla_root = "{FIXTURE_ROOT.as_posix()}"

[profiles]
vanilla = ["vanilla"]
""".strip(),
        encoding="utf-8",
    )
    return path


def _fixture_eu5_data(tmp_path: Path):
    return load_eu5_data(profile="vanilla", load_order_path=_load_order_file(tmp_path))


def _embedded_payload(html: str) -> dict:
    marker = "    const payload = "
    start = html.index(marker) + len(marker)
    end = html.index(";\n    const goods", start)
    return json.loads(html[start:end])


def _graph_supply(payload: dict, good: str, market_id: int | None = None) -> float:
    return sum(
        row["amount"]
        for row in payload["bucketFlows"]
        if row["good_id"] == good
        and row["direction"] == "supply"
        and (market_id is None or row["market_id"] == market_id)
    )


def _graph_demand(payload: dict, good: str, market_id: int | None = None) -> float:
    return sum(
        row["amount"]
        for row in payload["bucketFlows"]
        if row["good_id"] == good
        and row["direction"] == "demand"
        and (market_id is None or row["market_id"] == market_id)
    )


def _rgo_graph_rows(payload: dict, good: str, market_id: int | None = None) -> list[dict]:
    grouped: dict[str, dict] = {}
    for row in payload["rgoFlows"]:
        if row["good_id"] != good or (market_id is not None and row["market_id"] != market_id):
            continue
        current = grouped.setdefault(
            row["good_id"],
            {
                "good_id": row["good_id"],
                "allocated_amount": 0.0,
                "nominal_amount": 0.0,
                "rgo_employed": 0.0,
                "employed_laborers": 0.0,
                "max_raw_material_workers": 0.0,
                "location_count": 0,
            },
        )
        current["allocated_amount"] += row.get("allocated_amount") or 0.0
        current["nominal_amount"] += row.get("nominal_amount") or 0.0
        current["rgo_employed"] += row.get("rgo_employed") or 0.0
        current["employed_laborers"] += row.get("employed_laborers") or 0.0
        current["max_raw_material_workers"] += row.get("max_raw_material_workers") or 0.0
        current["location_count"] += 1
    return sorted(grouped.values(), key=lambda item: abs(item["allocated_amount"]), reverse=True)
