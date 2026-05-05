from __future__ import annotations

from pathlib import Path

import polars as pl
from typer.testing import CliRunner

from eu5gameparser.cli import app
from eu5gameparser.load_order import DataProfile
from eu5gameparser.savegame import notebook_analysis as ana
from eu5gameparser.savegame import notebook_dataset
from eu5gameparser.savegame import notebook_workbench as wb
from eu5gameparser.savegame.notebook_dataset import (
    SavegameNotebookDataset,
    build_savegame_notebook_dataset,
    rank_groups,
)


def test_notebook_builder_compacts_snapshot_tables_and_defaults_latest_playthrough(
    tmp_path: Path,
) -> None:
    source = tmp_path / "dataset"
    _write_manifest(
        source,
        [
            _manifest_row("aaa", "s1", mtime_ns=10),
            _manifest_row("bbb", "s2", mtime_ns=20),
        ],
    )
    _write_table(
        source,
        "market_goods",
        "aaa",
        "s1",
        [
            {
                **_snapshot("aaa", "s1"),
                "market_id": 1,
                "market_center_slug": "london",
                "market_name": "London",
                "good_id": "grain",
                "good_name": "Grain",
                "goods_category": "food",
                "price": 1.25,
                "supply": 100.0,
                "demand": 90.0,
                "junk_column": "drop me",
            }
        ],
    )
    _write_table(
        source,
        "market_goods",
        "bbb",
        "s2",
        [
            {
                **_snapshot("bbb", "s2"),
                "market_id": 2,
                "market_center_slug": "paris",
                "market_name": "Paris",
                "good_id": "iron",
                "good_name": "Iron",
                "goods_category": "raw_material",
                "price": 2.5,
                "supply": 50.0,
                "demand": 75.0,
                "junk_column": "drop me too",
            }
        ],
    )

    result = build_savegame_notebook_dataset(source, tmp_path / "notebooks" / "data")

    dataset = SavegameNotebookDataset(result.output)
    assert result.snapshots == 2
    assert dataset.latest_playthrough() == "bbb"
    assert dataset.snapshots().select("playthrough_id", "date_sort").to_dicts() == [
        {"playthrough_id": "aaa", "date_sort": 13370101},
        {"playthrough_id": "bbb", "date_sort": 13370101},
    ]
    goods = dataset.dim("goods")
    assert set(goods["good_id"]) == {"grain", "iron"}
    facts = dataset.scan_fact("market_goods", playthrough_id="bbb").collect()
    assert facts.height == 1
    assert "good_code" in facts.columns
    assert "market_code" in facts.columns
    assert "good_id" not in facts.columns
    assert "junk_column" not in facts.columns
    assert facts.schema["price"] == pl.Float32


def test_notebook_builder_writes_readable_labels_from_load_order(tmp_path: Path) -> None:
    source = tmp_path / "dataset"
    _write_manifest(source, [_manifest_row("aaa", "s1", mtime_ns=10)])
    _write_table(
        source,
        "locations",
        "aaa",
        "s1",
        [
            {
                **_snapshot("aaa", "s1"),
                "location_id": 10,
                "slug": "london",
                "province_slug": "middlesex",
                "area": "thames_area",
                "region": "england_region",
                "macro_region": "britain_macro_region",
                "super_region": "europe_super_region",
                "country_tag": "ENG",
                "market_id": 1,
                "total_population": 100.0,
            }
        ],
    )
    _write_table(
        source,
        "market_goods",
        "aaa",
        "s1",
        [_market_good("aaa", "s1", 1337, 1, "london", "grain", 10, 8, 2)],
    )
    _write_table(
        source,
        "market_food",
        "aaa",
        "s1",
        [
            {
                **_snapshot("aaa", "s1"),
                "market_id": 1,
                "center_location_id": 10,
                "market_center_slug": "london",
                "food": 50.0,
                "food_max": 100.0,
            }
        ],
    )
    _write_table(
        source,
        "building_methods",
        "aaa",
        "s1",
        [_building_method("aaa", "s1", 1337, 1, 10, "bakery", "pm_bake")],
    )
    load_order = _write_load_order(
        tmp_path,
        vanilla_localization={
            "grain": "Vanilla Grain",
            "bakery": "Vanilla Bakery",
            "pm_bake": "Vanilla Bake",
            "london": "Vanilla London",
        },
        mod_localization={
            "grain": "Fancy Grain",
            "bakery": "Bake House",
            "pm_bake": "$bakery$ Packing",
            "london": "London",
            "thames_area": "Thames",
            "england_region": "England",
            "ENG": "England",
        },
    )

    dataset = SavegameNotebookDataset(
        build_savegame_notebook_dataset(
            source,
            tmp_path / "notebooks" / "data",
            profile="constructor",
            load_order_path=load_order,
        ).output
    )

    assert dataset.dim("goods").item(0, "good_label") == "Fancy Grain"
    assert dataset.dim("building_types").item(0, "building_label") == "Bake House"
    assert dataset.dim("production_methods").item(0, "production_method_label") == "Bake House Packing"
    assert dataset.dim("locations").item(0, "location_label") == "London"
    assert dataset.dim("locations").item(0, "region_label") == "England"
    assert dataset.dim("markets").item(0, "market_label") == "London"
    assert dataset.dim("markets").item(0, "center_location_id") == 10
    assert dataset.dim("countries").item(0, "country_label") == "England"

    matches = ana.search_dimension(dataset, "goods", "fancy")
    assert matches.item(0, "good_id") == "grain"
    assert ana.resolve_codes(dataset, "goods", values="Fancy Grain") == [0]
    assert ana.resolve_codes(dataset, "markets", values="London") == [0]


def test_rank_groups_uses_raw_rows_for_sum_mean_and_median() -> None:
    frame = pl.DataFrame(
        {
            "group": ["a", "a", "b", "c", "c", "c"],
            "value": [100.0, 0.0, 51.0, 2.0, 4.0, 99.0],
        }
    ).lazy()

    sums = rank_groups(frame, group_by="group", metric="value", statistic="sum").collect()
    means = rank_groups(frame, group_by="group", metric="value", statistic="mean").collect()
    medians = rank_groups(frame, group_by="group", metric="value", statistic="median").collect()

    assert sums.row(0, named=True) == {"group": "c", "value": 105.0}
    assert means.row(0, named=True) == {"group": "b", "value": 51.0}
    assert medians.filter(pl.col("group") == "c").item(0, "value") == 4.0


def test_dataset_returns_typed_empty_snapshots_when_data_is_missing(tmp_path: Path) -> None:
    dataset = SavegameNotebookDataset(tmp_path / "missing")

    snapshots = dataset.snapshots()

    assert snapshots.is_empty()
    assert {"playthrough_id", "snapshot_id", "date_sort"}.issubset(snapshots.columns)
    assert dataset.latest_playthrough() is None


def test_goods_source_and_sink_scans_use_partitioned_flow_tables(tmp_path: Path) -> None:
    source = tmp_path / "dataset"
    _write_manifest(source, [_manifest_row("aaa", "s1", mtime_ns=10)])
    _write_table(
        source,
        "rgo_flows",
        "aaa",
        "s1",
        [
            {
                **_snapshot("aaa", "s1"),
                "market_id": 1,
                "market_center_slug": "london",
                "good_id": "grain",
                "good_name": "Grain",
                "goods_category": "food",
                "location_id": 10,
                "raw_material": "grain",
                "direction": "source",
                "allocated_amount": 7.0,
            }
        ],
    )
    _write_table(
        source,
        "production_method_good_flows",
        "aaa",
        "s1",
        [
            {
                **_snapshot("aaa", "s1"),
                "market_id": 1,
                "market_center_slug": "london",
                "good_id": "grain",
                "good_name": "Grain",
                "goods_category": "food",
                "production_method": "pm_grain_farm",
                "building_type": "grain_farm",
                "location_id": 10,
                "direction": "source",
                "allocated_amount": 3.0,
            },
            {
                **_snapshot("aaa", "s1"),
                "market_id": 1,
                "market_center_slug": "london",
                "good_id": "grain",
                "good_name": "Grain",
                "goods_category": "food",
                "production_method": "pm_bakery",
                "building_type": "bakery",
                "location_id": 10,
                "direction": "sink",
                "allocated_amount": 2.0,
            },
        ],
    )
    _write_table(
        source,
        "market_good_bucket_flows",
        "aaa",
        "s1",
        [
            {
                **_snapshot("aaa", "s1"),
                "market_id": 1,
                "market_center_slug": "london",
                "good_id": "grain",
                "good_name": "Grain",
                "goods_category": "food",
                "direction": "sink",
                "bucket": "Building",
                "amount": 6.0,
            }
        ],
    )

    output = tmp_path / "notebooks" / "data"
    build_savegame_notebook_dataset(source, output)
    dataset = SavegameNotebookDataset(output)

    sources = dataset.scan_good_sources("grain", playthrough_id="aaa").collect()
    sinks = dataset.scan_good_sinks("grain", playthrough_id="aaa").collect()

    assert set(sources["flow_table"]) == {"rgo", "production_method"}
    assert sources["allocated_amount"].sum() == 10.0
    assert set(sinks["flow_table"]) == {"market_bucket", "production_method"}
    assert sinks.select(pl.sum_horizontal("amount", "allocated_amount").sum()).item() == 8.0
    assert (output / "facts" / "rgo_flows" / "playthrough_id=aaa" / "good_code=0").is_dir()


def test_analysis_helpers_scope_overlapping_playthroughs_and_location_stats(tmp_path: Path) -> None:
    source = tmp_path / "dataset"
    _write_manifest(
        source,
        [
            _manifest_row("aaa", "a1", mtime_ns=10, year=1337),
            _manifest_row("aaa", "a2", mtime_ns=20, year=1342),
            _manifest_row("bbb", "b1", mtime_ns=30, year=1337),
            _manifest_row("bbb", "b2", mtime_ns=40, year=1342),
        ],
    )
    _write_location_snapshot(source, "aaa", "a1", 1337, [("north", 10.0), ("south", 30.0)])
    _write_location_snapshot(source, "aaa", "a2", 1342, [("north", 25.0), ("south", 5.0)])
    _write_location_snapshot(source, "bbb", "b1", 1337, [("north", 1000.0), ("south", 1000.0)])
    _write_location_snapshot(source, "bbb", "b2", 1342, [("north", 2000.0), ("south", 2000.0)])

    dataset = SavegameNotebookDataset(build_savegame_notebook_dataset(source, tmp_path / "out").output)

    assert ana.search_dimension(dataset, "regions", "north").item(0, "region") == "north"
    assert ana.resolve_codes(dataset, "regions", query="north") == [0]

    series = ana.location_time_series(
        dataset,
        playthrough_id="aaa",
        group_by="region",
        metric="total_population",
    ).collect()
    north = series.filter(pl.col("region_label") == "North").sort("date_sort")
    assert north["total_population"].to_list() == [10.0, 25.0]
    assert north["year"].to_list() == [1337, 1342]
    assert series["total_population"].max() < 1000.0

    latest = ana.location_latest_rank(
        dataset,
        playthrough_id="aaa",
        group_by="region",
        metric="total_population",
    ).collect()
    assert latest.row(0, named=True) == {"region_label": "North", "total_population": 25.0}

    delta = ana.location_first_last_delta(
        dataset,
        playthrough_id="aaa",
        group_by="region",
        metric="total_population",
    ).collect()
    assert delta.filter(pl.col("region_label") == "South").item(0, "delta") == -25.0


def test_analysis_goods_market_and_food_helpers(tmp_path: Path) -> None:
    source = tmp_path / "dataset"
    _write_manifest(
        source,
        [
            _manifest_row("aaa", "s1", mtime_ns=10, year=1337),
            _manifest_row("aaa", "s2", mtime_ns=20, year=1342),
        ],
    )
    _write_table(
        source,
        "market_goods",
        "aaa",
        "s1",
        [
            _market_good("aaa", "s1", 1337, 1, "london", "grain", 10, 20, 2, stockpile=5),
            _market_good("aaa", "s1", 1337, 2, "paris", "grain", 5, 0, 4, stockpile=1),
            _market_good("aaa", "s1", 1337, 1, "london", "iron", 30, 10, 3, stockpile=0),
            _market_good("aaa", "s1", 1337, 1, "london", "spice", 1, 1, 1000, stockpile=0),
        ],
    )
    _write_table(
        source,
        "market_goods",
        "aaa",
        "s2",
        [
            _market_good("aaa", "s2", 1342, 1, "london", "grain", 40, 20, 5, stockpile=10),
            _market_good("aaa", "s2", 1342, 2, "paris", "grain", 10, 0, 6, stockpile=2),
        ],
    )
    _write_table(
        source,
        "market_food",
        "aaa",
        "s1",
        [
            {
                **_snapshot("aaa", "s1", year=1337),
                "market_id": 1,
                "market_center_slug": "london",
                "food": 50.0,
                "food_max": 100.0,
                "food_price": 2.0,
                "food_balance": -5.0,
                "population": 1000.0,
            }
        ],
    )
    _write_table(
        source,
        "market_food",
        "aaa",
        "s2",
        [
            {
                **_snapshot("aaa", "s2", year=1342),
                "market_id": 1,
                "market_center_slug": "london",
                "food": 75.0,
                "food_max": 100.0,
                "food_price": 3.0,
                "food_balance": 5.0,
                "population": 1100.0,
            }
        ],
    )

    dataset = SavegameNotebookDataset(build_savegame_notebook_dataset(source, tmp_path / "out").output)

    goods = ana.goods_global_time_series(dataset, playthrough_id="aaa", goods=["grain"]).collect()
    assert goods.filter(pl.col("date_sort") == 13370101).item(0, "net") == -5.0
    assert goods.filter(pl.col("date_sort") == 13420101).item(0, "mean_price") == 5.5

    scarcity = ana.market_shortage_glut(dataset, playthrough_id="aaa", goods="grain").collect()
    zero_demand = scarcity.filter(pl.col("market_id") == 2).item(0, "supply_demand_ratio")
    assert zero_demand is None

    buckets = ana.goods_imbalance_buckets(dataset, playthrough_id="aaa", bucket_years=25).collect()
    grain_bucket = buckets.filter(pl.col("good_id") == "grain").select("supply", "demand", "net").row(0)
    assert grain_bucket == (65.0, 40.0, 25.0)
    assert set(buckets["good_id"].to_list()) == {"grain", "iron", "spice"}
    assert buckets.item(0, "good_id") == "grain"
    by_market_cap = ana.goods_imbalance_buckets(
        dataset,
        playthrough_id="aaa",
        bucket_years=25,
        sort_by="mean_market_cap",
    ).collect()
    assert by_market_cap.item(0, "good_id") == "spice"

    proxy = ana.market_flow_proxy(dataset, playthrough_id="aaa", group_by="good").collect()
    grain_proxy = proxy.filter((pl.col("date_sort") == 13420101) & (pl.col("good_label") == "Grain"))
    assert grain_proxy.item(0, "flow_value") == 100.0
    assert grain_proxy.item(0, "year") == 1342

    food = ana.food_global_stockpile(dataset, playthrough_id="aaa").collect()
    assert food.filter(pl.col("date_sort") == 13370101).item(0, "food_fill_ratio") == 0.5
    food_delta = ana.food_first_last_delta(
        dataset,
        playthrough_id="aaa",
        metric="food_fill_ratio",
        group_by="market",
    ).collect()
    assert food_delta.item(0, "delta") == 0.25


def test_analysis_goods_flows_and_pm_values(tmp_path: Path) -> None:
    source = tmp_path / "dataset"
    _write_manifest(source, [_manifest_row("aaa", "s1", mtime_ns=10)])
    _write_table(
        source,
        "market_goods",
        "aaa",
        "s1",
        [_market_good("aaa", "s1", 1337, 1, "london", "grain", 10, 9, 2, default_price=1.5)],
    )
    _write_table(
        source,
        "rgo_flows",
        "aaa",
        "s1",
        [
            {
                **_snapshot("aaa", "s1"),
                "market_id": 1,
                "market_center_slug": "london",
                "good_id": "grain",
                "good_name": "Grain",
                "goods_category": "food",
                "location_id": 10,
                "raw_material": "grain",
                "direction": "output",
                "allocated_amount": 7.0,
            }
        ],
    )
    _write_table(
        source,
        "production_method_good_flows",
        "aaa",
        "s1",
        [
            _pm_flow("aaa", "s1", "output", 3.0),
            _pm_flow("aaa", "s1", "input", 2.0),
        ],
    )
    _write_table(
        source,
        "market_good_bucket_flows",
        "aaa",
        "s1",
        [
            {
                **_snapshot("aaa", "s1"),
                "market_id": 1,
                "market_center_slug": "london",
                "good_id": "grain",
                "good_name": "Grain",
                "goods_category": "food",
                "direction": "demand",
                "bucket": "Building",
                "amount": 6.0,
            }
        ],
    )

    dataset = SavegameNotebookDataset(build_savegame_notebook_dataset(source, tmp_path / "out").output)

    sources = ana.good_flow_breakdown(
        dataset,
        good="grain",
        direction="source",
        playthrough_id="aaa",
        group_by="flow_table",
    ).collect()
    assert sources.filter(pl.col("flow_table") == "rgo").item(0, "amount") == 7.0
    assert sources.filter(pl.col("flow_table") == "production_method").item(0, "amount") == 3.0

    sinks = ana.good_flow_breakdown(
        dataset,
        good="grain",
        direction="sink",
        playthrough_id="aaa",
        group_by="flow_table",
    ).collect()
    assert sinks.filter(pl.col("flow_table") == "market_bucket").item(0, "amount") == 6.0
    assert sinks.filter(pl.col("flow_table") == "production_method").item(0, "amount") == 2.0

    values = ana.pm_value_mix(
        dataset,
        playthrough_id="aaa",
        good="grain",
        building_query="bakery",
        pm_query="pm_bake",
    ).collect()
    assert values["amount"].sum() == 5.0
    assert values["market_value"].sum() == 10.0
    assert values["default_value"].sum() == 7.5


def test_good_consumption_helpers_combine_bucket_and_pm_flows_and_filter_market(tmp_path: Path) -> None:
    source = tmp_path / "dataset"
    _write_manifest(source, [_manifest_row("aaa", "s1", mtime_ns=10)])
    _write_table(
        source,
        "market_good_bucket_flows",
        "aaa",
        "s1",
        [
            {
                **_snapshot("aaa", "s1"),
                "market_id": 1,
                "market_center_slug": "london",
                "good_id": "grain",
                "good_name": "Grain",
                "goods_category": "food",
                "direction": "demand",
                "bucket": "Building",
                "amount": 6.0,
            },
            {
                **_snapshot("aaa", "s1"),
                "market_id": 2,
                "market_center_slug": "paris",
                "good_id": "grain",
                "good_name": "Grain",
                "goods_category": "food",
                "direction": "demand",
                "bucket": "Building",
                "amount": 5.0,
            },
        ],
    )
    _write_table(
        source,
        "production_method_good_flows",
        "aaa",
        "s1",
        [_pm_flow("aaa", "s1", "input", 2.0)],
    )

    dataset = SavegameNotebookDataset(build_savegame_notebook_dataset(source, tmp_path / "out").output)

    global_consumption = ana.good_consumption_latest(
        dataset,
        good="grain",
        playthrough_id="aaa",
        group_by="bucket",
    ).collect()
    assert global_consumption.filter(pl.col("consumption_label") == "Building").item(0, "amount") == 11.0
    assert global_consumption.filter(pl.col("consumption_label") == "PM input: Pm Bake").item(0, "amount") == 2.0

    london_consumption = ana.good_consumption_latest(
        dataset,
        good="grain",
        playthrough_id="aaa",
        group_by="bucket",
        market_query="london",
    ).collect()
    assert london_consumption.filter(pl.col("consumption_label") == "Building").item(0, "amount") == 6.0
    assert london_consumption["amount"].sum() == 8.0

    by_market = ana.good_consumption_over_time(
        dataset,
        good="grain",
        playthrough_id="aaa",
        group_by="market",
    ).collect()
    assert set(by_market["market_label"].to_list()) == {"London", "Paris"}
    assert by_market["year"].to_list() == [1337, 1337]


def test_analysis_building_and_pm_helpers(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "dataset"
    _write_manifest(
        source,
        [
            _manifest_row("aaa", "s1", mtime_ns=10, year=1337),
            _manifest_row("aaa", "s2", mtime_ns=20, year=1342),
        ],
    )
    _write_location_snapshot(source, "aaa", "s1", 1337, [("north", 10.0), ("south", 10.0)])
    _write_location_snapshot(source, "aaa", "s2", 1342, [("north", 10.0), ("south", 10.0)])
    _write_table(
        source,
        "buildings",
        "aaa",
        "s1",
        [
            _building("aaa", "s1", 1337, 1, 10, "bakery", 2.0),
            _building("aaa", "s1", 1337, 2, 20, "bakery", 1.0),
        ],
    )
    _write_table(
        source,
        "buildings",
        "aaa",
        "s2",
        [
            _building("aaa", "s2", 1342, 1, 10, "bakery", 4.0),
            _building("aaa", "s2", 1342, 2, 20, "bakery", 2.0),
        ],
    )
    _write_table(
        source,
        "building_methods",
        "aaa",
        "s1",
        [
            _building_method("aaa", "s1", 1337, 1, 10, "bakery", "pm_bake_1"),
            _building_method("aaa", "s1", 1337, 2, 20, "bakery", "pm_bake_1"),
        ],
    )
    _write_table(
        source,
        "building_methods",
        "aaa",
        "s2",
        [
            _building_method("aaa", "s2", 1342, 1, 10, "bakery", "pm_bake_1"),
            _building_method("aaa", "s2", 1342, 2, 20, "bakery", "pm_bake_2"),
        ],
    )
    monkeypatch.setattr(
        notebook_dataset,
        "load_building_data",
        lambda **_: type(
            "BuildingData",
            (),
            {
                "production_methods": pl.DataFrame(
                    {
                        "name": ["pm_bake_1", "pm_bake_2"],
                        "building": ["bakery", "bakery"],
                        "production_method_group": ["bakery:0", "bakery:1"],
                        "production_method_group_index": [0, 1],
                    }
                )
            },
        )(),
    )

    dataset = SavegameNotebookDataset(
        build_savegame_notebook_dataset(
            source,
            tmp_path / "out",
            profile=DataProfile("constructor", ()),
        ).output
    )

    levels = ana.building_metric_time_series(
        dataset,
        playthrough_id="aaa",
        metric="level",
        group_by=["building_type", "region"],
        building_query="bakery",
    ).collect()
    assert levels.filter((pl.col("date_sort") == 13420101) & (pl.col("region_label") == "North")).item(0, "level") == 4.0

    adoption = ana.pm_adoption_over_time(
        dataset,
        playthrough_id="aaa",
        building_query="bakery",
    ).collect()
    assert adoption.filter((pl.col("date_sort") == 13420101) & (pl.col("production_method_label") == "Pm Bake 2")).item(0, "share") == 0.5
    assert adoption.filter(pl.col("date_sort") == 13420101).item(0, "year") == 1342

    regional = ana.pm_regional_preferences(
        dataset,
        playthrough_id="aaa",
        building_query="bakery",
    ).collect()
    assert regional.filter((pl.col("region_label") == "South") & (pl.col("production_method_label") == "Pm Bake 2")).item(0, "share") == 1.0

    slot_latest = ana.pm_slot_distribution_latest(
        dataset,
        playthrough_id="aaa",
        building_query="bakery",
    ).collect()
    assert slot_latest.filter(pl.col("slot_label") == "Slot 1").item(0, "share") == 1.0
    assert slot_latest.filter(pl.col("slot_label") == "Slot 2").item(0, "production_method_label") == "Pm Bake 2"

    slot_ts = ana.pm_slot_distribution_over_time(
        dataset,
        playthrough_id="aaa",
        building_query="bakery",
    ).collect()
    assert slot_ts.filter(pl.col("date_sort") == 13420101).item(0, "year") == 1342
    assert "slot_label" in slot_ts.columns


def test_workbench_pm_slot_views_ignore_pm_drilldown_search(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "dataset"
    _write_manifest(
        source,
        [
            _manifest_row("aaa", "s1", mtime_ns=10, year=1337),
            _manifest_row("aaa", "s2", mtime_ns=20, year=1342),
        ],
    )
    _write_location_snapshot(source, "aaa", "s1", 1337, [("north", 10.0), ("south", 10.0)])
    _write_location_snapshot(source, "aaa", "s2", 1342, [("north", 10.0), ("south", 10.0)])
    _write_table(
        source,
        "building_methods",
        "aaa",
        "s1",
        [
            _building_method("aaa", "s1", 1337, 1, 10, "cookery", "meal_a"),
            _building_method("aaa", "s1", 1337, 1, 10, "cookery", "drink_a"),
            _building_method("aaa", "s1", 1337, 1, 10, "cookery", "pack_none"),
            _building_method("aaa", "s1", 1337, 2, 20, "cookery", "meal_a"),
            _building_method("aaa", "s1", 1337, 2, 20, "cookery", "drink_a"),
            _building_method("aaa", "s1", 1337, 2, 20, "cookery", "pack_none"),
        ],
    )
    _write_table(
        source,
        "building_methods",
        "aaa",
        "s2",
        [
            _building_method("aaa", "s2", 1342, 1, 10, "cookery", "meal_b"),
            _building_method("aaa", "s2", 1342, 1, 10, "cookery", "drink_a"),
            _building_method("aaa", "s2", 1342, 1, 10, "cookery", "pack_box"),
            _building_method("aaa", "s2", 1342, 2, 20, "cookery", "meal_a"),
            _building_method("aaa", "s2", 1342, 2, 20, "cookery", "drink_b"),
            _building_method("aaa", "s2", 1342, 2, 20, "cookery", "pack_box"),
        ],
    )
    monkeypatch.setattr(
        notebook_dataset,
        "load_building_data",
        lambda **_: type(
            "BuildingData",
            (),
            {
                "production_methods": pl.DataFrame(
                    {
                        "name": ["meal_a", "meal_b", "drink_a", "drink_b", "pack_none", "pack_box"],
                        "building": ["cookery"] * 6,
                        "production_method_group": [
                            "cookery:0",
                            "cookery:0",
                            "cookery:1",
                            "cookery:1",
                            "cookery:2",
                            "cookery:2",
                        ],
                        "production_method_group_index": [0, 0, 1, 1, 2, 2],
                    }
                )
            },
        )(),
    )
    output = build_savegame_notebook_dataset(
        source,
        tmp_path / "out",
        profile=DataProfile("constructor", ()),
    ).output
    (tmp_path / "constructor.toml").write_text('name = "test"\n', encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    workbench = wb.open_workbench(
        wb.WorkbenchConfig(
            data_root=output,
            playthrough="aaa",
            building_search="cookery",
            pm_drilldown_search="pack",
            building_scope="macro_region",
        )
    )
    result = workbench.buildings()

    assert set(result.pm_slot_latest["slot_label"].to_list()) == {"Slot 1", "Slot 2", "Slot 3"}
    assert set(result.pm_slot_latest["production_method_label"].to_list()) == {
        "Meal A",
        "Meal B",
        "Drink A",
        "Drink B",
        "Pack Box",
    }
    slot_one = result.pm_slot_time_series.filter(
        (pl.col("date_sort") == 13420101) & (pl.col("slot_label") == "Slot 1")
    )
    assert slot_one["share"].to_list() == [0.5, 0.5]
    assert {"slot_label", "macro_region_label", "production_method_label"}.issubset(result.pm_preferences.columns)


def test_notebook_builder_adds_unslotted_pm_metadata_without_profile(tmp_path: Path) -> None:
    source = tmp_path / "dataset"
    _write_manifest(source, [_manifest_row("aaa", "s1", mtime_ns=10)])
    _write_table(
        source,
        "building_methods",
        "aaa",
        "s1",
        [_building_method("aaa", "s1", 1337, 1, 10, "bakery", "pm_bake")],
    )

    dataset = SavegameNotebookDataset(build_savegame_notebook_dataset(source, tmp_path / "out").output)

    methods = dataset.dim("production_methods")
    assert methods.item(0, "slot_label") == "Unslotted"
    assert methods.item(0, "production_method_group") is None


def test_savegame_notebooks_build_cli(tmp_path: Path) -> None:
    source = tmp_path / "dataset"
    _write_manifest(source, [_manifest_row("aaa", "s1", mtime_ns=10)])
    output = tmp_path / "notebook-data"

    result = CliRunner().invoke(
        app,
        [
            "savegame-notebooks",
            "build",
            "--dataset",
            str(source),
            "--output",
            str(output),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "snapshots: 1" in result.output
    assert (output / "snapshots.parquet").is_file()


def _manifest_row(
    playthrough_id: str,
    snapshot_id: str,
    *,
    mtime_ns: int,
    year: int = 1337,
    month: int = 1,
    day: int = 1,
) -> dict[str, object]:
    return {
        **_snapshot(playthrough_id, snapshot_id, year=year, month=month, day=day),
        "manifest_version": 1,
        "path": f"/saves/{snapshot_id}.eu5",
        "mtime_ns": mtime_ns,
        "size": 1000,
        "quick_hash": snapshot_id,
    }


def _snapshot(
    playthrough_id: str,
    snapshot_id: str,
    *,
    year: int = 1337,
    month: int = 1,
    day: int = 1,
) -> dict[str, object]:
    return {
        "snapshot_id": snapshot_id,
        "playthrough_id": playthrough_id,
        "source_path": f"/saves/{snapshot_id}.eu5",
        "date": f"{year}.{month}.{day}",
        "year": year,
        "month": month,
        "day": day,
    }


def _write_location_snapshot(
    root: Path,
    playthrough_id: str,
    snapshot_id: str,
    year: int,
    rows: list[tuple[str, float]],
) -> None:
    _write_table(
        root,
        "locations",
        playthrough_id,
        snapshot_id,
        [
            {
                **_snapshot(playthrough_id, snapshot_id, year=year),
                "location_id": index * 10,
                "slug": f"loc_{index}",
                "province_slug": f"province_{index}",
                "area": f"area_{region}",
                "region": region,
                "macro_region": "macro",
                "super_region": "super",
                "country_tag": f"C{index}",
                "market_id": index,
                "market_center_slug": f"market_{index}",
                "total_population": population,
                "development": population / 10,
                "tax": population / 20,
                "control": 0.5,
            }
            for index, (region, population) in enumerate(rows, start=1)
        ],
    )


def _market_good(
    playthrough_id: str,
    snapshot_id: str,
    year: int,
    market_id: int,
    market_slug: str,
    good_id: str,
    supply: float,
    demand: float,
    price: float,
    *,
    default_price: float = 1.0,
    stockpile: float = 0.0,
) -> dict[str, object]:
    return {
        **_snapshot(playthrough_id, snapshot_id, year=year),
        "market_id": market_id,
        "market_center_slug": market_slug,
        "market_name": market_slug.title(),
        "good_id": good_id,
        "good_name": good_id.title(),
        "goods_category": "food" if good_id == "grain" else "raw_material",
        "supply": supply,
        "demand": demand,
        "net": supply - demand,
        "price": price,
        "default_price": default_price,
        "stockpile": stockpile,
    }


def _pm_flow(
    playthrough_id: str,
    snapshot_id: str,
    direction: str,
    amount: float,
) -> dict[str, object]:
    return {
        **_snapshot(playthrough_id, snapshot_id),
        "market_id": 1,
        "market_center_slug": "london",
        "good_id": "grain",
        "good_name": "Grain",
        "goods_category": "food",
        "production_method": "pm_bake",
        "building_id": 100,
        "building_type": "bakery",
        "location_id": 10,
        "direction": direction,
        "allocated_amount": amount,
        "nominal_amount": amount,
        "level_sum": 1.0,
    }


def _building(
    playthrough_id: str,
    snapshot_id: str,
    year: int,
    building_id: int,
    location_id: int,
    building_type: str,
    level: float,
) -> dict[str, object]:
    return {
        **_snapshot(playthrough_id, snapshot_id, year=year),
        "building_id": building_id,
        "building_type": building_type,
        "location_id": location_id,
        "market_id": location_id,
        "level": level,
        "employment": level * 10,
        "last_months_profit": level * 2,
    }


def _building_method(
    playthrough_id: str,
    snapshot_id: str,
    year: int,
    building_id: int,
    location_id: int,
    building_type: str,
    production_method: str,
) -> dict[str, object]:
    return {
        **_snapshot(playthrough_id, snapshot_id, year=year),
        "building_id": building_id,
        "building_type": building_type,
        "location_id": location_id,
        "market_id": location_id,
        "production_method": production_method,
    }


def _write_manifest(root: Path, rows: list[dict[str, object]]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, infer_schema_length=None).write_parquet(root / "manifest.parquet")


def _write_table(
    root: Path,
    table: str,
    playthrough_id: str,
    snapshot_id: str,
    rows: list[dict[str, object]],
) -> None:
    path = root / "tables" / table / f"playthrough_id={playthrough_id}" / f"{snapshot_id}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, infer_schema_length=None).write_parquet(path)


def _write_load_order(
    root: Path,
    *,
    vanilla_localization: dict[str, str],
    mod_localization: dict[str, str],
) -> Path:
    vanilla = root / "vanilla"
    mod = root / "mod"
    vanilla_loc = vanilla / "game" / "main_menu" / "localization" / "english"
    mod_loc = mod / "main_menu" / "localization" / "english"
    vanilla_loc.mkdir(parents=True)
    mod_loc.mkdir(parents=True)
    _write_localization(vanilla_loc / "vanilla_l_english.yml", vanilla_localization)
    _write_localization(mod_loc / "mod_l_english.yml", mod_localization)

    load_order = root / "load_order.toml"
    load_order.write_text(
        "[paths]\n"
        f'vanilla_root = "{vanilla.as_posix()}"\n\n'
        "[[mods]]\n"
        'id = "constructor"\n'
        'name = "Constructor"\n'
        f'root = "{mod.as_posix()}"\n\n'
        "[profiles]\n"
        'constructor = ["vanilla", "constructor"]\n',
        encoding="utf-8",
    )
    return load_order


def _write_localization(path: Path, entries: dict[str, str]) -> None:
    lines = ["l_english:"]
    for key, value in entries.items():
        escaped = value.replace('"', r"\"")
        lines.append(f' {key}: "{escaped}"')
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
