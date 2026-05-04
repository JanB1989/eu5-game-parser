# ruff: noqa: E501

import os
import shutil
import time
from pathlib import Path

from typer.testing import CliRunner

import eu5gameparser.cli as cli_module
import eu5gameparser.savegame.dataset as dataset_module
from eu5gameparser.cli import app
from eu5gameparser.domain.eu5 import load_eu5_data
from eu5gameparser.savegame import (
    SavegameDataset,
    ingest_savegame_dataset,
    load_savegame_tables,
    parse_ingame_date,
    playthrough_id_from_path,
    scan_for_work,
    watch_savegame_dataset,
    write_savegame_progression_html,
)
from eu5gameparser.savegame.dataset import IngestResult
from eu5gameparser.savegame.hierarchy import load_location_hierarchy

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "eu5"
SAVE_FIXTURE = Path(__file__).parent / "fixtures" / "savegames" / "minimal_text_save.eu5"


def test_playthrough_id_from_autosave_and_manual_names() -> None:
    assert (
        playthrough_id_from_path("autosave_63a1e8ba-d746-47ff-8fc7-7c0c089e54c2_6.eu5")
        == "63a1e8ba_d746_47ff_8fc7_7c0c089e54c2"
    )
    assert playthrough_id_from_path("SP Observer 1482.eu5") == "SP_Observer_1482"


def test_parse_ingame_date() -> None:
    assert parse_ingame_date("1346.3.1") == (1346, 3, 1)
    assert parse_ingame_date("not-a-date") == (None, None, None)


def test_scan_for_work_dedupes_processed_state(tmp_path: Path) -> None:
    save_dir = tmp_path / "saves"
    save_dir.mkdir()
    save = save_dir / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.eu5"
    save.write_text(SAVE_FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")
    now = time.time() - 20
    os.utime(save, (now, now))

    work, skipped = scan_for_work(tmp_path / "dataset", save_dir=save_dir, min_file_age_seconds=0)

    assert work == [save.resolve()]
    assert skipped == []


def test_ingest_writes_partitioned_dataset_and_lazy_cubes(tmp_path: Path) -> None:
    save = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_1.eu5"
    save.write_text(SAVE_FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")
    now = time.time() - 20
    os.utime(save, (now, now))

    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save],
        profile="vanilla",
        load_order_path=_load_order_file(tmp_path),
        min_file_age_seconds=0,
        include_extended=True,
    )

    assert not result.failures
    assert len(result.processed) == 1
    dataset = SavegameDataset(tmp_path / "dataset")
    manifest = dataset.snapshots()
    assert manifest.height == 1
    assert manifest.item(0, "playthrough_id") == "aaaaaaaa_bbbb_cccc_dddd_eeeeeeeeeeee"
    assert dataset.table_files("market_goods")
    assert dataset.scan("market_goods").collect().height == 3
    payload = dataset.build_progression_cubes(top_n=5)
    explorer_rows = payload["explorer"]["rows"]
    assert payload["snapshots"]
    assert payload["schemaVersion"] == 2
    assert payload["overviewSeries"]["development"][0]["development"] == 40.0
    assert any(
        row["domain"] == "goods"
        and row["metric"] == "supply"
        and row["dimension"] == "good_id"
        and row["entity_key"] == "masonry"
        for row in explorer_rows
    )
    assert any(
        row["domain"] == "buildings"
        and row["metric"] == "level_sum"
        and row["dimension"] == "building_type"
        and row["entity_key"] == "mason"
        for row in explorer_rows
    )
    assert payload["payloadSummary"]["explorerRows"] == len(explorer_rows)


def test_ingest_treats_save_change_during_staging_as_transient(
    tmp_path: Path, monkeypatch
) -> None:
    save = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_1.eu5"
    save.write_text(SAVE_FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")
    original_copy2 = dataset_module.shutil.copy2

    def changing_copy(source, target):
        copied = original_copy2(source, target)
        Path(target).write_text(
            SAVE_FIXTURE.read_text(encoding="utf-8") + "\n# changed\n",
            encoding="utf-8",
        )
        return copied

    monkeypatch.setattr(dataset_module.shutil, "copy2", changing_copy)

    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save],
        profile="vanilla",
        load_order_path=_load_order_file(tmp_path),
        min_file_age_seconds=0,
    )

    assert not result.processed
    assert not result.failures
    assert result.transient
    assert result.transient[0]["type"] == "SaveChangedError"
    assert "busy" in result.transient[0]["error"]


def test_ingest_parses_staged_copy_when_source_is_renamed_after_copy(
    tmp_path: Path, monkeypatch
) -> None:
    save = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_1.eu5"
    renamed = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_2.eu5"
    save.write_text(SAVE_FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")
    original_copy2 = dataset_module.shutil.copy2

    def renaming_copy(source, target):
        copied = original_copy2(source, target)
        Path(source).rename(renamed)
        return copied

    monkeypatch.setattr(dataset_module.shutil, "copy2", renaming_copy)

    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save],
        profile="vanilla",
        load_order_path=_load_order_file(tmp_path),
        min_file_age_seconds=0,
    )

    assert len(result.processed) == 1
    assert result.processed[0]["path"] == str(save.resolve())
    assert not result.failures
    assert not result.transient


def test_ingest_uses_staged_copy_when_original_changes_during_parse(
    tmp_path: Path, monkeypatch
) -> None:
    save = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_1.eu5"
    save.write_text(SAVE_FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")
    original_loader = dataset_module.load_savegame_tables

    def changing_loader(*args, **kwargs):
        save.write_text(SAVE_FIXTURE.read_text(encoding="utf-8") + "\n# changed\n", encoding="utf-8")
        return original_loader(*args, **kwargs)

    monkeypatch.setattr(dataset_module, "load_savegame_tables", changing_loader)

    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save],
        profile="vanilla",
        load_order_path=_load_order_file(tmp_path),
        min_file_age_seconds=0,
    )

    assert len(result.processed) == 1
    assert not result.transient
    assert not result.failures


def test_ingest_skips_renamed_autosave_with_same_content(tmp_path: Path) -> None:
    save_a = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_1.eu5"
    save_b = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_2.eu5"
    save_a.write_text(SAVE_FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")
    save_b.write_text(SAVE_FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")

    first = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save_a],
        profile="vanilla",
        load_order_path=_load_order_file(tmp_path),
        min_file_age_seconds=0,
    )
    second = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save_b],
        profile="vanilla",
        load_order_path=_load_order_file(tmp_path),
        min_file_age_seconds=0,
    )

    assert len(first.processed) == 1
    assert not second.processed
    assert second.skipped == [save_b.resolve()]
    assert not second.failures


def test_parallel_ingest_writes_manifest_rows_as_workers_finish(
    tmp_path: Path, monkeypatch
) -> None:
    save_a = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_1.eu5"
    save_b = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_2.eu5"
    save_a.write_text("a", encoding="utf-8")
    save_b.write_text("b", encoding="utf-8")
    rows = {
        str(save_a.resolve()): _manifest_row("snapshot-a", save_a),
        str(save_b.resolve()): _manifest_row("snapshot-b", save_b),
    }
    write_calls: list[list[dict]] = []
    original_write_manifest = dataset_module.SavegameDataset.write_manifest

    class FakeFuture:
        def __init__(self, row: dict) -> None:
            self._row = row

        def result(self) -> dict:
            return self._row

    class FakeExecutor:
        def __init__(self, max_workers: int) -> None:
            self.max_workers = max_workers

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback) -> None:
            return None

        def submit(self, _fn, _root, save_path, *_args) -> FakeFuture:
            return FakeFuture(rows[str(Path(save_path).resolve())])

    def recording_write_manifest(self, rows_arg):
        materialized = list(rows_arg)
        write_calls.append(materialized)
        return original_write_manifest(self, materialized)

    monkeypatch.setattr(
        dataset_module,
        "scan_for_work",
        lambda *args, **kwargs: ([save_a.resolve(), save_b.resolve()], []),
    )
    monkeypatch.setattr(dataset_module, "ProcessPoolExecutor", FakeExecutor)
    monkeypatch.setattr(dataset_module, "as_completed", lambda futures: list(futures))
    monkeypatch.setattr(dataset_module.SavegameDataset, "write_manifest", recording_write_manifest)

    result = ingest_savegame_dataset(tmp_path / "dataset", workers=2)

    assert not result.failures
    assert len(result.processed) == 2
    assert [call[0]["snapshot_id"] for call in write_calls] == ["snapshot-a", "snapshot-b"]
    assert SavegameDataset(tmp_path / "dataset").read_manifest().height == 2


def test_ingest_cleans_stale_staging_files(tmp_path: Path) -> None:
    dataset = tmp_path / "dataset"
    staging = dataset / ".ingest_staging"
    staging.mkdir(parents=True)
    stale = staging / "stale.eu5"
    fresh = staging / "fresh.eu5"
    stale.write_text("stale", encoding="utf-8")
    fresh.write_text("fresh", encoding="utf-8")
    old = time.time() - (2 * 60 * 60)
    os.utime(stale, (old, old))

    result = ingest_savegame_dataset(dataset, save_paths=[], min_file_age_seconds=0)

    assert not result.failures
    assert not stale.exists()
    assert fresh.exists()


def test_watch_savegame_dataset_stops_after_max_cycles(tmp_path: Path, monkeypatch) -> None:
    calls: list[int] = []

    def fake_ingest(*args, **kwargs):
        calls.append(1)
        return IngestResult(
            dataset=SavegameDataset(tmp_path / "dataset"),
            processed=[],
            skipped=[],
            transient=[],
            failures=[],
            elapsed_seconds=0.01,
        )

    monkeypatch.setattr(dataset_module, "ingest_savegame_dataset", fake_ingest)

    results = watch_savegame_dataset(
        tmp_path / "dataset",
        save_dir=tmp_path,
        interval_seconds=0,
        max_cycles=2,
    )

    assert len(results) == 2
    assert len(calls) == 2


def test_savegame_watch_prints_busy_summary_without_spam(
    tmp_path: Path, monkeypatch
) -> None:
    runner = CliRunner()
    busy_result = IngestResult(
        dataset=SavegameDataset(tmp_path / "dataset"),
        processed=[],
        skipped=[],
        transient=[
            {
                "path": str(tmp_path / "autosave.eu5"),
                "error": "Save was busy while it was being staged",
                "type": "SaveChangedError",
            }
        ],
        failures=[],
        elapsed_seconds=0.01,
    )

    def fake_watch(*args, **kwargs):
        kwargs["on_cycle"](1, busy_result)
        return [busy_result]

    monkeypatch.setattr(cli_module, "watch_savegame_dataset", fake_watch)

    quiet = runner.invoke(
        app,
        [
            "savegame",
            "watch",
            "--save-dir",
            str(tmp_path),
            "--output",
            str(tmp_path / "dataset"),
            "--max-cycles",
            "1",
        ],
    )
    verbose = runner.invoke(
        app,
        [
            "savegame",
            "watch",
            "--save-dir",
            str(tmp_path),
            "--output",
            str(tmp_path / "dataset"),
            "--max-cycles",
            "1",
            "--verbose-busy",
        ],
    )

    assert quiet.exit_code == 0
    assert "busy=1" in quiet.output
    assert "autosave.eu5" not in quiet.output
    assert verbose.exit_code == 0
    assert "autosave.eu5" in verbose.output


def test_ingest_fails_when_location_hierarchy_is_missing(tmp_path: Path) -> None:
    fixture_root = tmp_path / "fixture_without_hierarchy"
    shutil.copytree(FIXTURE_ROOT / "game", fixture_root / "game")
    definitions = fixture_root / "game" / "in_game" / "map_data" / "definitions.txt"
    definitions.unlink()
    save = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_1.eu5"
    save.write_text(SAVE_FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")

    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save],
        profile="vanilla",
        load_order_path=_load_order_for_root(tmp_path, fixture_root),
        min_file_age_seconds=0,
    )

    assert not result.processed
    assert result.failures
    assert result.failures[0]["type"] == "MissingLocationHierarchyError"
    assert "macro_region" in result.failures[0]["error"]
    assert not SavegameDataset(tmp_path / "dataset").table_files("locations")


def test_location_pop_ids_resolve_to_raw_pop_sizes(tmp_path: Path) -> None:
    save = tmp_path / "pop_ids.eu5"
    save.write_text(
        _save_with_population_section(
            population_section="""
population={
  database={
    100={ type=peasants size=1.5 }
    200={ type=laborers size=2.25 }
    300={ type=burghers size=0.75 }
  }
}
countries={ database={ 1={ definition=SWE country_name="Sweden" } } }
provinces={ database={ 5={ province_definition=uppland_province } } }
""",
        ),
        encoding="utf-8",
    )

    tables = load_savegame_tables(
        save_path=save,
        eu5_data=load_eu5_data(profile="vanilla", load_order_path=_load_order_file(tmp_path)),
        profile="vanilla",
        load_order_path=_load_order_file(tmp_path),
    )

    locations = tables.locations.sort("location_id")
    assert locations.item(0, "total_population") == 3.75
    assert locations.item(1, "total_population") == 0.75
    assert locations.item(0, "population_peasants") == 1.5
    assert locations.item(0, "population_laborers") == 2.25
    assert locations.item(1, "population_burghers") == 0.75
    assert locations.item(0, "country_tag") == "SWE"
    assert locations.item(0, "owner_name") == "Sweden"
    assert locations.item(0, "province_slug") == "uppland_province"


def test_location_population_is_null_when_pop_database_is_missing(tmp_path: Path) -> None:
    save = tmp_path / "missing_pop_database.eu5"
    save.write_text(_save_with_population_section(population_section=""), encoding="utf-8")

    tables = load_savegame_tables(
        save_path=save,
        eu5_data=load_eu5_data(profile="vanilla", load_order_path=_load_order_file(tmp_path)),
        profile="vanilla",
        load_order_path=_load_order_file(tmp_path),
    )

    assert tables.locations.item(0, "total_population") is None


def test_location_hierarchy_loads_from_profile_roots(tmp_path: Path) -> None:
    vanilla_root = tmp_path / "vanilla"
    definitions = vanilla_root / "game" / "in_game" / "map_data" / "definitions.txt"
    definitions.parent.mkdir(parents=True)
    definitions.write_text(
        """
europe={
  western_europe={
    scandinavian_region={
      svealand_area={
        uppland_province={ stockholm norrtalje }
      }
    }
  }
}
""".strip(),
        encoding="utf-8",
    )
    load_order = tmp_path / "load_order.toml"
    load_order.write_text(
        f"""
[paths]
vanilla_root = "{vanilla_root.as_posix()}"

[profiles]
vanilla = ["vanilla"]
""".strip(),
        encoding="utf-8",
    )

    hierarchy = load_location_hierarchy(profile="vanilla", load_order_path=load_order)

    assert hierarchy["stockholm"]["province_slug"] == "uppland_province"
    assert hierarchy["stockholm"]["area"] == "svealand_area"
    assert hierarchy["stockholm"]["region"] == "scandinavian_region"
    assert hierarchy["stockholm"]["macro_region"] == "western_europe"
    assert hierarchy["stockholm"]["super_region"] == "europe"


def test_aggregate_payload_groups_by_region_and_country(tmp_path: Path) -> None:
    fixture_root = _fixture_root_with_hierarchy(tmp_path)
    load_order = _load_order_for_root(tmp_path, fixture_root)
    save = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_2.eu5"
    save.write_text(
        _save_with_population_section(
            population_section="""
population={
  database={
    100={ type=peasants size=1 }
    200={ type=laborers size=2 }
    300={ type=peasants size=3 }
  }
}
countries={ database={ 1={ definition=SWE country_name="Sweden" } } }
provinces={ database={ 5={ province_definition=uppland_province } } }
""",
        ),
        encoding="utf-8",
    )

    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save],
        profile="vanilla",
        load_order_path=load_order,
        min_file_age_seconds=0,
    )
    assert not result.failures
    payload = SavegameDataset(tmp_path / "dataset").build_progression_cubes(top_n=10)

    rows = payload["explorer"]["rows"]
    region = [
        row
        for row in rows
        if row["domain"] == "population"
        and row["metric"] == "pops"
        and row["dimension"] == "region"
        and row["entity_key"] == "scandinavian_region"
    ]
    country = [
        row
        for row in rows
        if row["domain"] == "population"
        and row["metric"] == "development"
        and row["dimension"] == "country_tag"
        and row["entity_key"] == "SWE"
    ]
    assert region and region[0]["value"] == 6
    assert country and country[0]["value"] == 30


def test_overview_series_derives_pop_types_employment_tax_and_food(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path)
    save = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_3.eu5"
    save.write_text(
        _save_with_population_section(
            population_section="""
population={
  database={
    100={ type=peasants size=1 }
    200={ type=laborers size=2 }
    300={ type=peasants size=3 }
    400={ size=4 }
  }
}
countries={ database={ 1={ definition=SWE country_name="Sweden" } } }
provinces={ database={ 5={ province_definition=uppland_province } } }
""",
        ).replace(
            "center=1 food=10 max=20",
            "center=1 food=10 max=20 food_supply=5 food_consumption=3 missing=1",
        ).replace(
            "population={ pops={ 300 } }",
            "population={ pops={ 300 400 } }",
        ),
        encoding="utf-8",
    )

    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save],
        profile="vanilla",
        load_order_path=load_order,
        min_file_age_seconds=0,
    )
    assert not result.failures
    payload = SavegameDataset(tmp_path / "dataset").build_progression_cubes()

    pops = {
        row["pop_type"]: row["value"]
        for row in payload["overviewSeries"]["popsByType"]
    }
    employment = payload["overviewSeries"]["employment"][0]
    tax = payload["overviewSeries"]["tax"][0]
    food = payload["overviewSeries"]["food"][0]

    assert pops["peasants"] == 4
    assert pops["laborers"] == 2
    assert pops["unknown"] == 4
    assert employment["total_pops"] == 10
    assert employment["employed_pops"] == 1
    assert employment["unemployed_pops"] == 0
    assert tax["collected_tax"] == 6
    assert tax["possible_tax"] == 8
    assert tax["uncollected_tax"] == 2
    assert food["food"] == 10
    assert food["food_max"] == 20
    assert food["food_supply"] == 5
    assert food["food_consumption"] == 3
    assert food["missing"] == 1


def test_explorer_cube_supports_books_good_metric(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path)
    save = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_4.eu5"
    save.write_text(
        _save_with_population_section(population_section="").replace(
            "masonry={ supply=1 demand=2 price=3 }",
            "masonry={ supply=1 demand=2 price=3 } books={ supply=7 demand=1 price=2 }",
        ),
        encoding="utf-8",
    )

    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save],
        profile="vanilla",
        load_order_path=load_order,
        min_file_age_seconds=0,
    )
    assert not result.failures
    rows = SavegameDataset(tmp_path / "dataset").build_progression_cubes()["explorer"]["rows"]

    books = [
        row
        for row in rows
        if row["domain"] == "goods"
        and row["metric"] == "supply"
        and row["dimension"] == "good_id"
        and row["entity_key"] == "books"
    ]
    global_supply = [
        row
        for row in rows
        if row["domain"] == "goods"
        and row["metric"] == "supply"
        and row["dimension"] == "global"
        and row["entity_key"] == "world"
    ]
    assert books and books[0]["value"] == 7
    assert global_supply and global_supply[0]["value"] == 8


def test_extended_tables_parse_legacy_sections(tmp_path: Path) -> None:
    save = tmp_path / "extended.eu5"
    save.write_text(
        SAVE_FIXTURE.read_text(encoding="utf-8")
        + """
countries={
  database={
    1={
      definition=SWE
      country_name="Sweden"
      population=12
      currency_data={ gold=33 stability=1 prestige=2 government_power=3 }
      economy={ expense=4 loan_capacity=5 coin_minting=6 tax_rates={ nobles_estate=0.1 } }
      score={ score_rating={ ADM=7 MIL=8 } }
      owned_locations={ 1 2 }
    }
  }
}
population={
  database={
    10={ type=peasants estate=peasants culture=1 religion=1 size=2 satisfaction=0.5 goods=1 price=1 }
  }
}
culture_manager={ database={ 1={ name=swedish culture_definition=swedish size=2 language=norse color={ rgb={ 1 2 3 } } } } }
religion_manager={ database={ 1={ name=catholic key=catholic group=christian color={ rgb={ 4 5 6 } } } } }
provinces={ database={ 1={ province_definition=stockholm owner=1 food={ current=3 } max_food_value=4 } } }
estate_manager={ database={ 1={ estate_type=nobles_estate country=1 wealth_impact=0.2 satisfaction=0.3 existence=yes } } }
loan_manager={ database={ 1={ borrower=1 amount=100 interest=0.05 month=12 } } }
character_db={ database={ 1={ country=1 first_name=Erik adm=1 dip=2 mil=3 culture=1 religion=1 traits={ brave } birth_date="1300.1.1" } } }
dynasty_manager={ database={ 1={ key=folkunga name=Folkunga home=1 important=yes } } }
""",
        encoding="utf-8",
    )

    tables = load_savegame_tables(
        save_path=save,
        eu5_data=load_eu5_data(profile="vanilla", load_order_path=_load_order_file(tmp_path)),
        include_extended=True,
    )

    assert tables.countries.item(0, "country_tag") == "SWE"
    assert tables.population.item(0, "culture_name") == "swedish"
    assert tables.provinces.item(0, "owner_tag") == "SWE"
    assert tables.cultures.item(0, "color_b") == 3
    assert tables.religions.item(0, "color_r") == 4
    assert tables.estates.item(0, "country_tag") == "SWE"
    assert tables.loans.item(0, "borrower_tag") == "SWE"
    assert tables.characters.item(0, "traits") == "brave"
    assert tables.dynasties.item(0, "important") is True


def test_progression_html_smoke(tmp_path: Path) -> None:
    payload = {
        "schemaVersion": 2,
        "snapshots": [{"snapshot_id": "s1", "date": "1337.1.1", "date_sort": 13370101}],
        "overviewSeries": {
            "popsByType": [{"snapshot_id": "s1", "date": "1337.1.1", "date_sort": 13370101, "pop_type": "peasants", "value": 1}],
            "employment": [{"snapshot_id": "s1", "date": "1337.1.1", "date_sort": 13370101, "total_pops": 1, "employed_pops": 1, "unemployed_pops": 0}],
            "development": [{"snapshot_id": "s1", "date": "1337.1.1", "date_sort": 13370101, "development": 1}],
            "tax": [{"snapshot_id": "s1", "date": "1337.1.1", "date_sort": 13370101, "collected_tax": 1, "uncollected_tax": 2, "possible_tax": 3}],
            "food": [{"snapshot_id": "s1", "date": "1337.1.1", "date_sort": 13370101, "food": 10, "food_max": 20, "food_supply": 3, "food_consumption": 2, "food_balance": 1, "missing": 0}],
        },
        "explorer": {
            "metrics": [{"domain": "population", "key": "pops", "label": "Pops", "unit": "Pops", "formatter": "whole", "defaultSort": "desc"}],
            "dimensions": [{"key": "super_region", "label": "Super Region", "scope": "geography", "order": 10}],
            "aggregations": ["sum", "mean", "median", "min", "max"],
            "rows": [{"snapshot_id": "s1", "date": "1337.1.1", "date_sort": 13370101, "domain": "population", "metric": "pops", "dimension": "super_region", "entity_key": "scandinavia", "entity_label": "Scandinavia", "value": 1}],
        },
        "payloadSummary": {},
    }

    path = write_savegame_progression_html(payload, tmp_path / "savegame_progression.html")
    html = path.read_text(encoding="utf-8")

    assert "EU5 Savegame Progression" in html
    assert html.index("data-tab=\"overview\"") < html.index("data-tab=\"explorer\"")
    assert "data-tab=\"population\"" not in html
    assert "data-tab=\"markets\"" not in html
    assert "data-tab=\"food\"" not in html
    assert "data-tab=\"buildings\"" not in html
    assert "data-tab=\"compare\"" not in html
    assert "data-tab=\"locations\"" not in html
    assert "data-tab=\"aggregates\"" not in html
    assert "aggregateDimension" not in html
    assert "Overview" in html
    assert "Explorer" in html
    assert "echarts.init" in html
    assert "src=\"https://" not in html
    assert "src='https://" not in html
    assert "cdn.jsdelivr" not in html.lower()
    assert "Pops" in html
    assert "Collected Tax" in html
    assert "Uncollected Tax" in html
    assert "Pops by Type" in html
    assert "Food/month" in html
    assert "domainSelect" in html
    assert "metricSelect" in html
    assert "scopeSelect" in html
    assert "dimensionSelect" in html
    assert "aggregationSelect" in html
    assert "rankSelect" in html
    assert "limitSelect" in html
    assert "entityFilter" in html
    assert "combo-menu" in html
    assert "aggregate(values, kind)" in html
    assert "median(values)" in html
    assert "Export CSV" in html
    assert "aggregate" in html
    assert "delta" in html
    assert "In-game date" in html
    assert "name: unit" in html
    assert "payloadSummary" in html
    assert "locationsRaw" not in html


def test_savegame_progress_cli_writes_html(tmp_path: Path) -> None:
    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[SAVE_FIXTURE],
        profile="vanilla",
        load_order_path=_load_order_file(tmp_path),
        min_file_age_seconds=0,
    )
    assert not result.failures

    output = tmp_path / "progression.html"
    cli = CliRunner().invoke(
        app,
        [
            "savegame",
            "progress",
            "--dataset",
            str(tmp_path / "dataset"),
            "--output",
            str(output),
        ],
    )

    assert cli.exit_code == 0, cli.stdout
    assert output.exists()
    assert "snapshots: 1" in cli.stdout


def _manifest_row(snapshot_id: str, save: Path) -> dict:
    return {
        "manifest_version": 1,
        "snapshot_id": snapshot_id,
        "playthrough_id": "aaaaaaaa_bbbb_cccc_dddd_eeeeeeeeeeee",
        "save_playthrough_id": None,
        "playthrough_name": None,
        "save_label": None,
        "date": "1337.1.1",
        "year": 1337,
        "month": 1,
        "day": 1,
        "date_sort": 13370101,
        "path": str(save.resolve()),
        "mtime": 1.0,
        "mtime_ns": 1,
        "size": 1,
        "partial_hash": snapshot_id,
        "state_key": f"{save.resolve()}|1|1",
        "source_format": "text",
        "parser_profile": "vanilla",
        "processed_at": "2026-01-01T00:00:00",
        "parse_seconds": 0.01,
        "row_counts_json": "{}",
    }


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


def _load_order_for_root(tmp_path: Path, root: Path) -> Path:
    path = tmp_path / "load_order.toml"
    path.write_text(
        f"""
[paths]
vanilla_root = "{root.as_posix()}"

[profiles]
vanilla = ["vanilla"]
""".strip(),
        encoding="utf-8",
    )
    return path


def _fixture_root_with_hierarchy(tmp_path: Path) -> Path:
    root = tmp_path / "fixture_eu5"
    shutil.copytree(FIXTURE_ROOT / "game", root / "game")
    definitions = root / "game" / "in_game" / "map_data" / "definitions.txt"
    definitions.parent.mkdir(parents=True, exist_ok=True)
    definitions.write_text(
        """
europe={
  western_europe={
    scandinavian_region={
      svealand_area={
        uppland_province={ stockholm norrtalje }
      }
    }
  }
}
""".strip(),
        encoding="utf-8",
    )
    return root


def _save_with_population_section(*, population_section: str) -> str:
    return f"""SAV02001e1576a50004dbc200000000
metadata={{
    date="1337.1.1"
    compatibility={{ locations={{stockholm norrtalje}} }}
}}
locations={{
    locations={{
        1={{
            owner=1
            controller=1
            market=1
            province=5
            development=10
            tax=2
            possible_tax=3
            population={{ pops={{ 100 200 }} }}
        }}
        2={{
            owner=1
            controller=1
            market=1
            province=5
            development=20
            tax=4
            possible_tax=5
            population={{ pops={{ 300 }} }}
        }}
    }}
}}
market_manager={{ database={{ 1={{ center=1 food=10 max=20 goods={{ masonry={{ supply=1 demand=2 price=3 }} }} }} }} }}
building_manager={{ database={{ 1={{ type=mason level=1 employed=1 location=1 owner=1 stone_bricks={{}} }} }} }}
{population_section}
"""
