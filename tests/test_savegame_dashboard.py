import json
import shutil
from pathlib import Path

import polars as pl

from eu5gameparser.savegame import ingest_savegame_dataset
from eu5gameparser.savegame.dashboard import create_dashboard_app
from eu5gameparser.savegame.dashboard_adapter import (
    BuildingIconResolver,
    SavegameDashboardAdapter,
)
from eu5gameparser.savegame.dashboard_lifecycle import (
    dashboard_log_path,
    dashboard_state_path,
    dashboard_status,
    stop_dashboard_process,
)

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "eu5"


def test_duckdb_adapter_scans_dataset_and_builds_overview(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path, FIXTURE_ROOT)
    save = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_1.eu5"
    save.write_text(_save_with_population_section(), encoding="utf-8")

    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save],
        profile="vanilla",
        load_order_path=load_order,
        min_file_age_seconds=0,
    )
    assert not result.failures

    adapter = SavegameDashboardAdapter(
        tmp_path / "dataset",
        profile="vanilla",
        load_order_path=load_order,
    )
    snapshots = adapter.snapshots()
    overview = adapter.overview()

    assert len(snapshots) == 1
    assert overview["employment"][0]["total_pops"] == 10
    assert overview["employment"][0]["employed_pops"] == 1
    assert overview["employment"][0]["unemployed_pops"] == 0
    assert overview["tax"][0]["uncollected_tax"] == 2
    assert overview["food"][0]["food"] == 10


def test_explorer_default_query_and_metric_changes(tmp_path: Path) -> None:
    fixture_root = _fixture_root_with_hierarchy(tmp_path)
    load_order = _load_order_file(tmp_path, fixture_root)
    save = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_2.eu5"
    save.write_text(_save_with_population_section(), encoding="utf-8")

    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save],
        profile="vanilla",
        load_order_path=load_order,
        min_file_age_seconds=0,
    )
    assert not result.failures

    adapter = SavegameDashboardAdapter(
        tmp_path / "dataset",
        profile="vanilla",
        load_order_path=load_order,
    )
    pops = adapter.explorer_query(
        domain="population",
        metric="pops",
        dimension="super_region",
        aggregation="sum",
        rank="top",
        limit=5,
    )
    development = adapter.explorer_query(
        domain="population",
        metric="development",
        dimension="super_region",
        aggregation="sum",
        rank="top",
        limit=5,
    )

    assert pops.ranking[0]["entity_key"] == "europe"
    assert pops.ranking[0]["aggregate"] == 10
    assert development.ranking[0]["aggregate"] == 30
    assert pops.ranking[0]["aggregate"] != development.ranking[0]["aggregate"]


def test_template_query_returns_six_ranked_panels(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path, FIXTURE_ROOT)
    save_1 = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_1.eu5"
    save_2 = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_2.eu5"
    save_1.write_text(_two_country_save("1337.1.1", 1000, 1), encoding="utf-8")
    save_2.write_text(_two_country_save("1338.1.1", 1000, 900), encoding="utf-8")

    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save_1, save_2],
        profile="vanilla",
        load_order_path=load_order,
        min_file_age_seconds=0,
    )
    assert not result.failures

    adapter = SavegameDashboardAdapter(
        tmp_path / "dataset",
        profile="vanilla",
        load_order_path=load_order,
    )
    query = adapter.template_query(
        metric_key="population:pops",
        scope="country_tag",
        limit=1,
    )

    assert set(query.panels) == {
        "top_sum",
        "bottom_sum",
        "top_mean",
        "bottom_mean",
        "top_change",
        "bottom_change",
    }
    assert query.panels["top_sum"]["ranking"][0]["entity_key"] == "AAA"
    assert query.panels["top_change"]["ranking"][0]["entity_key"] == "BBB"
    assert query.panels["bottom_change"]["ranking"][0]["entity_key"] == "AAA"
    assert query.ranking[0]["sum"] == 2000
    assert query.chips[:2] == ["Pops", "Group by Country"]


def test_template_query_groups_population_by_macro_region(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path, FIXTURE_ROOT)
    save_1 = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_1.eu5"
    save_2 = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_2.eu5"
    save_1.write_text(_two_country_save("1337.1.1", 1000, 1), encoding="utf-8")
    save_2.write_text(_two_country_save("1338.1.1", 1000, 900), encoding="utf-8")
    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save_1, save_2],
        profile="vanilla",
        load_order_path=load_order,
        min_file_age_seconds=0,
    )
    assert not result.failures

    adapter = SavegameDashboardAdapter(
        tmp_path / "dataset",
        profile="vanilla",
        load_order_path=load_order,
    )
    query = adapter.template_query(
        metric_key="population:pops",
        scope="macro_region",
        limit=5,
    )

    ranking = {row["entity_key"]: row for row in query.ranking}
    panel_entities = {row["entity_key"] for row in query.panels["top_sum"]["rows"]}
    assert query.chips[:2] == ["Pops", "Group by Macro Region"]
    assert set(ranking) == {"eastern_europe", "east_asia"}
    assert ranking["eastern_europe"]["sum"] == 2000
    assert ranking["east_asia"]["sum"] == 901
    assert panel_entities == {"eastern_europe", "east_asia"}
    assert "world" not in panel_entities
    assert "unknown" not in panel_entities


def test_template_query_groups_population_by_super_region_not_global(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path, FIXTURE_ROOT)
    save_1 = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_1.eu5"
    save_2 = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_2.eu5"
    save_1.write_text(_two_country_save("1337.1.1", 1000, 1), encoding="utf-8")
    save_2.write_text(_two_country_save("1338.1.1", 1000, 900), encoding="utf-8")
    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save_1, save_2],
        profile="vanilla",
        load_order_path=load_order,
        min_file_age_seconds=0,
    )
    assert not result.failures

    adapter = SavegameDashboardAdapter(
        tmp_path / "dataset",
        profile="vanilla",
        load_order_path=load_order,
    )
    query = adapter.template_query(
        metric_key="population:pops",
        scope="super_region",
        limit=5,
    )

    ranking = {row["entity_key"]: row for row in query.ranking}
    panel_entities = {row["entity_key"] for row in query.panels["top_sum"]["rows"]}
    assert query.chips[:2] == ["Pops", "Group by Super Region"]
    assert set(ranking) == {"europe", "asia"}
    assert ranking["europe"]["sum"] == 2000
    assert ranking["asia"]["sum"] == 901
    assert panel_entities == {"europe", "asia"}
    assert "world" not in panel_entities


def test_template_metadata_hides_all_null_geography_scopes(tmp_path: Path) -> None:
    dataset = tmp_path / "dataset"
    locations = dataset / "tables" / "locations" / "playthrough_id=test" / "1337.parquet"
    locations.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "snapshot_id": "1337",
                "playthrough_id": "test",
                "source_path": "test.eu5",
                "date": "1337.1.1",
                "year": 1337,
                "month": 1,
                "day": 1,
                "date_sort": 13370101,
                "location_id": 1,
                "slug": "stockholm",
                "province_slug": None,
                "area": None,
                "region": None,
                "macro_region": None,
                "super_region": None,
                "country_tag": "SWE",
                "total_population": 10.0,
                "population_peasants": 10.0,
            }
        ],
        schema_overrides={
            "province_slug": pl.String,
            "area": pl.String,
            "region": pl.String,
            "macro_region": pl.String,
            "super_region": pl.String,
        },
    ).write_parquet(locations)
    adapter = SavegameDashboardAdapter(dataset)

    metadata = adapter.template_metadata()
    pops = _metric(metadata, "population:pops")
    query = adapter.template_query(metric_key="population:pops", scope="macro_region")

    assert "macro_region" not in pops["validScopes"]
    assert "country_tag" in pops["validScopes"]
    assert query.scope["key"] == "global"
    assert query.chips[:2] == ["Pops", "Group by World"]


def test_template_metadata_exposes_cascading_controls(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path, FIXTURE_ROOT)
    save = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_1.eu5"
    save.write_text(_save_with_population_section(), encoding="utf-8")
    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save],
        profile="vanilla",
        load_order_path=load_order,
        min_file_age_seconds=0,
    )
    assert not result.failures

    adapter = SavegameDashboardAdapter(
        tmp_path / "dataset",
        profile="vanilla",
        load_order_path=load_order,
    )
    metadata = adapter.template_metadata()

    assert {"label": "Goods", "value": "goods"} in metadata["domains"]
    assert "global" in _metric(metadata, "goods:supply")["validScopes"]
    assert any(scope["group"] == "World" for scope in metadata["scopes"])
    assert any(item["key"] == "good_id" for item in metadata["filters"])


def test_template_query_filters_specific_good_by_world_and_market(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path, FIXTURE_ROOT)
    save = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_1.eu5"
    save.write_text(_save_with_population_section(), encoding="utf-8")
    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save],
        profile="vanilla",
        load_order_path=load_order,
        min_file_age_seconds=0,
    )
    assert not result.failures

    adapter = SavegameDashboardAdapter(
        tmp_path / "dataset",
        profile="vanilla",
        load_order_path=load_order,
    )
    good_options = adapter.template_filter_options("good_id")
    world = adapter.template_query(
        metric_key="goods:supply",
        scope="global",
        filters={"good_id": "masonry"},
    )
    market = adapter.template_query(
        metric_key="goods:supply",
        scope="market_center_slug",
        filters={"good_id": "masonry"},
    )

    assert {"label": "masonry", "value": "masonry"} in good_options
    assert world.ranking[0]["entity_key"] == "world"
    assert world.ranking[0]["sum"] == 1
    assert market.ranking[0]["entity_key"] == "stockholm"
    assert market.ranking[0]["sum"] == 1
    assert "Good: masonry" in world.chips


def test_template_query_normalizes_stale_reversed_and_missing_dates(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path, FIXTURE_ROOT)
    save_1 = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_1.eu5"
    save_2 = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_2.eu5"
    save_1.write_text(_two_country_save("1337.1.1", 100, 10), encoding="utf-8")
    save_2.write_text(_two_country_save("1338.1.1", 150, 20), encoding="utf-8")
    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save_1, save_2],
        profile="vanilla",
        load_order_path=load_order,
        min_file_age_seconds=0,
    )
    assert not result.failures
    adapter = SavegameDashboardAdapter(
        tmp_path / "dataset",
        profile="vanilla",
        load_order_path=load_order,
    )

    full = adapter.template_query(metric_key="population:pops", scope="country_tag")
    stale = adapter.template_query(
        metric_key="population:pops",
        scope="country_tag",
        from_date_sort=99999999,
        to_date_sort=1,
    )
    missing = adapter.template_query(
        metric_key="population:pops",
        scope="country_tag",
        from_date_sort=None,
        to_date_sort=None,
    )

    assert full.ranking == stale.ranking == missing.ranking
    assert stale.empty_message is None
    assert stale.panels["top_sum"]["rows"]


def test_overview_normalizes_stale_dates_and_playthrough_selection(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path, FIXTURE_ROOT)
    save_1 = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_1.eu5"
    save_2 = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_2.eu5"
    save_1.write_text(_two_country_save("1337.1.1", 100, 10), encoding="utf-8")
    save_2.write_text(_two_country_save("1338.1.1", 150, 20), encoding="utf-8")
    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save_1, save_2],
        profile="vanilla",
        load_order_path=load_order,
        min_file_age_seconds=0,
    )
    assert not result.failures
    adapter = SavegameDashboardAdapter(
        tmp_path / "dataset",
        profile="vanilla",
        load_order_path=load_order,
    )

    overview = adapter.overview(
        playthrough_id="aaaaaaaa_bbbb_cccc_dddd_eeeeeeeeeeee",
        from_date_sort=99999999,
        to_date_sort=1,
    )

    assert len(overview["employment"]) == 2
    assert overview["employment"][0]["total_pops"] == 110
    assert overview["employment"][1]["total_pops"] == 170
    assert len(overview["food"]) == 2


def test_template_metadata_hides_invalid_and_missing_religion_scopes(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path, FIXTURE_ROOT)
    save = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_4.eu5"
    save.write_text(_save_with_population_section(), encoding="utf-8")
    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save],
        profile="vanilla",
        load_order_path=load_order,
        min_file_age_seconds=0,
    )
    assert not result.failures

    adapter = SavegameDashboardAdapter(
        tmp_path / "dataset",
        profile="vanilla",
        load_order_path=load_order,
    )
    metadata = adapter.template_metadata()
    pops = _metric(metadata, "population:pops")
    development = _metric(metadata, "population:development")

    assert "religion_name" not in pops["validScopes"]
    assert "pop_type" in pops["validScopes"]
    assert "pop_type" not in development["validScopes"]


def test_dashboard_cache_invalidates_on_schema_and_manifest_state(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path, FIXTURE_ROOT)
    save_1 = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_1.eu5"
    save_2 = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_2.eu5"
    save_1.write_text(_two_country_save("1337.1.1", 100, 10), encoding="utf-8")
    save_2.write_text(_two_country_save("1338.1.1", 150, 20), encoding="utf-8")
    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save_1, save_2],
        profile="vanilla",
        load_order_path=load_order,
        min_file_age_seconds=0,
    )
    assert not result.failures
    dataset = tmp_path / "dataset"

    SavegameDashboardAdapter(dataset, profile="vanilla", load_order_path=load_order)
    cache_manifest = dataset / "dashboard_cache" / "manifest.json"
    original = json.loads(cache_manifest.read_text(encoding="utf-8"))
    assert original["schemaVersion"] == 1
    assert (dataset / "dashboard_cache" / "explorer_series.parquet").is_file()

    cache_manifest.write_text(
        json.dumps({**original, "schemaVersion": -1}),
        encoding="utf-8",
    )
    SavegameDashboardAdapter(dataset, profile="vanilla", load_order_path=load_order)
    restored = json.loads(cache_manifest.read_text(encoding="utf-8"))
    assert restored["schemaVersion"] == 1

    manifest = pl.read_parquet(dataset / "manifest.parquet").with_columns(
        pl.lit("changed-state").alias("state_key")
    )
    manifest.write_parquet(dataset / "manifest.parquet")
    SavegameDashboardAdapter(dataset, profile="vanilla", load_order_path=load_order)
    refreshed = json.loads(cache_manifest.read_text(encoding="utf-8"))
    assert {row["state_key"] for row in refreshed["sourceState"]} == {"changed-state"}


def test_adapter_refreshes_cache_when_manifest_changes_after_startup(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path, FIXTURE_ROOT)
    dataset = tmp_path / "dataset"
    adapter = SavegameDashboardAdapter(dataset, profile="vanilla", load_order_path=load_order)
    assert adapter.snapshots() == []

    save = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_1.eu5"
    save.write_text(_save_with_population_section(), encoding="utf-8")
    result = ingest_savegame_dataset(
        dataset,
        save_paths=[save],
        profile="vanilla",
        load_order_path=load_order,
        min_file_age_seconds=0,
    )
    assert not result.failures

    assert len(adapter.snapshots()) == 1
    assert adapter.date_options()[0]["label"] == "1337.1.1"
    assert adapter.overview()["employment"][0]["total_pops"] == 10


def test_template_religion_scope_works_for_extended_pop_data(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path, FIXTURE_ROOT)
    save = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_5.eu5"
    save.write_text(_save_with_religion(), encoding="utf-8")
    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save],
        profile="vanilla",
        load_order_path=load_order,
        min_file_age_seconds=0,
        include_extended=True,
    )
    assert not result.failures

    adapter = SavegameDashboardAdapter(
        tmp_path / "dataset",
        profile="vanilla",
        load_order_path=load_order,
    )
    metadata = adapter.template_metadata()
    pops = _metric(metadata, "population:pops")
    query = adapter.template_query(metric_key="population:pops", scope="religion_name", limit=5)

    assert "religion_name" in pops["validScopes"]
    assert query.ranking[0]["entity_key"] == "catholic"
    assert query.ranking[0]["sum"] == 10


def test_dashboard_app_layout_contains_primary_tabs(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path, FIXTURE_ROOT)
    save = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_3.eu5"
    save.write_text(_save_with_population_section(), encoding="utf-8")
    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save],
        profile="vanilla",
        load_order_path=load_order,
        min_file_age_seconds=0,
    )
    assert not result.failures

    app = create_dashboard_app(
        tmp_path / "dataset",
        profile="vanilla",
        load_order_path=load_order,
        refresh_ms=1234,
    )
    tabs = app.layout.children[3].children
    html_text = str(app.layout)

    assert app.layout.children[0].id == "dashboard-refresh"
    assert app.layout.children[0].interval == 1234
    assert [tab.label for tab in tabs] == ["Overview", "Explorer", "Game Data"]
    assert "EU5 Progression Dashboard" in app.title
    assert "template-domain" in html_text
    assert "template-metric" in html_text
    assert "template-scope-group" in html_text
    assert "template-group-by" in html_text
    assert "template-limit" in html_text
    assert "template-filter-good-id" in html_text
    assert "template-top-sum-figure" in html_text
    assert "template-bottom-change-figure" in html_text
    assert "template-ranking" in html_text
    assert "explorer-domain" not in html_text
    assert "explorer-aggregation" not in html_text
    assert "explorer-ranking" not in html_text


def test_dashboard_callback_handles_invalid_dates_with_non_empty_figures(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path, FIXTURE_ROOT)
    save_1 = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_1.eu5"
    save_2 = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_2.eu5"
    save_1.write_text(_two_country_save("1337.1.1", 100, 10), encoding="utf-8")
    save_2.write_text(_two_country_save("1338.1.1", 150, 20), encoding="utf-8")
    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save_1, save_2],
        profile="vanilla",
        load_order_path=load_order,
        min_file_age_seconds=0,
    )
    assert not result.failures

    app = create_dashboard_app(tmp_path / "dataset", profile="vanilla", load_order_path=load_order)
    response = _post_template_callback(app, from_date=99999999, to_date=1)
    text = response.data.decode("utf-8", errors="replace")

    assert response.status_code == 200
    assert "template-ranking" in text
    assert "AAA" in text
    assert text.count('"type":"scatter"') > 0


def test_dashboard_callback_uses_super_region_grouping(tmp_path: Path) -> None:
    load_order = _load_order_file(tmp_path, FIXTURE_ROOT)
    save_1 = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_1.eu5"
    save_2 = tmp_path / "autosave_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_2.eu5"
    save_1.write_text(_two_country_save("1337.1.1", 1000, 1), encoding="utf-8")
    save_2.write_text(_two_country_save("1338.1.1", 1000, 900), encoding="utf-8")
    result = ingest_savegame_dataset(
        tmp_path / "dataset",
        save_paths=[save_1, save_2],
        profile="vanilla",
        load_order_path=load_order,
        min_file_age_seconds=0,
    )
    assert not result.failures

    app = create_dashboard_app(tmp_path / "dataset", profile="vanilla", load_order_path=load_order)
    response = _post_template_callback(
        app,
        from_date=None,
        to_date=None,
        group_by="super_region",
    )
    text = response.data.decode("utf-8", errors="replace")

    assert response.status_code == 200
    assert "Group by Super Region" in text
    assert "europe" in text
    assert "asia" in text
    assert '"entity_key":"world"' not in text


def test_dashboard_lifecycle_status_reads_state_without_running_process(tmp_path: Path) -> None:
    port = 58050
    state_path = dashboard_state_path(port)
    state_path.write_text(
        f"""{{
  "pid": 999999,
  "host": "127.0.0.1",
  "port": {port},
  "url": "http://127.0.0.1:{port}",
  "dataset": "{tmp_path.as_posix()}",
  "log_path": "{dashboard_log_path(port).as_posix()}"
}}""",
        encoding="utf-8",
    )

    info = dashboard_status(port=port)
    stopped = stop_dashboard_process(port=port)

    assert info.healthy is False
    assert info.running is False
    assert Path(info.dataset) == tmp_path
    assert stopped.state_path == state_path
    assert not state_path.exists()


def test_building_icon_resolver_uses_dashboard_asset_cache(tmp_path: Path) -> None:
    source_file = tmp_path / "game" / "in_game" / "common" / "building_types" / "buildings.txt"
    icon_file = (
        tmp_path
        / "game"
        / "in_game"
        / "gfx"
        / "interface"
        / "icons"
        / "buildings"
        / "farming_village.png"
    )
    source_file.parent.mkdir(parents=True)
    icon_file.parent.mkdir(parents=True)
    source_file.write_text("farming_village={}", encoding="utf-8")
    icon_file.write_bytes(b"\x89PNG\r\n\x1a\n")
    load_order = _load_order_file(tmp_path, tmp_path)
    resolver = BuildingIconResolver(
        profile="vanilla",
        load_order_path=load_order,
        asset_root=tmp_path / "assets",
    )

    url = resolver.icon_url(
        {
            "name": "farming_village",
            "icon": None,
            "source_file": str(source_file),
            "source_history": "[]",
        }
    )

    assert url == "/assets/building_icons/farming_village.png"
    assert (tmp_path / "assets" / "building_icons" / "farming_village.png").is_file()


def _load_order_file(tmp_path: Path, root: Path) -> Path:
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


def _metric(metadata: dict, key: str) -> dict:
    for metric in metadata["metrics"]:
        if metric["key"] == key:
            return metric
    raise AssertionError(f"Missing metric: {key}")


def _post_template_callback(
    app,
    *,
    from_date: int | None,
    to_date: int | None,
    group_by: str = "country_tag",
):
    output = (
        "..template-query-chips.children...template-top-sum-figure.figure..."
        "template-bottom-sum-figure.figure...template-top-mean-figure.figure..."
        "template-bottom-mean-figure.figure...template-top-change-figure.figure..."
        "template-bottom-change-figure.figure...template-ranking.data.."
    )
    payload = {
        "output": output,
        "outputs": [
            {"id": "template-query-chips", "property": "children"},
            {"id": "template-top-sum-figure", "property": "figure"},
            {"id": "template-bottom-sum-figure", "property": "figure"},
            {"id": "template-top-mean-figure", "property": "figure"},
            {"id": "template-bottom-mean-figure", "property": "figure"},
            {"id": "template-top-change-figure", "property": "figure"},
            {"id": "template-bottom-change-figure", "property": "figure"},
            {"id": "template-ranking", "property": "data"},
        ],
        "inputs": [
            {"id": "template-metric", "property": "value", "value": "population:pops"},
            {"id": "template-group-by", "property": "value", "value": group_by},
            {"id": "template-limit", "property": "value", "value": 5},
            {"id": "playthrough-select", "property": "value", "value": ""},
            {"id": "from-date", "property": "value", "value": from_date},
            {"id": "to-date", "property": "value", "value": to_date},
            {"id": "template-filter-good-id", "property": "value", "value": None},
            {"id": "template-filter-goods-category", "property": "value", "value": None},
            {"id": "template-filter-goods-designation", "property": "value", "value": None},
            {"id": "template-filter-market-center-slug", "property": "value", "value": None},
            {"id": "template-filter-building-type", "property": "value", "value": None},
            {"id": "template-filter-production-method", "property": "value", "value": None},
            {"id": "template-filter-country-tag", "property": "value", "value": None},
            {"id": "template-filter-pop-type", "property": "value", "value": None},
            {"id": "template-filter-religion-name", "property": "value", "value": None},
            {"id": "dashboard-refresh", "property": "n_intervals", "value": 0},
        ],
        "state": [],
        "changedPropIds": ["template-metric.value"],
    }
    return app.server.test_client().post("/_dash-update-component", json=payload)


def _save_with_population_section() -> str:
    return """SAV02001e1576a50004dbc200000000
metadata={
    date="1337.1.1"
    compatibility={ locations={stockholm norrtalje} }
}
locations={
    locations={
        1={
            owner=1
            controller=1
            market=1
            province=5
            development=10
            tax=2
            possible_tax=3
            population={ pops={ 100 200 } }
        }
        2={
            owner=1
            controller=1
            market=1
            province=5
            development=20
            tax=4
            possible_tax=5
            population={ pops={ 300 400 } }
        }
    }
}
market_manager={
  database={
    1={
      center=1
      food=10
      max=20
      food_supply=5
      food_consumption=3
      missing=1
      goods={ masonry={ supply=1 demand=2 price=3 } }
    }
  }
}
building_manager={
  database={
    1={ type=mason level=1 employed=1 location=1 owner=1 stone_bricks={} }
  }
}
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
"""


def _save_with_religion() -> str:
    return (
        _save_with_population_section()
        .replace("100={ type=peasants size=1 }", "100={ type=peasants size=1 religion=1 }")
        .replace("200={ type=laborers size=2 }", "200={ type=laborers size=2 religion=1 }")
        .replace("300={ type=peasants size=3 }", "300={ type=peasants size=3 religion=1 }")
        .replace("400={ size=4 }", "400={ size=4 religion=1 }")
        + """
religion_manager={ database={ 1={ name=catholic key=catholic } } }
"""
    )


def _two_country_save(date: str, country_a_pops: int, country_b_pops: int) -> str:
    return f"""SAV02001e1576a50004dbc200000000
metadata={{
    date="{date}"
    compatibility={{ locations={{alpha beta}} }}
}}
locations={{
    locations={{
        1={{
            owner=1
            controller=1
            market=1
            province=1
            development=10
            tax=2
            possible_tax=3
            population={{ pops={{ 100 }} }}
        }}
        2={{
            owner=2
            controller=2
            market=1
            province=2
            development=10
            tax=2
            possible_tax=3
            population={{ pops={{ 200 }} }}
        }}
    }}
}}
market_manager={{
  database={{
    1={{
      center=1
      food=10
      max=20
      food_supply=5
      food_consumption=3
      goods={{ masonry={{ supply=1 demand=2 price=3 }} }}
    }}
  }}
}}
building_manager={{ database={{}} }}
population={{
  database={{
    100={{ type=peasants size={country_a_pops} }}
    200={{ type=peasants size={country_b_pops} }}
  }}
}}
countries={{
  database={{
    1={{ definition=AAA country_name="Alpha" }}
    2={{ definition=BBB country_name="Beta" }}
  }}
}}
provinces={{
  database={{
    1={{ province_definition=alpha_province }}
    2={{ province_definition=beta_province }}
  }}
}}
"""
