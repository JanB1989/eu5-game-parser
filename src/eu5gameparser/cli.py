from __future__ import annotations

import json
import time
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import polars as pl
import typer

from eu5gameparser.config import ParserConfig
from eu5gameparser.domain.advancements import load_advancement_data
from eu5gameparser.domain.availability import (
    filter_building_data_by_age,
    filter_eu5_data_by_age,
    hidden_counts,
)
from eu5gameparser.domain.buildings import load_building_data
from eu5gameparser.domain.eu5 import load_eu5_data
from eu5gameparser.domain.goods import load_goods_data
from eu5gameparser.load_order import DEFAULT_LOAD_ORDER_PATH, LoadOrderConfig
from eu5gameparser.savegame import (
    DEFAULT_SAVE_GAMES_DIR,
    SavegameDataset,
    benchmark_savegame_progression,
    ingest_savegame_dataset,
    run_dashboard,
    watch_savegame_dataset,
    write_savegame_explorer_html,
    write_savegame_parquet,
    write_savegame_progression_html,
)
from eu5gameparser.savegame.dashboard_adapter import SavegameDashboardAdapter
from eu5gameparser.savegame.dashboard_lifecycle import (
    dashboard_status,
    start_dashboard_process,
    stop_dashboard_process,
)

app = typer.Typer(help="Parse Europa Universalis V game files.")
savegame_app = typer.Typer(help="Parse and analyze EU5 savegames.")
dashboard_app = typer.Typer(
    help="Run the local savegame progression dashboard.",
    invoke_without_command=True,
)
app.add_typer(savegame_app, name="savegame")
app.add_typer(dashboard_app, name="dashboard")


class OutputFormat(StrEnum):
    parquet = "parquet"
    csv = "csv"


@app.command("inspect-paths")
def inspect_paths(
    game_root: Annotated[Path | None, typer.Option(help="EU5 installation root.")] = None,
    load_order: Annotated[
        Path, typer.Option(help="Load-order TOML file.")
    ] = DEFAULT_LOAD_ORDER_PATH,
    profile: Annotated[str | None, typer.Option(help="Data profile to inspect.")] = None,
) -> None:
    if profile is not None:
        load_config = LoadOrderConfig.load(load_order)
        data_profile = load_config.profile(profile)
        for layer in data_profile.layers:
            typer.echo(f"{layer.id}: {layer.root}")
            for name, path in {
                "common": layer.common_dir,
                "advances": layer.common_dir / "advances",
                "building_categories": layer.common_dir / "building_categories",
                "building_types": layer.common_dir / "building_types",
                "goods": layer.common_dir / "goods",
                "production_methods": layer.common_dir / "production_methods",
            }.items():
                status = "ok" if path.exists() else "missing"
                typer.echo(f"  {name}: {path} [{status}]")
        return

    config = ParserConfig.from_env(game_root)
    for name, path in config.paths().items():
        status = "ok" if path.exists() else "missing"
        typer.echo(f"{name}: {path} [{status}]")


@app.command()
def advancements(
    load_order: Annotated[
        Path, typer.Option(help="Load-order TOML file.")
    ] = DEFAULT_LOAD_ORDER_PATH,
    profile: Annotated[str, typer.Option(help="Data profile to parse.")] = "merged_default",
    output: Annotated[Path, typer.Option(help="Output directory.")] = Path("out"),
    format: Annotated[OutputFormat, typer.Option("--format", "-f")] = OutputFormat.parquet,
) -> None:
    data = load_advancement_data(profile=profile, load_order_path=load_order)
    output.mkdir(parents=True, exist_ok=True)
    _write_table(data.advancements, output / f"advancements.{format.value}", format)
    _print_header(profile, output)
    _print_table_summary("advancements", data.advancements)
    _print_warnings(data.warnings)


@app.command()
def buildings(
    load_order: Annotated[
        Path, typer.Option(help="Load-order TOML file.")
    ] = DEFAULT_LOAD_ORDER_PATH,
    profile: Annotated[str, typer.Option(help="Data profile to parse.")] = "merged_default",
    output: Annotated[Path, typer.Option(help="Output directory.")] = Path("out"),
    format: Annotated[OutputFormat, typer.Option("--format", "-f")] = OutputFormat.parquet,
    age: Annotated[str | None, typer.Option(help="Maximum age to include.")] = None,
    include_specific_unlocks: Annotated[
        bool,
        typer.Option(help="Include country, region, and religion-specific unlocks."),
    ] = False,
) -> None:
    data = load_building_data(profile=profile, load_order_path=load_order)
    hidden = {}
    if age is not None:
        advancement_data = load_advancement_data(profile=profile, load_order_path=load_order)
        filtered = filter_building_data_by_age(
            data,
            advancement_data.advancements,
            age,
            include_specific_unlocks=include_specific_unlocks,
        )
        hidden = hidden_counts(data, filtered)
        data = filtered
    output.mkdir(parents=True, exist_ok=True)
    _write_tables(
        output,
        format,
        building_categories=data.categories,
        buildings=data.buildings,
        production_methods=data.production_methods,
        unresolved_production_methods=data.unresolved_production_methods,
        duplicate_production_methods=data.duplicate_production_methods,
    )
    _print_header(profile, output)
    _print_age_filter(age, include_specific_unlocks, hidden)
    _print_table_summary("building_categories", data.categories)
    _print_table_summary("buildings", data.buildings)
    _print_table_summary("production_methods", data.production_methods)
    _print_table_summary("unresolved_production_methods", data.unresolved_production_methods)
    _print_table_summary("duplicate_production_methods", data.duplicate_production_methods)
    _print_warnings(data.warnings)


@app.command()
def goods(
    load_order: Annotated[
        Path, typer.Option(help="Load-order TOML file.")
    ] = DEFAULT_LOAD_ORDER_PATH,
    profile: Annotated[str, typer.Option(help="Data profile to parse.")] = "merged_default",
    output: Annotated[Path, typer.Option(help="Output directory.")] = Path("out"),
    format: Annotated[OutputFormat, typer.Option("--format", "-f")] = OutputFormat.parquet,
) -> None:
    data = load_goods_data(profile=profile, load_order_path=load_order)
    output.mkdir(parents=True, exist_ok=True)
    _write_table(data.goods, output / f"goods.{format.value}", format)
    _print_header(profile, output)
    _print_table_summary("goods", data.goods)
    _print_warnings(data.warnings)


@savegame_app.callback(invoke_without_command=True)
def savegame(
    ctx: typer.Context,
    save: Annotated[Path | None, typer.Option(help="Explicit .eu5 save file.")] = None,
    save_dir: Annotated[
        Path,
        typer.Option(help="Directory used when --save is omitted."),
    ] = DEFAULT_SAVE_GAMES_DIR,
    load_order: Annotated[
        Path, typer.Option(help="Load-order TOML file.")
    ] = DEFAULT_LOAD_ORDER_PATH,
    profile: Annotated[str, typer.Option(help="Data profile to parse.")] = "merged_default",
    output: Annotated[Path, typer.Option(help="Output directory.")] = Path("out/savegame"),
    force_rakaly: Annotated[
        bool,
        typer.Option(
            help="Force the optional Rakaly/pyeu5 fallback instead of native text parsing."
        ),
    ] = False,
) -> None:
    if ctx.invoked_subcommand is not None:
        return
    tables = write_savegame_parquet(
        output,
        save_path=save,
        save_dir=save_dir,
        profile=profile,
        load_order_path=load_order,
        force_rakaly=force_rakaly,
    )
    explorer_path = write_savegame_explorer_html(tables, output / "savegame_explorer.html")
    _print_header(profile, output)
    typer.echo(f"explorer: {explorer_path}")
    typer.echo("")
    typer.echo("Savegame")
    for name, table in tables.as_dict().items():
        _print_table_summary(name, table)


@savegame_app.command("ingest")
def savegame_ingest(
    save_dir: Annotated[
        Path,
        typer.Option(help="Directory containing .eu5 saves."),
    ] = DEFAULT_SAVE_GAMES_DIR,
    output: Annotated[
        Path,
        typer.Option(help="Progression dataset output directory."),
    ] = Path("out/savegame_progression/dataset"),
    load_order: Annotated[
        Path, typer.Option(help="Load-order TOML file.")
    ] = DEFAULT_LOAD_ORDER_PATH,
    profile: Annotated[str, typer.Option(help="Data profile to parse.")] = "merged_default",
    workers: Annotated[int, typer.Option(help="Parallel save parser workers.")] = 1,
    min_file_age: Annotated[
        float,
        typer.Option(help="Skip saves modified less than this many seconds ago."),
    ] = 10.0,
    force_rakaly: Annotated[
        bool,
        typer.Option(help="Force Rakaly/pyeu5 decoding instead of native text parsing."),
    ] = False,
    extended: Annotated[
        bool,
        typer.Option(help="Include slower legacy parity tables such as countries and population."),
    ] = False,
) -> None:
    result = ingest_savegame_dataset(
        output,
        save_dir=save_dir,
        profile=profile,
        load_order_path=load_order,
        workers=workers,
        min_file_age_seconds=min_file_age,
        force_rakaly=force_rakaly,
        include_extended=extended,
    )
    _print_header(profile, output)
    typer.echo("Savegame progression ingest")
    typer.echo(f"processed: {len(result.processed)}")
    typer.echo(f"skipped: {len(result.skipped)}")
    typer.echo(f"busy: {len(result.transient)}")
    typer.echo(f"failures: {len(result.failures)}")
    typer.echo(f"seconds: {result.elapsed_seconds:.2f}")
    if result.transient:
        for transient in result.transient[:10]:
            typer.echo(f"~ {transient['path']}: busy: {transient['error']}")
    if result.failures:
        for failure in result.failures[:10]:
            typer.echo(f"- {failure['path']}: {failure['type']}: {failure['error']}")


@savegame_app.command("watch")
def savegame_watch(
    save_dir: Annotated[
        Path,
        typer.Option(help="Directory containing .eu5 saves."),
    ] = DEFAULT_SAVE_GAMES_DIR,
    output: Annotated[
        Path,
        typer.Option(help="Progression dataset output directory."),
    ] = Path("out/savegame_progression/dataset"),
    load_order: Annotated[
        Path, typer.Option(help="Load-order TOML file.")
    ] = DEFAULT_LOAD_ORDER_PATH,
    profile: Annotated[str, typer.Option(help="Data profile to parse.")] = "merged_default",
    workers: Annotated[int, typer.Option(help="Parallel save parser workers.")] = 1,
    interval: Annotated[float, typer.Option(help="Seconds between ingest cycles.")] = 30.0,
    min_file_age: Annotated[
        float,
        typer.Option(help="Skip saves modified less than this many seconds ago."),
    ] = 0.0,
    force_rakaly: Annotated[
        bool,
        typer.Option(help="Force Rakaly/pyeu5 decoding instead of native text parsing."),
    ] = False,
    extended: Annotated[
        bool,
        typer.Option(help="Include slower legacy parity tables such as countries and population."),
    ] = False,
    max_cycles: Annotated[
        int | None,
        typer.Option(help="Stop after this many cycles. Intended for tests and smoke checks."),
    ] = None,
    verbose_busy: Annotated[
        bool,
        typer.Option(help="Print individual busy autosaves for each watch cycle."),
    ] = False,
) -> None:
    def print_cycle(cycle: int, result) -> None:
        typer.echo(
            f"cycle {cycle}: processed={len(result.processed)} "
            f"skipped={len(result.skipped)} busy={len(result.transient)} "
            f"failures={len(result.failures)} seconds={result.elapsed_seconds:.2f}"
        )
        if verbose_busy:
            for transient in result.transient[:10]:
                typer.echo(f"~ {transient['path']}: busy: {transient['error']}")
        for failure in result.failures[:10]:
            typer.echo(f"- {failure['path']}: {failure['type']}: {failure['error']}")

    _print_header(profile, output)
    typer.echo("Savegame progression watch")
    watch_savegame_dataset(
        output,
        save_dir=save_dir,
        profile=profile,
        load_order_path=load_order,
        workers=workers,
        interval_seconds=interval,
        min_file_age_seconds=min_file_age,
        force_rakaly=force_rakaly,
        include_extended=extended,
        max_cycles=max_cycles,
        on_cycle=print_cycle,
    )


@savegame_app.command("progress")
def savegame_progress(
    dataset: Annotated[
        Path,
        typer.Option(help="Progression dataset directory."),
    ] = Path("out/savegame_progression/dataset"),
    output: Annotated[
        Path,
        typer.Option(help="Output HTML path."),
    ] = Path("out/savegame_progression/savegame_progression.html"),
    playthrough: Annotated[
        str | None,
        typer.Option(help="Specific playthrough id to render."),
    ] = None,
    top_n: Annotated[int, typer.Option(help="Top entities to include in heavy drilldowns.")] = 40,
) -> None:
    savegame_dataset = SavegameDataset(dataset)
    payload = savegame_dataset.build_progression_cubes(playthrough_id=playthrough, top_n=top_n)
    html_path = write_savegame_progression_html(payload, output)
    typer.echo(f"dataset: {dataset}")
    typer.echo(f"wrote: {html_path}")
    typer.echo(f"snapshots: {len(payload.get('snapshots') or [])}")
    typer.echo(f"payload_bytes: {payload.get('payloadSummary', {}).get('jsonBytes', 0)}")


@savegame_app.command("benchmark")
def savegame_benchmark(
    save_dir: Annotated[
        Path,
        typer.Option(help="Directory containing .eu5 saves."),
    ] = DEFAULT_SAVE_GAMES_DIR,
    output: Annotated[
        Path,
        typer.Option(help="Benchmark output directory."),
    ] = Path("out/savegame_progression/benchmark"),
    sample: Annotated[
        str,
        typer.Option(help="latest, full-playthrough, or a positive integer snapshot count."),
    ] = "latest",
    load_order: Annotated[
        Path, typer.Option(help="Load-order TOML file.")
    ] = DEFAULT_LOAD_ORDER_PATH,
    profile: Annotated[str, typer.Option(help="Data profile to parse.")] = "merged_default",
    workers: Annotated[int, typer.Option(help="Parallel save parser workers.")] = 1,
    top_n: Annotated[int, typer.Option(help="Top entities to include in the dashboard.")] = 40,
    force_rakaly: Annotated[
        bool,
        typer.Option(help="Force Rakaly/pyeu5 decoding instead of native text parsing."),
    ] = False,
    extended: Annotated[
        bool,
        typer.Option(help="Include slower legacy parity tables during benchmark ingestion."),
    ] = False,
    profile_output: Annotated[
        Path | None,
        typer.Option(help="Optional cProfile text output path."),
    ] = None,
) -> None:
    result = benchmark_savegame_progression(
        output,
        save_dir=save_dir,
        sample=sample,
        profile=profile,
        load_order_path=load_order,
        workers=workers,
        top_n=top_n,
        force_rakaly=force_rakaly,
        include_extended=extended,
        profile_output=profile_output,
    )
    typer.echo(f"report: {result.report_path}")
    if result.html_path is not None:
        typer.echo(f"html: {result.html_path}")
    if result.profile_path is not None:
        typer.echo(f"profile: {result.profile_path}")
    typer.echo(f"processed: {result.report.get('processed')}")
    typer.echo(f"seconds: {result.report.get('elapsed_seconds'):.2f}")
    typer.echo(f"peak_rss_bytes: {result.report.get('peak_rss_bytes')}")
    typer.echo(f"dataset_bytes: {result.report.get('dataset_bytes')}")


@dashboard_app.callback()
def dashboard(
    ctx: typer.Context,
    dataset: Annotated[
        Path,
        typer.Option(help="Progression dataset directory."),
    ] = Path("out/savegame_progression/dataset"),
    load_order: Annotated[
        Path, typer.Option(help="Load-order TOML file.")
    ] = DEFAULT_LOAD_ORDER_PATH,
    profile: Annotated[
        str,
        typer.Option(help="Data profile to parse for game-data references."),
    ] = "merged_default",
    host: Annotated[str, typer.Option(help="Dashboard bind host.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Dashboard bind port.")] = 8050,
    debug: Annotated[bool, typer.Option(help="Run Dash in debug mode.")] = False,
    refresh_ms: Annotated[
        int,
        typer.Option(help="Browser refresh interval in milliseconds."),
    ] = 5000,
) -> None:
    if ctx.invoked_subcommand is not None:
        return
    run_dashboard(
        dataset,
        profile=profile,
        load_order_path=load_order,
        host=host,
        port=port,
        debug=debug,
        refresh_ms=refresh_ms,
    )


@dashboard_app.command("serve")
def dashboard_serve(
    dataset: Annotated[
        Path,
        typer.Option(help="Progression dataset directory."),
    ] = Path("out/savegame_progression/dataset"),
    load_order: Annotated[
        Path, typer.Option(help="Load-order TOML file.")
    ] = DEFAULT_LOAD_ORDER_PATH,
    profile: Annotated[
        str,
        typer.Option(help="Data profile to parse for game-data references."),
    ] = "merged_default",
    host: Annotated[str, typer.Option(help="Dashboard bind host.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Dashboard bind port.")] = 8050,
    debug: Annotated[bool, typer.Option(help="Run Dash in debug mode.")] = False,
    refresh_ms: Annotated[
        int,
        typer.Option(help="Browser refresh interval in milliseconds."),
    ] = 5000,
) -> None:
    run_dashboard(
        dataset,
        profile=profile,
        load_order_path=load_order,
        host=host,
        port=port,
        debug=debug,
        refresh_ms=refresh_ms,
    )


@dashboard_app.command("benchmark")
def dashboard_benchmark(
    dataset: Annotated[
        Path,
        typer.Option(help="Progression dataset directory."),
    ] = Path("out/savegame_progression/dataset"),
    output: Annotated[
        Path,
        typer.Option(help="Benchmark JSON report path."),
    ] = Path("out/savegame_progression/dashboard_benchmark_report.json"),
    load_order: Annotated[
        Path, typer.Option(help="Load-order TOML file.")
    ] = DEFAULT_LOAD_ORDER_PATH,
    profile: Annotated[
        str,
        typer.Option(help="Data profile to parse for game-data references."),
    ] = "merged_default",
) -> None:
    peak_rss = _current_rss_bytes()
    timings: dict[str, float] = {}

    started = time.perf_counter()
    adapter = SavegameDashboardAdapter(
        dataset,
        profile=profile,
        load_order_path=load_order,
    )
    timings["startup_seconds"] = time.perf_counter() - started
    peak_rss = max(peak_rss or 0, _current_rss_bytes() or 0) or None

    for key, operation in [
        ("metadata_seconds", adapter.template_metadata),
        ("overview_seconds", adapter.overview),
        (
            "template_super_region_seconds",
            lambda: adapter.template_query(
                metric_key="population:pops",
                scope="super_region",
                limit=5,
            ),
        ),
    ]:
        phase_started = time.perf_counter()
        operation()
        timings[key] = time.perf_counter() - phase_started
        peak_rss = max(peak_rss or 0, _current_rss_bytes() or 0) or None

    report = {
        "dataset": str(dataset),
        "profile": profile,
        "load_order": str(load_order),
        "timings": timings,
        "peak_rss_bytes": peak_rss,
        "cache": adapter.cache_info(),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    typer.echo(f"report: {output}")
    typer.echo(f"startup_seconds: {timings['startup_seconds']:.3f}")
    typer.echo(f"metadata_seconds: {timings['metadata_seconds']:.3f}")
    typer.echo(f"template_super_region_seconds: {timings['template_super_region_seconds']:.3f}")
    typer.echo(f"peak_rss_bytes: {peak_rss}")


@dashboard_app.command("start")
def dashboard_start(
    dataset: Annotated[
        Path,
        typer.Option(help="Progression dataset directory."),
    ] = Path("out/savegame_progression/dataset"),
    load_order: Annotated[
        Path, typer.Option(help="Load-order TOML file.")
    ] = DEFAULT_LOAD_ORDER_PATH,
    profile: Annotated[
        str,
        typer.Option(help="Data profile to parse for game-data references."),
    ] = "merged_default",
    host: Annotated[str, typer.Option(help="Dashboard bind host.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Dashboard bind port.")] = 8050,
    timeout: Annotated[float, typer.Option(help="Health-check timeout in seconds.")] = 20.0,
    refresh_ms: Annotated[
        int,
        typer.Option(help="Browser refresh interval in milliseconds."),
    ] = 5000,
) -> None:
    info = start_dashboard_process(
        dataset=dataset,
        profile=profile,
        load_order_path=load_order,
        host=host,
        port=port,
        timeout_seconds=timeout,
        refresh_ms=refresh_ms,
    )
    _print_dashboard_status(info)
    if not info.healthy:
        raise typer.Exit(1)


@dashboard_app.command("stop")
def dashboard_stop(
    port: Annotated[int, typer.Option(help="Dashboard bind port.")] = 8050,
) -> None:
    info = stop_dashboard_process(port=port)
    _print_dashboard_status(info)


@dashboard_app.command("status")
def dashboard_status_command(
    host: Annotated[str, typer.Option(help="Dashboard bind host.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Dashboard bind port.")] = 8050,
) -> None:
    info = dashboard_status(host=host, port=port)
    _print_dashboard_status(info)
    if not info.healthy:
        raise typer.Exit(1)


@app.command("goods-flow", hidden=True)
def goods_flow(
    load_order: Annotated[
        Path, typer.Option(help="Load-order TOML file.")
    ] = DEFAULT_LOAD_ORDER_PATH,
    profile: Annotated[str, typer.Option(help="Data profile to parse.")] = "merged_default",
    output: Annotated[Path, typer.Option(help="Output directory.")] = Path("out"),
    format: Annotated[OutputFormat, typer.Option("--format", "-f")] = OutputFormat.parquet,
    age: Annotated[str | None, typer.Option(help="Maximum age to include.")] = None,
    include_specific_unlocks: Annotated[
        bool,
        typer.Option(help="Include country, region, and religion-specific unlocks."),
    ] = False,
) -> None:
    data = load_building_data(profile=profile, load_order_path=load_order)
    hidden = {}
    if age is not None:
        advancement_data = load_advancement_data(profile=profile, load_order_path=load_order)
        filtered = filter_building_data_by_age(
            data,
            advancement_data.advancements,
            age,
            include_specific_unlocks=include_specific_unlocks,
        )
        hidden = hidden_counts(data, filtered)
        data = filtered
    output.mkdir(parents=True, exist_ok=True)
    _write_table(data.goods_flow_nodes, output / f"goods_flow_nodes.{format.value}", format)
    _write_table(data.goods_flow_edges, output / f"goods_flow_edges.{format.value}", format)
    _print_header(profile, output)
    _print_age_filter(age, include_specific_unlocks, hidden)
    _print_table_summary("goods_flow_nodes", data.goods_flow_nodes)
    _print_table_summary("goods_flow_edges", data.goods_flow_edges)
    _print_warnings(data.warnings)


@app.command("all")
def all_data(
    load_order: Annotated[
        Path, typer.Option(help="Load-order TOML file.")
    ] = DEFAULT_LOAD_ORDER_PATH,
    profile: Annotated[str, typer.Option(help="Data profile to parse.")] = "merged_default",
    output: Annotated[Path, typer.Option(help="Output directory.")] = Path("out"),
    format: Annotated[OutputFormat, typer.Option("--format", "-f")] = OutputFormat.parquet,
    age: Annotated[str | None, typer.Option(help="Maximum age to include.")] = None,
    include_specific_unlocks: Annotated[
        bool,
        typer.Option(help="Include country, region, and religion-specific unlocks."),
    ] = False,
) -> None:
    data = load_eu5_data(profile=profile, load_order_path=load_order)
    hidden = {}
    if age is not None:
        filtered = filter_eu5_data_by_age(
            data,
            age,
            include_specific_unlocks=include_specific_unlocks,
        )
        hidden = hidden_counts(data.building_data, filtered.building_data)
        data = filtered
    output.mkdir(parents=True, exist_ok=True)
    _write_tables(
        output,
        format,
        advancements=data.advancements,
        goods=data.goods,
        goods_summary=data.goods_summary,
        building_categories=data.building_data.categories,
        buildings=data.buildings,
        production_methods=data.production_methods,
        unresolved_production_methods=data.building_data.unresolved_production_methods,
        duplicate_production_methods=data.building_data.duplicate_production_methods,
        goods_flow_nodes=data.goods_flow_nodes,
        goods_flow_edges=data.goods_flow_edges,
    )
    _print_header(profile, output)
    _print_age_filter(age, include_specific_unlocks, hidden)
    typer.echo("Goods")
    _print_table_summary("goods", data.goods)
    _print_table_summary("goods_summary", data.goods_summary)
    typer.echo("")
    typer.echo("Buildings")
    _print_table_summary("building_categories", data.building_data.categories)
    _print_table_summary("buildings", data.buildings)
    _print_table_summary("production_methods", data.production_methods)
    typer.echo("")
    typer.echo("Advancements")
    _print_table_summary("advancements", data.advancements)
    typer.echo("")
    typer.echo("Graphs")
    _print_table_summary("goods_flow_nodes", data.goods_flow_nodes)
    _print_table_summary("goods_flow_edges", data.goods_flow_edges)
    _print_warnings(data.warnings)


def _write_table(table, path: Path, format: OutputFormat) -> None:
    if format is OutputFormat.csv:
        _csv_safe_table(table).write_csv(path)
    else:
        table.write_parquet(path)


def _write_tables(output: Path, format: OutputFormat, **tables) -> None:
    for name, table in tables.items():
        _write_table(table, output / f"{name}.{format.value}", format)


def _print_header(profile: str, output: Path) -> None:
    typer.echo(f"profile: {profile}")
    typer.echo(f"wrote: {output}")
    typer.echo("")


def _print_age_filter(
    age: str | None,
    include_specific_unlocks: bool,
    hidden: dict[str, int],
) -> None:
    if age is None:
        return
    typer.echo(f"age: {age}")
    typer.echo(f"specific_unlocks: {'included' if include_specific_unlocks else 'excluded'}")
    if hidden:
        typer.echo(
            "hidden: "
            f"buildings {hidden.get('buildings', 0)}, "
            f"production_methods {hidden.get('production_methods', 0)}"
        )
    typer.echo("")


def _print_table_summary(name: str, table: pl.DataFrame) -> None:
    typer.echo(f"{name}: {table.height}")
    source_counts = _source_counts(table)
    if source_counts:
        for source, count in source_counts.items():
            typer.echo(f"  {source}: {count}")
    mode_counts = _mode_counts(table)
    if mode_counts:
        typer.echo(f"  modes: {_format_counts(mode_counts)}")


def _source_counts(table: pl.DataFrame) -> dict[str, int]:
    if not {"source_layer", "source_mod"}.issubset(table.columns):
        return {}
    counts: dict[str, int] = {}
    for row in table.select(
        [
            "source_layer",
            "source_mod",
            *([] if "source_kind" not in table.columns else ["source_kind"]),
        ]
    ).to_dicts():
        if row.get("source_kind") == "generated_rgo":
            source = "generated_rgo"
        else:
            source = row.get("source_mod") or row.get("source_layer") or "unknown"
        counts[source] = counts.get(source, 0) + 1
    return _ordered_counts(counts)


def _mode_counts(table: pl.DataFrame) -> dict[str, int]:
    modes: dict[str, int] = {}
    if "source_mode" in table.columns:
        for mode in table["source_mode"].to_list():
            if mode is not None:
                modes[str(mode)] = modes.get(str(mode), 0) + 1
    if "source_kind" in table.columns:
        generated = sum(1 for kind in table["source_kind"].to_list() if kind == "generated_rgo")
        if generated:
            modes["generated_rgo"] = generated
    return _ordered_counts(modes)


def _ordered_counts(counts: dict[str, int]) -> dict[str, int]:
    return dict(sorted(counts.items(), key=lambda item: (item[0] != "vanilla", item[0])))


def _format_counts(counts: dict[str, int]) -> str:
    return ", ".join(f"{name} {count}" for name, count in counts.items())


def _print_warnings(warnings: list[str]) -> None:
    if not warnings:
        return
    typer.echo("")
    typer.echo(f"warnings: {len(warnings)}")
    for warning in warnings[:20]:
        typer.echo(f"- {warning}")
    if len(warnings) > 20:
        typer.echo(f"- ... {len(warnings) - 20} more")


def _print_dashboard_status(info) -> None:
    typer.echo(f"dashboard: {info.url}")
    typer.echo(f"healthy: {info.healthy}")
    typer.echo(f"running: {info.running}")
    typer.echo(f"pid: {info.pid or ''}")
    if info.dataset:
        typer.echo(f"dataset: {info.dataset}")
    typer.echo(f"log: {info.log_path}")
    typer.echo(f"state: {info.state_path}")


def _current_rss_bytes() -> int | None:
    try:
        import psutil
    except ModuleNotFoundError:
        return None
    return psutil.Process().memory_info().rss


def _csv_safe_table(table):
    nested_columns = [
        name for name, dtype in zip(table.columns, table.dtypes, strict=True) if dtype.is_nested()
    ]
    if not nested_columns:
        return table
    return table.with_columns(
        [
            pl.col(name)
            .map_elements(_json_cell, return_dtype=pl.String)
            .alias(name)
            for name in nested_columns
        ]
    )


def _json_cell(value) -> str | None:
    if value is None:
        return None
    if hasattr(value, "to_list"):
        value = value.to_list()
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


if __name__ == "__main__":
    app()
