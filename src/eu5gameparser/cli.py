from __future__ import annotations

import json
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
    write_savegame_explorer_html,
    write_savegame_parquet,
)

app = typer.Typer(help="Parse Europa Universalis V game files.")


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


@app.command("savegame")
def savegame(
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
