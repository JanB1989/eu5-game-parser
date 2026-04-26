from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer

from eu5gameparser.config import ParserConfig
from eu5gameparser.domain.buildings import load_building_data
from eu5gameparser.domain.eu5 import load_eu5_data
from eu5gameparser.domain.goods import load_goods_data
from eu5gameparser.load_order import DEFAULT_LOAD_ORDER_PATH, LoadOrderConfig

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
def buildings(
    game_root: Annotated[Path | None, typer.Option(help="EU5 installation root.")] = None,
    load_order: Annotated[
        Path, typer.Option(help="Load-order TOML file.")
    ] = DEFAULT_LOAD_ORDER_PATH,
    profile: Annotated[str | None, typer.Option(help="Data profile to parse.")] = None,
) -> None:
    config = None if profile else ParserConfig.from_env(game_root)
    data = load_building_data(config, profile=profile, load_order_path=load_order)
    typer.echo(f"categories: {data.categories.height}")
    typer.echo(f"buildings: {data.buildings.height}")
    typer.echo(f"production_methods: {data.production_methods.height}")
    typer.echo(f"unresolved_production_methods: {data.unresolved_production_methods.height}")
    if data.warnings:
        typer.echo("warnings:")
        for warning in data.warnings[:20]:
            typer.echo(f"- {warning}")
        if len(data.warnings) > 20:
            typer.echo(f"- ... {len(data.warnings) - 20} more")


@app.command()
def goods(
    game_root: Annotated[Path | None, typer.Option(help="EU5 installation root.")] = None,
    load_order: Annotated[
        Path, typer.Option(help="Load-order TOML file.")
    ] = DEFAULT_LOAD_ORDER_PATH,
    profile: Annotated[str | None, typer.Option(help="Data profile to parse.")] = None,
    output: Annotated[Path, typer.Option(help="Output directory.")] = Path("out"),
    format: Annotated[OutputFormat, typer.Option("--format", "-f")] = OutputFormat.parquet,
) -> None:
    config = None if profile else ParserConfig.from_env(game_root)
    data = load_goods_data(config, profile=profile, load_order_path=load_order)
    output.mkdir(parents=True, exist_ok=True)
    _write_table(data.goods, output / f"goods.{format.value}", format)
    typer.echo(f"goods: {data.goods.height}")
    typer.echo(f"wrote goods to {output}")


@app.command("goods-flow")
def goods_flow(
    game_root: Annotated[Path | None, typer.Option(help="EU5 installation root.")] = None,
    load_order: Annotated[
        Path, typer.Option(help="Load-order TOML file.")
    ] = DEFAULT_LOAD_ORDER_PATH,
    profile: Annotated[str | None, typer.Option(help="Data profile to parse.")] = None,
    output: Annotated[Path, typer.Option(help="Output directory.")] = Path("out"),
    format: Annotated[OutputFormat, typer.Option("--format", "-f")] = OutputFormat.parquet,
) -> None:
    config = None if profile else ParserConfig.from_env(game_root)
    data = load_building_data(config, profile=profile, load_order_path=load_order)
    output.mkdir(parents=True, exist_ok=True)
    _write_table(data.goods_flow_nodes, output / f"goods_flow_nodes.{format.value}", format)
    _write_table(data.goods_flow_edges, output / f"goods_flow_edges.{format.value}", format)
    typer.echo(f"wrote nodes and edges to {output}")


@app.command("all")
def all_data(
    load_order: Annotated[
        Path, typer.Option(help="Load-order TOML file.")
    ] = DEFAULT_LOAD_ORDER_PATH,
    profile: Annotated[str, typer.Option(help="Data profile to parse.")] = "merged_default",
) -> None:
    data = load_eu5_data(profile=profile, load_order_path=load_order)
    typer.echo(f"buildings: {data.buildings.height}")
    typer.echo(f"goods: {data.goods.height}")
    typer.echo(f"production_methods: {data.production_methods.height}")
    typer.echo(f"goods_flow_nodes: {data.goods_flow_nodes.height}")
    typer.echo(f"goods_flow_edges: {data.goods_flow_edges.height}")
    if data.warnings:
        typer.echo("warnings:")
        for warning in data.warnings[:20]:
            typer.echo(f"- {warning}")
        if len(data.warnings) > 20:
            typer.echo(f"- ... {len(data.warnings) - 20} more")


def _write_table(table, path: Path, format: OutputFormat) -> None:
    if format is OutputFormat.csv:
        table.write_csv(path)
    else:
        table.write_parquet(path)


if __name__ == "__main__":
    app()
