from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer

from eu5gameparser.config import ParserConfig
from eu5gameparser.domain.buildings import load_building_data

app = typer.Typer(help="Parse Europa Universalis V game files.")


class OutputFormat(StrEnum):
    parquet = "parquet"
    csv = "csv"


@app.command("inspect-paths")
def inspect_paths(
    game_root: Annotated[Path | None, typer.Option(help="EU5 installation root.")] = None,
) -> None:
    config = ParserConfig.from_env(game_root)
    for name, path in config.paths().items():
        status = "ok" if path.exists() else "missing"
        typer.echo(f"{name}: {path} [{status}]")


@app.command()
def buildings(
    game_root: Annotated[Path | None, typer.Option(help="EU5 installation root.")] = None,
) -> None:
    data = load_building_data(ParserConfig.from_env(game_root))
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


@app.command("goods-flow")
def goods_flow(
    game_root: Annotated[Path | None, typer.Option(help="EU5 installation root.")] = None,
    output: Annotated[Path, typer.Option(help="Output directory.")] = Path("out"),
    format: Annotated[OutputFormat, typer.Option("--format", "-f")] = OutputFormat.parquet,
) -> None:
    data = load_building_data(ParserConfig.from_env(game_root))
    output.mkdir(parents=True, exist_ok=True)
    _write_table(data.goods_flow_nodes, output / f"goods_flow_nodes.{format.value}", format)
    _write_table(data.goods_flow_edges, output / f"goods_flow_edges.{format.value}", format)
    typer.echo(f"wrote nodes and edges to {output}")


def _write_table(table, path: Path, format: OutputFormat) -> None:
    if format is OutputFormat.csv:
        table.write_csv(path)
    else:
        table.write_parquet(path)


if __name__ == "__main__":
    app()
