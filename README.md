# EU5 Game Parser

Fast, config-driven Python parsers for vanilla Europa Universalis V game data.

The first module parses building categories, building types, global production methods, and
building-local `unique_production_methods`, then exposes analysis-ready Polars tables and goods
flow node/edge tables.

## Setup

```powershell
uv sync --dev
```

The default game root is:

```text
C:\Games\steamapps\common\Europa Universalis V
```

You can override it with `--game-root` or `EU5_GAME_ROOT`.

## CLI

```powershell
uv run eu5parse inspect-paths
uv run eu5parse buildings
uv run eu5parse goods-flow --format csv --output .\out
```

## Tests

```powershell
uv run pytest
```

Integration tests that read the local game install are skipped unless explicitly enabled:

```powershell
$env:EU5_RUN_INTEGRATION="1"
uv run pytest -m integration
```

## Notes

Vanilla game files are read from disk and are not committed to this repository. Tests use small
synthetic fixtures so the parser remains easy to run and safe to publish.
