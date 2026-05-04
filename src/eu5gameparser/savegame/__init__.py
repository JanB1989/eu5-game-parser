from __future__ import annotations

from eu5gameparser.savegame.dashboard import create_dashboard_app, run_dashboard
from eu5gameparser.savegame.dashboard_adapter import (
    BuildingIconResolver,
    SavegameDashboardAdapter,
)
from eu5gameparser.savegame.dataset import (
    SavegameDataset,
    benchmark_savegame_progression,
    discover_playthroughs,
    ingest_savegame_dataset,
    parse_ingame_date,
    playthrough_id_from_path,
    scan_for_work,
    select_sample_saves,
    watch_savegame_dataset,
)
from eu5gameparser.savegame.exporter import (
    DEFAULT_SAVE_GAMES_DIR,
    SavegameTables,
    is_text_save,
    latest_save_path,
    load_savegame_tables,
    write_savegame_parquet,
)
from eu5gameparser.savegame.html import write_savegame_explorer_html
from eu5gameparser.savegame.progression_html import write_savegame_progression_html

__all__ = [
    "DEFAULT_SAVE_GAMES_DIR",
    "SavegameTables",
    "SavegameDataset",
    "BuildingIconResolver",
    "SavegameDashboardAdapter",
    "benchmark_savegame_progression",
    "create_dashboard_app",
    "discover_playthroughs",
    "ingest_savegame_dataset",
    "is_text_save",
    "latest_save_path",
    "load_savegame_tables",
    "parse_ingame_date",
    "playthrough_id_from_path",
    "run_dashboard",
    "scan_for_work",
    "select_sample_saves",
    "watch_savegame_dataset",
    "write_savegame_parquet",
    "write_savegame_explorer_html",
    "write_savegame_progression_html",
]
