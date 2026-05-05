from __future__ import annotations

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
from eu5gameparser.savegame.notebook_dataset import (
    NotebookBuildResult,
    SavegameNotebookDataset,
    build_savegame_notebook_dataset,
    rank_groups,
)
from eu5gameparser.savegame import notebook_analysis
from eu5gameparser.savegame import notebook_workbench
from eu5gameparser.savegame.progression_html import write_savegame_progression_html

__all__ = [
    "DEFAULT_SAVE_GAMES_DIR",
    "SavegameTables",
    "SavegameDataset",
    "SavegameNotebookDataset",
    "NotebookBuildResult",
    "notebook_analysis",
    "notebook_workbench",
    "benchmark_savegame_progression",
    "build_savegame_notebook_dataset",
    "discover_playthroughs",
    "ingest_savegame_dataset",
    "is_text_save",
    "latest_save_path",
    "load_savegame_tables",
    "parse_ingame_date",
    "playthrough_id_from_path",
    "rank_groups",
    "scan_for_work",
    "select_sample_saves",
    "watch_savegame_dataset",
    "write_savegame_parquet",
    "write_savegame_explorer_html",
    "write_savegame_progression_html",
]
