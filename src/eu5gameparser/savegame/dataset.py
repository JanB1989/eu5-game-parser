from __future__ import annotations

import cProfile
import hashlib
import json
import os
import pstats
import re
import shutil
import threading
import time
from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from eu5gameparser.domain.eu5 import load_eu5_data
from eu5gameparser.load_order import DEFAULT_LOAD_ORDER_PATH
from eu5gameparser.savegame.exporter import (
    DEFAULT_SAVE_GAMES_DIR,
    POP_TOTAL_COLUMNS,
    POP_TYPES,
    is_text_save,
    latest_save_path,
    load_savegame_tables,
)

AUTOSAVE_RE = re.compile(r"^autosave_([a-f0-9-]+)(?:_\d+)?\.eu5$", re.IGNORECASE)
PARTIAL_HASH_BYTES = 64 * 1024
MANIFEST_VERSION = 1
STAGING_MAX_AGE_SECONDS = 60 * 60
EXPLORER_HIGH_CARDINALITY_DIMENSIONS = frozenset({"province_slug", "area", "country_tag"})
REQUIRED_LOCATION_HIERARCHY_COLUMNS = (
    "province_slug",
    "area",
    "region",
    "macro_region",
    "super_region",
)


@dataclass(frozen=True)
class SaveFileState:
    path: Path
    mtime: float
    mtime_ns: int
    size: int

    @property
    def state_key(self) -> str:
        return f"{self.path.resolve()}|{self.mtime_ns}|{self.size}"


@dataclass(frozen=True)
class IngestResult:
    dataset: SavegameDataset
    processed: list[dict[str, Any]]
    skipped: list[Path]
    transient: list[dict[str, Any]]
    failures: list[dict[str, Any]]
    elapsed_seconds: float


@dataclass(frozen=True)
class BenchmarkResult:
    report: dict[str, Any]
    report_path: Path
    html_path: Path | None = None
    profile_path: Path | None = None


class TransientSaveError(RuntimeError):
    pass


class SaveChangedError(TransientSaveError):
    pass


class MissingLocationHierarchyError(RuntimeError):
    pass


class SavegameDataset:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    @property
    def manifest_path(self) -> Path:
        return self.root / "manifest.parquet"

    @property
    def tables_root(self) -> Path:
        return self.root / "tables"

    def read_manifest(self) -> pl.DataFrame:
        if not self.manifest_path.exists():
            return _empty_manifest()
        return pl.read_parquet(self.manifest_path)

    def write_manifest(self, rows: Iterable[dict[str, Any]]) -> pl.DataFrame:
        self.root.mkdir(parents=True, exist_ok=True)
        new_rows = list(rows)
        if not new_rows:
            manifest = self.read_manifest()
        else:
            new_manifest = pl.DataFrame(new_rows, infer_schema_length=None)
            current = self.read_manifest()
            if current.is_empty():
                manifest = new_manifest
            else:
                manifest = pl.concat([current, new_manifest], how="diagonal_relaxed")
            if "snapshot_id" in manifest.columns:
                manifest = manifest.unique(subset=["snapshot_id"], keep="last", maintain_order=True)
            manifest = _sort_manifest(manifest)
        temporary = self.root / f".manifest.{os.getpid()}.{threading.get_ident()}.parquet"
        manifest.write_parquet(temporary, compression="zstd")
        temporary.replace(self.manifest_path)
        return manifest

    def snapshots(self, playthrough_id: str | None = None) -> pl.DataFrame:
        manifest = self.read_manifest()
        if manifest.is_empty():
            return manifest
        if playthrough_id is not None and "playthrough_id" in manifest.columns:
            manifest = manifest.filter(pl.col("playthrough_id") == playthrough_id)
        return _sort_manifest(manifest)

    def scan(self, table: str, playthrough_id: str | None = None) -> pl.LazyFrame:
        files = self.table_files(table, playthrough_id=playthrough_id)
        if not files:
            return pl.DataFrame().lazy()
        return pl.scan_parquet(
            [str(path) for path in files],
            hive_partitioning=False,
            missing_columns="insert",
            extra_columns="ignore",
        )

    def table_files(self, table: str, playthrough_id: str | None = None) -> list[Path]:
        table_root = self.tables_root / table
        if playthrough_id is not None:
            table_root = table_root / f"playthrough_id={_safe_id(playthrough_id)}"
        if not table_root.exists():
            return []
        return sorted(table_root.rglob("*.parquet"))

    def build_progression_cubes(
        self,
        *,
        playthrough_id: str | None = None,
        top_n: int = 40,
    ) -> dict[str, Any]:
        snapshots = self.snapshots(playthrough_id)
        snapshot_rows = snapshots.to_dicts()
        overview_series = _overview_series(self, playthrough_id)
        explorer = _explorer_payload(self, playthrough_id, top_n=top_n)
        return {
            "schemaVersion": 2,
            "generatedAt": datetime.now().isoformat(timespec="seconds"),
            "playthroughId": playthrough_id,
            "snapshots": snapshot_rows,
            "overviewSeries": overview_series,
            "explorer": explorer,
            "payloadSummary": {
                "overviewRows": sum(len(rows) for rows in overview_series.values()),
                "explorerRows": len(explorer["rows"]),
                "topNDefault": top_n,
            },
        }


def playthrough_id_from_path(path: str | Path) -> str:
    save_path = Path(path)
    match = AUTOSAVE_RE.match(save_path.name)
    if match:
        return _safe_id(match.group(1))
    return _safe_id(save_path.stem)


def parse_ingame_date(value: Any) -> tuple[int | None, int | None, int | None]:
    if value is None:
        return None, None, None
    match = re.match(r"^\s*(\d+)\.(\d+)\.(\d+)", str(value))
    if not match:
        return None, None, None
    return int(match.group(1)), int(match.group(2)), int(match.group(3))


def discover_playthroughs(save_dir: str | Path = DEFAULT_SAVE_GAMES_DIR) -> dict[str, list[Path]]:
    directory = Path(save_dir)
    groups: dict[str, list[Path]] = {}
    if not directory.is_dir():
        return groups
    for path in sorted(directory.glob("*.eu5"), key=lambda item: item.stat().st_mtime):
        groups.setdefault(playthrough_id_from_path(path), []).append(path.resolve())
    return groups


def select_sample_saves(
    save_dir: str | Path = DEFAULT_SAVE_GAMES_DIR,
    *,
    sample: str = "latest",
) -> list[Path]:
    if sample == "latest":
        latest = latest_save_path(save_dir)
        return [] if latest is None else [latest]
    groups = discover_playthroughs(save_dir)
    if not groups:
        return []
    if sample == "full-playthrough":
        _, saves = max(groups.items(), key=lambda item: (len(item[1]), _latest_mtime(item[1])))
        return sorted(saves, key=lambda path: path.stat().st_mtime)
    if sample.isdigit():
        count = max(1, int(sample))
        _, saves = max(groups.items(), key=lambda item: _latest_mtime(item[1]))
        return sorted(saves, key=lambda path: path.stat().st_mtime)[-count:]
    raise ValueError("sample must be latest, full-playthrough, or a positive integer")


def scan_for_work(
    output: str | Path,
    *,
    save_dir: str | Path = DEFAULT_SAVE_GAMES_DIR,
    save_paths: Iterable[str | Path] | None = None,
    min_file_age_seconds: float = 10.0,
) -> tuple[list[Path], list[Path]]:
    dataset = SavegameDataset(output)
    manifest = dataset.read_manifest()
    processed_states = (
        set(manifest.get_column("state_key").to_list())
        if "state_key" in manifest
        else set()
    )
    processed_hashes = (
        set(manifest.select(["size", "partial_hash"]).iter_rows())
        if {"size", "partial_hash"}.issubset(manifest.columns)
        else set()
    )

    if save_paths is None:
        candidates = [
            path.resolve()
            for paths in discover_playthroughs(save_dir).values()
            for path in paths
            if is_save_stable(path, min_file_age_seconds=min_file_age_seconds)
        ]
    else:
        candidates = [
            Path(path).resolve()
            for path in save_paths
            if Path(path).is_file()
            and is_save_stable(path, min_file_age_seconds=min_file_age_seconds)
        ]

    work: list[Path] = []
    skipped: list[Path] = []
    for path in sorted(candidates, key=lambda item: item.stat().st_mtime):
        state = file_state(path)
        quick_hash = partial_content_hash(path, state=state)
        if state.state_key in processed_states or (state.size, quick_hash) in processed_hashes:
            skipped.append(path)
            continue
        work.append(path)
    return work, skipped


def ingest_savegame_dataset(
    output: str | Path,
    *,
    save_dir: str | Path = DEFAULT_SAVE_GAMES_DIR,
    save_paths: Iterable[str | Path] | None = None,
    profile: str = "merged_default",
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
    workers: int = 1,
    min_file_age_seconds: float = 10.0,
    force_rakaly: bool = False,
    include_extended: bool = False,
) -> IngestResult:
    started = time.perf_counter()
    dataset = SavegameDataset(output)
    dataset.root.mkdir(parents=True, exist_ok=True)
    _cleanup_ingest_staging(dataset)
    work, skipped = scan_for_work(
        output,
        save_dir=save_dir,
        save_paths=save_paths,
        min_file_age_seconds=min_file_age_seconds,
    )
    processed: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    transient: list[dict[str, Any]] = []

    if not work:
        _cleanup_ingest_staging(dataset)
        return IngestResult(
            dataset=dataset,
            processed=processed,
            skipped=skipped,
            transient=transient,
            failures=failures,
            elapsed_seconds=time.perf_counter() - started,
        )

    if workers <= 1 or len(work) <= 1:
        eu5_data = load_eu5_data(profile=profile, load_order_path=load_order_path)
        for save_path in work:
            try:
                row = write_snapshot_to_dataset(
                    dataset.root,
                    save_path,
                    profile=profile,
                    load_order_path=load_order_path,
                    force_rakaly=force_rakaly,
                    include_extended=include_extended,
                    eu5_data=eu5_data,
                )
                processed.append(row)
                dataset.write_manifest([row])
            except TransientSaveError as exc:
                transient.append(
                    {"path": str(save_path), "error": str(exc), "type": type(exc).__name__}
                )
            except Exception as exc:  # pragma: no cover - surfaced in CLI and benchmark reports
                failures.append(
                    {"path": str(save_path), "error": str(exc), "type": type(exc).__name__}
                )
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    _write_snapshot_worker,
                    str(dataset.root),
                    str(save_path),
                    profile,
                    str(load_order_path),
                    force_rakaly,
                    include_extended,
                ): save_path
                for save_path in work
            }
            for future in as_completed(futures):
                save_path = futures[future]
                try:
                    row = future.result()
                    processed.append(row)
                    dataset.write_manifest([row])
                except TransientSaveError as exc:
                    transient.append(
                        {"path": str(save_path), "error": str(exc), "type": type(exc).__name__}
                    )
                except Exception as exc:  # pragma: no cover - surfaced in CLI and benchmark reports
                    failures.append(
                        {"path": str(save_path), "error": str(exc), "type": type(exc).__name__}
                    )

    _cleanup_ingest_staging(dataset)
    elapsed = time.perf_counter() - started
    return IngestResult(
        dataset=dataset,
        processed=processed,
        skipped=skipped,
        transient=transient,
        failures=failures,
        elapsed_seconds=elapsed,
    )


def write_snapshot_to_dataset(
    output: str | Path,
    save_path: str | Path,
    *,
    profile: str = "merged_default",
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
    force_rakaly: bool = False,
    include_extended: bool = False,
    eu5_data: Any | None = None,
) -> dict[str, Any]:
    dataset = SavegameDataset(output)
    save = Path(save_path).resolve()
    staged_save, before, quick_hash = _stage_stable_save_copy(dataset, save)
    parse_started = time.perf_counter()
    try:
        source_format = "text" if is_text_save(staged_save) and not force_rakaly else "rakaly"
        tables = load_savegame_tables(
            save_path=staged_save,
            profile=profile,
            load_order_path=load_order_path,
            force_rakaly=force_rakaly,
            eu5_data=eu5_data,
            include_extended=include_extended,
        )
        _validate_progression_location_hierarchy(
            tables.locations,
            profile=profile,
            load_order_path=load_order_path,
        )
    finally:
        staged_save.unlink(missing_ok=True)

    metadata = tables.save_metadata.to_dicts()[0] if not tables.save_metadata.is_empty() else {}
    year, month, day = parse_ingame_date(metadata.get("date"))
    playthrough_id = playthrough_id_from_path(save)
    snapshot_id = _snapshot_id(before, quick_hash, year=year, month=month, day=day)
    row_counts: dict[str, int] = {}
    for table_name, table in tables.as_dict().items():
        row_counts[table_name] = table.height
        target = (
            dataset.tables_root
            / table_name
            / f"playthrough_id={_safe_id(playthrough_id)}"
            / f"{snapshot_id}.parquet"
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        _table_with_snapshot_columns(
            table,
            snapshot_id=snapshot_id,
            playthrough_id=playthrough_id,
            source_path=save,
            date=metadata.get("date"),
            year=year,
            month=month,
            day=day,
        ).write_parquet(target, compression="zstd")

    parse_elapsed = time.perf_counter() - parse_started
    return {
        "manifest_version": MANIFEST_VERSION,
        "snapshot_id": snapshot_id,
        "playthrough_id": playthrough_id,
        "save_playthrough_id": metadata.get("playthrough_id"),
        "playthrough_name": metadata.get("playthrough_name"),
        "save_label": metadata.get("save_label"),
        "date": metadata.get("date"),
        "year": year,
        "month": month,
        "day": day,
        "date_sort": _date_sort(year, month, day),
        "path": str(save),
        "mtime": before.mtime,
        "mtime_ns": before.mtime_ns,
        "size": before.size,
        "partial_hash": quick_hash,
        "state_key": before.state_key,
        "source_format": source_format,
        "parser_profile": profile,
        "processed_at": datetime.now().isoformat(timespec="seconds"),
        "parse_seconds": parse_elapsed,
        "row_counts_json": json.dumps(row_counts, sort_keys=True, separators=(",", ":")),
    }


def _stage_stable_save_copy(
    dataset: SavegameDataset, save: Path
) -> tuple[Path, SaveFileState, str]:
    try:
        before = file_state(save)
        quick_hash = partial_content_hash(save, state=before)
    except OSError as exc:
        raise SaveChangedError(f"Save disappeared before it could be staged: {save}") from exc

    staging_root = dataset.root / ".ingest_staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    staged = (
        staging_root
        / f"{os.getpid()}_{threading.get_ident()}_{_safe_id(save.stem)}_{before.mtime_ns}.eu5"
    )
    try:
        shutil.copy2(save, staged)
        staged_state = file_state(staged)
        staged_hash = partial_content_hash(staged, state=staged_state)
    except OSError as exc:
        staged.unlink(missing_ok=True)
        raise SaveChangedError(
            f"Save changed or disappeared while it was being staged: {save}"
        ) from exc

    if before.size != staged_state.size or quick_hash != staged_hash:
        staged.unlink(missing_ok=True)
        raise SaveChangedError(f"Save was busy while it was being staged: {save}")
    return staged, before, quick_hash


def _cleanup_ingest_staging(
    dataset: SavegameDataset,
    *,
    max_age_seconds: float = STAGING_MAX_AGE_SECONDS,
) -> None:
    staging_root = dataset.root / ".ingest_staging"
    if not staging_root.is_dir():
        return
    threshold = time.time() - max(0.0, max_age_seconds)
    for path in staging_root.glob("*.eu5"):
        try:
            if path.stat().st_mtime <= threshold:
                path.unlink()
        except OSError:
            continue
    try:
        staging_root.rmdir()
    except OSError:
        pass


def watch_savegame_dataset(
    output: str | Path,
    *,
    save_dir: str | Path = DEFAULT_SAVE_GAMES_DIR,
    profile: str = "merged_default",
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
    workers: int = 1,
    interval_seconds: float = 30.0,
    min_file_age_seconds: float = 0.0,
    force_rakaly: bool = False,
    include_extended: bool = False,
    max_cycles: int | None = None,
    on_cycle: Any | None = None,
) -> list[IngestResult]:
    results: list[IngestResult] = []
    cycle = 0
    while max_cycles is None or cycle < max_cycles:
        cycle += 1
        result = ingest_savegame_dataset(
            output,
            save_dir=save_dir,
            profile=profile,
            load_order_path=load_order_path,
            workers=workers,
            min_file_age_seconds=min_file_age_seconds,
            force_rakaly=force_rakaly,
            include_extended=include_extended,
        )
        if max_cycles is not None:
            results.append(result)
        if on_cycle is not None:
            on_cycle(cycle, result)
        if max_cycles is not None and cycle >= max_cycles:
            break
        time.sleep(interval_seconds)
    return results


def _validate_progression_location_hierarchy(
    locations: pl.DataFrame,
    *,
    profile: str,
    load_order_path: str | Path,
) -> None:
    if locations.is_empty():
        raise MissingLocationHierarchyError(
            "Progression dataset ingest requires exported locations with geography "
            f"hierarchy. profile={profile!r}, load_order_path={str(load_order_path)!r}."
        )
    missing_columns = [
        column for column in REQUIRED_LOCATION_HIERARCHY_COLUMNS if column not in locations.columns
    ]
    blank_columns: list[str] = []
    for column in REQUIRED_LOCATION_HIERARCHY_COLUMNS:
        if column not in locations.columns:
            continue
        blank_rows = locations.filter(
            pl.col(column).is_null() | (pl.col(column).cast(pl.Utf8) == "")
        ).height
        if blank_rows:
            blank_columns.append(f"{column} ({blank_rows}/{locations.height} blank)")
    if not missing_columns and not blank_columns:
        return
    details = []
    if missing_columns:
        details.append("missing columns: " + ", ".join(missing_columns))
    if blank_columns:
        details.append("blank hierarchy values: " + ", ".join(blank_columns))
    raise MissingLocationHierarchyError(
        "Progression dataset ingest requires complete location geography hierarchy; "
        + "; ".join(details)
        + f". profile={profile!r}, load_order_path={str(load_order_path)!r}. "
        "Check that the load-order roots resolve to EU5 game/mod data containing "
        "game/in_game/map_data/definitions.txt."
    )


def benchmark_savegame_progression(
    output: str | Path,
    *,
    save_dir: str | Path = DEFAULT_SAVE_GAMES_DIR,
    sample: str = "latest",
    profile: str = "merged_default",
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
    workers: int = 1,
    top_n: int = 40,
    force_rakaly: bool = False,
    include_extended: bool = False,
    profile_output: str | Path | None = None,
) -> BenchmarkResult:
    from eu5gameparser.savegame.progression_html import write_savegame_progression_html

    output_path = Path(output)
    output_path.mkdir(parents=True, exist_ok=True)
    dataset_path = output_path / "dataset"
    sample_paths = select_sample_saves(save_dir, sample=sample)
    memory = _MemorySampler(include_children=workers > 1)
    memory.start()
    started = time.perf_counter()

    profiler: cProfile.Profile | None = cProfile.Profile() if profile_output else None
    if profiler is not None:
        profiler.enable()
    ingest = ingest_savegame_dataset(
        dataset_path,
        save_dir=save_dir,
        save_paths=sample_paths,
        profile=profile,
        load_order_path=load_order_path,
        workers=workers,
        min_file_age_seconds=0.0,
        force_rakaly=force_rakaly,
        include_extended=include_extended,
    )
    payload_started = time.perf_counter()
    playthrough = ingest.processed[0]["playthrough_id"] if ingest.processed else None
    payload = ingest.dataset.build_progression_cubes(playthrough_id=playthrough, top_n=top_n)
    payload_seconds = time.perf_counter() - payload_started
    html_path = write_savegame_progression_html(payload, output_path / "savegame_progression.html")
    if profiler is not None:
        profiler.disable()

    elapsed = time.perf_counter() - started
    peak_rss = memory.stop()
    dataset_bytes = _directory_size(dataset_path)
    html_bytes = html_path.stat().st_size
    profile_path = Path(profile_output) if profile_output else None
    if profiler is not None and profile_path is not None:
        profile_path.parent.mkdir(parents=True, exist_ok=True)
        with profile_path.open("w", encoding="utf-8") as handle:
            stats = pstats.Stats(profiler, stream=handle).sort_stats("cumulative")
            stats.print_stats(80)

    legacy_size = _legacy_pickle_size(playthrough)
    report = {
        "sample": sample,
        "include_extended": include_extended,
        "sample_count": len(sample_paths),
        "sample_bytes": sum(path.stat().st_size for path in sample_paths),
        "save_paths": [str(path) for path in sample_paths],
        "processed": len(ingest.processed),
        "skipped": len(ingest.skipped),
        "transient": ingest.transient,
        "failures": ingest.failures,
        "elapsed_seconds": elapsed,
        "ingest_seconds": ingest.elapsed_seconds,
        "payload_seconds": payload_seconds,
        "saves_per_second": len(ingest.processed) / ingest.elapsed_seconds
        if ingest.elapsed_seconds > 0
        else None,
        "peak_rss_bytes": peak_rss,
        "dataset_bytes": dataset_bytes,
        "html_bytes": html_bytes,
        "legacy_pickle_bytes_for_playthrough": legacy_size,
        "storage_ratio_vs_legacy": dataset_bytes / legacy_size if legacy_size else None,
        "row_counts": _combined_row_counts(ingest.processed),
    }
    report_path = output_path / "benchmark_report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return BenchmarkResult(
        report=report,
        report_path=report_path,
        html_path=html_path,
        profile_path=profile_path,
    )


def _write_snapshot_worker(
    output: str,
    save_path: str,
    profile: str,
    load_order_path: str,
    force_rakaly: bool,
    include_extended: bool,
) -> dict[str, Any]:
    return write_snapshot_to_dataset(
        output,
        save_path,
        profile=profile,
        load_order_path=load_order_path,
        force_rakaly=force_rakaly,
        include_extended=include_extended,
    )


def file_state(path: str | Path) -> SaveFileState:
    save = Path(path).resolve()
    stat = save.stat()
    return SaveFileState(
        path=save,
        mtime=stat.st_mtime,
        mtime_ns=stat.st_mtime_ns,
        size=stat.st_size,
    )


def is_save_stable(
    path: str | Path,
    *,
    min_file_age_seconds: float = 10.0,
    settle_seconds: float = 0.05,
) -> bool:
    save = Path(path)
    if not save.is_file():
        return False
    first = file_state(save)
    if min_file_age_seconds > 0 and time.time() - first.mtime < min_file_age_seconds:
        return False
    if settle_seconds > 0:
        time.sleep(settle_seconds)
    second = file_state(save)
    return first.mtime_ns == second.mtime_ns and first.size == second.size


def partial_content_hash(path: str | Path, *, state: SaveFileState | None = None) -> str:
    save = Path(path)
    state = state or file_state(save)
    digest = hashlib.sha256()
    with save.open("rb") as handle:
        digest.update(handle.read(PARTIAL_HASH_BYTES))
    digest.update(state.size.to_bytes(8, "little", signed=False))
    return digest.hexdigest()


def _table_with_snapshot_columns(
    table: pl.DataFrame,
    *,
    snapshot_id: str,
    playthrough_id: str,
    source_path: Path,
    date: Any,
    year: int | None,
    month: int | None,
    day: int | None,
) -> pl.DataFrame:
    output = table
    if "playthrough_id" in output.columns:
        output = output.rename({"playthrough_id": "save_playthrough_id"})
    return output.with_columns(
        [
            pl.lit(snapshot_id).alias("snapshot_id"),
            pl.lit(playthrough_id).alias("playthrough_id"),
            pl.lit(str(source_path)).alias("source_path"),
            pl.lit(date).alias("date"),
            pl.lit(year, dtype=pl.Int64).alias("year"),
            pl.lit(month, dtype=pl.Int64).alias("month"),
            pl.lit(day, dtype=pl.Int64).alias("day"),
            pl.lit(_date_sort(year, month, day), dtype=pl.Int64).alias("date_sort"),
        ]
    )


def _overview_series(
    dataset: SavegameDataset,
    playthrough_id: str | None,
) -> dict[str, list[dict[str, Any]]]:
    locations = dataset.scan("locations", playthrough_id)
    location_columns = _columns(locations)
    if not location_columns:
        return {
            "popsByType": [],
            "employment": [],
            "development": [],
            "tax": [],
            "food": [],
        }
    group = _snapshot_group_columns(location_columns)
    pop_columns = [column for column in POP_TOTAL_COLUMNS if column in location_columns]
    location_exprs = [
        _sum_or_zero(location_columns, "total_population"),
        _sum_or_zero(location_columns, "development"),
        _sum_or_zero(location_columns, "tax"),
        _sum_or_zero(location_columns, "possible_tax"),
        _sum_or_zero(location_columns, "rgo_employed"),
        _sum_or_zero(location_columns, "unemployed_total"),
        *(_sum_or_zero(location_columns, column) for column in pop_columns),
    ]
    overview = locations.group_by(group).agg(location_exprs)

    buildings = dataset.scan("buildings", playthrough_id)
    building_columns = _columns(buildings)
    if {"snapshot_id", "employed"}.issubset(building_columns):
        building_employment = buildings.group_by(_snapshot_group_columns(building_columns)).agg(
            _sum_or_zero(building_columns, "employed", "building_employed")
        )
        overview = overview.join(building_employment, on=group, how="left")
    else:
        overview = overview.with_columns(pl.lit(0.0).alias("building_employed"))

    overview_rows = _collect_sorted(overview)
    overview_by_snapshot = {row["snapshot_id"]: row for row in overview_rows}

    pops_by_type: list[dict[str, Any]] = []
    for row in overview_rows:
        for column in pop_columns:
            value = _number(row.get(column))
            if value == 0:
                continue
            pop_type = column.removeprefix("population_")
            pops_by_type.append(
                _series_row(row, pop_type=pop_type, value=value)
            )

    employment_rows = [
        {
            **_snapshot_values(row),
            "total_pops": row.get("total_population"),
            "employed_pops": _number(row.get("rgo_employed"))
            + _number(row.get("building_employed")),
            "unemployed_pops": row.get("unemployed_total"),
        }
        for row in overview_rows
    ]
    development_rows = [
        {**_snapshot_values(row), "development": row.get("development")} for row in overview_rows
    ]
    tax_rows = [
        {
            **_snapshot_values(row),
            "collected_tax": row.get("tax"),
            "uncollected_tax": max(_number(row.get("possible_tax")) - _number(row.get("tax")), 0.0),
            "possible_tax": row.get("possible_tax"),
        }
        for row in overview_rows
    ]

    food = dataset.scan("market_food", playthrough_id)
    food_columns = _columns(food)
    food_rows: list[dict[str, Any]] = []
    if food_columns:
        food_summary = food.group_by(_snapshot_group_columns(food_columns)).agg(
            [
                _sum_or_zero(food_columns, "food"),
                _sum_or_zero(food_columns, "food_max"),
                _sum_or_zero(food_columns, "food_supply"),
                _sum_or_zero(food_columns, "food_consumption"),
                _sum_or_zero(food_columns, "food_balance"),
                _sum_or_zero(food_columns, "missing"),
            ]
        )
        food_rows = _collect_sorted(food_summary)
    for row in food_rows:
        base = overview_by_snapshot.get(row["snapshot_id"], row)
        row.setdefault("date", base.get("date"))
        row.setdefault("date_sort", base.get("date_sort"))

    return {
        "popsByType": pops_by_type,
        "employment": employment_rows,
        "development": development_rows,
        "tax": tax_rows,
        "food": [
            {
                **_snapshot_values(row),
                "food": row.get("food"),
                "food_max": row.get("food_max"),
                "food_supply": row.get("food_supply"),
                "food_consumption": row.get("food_consumption"),
                "food_balance": row.get("food_balance"),
                "missing": row.get("missing"),
            }
            for row in food_rows
        ],
    }


def _explorer_payload(
    dataset: SavegameDataset,
    playthrough_id: str | None,
    *,
    top_n: int,
) -> dict[str, Any]:
    latest_snapshot_id = _latest_snapshot_id(dataset, playthrough_id)
    rows: list[dict[str, Any]] = []
    rows.extend(_population_explorer_rows(dataset, playthrough_id, latest_snapshot_id, top_n))
    rows.extend(_goods_explorer_rows(dataset, playthrough_id, latest_snapshot_id, top_n))
    rows.extend(_food_explorer_rows(dataset, playthrough_id, latest_snapshot_id, top_n))
    rows.extend(_building_explorer_rows(dataset, playthrough_id, latest_snapshot_id, top_n))
    rows.extend(_method_explorer_rows(dataset, playthrough_id, latest_snapshot_id, top_n))
    return {
        "metrics": _explorer_metrics(),
        "dimensions": _explorer_dimensions(),
        "aggregations": ["sum", "mean", "median", "min", "max"],
        "rows": rows,
    }


def _population_explorer_rows(
    dataset: SavegameDataset,
    playthrough_id: str | None,
    latest_snapshot_id: str | None,
    top_n: int,
) -> list[dict[str, Any]]:
    locations = dataset.scan("locations", playthrough_id)
    columns = _columns(locations)
    dimensions = [
        "global",
        "super_region",
        "macro_region",
        "region",
        "province_slug",
        "country_tag",
    ]
    metrics = [
        ("total_population", "pops", "sum"),
        ("rgo_employed", "employed", "sum"),
        ("unemployed_total", "unemployed", "sum"),
        ("development", "development", "sum"),
        ("tax", "collected_tax", "sum"),
        ("possible_tax", "possible_tax", "sum"),
    ]
    rows = _cube_rows(
        locations,
        columns,
        domain="population",
        dimensions=dimensions,
        metrics=metrics,
        latest_snapshot_id=latest_snapshot_id,
        top_n=top_n,
    )
    building_employment = _with_location_context(dataset, "buildings", playthrough_id)
    building_columns = _columns(building_employment)
    _merge_metric_rows(
        rows,
        _cube_rows(
            building_employment,
            building_columns,
            domain="population",
            dimensions=dimensions,
            metrics=[("employed", "employed", "sum")],
            latest_snapshot_id=latest_snapshot_id,
            top_n=top_n,
        ),
    )
    rows.extend(_population_by_type_rows(locations, columns))
    _add_uncollected_tax_rows(rows, domain="population")
    return rows


def _population_by_type_rows(lf: pl.LazyFrame, columns: set[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    snapshot_columns = _snapshot_group_columns(columns)
    for pop_type in (*POP_TYPES, "unknown"):
        column = f"population_{pop_type}"
        if column not in columns:
            continue
        grouped = lf.group_by(snapshot_columns).agg(pl.col(column).fill_null(0).sum().alias("pops"))
        wide_rows = _collect_sorted(grouped.with_columns(pl.lit(pop_type).alias("_entity_key")))
        rows.extend(
            _wide_metric_rows(
                wide_rows,
                domain="population",
                dimension="pop_type",
                metrics=[("pops", "pops")],
                entity_label_key="_entity_key",
            )
        )
    return rows


def _goods_explorer_rows(
    dataset: SavegameDataset,
    playthrough_id: str | None,
    latest_snapshot_id: str | None,
    top_n: int,
) -> list[dict[str, Any]]:
    goods = dataset.scan("market_goods", playthrough_id)
    columns = _columns(goods)
    return _cube_rows(
        goods,
        columns,
        domain="goods",
        dimensions=[
            "global",
            "good_id",
            "goods_category",
            "goods_designation",
            "market_center_slug",
        ],
        metrics=[
            ("supply", "supply", "sum"),
            ("demand", "demand", "sum"),
            ("net", "net", "sum"),
            ("stockpile", "stockpile", "sum"),
            ("price", "avg_price", "mean"),
            ("supplied_Production", "production_supply", "sum"),
            ("demanded_Building", "building_demand", "sum"),
        ],
        latest_snapshot_id=latest_snapshot_id,
        top_n=top_n,
    )


def _food_explorer_rows(
    dataset: SavegameDataset,
    playthrough_id: str | None,
    latest_snapshot_id: str | None,
    top_n: int,
) -> list[dict[str, Any]]:
    food = dataset.scan("market_food", playthrough_id)
    columns = _columns(food)
    return _cube_rows(
        food,
        columns,
        domain="food",
        dimensions=["global", "market_center_slug"],
        metrics=[
            ("food", "food", "sum"),
            ("food_max", "capacity", "sum"),
            ("food_supply", "supply", "sum"),
            ("food_consumption", "demand", "sum"),
            ("food_balance", "balance", "sum"),
            ("missing", "missing_food", "sum"),
            ("food_fill_percent", "fill_percent", "mean"),
            ("months_of_food", "months_of_food", "mean"),
        ],
        latest_snapshot_id=latest_snapshot_id,
        top_n=top_n,
    )


def _building_explorer_rows(
    dataset: SavegameDataset,
    playthrough_id: str | None,
    latest_snapshot_id: str | None,
    top_n: int,
) -> list[dict[str, Any]]:
    buildings = _with_location_context(dataset, "buildings", playthrough_id)
    columns = _columns(buildings)
    return _cube_rows(
        buildings,
        columns,
        domain="buildings",
        dimensions=[
            "global",
            "building_type",
            "super_region",
            "macro_region",
            "region",
            "country_tag",
        ],
        metrics=[
            ("building_id", "building_count", "count"),
            ("level", "level_sum", "sum"),
            ("employed", "employed", "sum"),
            ("employment", "employment_capacity", "sum"),
            ("last_months_profit", "profit", "sum"),
        ],
        latest_snapshot_id=latest_snapshot_id,
        top_n=top_n,
    )


def _method_explorer_rows(
    dataset: SavegameDataset,
    playthrough_id: str | None,
    latest_snapshot_id: str | None,
    top_n: int,
) -> list[dict[str, Any]]:
    methods = _with_location_context(dataset, "building_methods", playthrough_id)
    columns = _columns(methods)
    return _cube_rows(
        methods,
        columns,
        domain="methods",
        dimensions=[
            "global",
            "production_method",
            "building_type",
            "country_tag",
        ],
        metrics=[
            ("production_method", "method_count", "count"),
            ("building_id", "building_count", "n_unique"),
        ],
        latest_snapshot_id=latest_snapshot_id,
        top_n=top_n,
    )


def _cube_rows(
    lf: pl.LazyFrame,
    columns: set[str],
    *,
    domain: str,
    dimensions: list[str],
    metrics: list[tuple[str, str, str]],
    latest_snapshot_id: str | None,
    top_n: int,
) -> list[dict[str, Any]]:
    if "snapshot_id" not in columns:
        return []
    rows: list[dict[str, Any]] = []
    snapshot_columns = _snapshot_group_columns(columns)
    metric_exprs = _aggregate_metric_exprs(columns, metrics)
    if not metric_exprs:
        return rows
    for dimension in dimensions:
        if dimension == "global":
            grouped = lf.group_by(snapshot_columns).agg(metric_exprs).with_columns(
                pl.lit("world").alias("_entity_key"),
                pl.lit("World").alias("_entity_label"),
            )
        elif dimension in columns:
            grouped = (
                lf.filter(pl.col(dimension).is_not_null())
                .with_columns(pl.col(dimension).cast(pl.String).alias("_entity_key"))
                .filter(pl.col("_entity_key") != "")
                .group_by([*snapshot_columns, "_entity_key"])
                .agg(metric_exprs)
                .with_columns(pl.col("_entity_key").alias("_entity_label"))
            )
        else:
            continue
        wide_rows = _collect_sorted(grouped, ["date_sort", "_entity_key"])
        wide_rows = _prune_high_cardinality_rows(
            wide_rows,
            dimension=dimension,
            latest_snapshot_id=latest_snapshot_id,
            top_n=top_n,
            rank_metric=metrics[0][1],
        )
        rows.extend(
            _wide_metric_rows(
                wide_rows,
                domain=domain,
                dimension=dimension,
                metrics=[(alias, alias) for _, alias, _ in metrics],
                entity_label_key="_entity_label",
            )
        )
    return rows


def _wide_metric_rows(
    rows: list[dict[str, Any]],
    *,
    domain: str,
    dimension: str,
    metrics: list[tuple[str, str]],
    entity_label_key: str,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        entity_key = str(row.get("_entity_key") or "")
        if not entity_key:
            continue
        for source, metric in metrics:
            value = row.get(source)
            if value is None:
                continue
            output.append(
                {
                    **_snapshot_values(row),
                    "domain": domain,
                    "metric": metric,
                    "dimension": dimension,
                    "entity_key": entity_key,
                    "entity_label": str(row.get(entity_label_key) or entity_key),
                    "value": value,
                }
            )
    return output


def _prune_high_cardinality_rows(
    rows: list[dict[str, Any]],
    *,
    dimension: str,
    latest_snapshot_id: str | None,
    top_n: int,
    rank_metric: str,
) -> list[dict[str, Any]]:
    if dimension not in EXPLORER_HIGH_CARDINALITY_DIMENSIONS or latest_snapshot_id is None:
        return rows
    if top_n <= 0:
        return rows
    latest_rows = [row for row in rows if row.get("snapshot_id") == latest_snapshot_id]
    if len(latest_rows) <= top_n:
        return rows
    selected = {
        row["_entity_key"]
        for row in sorted(
            latest_rows,
            key=lambda item: _number(item.get(rank_metric)),
            reverse=True,
        )[:top_n]
    }
    return [row for row in rows if row.get("_entity_key") in selected]


def _add_uncollected_tax_rows(rows: list[dict[str, Any]], *, domain: str) -> None:
    grouped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        if row["domain"] != domain or row["metric"] not in {"collected_tax", "possible_tax"}:
            continue
        key = (
            row["snapshot_id"],
            row["dimension"],
            row["entity_key"],
            row["entity_label"],
            row.get("date"),
            row.get("date_sort"),
        )
        grouped.setdefault(key, {})[row["metric"]] = _number(row.get("value"))
    for key, values in grouped.items():
        if "possible_tax" not in values:
            continue
        snapshot_id, dimension, entity_key, entity_label, date, date_sort = key
        rows.append(
            {
                "snapshot_id": snapshot_id,
                "date": date,
                "date_sort": date_sort,
                "domain": domain,
                "metric": "uncollected_tax",
                "dimension": dimension,
                "entity_key": entity_key,
                "entity_label": entity_label,
                "value": max(values["possible_tax"] - values.get("collected_tax", 0.0), 0.0),
            }
        )


def _merge_metric_rows(rows: list[dict[str, Any]], additions: list[dict[str, Any]]) -> None:
    index = {
        (
            row.get("snapshot_id"),
            row.get("domain"),
            row.get("metric"),
            row.get("dimension"),
            row.get("entity_key"),
        ): row
        for row in rows
    }
    for addition in additions:
        key = (
            addition.get("snapshot_id"),
            addition.get("domain"),
            addition.get("metric"),
            addition.get("dimension"),
            addition.get("entity_key"),
        )
        existing = index.get(key)
        if existing is None:
            rows.append(addition)
            index[key] = addition
        else:
            existing["value"] = _number(existing.get("value")) + _number(addition.get("value"))


def _explorer_metrics() -> list[dict[str, Any]]:
    return [
        _metric("population", "pops", "Pops", "Pops", "whole"),
        _metric("population", "employed", "Employed", "Pops", "whole"),
        _metric("population", "unemployed", "Unemployed", "Pops", "whole"),
        _metric("population", "development", "Development", "Development", "whole"),
        _metric("population", "collected_tax", "Collected Tax", "Gold", "money"),
        _metric("population", "uncollected_tax", "Uncollected Tax", "Gold", "money"),
        _metric("population", "possible_tax", "Possible Tax", "Gold", "money"),
        _metric("goods", "supply", "Supply", "Goods", "decimal"),
        _metric("goods", "demand", "Demand", "Goods", "decimal"),
        _metric("goods", "net", "Net", "Goods", "decimal"),
        _metric("goods", "stockpile", "Stockpile", "Goods", "decimal"),
        _metric("goods", "avg_price", "Average Price", "Gold", "money"),
        _metric("goods", "production_supply", "Production Supply", "Goods", "decimal"),
        _metric("goods", "building_demand", "Building Demand", "Goods", "decimal"),
        _metric("food", "food", "Food", "Food", "whole"),
        _metric("food", "capacity", "Capacity", "Food", "whole"),
        _metric("food", "supply", "Supply", "Food/month", "decimal"),
        _metric("food", "demand", "Demand", "Food/month", "decimal"),
        _metric("food", "balance", "Balance", "Food/month", "decimal"),
        _metric("food", "missing_food", "Missing Food", "Food", "whole"),
        _metric("food", "fill_percent", "Fill", "%", "percent"),
        _metric("food", "months_of_food", "Months of Food", "Months", "decimal"),
        _metric("buildings", "building_count", "Buildings", "Buildings", "whole"),
        _metric("buildings", "level_sum", "Levels", "Levels", "whole"),
        _metric("buildings", "employed", "Employed", "Pops", "whole"),
        _metric("buildings", "employment_capacity", "Employment Capacity", "Pops", "whole"),
        _metric("buildings", "profit", "Profit", "Gold", "money"),
        _metric("methods", "method_count", "PM Uses", "Uses", "whole"),
        _metric("methods", "building_count", "Buildings", "Buildings", "whole"),
    ]


def _explorer_dimensions() -> list[dict[str, Any]]:
    return [
        _dimension("global", "World", "world", 0),
        _dimension("super_region", "Super Region", "geography", 10),
        _dimension("macro_region", "Macro Region", "geography", 20),
        _dimension("region", "Region", "geography", 30),
        _dimension("area", "Area", "geography", 40),
        _dimension("province_slug", "Province", "geography", 50),
        _dimension("country_tag", "Country", "political", 60),
        _dimension("market_center_slug", "Market", "markets", 70),
        _dimension("market_id", "Market ID", "markets", 80),
        _dimension("goods_category", "Goods Category", "goods", 90),
        _dimension("goods_designation", "Goods Designation", "goods", 100),
        _dimension("good_id", "Good", "goods", 110),
        _dimension("building_type", "Building", "production", 120),
        _dimension("production_method", "Production Method", "production", 130),
        _dimension("pop_type", "Pop Type", "population", 140),
    ]


def _metric(
    domain: str,
    key: str,
    label: str,
    unit: str,
    formatter: str,
    default_sort: str = "desc",
) -> dict[str, str]:
    return {
        "domain": domain,
        "key": key,
        "label": label,
        "unit": unit,
        "formatter": formatter,
        "defaultSort": default_sort,
    }


def _dimension(key: str, label: str, scope: str, order: int) -> dict[str, Any]:
    return {"key": key, "label": label, "scope": scope, "order": order}


def _snapshot_values(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: row.get(key)
        for key in ["snapshot_id", "playthrough_id", "date", "year", "month", "day", "date_sort"]
        if key in row
    }


def _series_row(row: dict[str, Any], **values: Any) -> dict[str, Any]:
    return {**_snapshot_values(row), **values}


def _number(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _overview_rows(dataset: SavegameDataset, playthrough_id: str | None) -> list[dict[str, Any]]:
    locations = dataset.scan("locations", playthrough_id)
    location_columns = _columns(locations)
    if not location_columns:
        return []
    group = _snapshot_group_columns(location_columns)
    exprs = [
        _sum_or_zero(location_columns, "total_population"),
        _sum_or_zero(location_columns, "development"),
        _sum_or_zero(location_columns, "tax"),
        _sum_or_zero(location_columns, "possible_tax"),
        _sum_or_zero(location_columns, "rgo_employed"),
        _sum_or_zero(location_columns, "unemployed_total"),
    ]
    overview = locations.group_by(group).agg(exprs)

    food = dataset.scan("market_food", playthrough_id)
    food_columns = _columns(food)
    if food_columns:
        food_group = _snapshot_group_columns(food_columns)
        food_summary = food.group_by(food_group).agg(
            [
                _sum_or_zero(food_columns, "food"),
                _sum_or_zero(food_columns, "food_max"),
                _sum_or_zero(food_columns, "food_supply"),
                _sum_or_zero(food_columns, "food_consumption"),
                _sum_or_zero(food_columns, "food_balance"),
                _sum_or_zero(food_columns, "missing"),
            ]
        )
        overview = overview.join(food_summary, on=group, how="left")
    return _collect_sorted(overview)


def _goods_rows(dataset: SavegameDataset, playthrough_id: str | None) -> list[dict[str, Any]]:
    goods = dataset.scan("market_goods", playthrough_id)
    columns = _columns(goods)
    if not {"snapshot_id", "good_id"}.issubset(columns):
        return []
    group = [*_snapshot_group_columns(columns), "good_id"]
    exprs = [
        _sum_or_zero(columns, "supply"),
        _sum_or_zero(columns, "demand"),
        _sum_or_zero(columns, "net"),
        _sum_or_zero(columns, "stockpile"),
        _sum_or_zero(columns, "supplied_Production"),
        _sum_or_zero(columns, "demanded_Building"),
        _mean_or_zero(columns, "price", "avg_price"),
        _max_or_zero(columns, "default_price"),
    ]
    return _collect_sorted(goods.group_by(group).agg(exprs), ["date_sort", "good_id"])


def _food_rows(dataset: SavegameDataset, playthrough_id: str | None) -> list[dict[str, Any]]:
    food = dataset.scan("market_food", playthrough_id)
    columns = _columns(food)
    if not {"snapshot_id", "market_id"}.issubset(columns):
        return []
    group = [*_snapshot_group_columns(columns), "market_id"]
    if "market_center_slug" in columns:
        group.append("market_center_slug")
    exprs = [
        _sum_or_zero(columns, "food"),
        _sum_or_zero(columns, "food_max"),
        _mean_or_zero(columns, "food_fill_percent"),
        _mean_or_zero(columns, "food_price"),
        _sum_or_zero(columns, "food_supply"),
        _sum_or_zero(columns, "food_consumption"),
        _sum_or_zero(columns, "food_balance"),
        _sum_or_zero(columns, "missing"),
        _sum_or_zero(columns, "population"),
        _sum_or_zero(columns, "capacity"),
        _mean_or_zero(columns, "months_of_food"),
    ]
    return _collect_sorted(food.group_by(group).agg(exprs), ["date_sort", "market_id"])


def _building_rows(dataset: SavegameDataset, playthrough_id: str | None) -> list[dict[str, Any]]:
    buildings = dataset.scan("buildings", playthrough_id)
    columns = _columns(buildings)
    if not {"snapshot_id", "building_type"}.issubset(columns):
        return []
    group = [*_snapshot_group_columns(columns), "building_type"]
    exprs = [
        pl.len().alias("building_count"),
        _sum_or_zero(columns, "level").alias("level_sum"),
        _sum_or_zero(columns, "employed"),
        _sum_or_zero(columns, "employment"),
        _sum_or_zero(columns, "last_months_profit"),
    ]
    return _collect_sorted(buildings.group_by(group).agg(exprs), ["date_sort", "building_type"])


def _method_rows(dataset: SavegameDataset, playthrough_id: str | None) -> list[dict[str, Any]]:
    methods = dataset.scan("building_methods", playthrough_id)
    columns = _columns(methods)
    if not {"snapshot_id", "building_type", "production_method"}.issubset(columns):
        return []
    group = [*_snapshot_group_columns(columns), "building_type", "production_method"]
    return _collect_sorted(
        methods.group_by(group).agg(pl.len().alias("method_count")),
        ["date_sort", "building_type", "production_method"],
    )


def _location_rows(
    dataset: SavegameDataset,
    playthrough_id: str | None,
    *,
    top_n: int,
) -> list[dict[str, Any]]:
    locations = dataset.scan("locations", playthrough_id)
    columns = _columns(locations)
    if not {"snapshot_id", "location_id"}.issubset(columns):
        return []
    latest = _latest_snapshot_id(dataset, playthrough_id)
    if latest is None:
        return []
    latest_locations = locations.filter(pl.col("snapshot_id") == latest)
    rank_expr = _sum_metric_expr(columns, ["total_population", "development", "rgo_employed"])
    top_locations = (
        latest_locations.with_columns(rank_expr.alias("_rank_value"))
        .sort("_rank_value", descending=True)
        .select("location_id")
        .limit(top_n)
        .collect()
    )
    ids = top_locations["location_id"].to_list() if "location_id" in top_locations.columns else []
    if not ids:
        return []
    selected = locations.filter(pl.col("location_id").is_in(ids))
    keep = [
        column
        for column in [
            *_snapshot_group_columns(columns),
            "location_id",
            "slug",
            "country_tag",
            "owner_name",
            "province_slug",
            "rank",
            "development",
            "total_population",
            "tax",
            "possible_tax",
            "rgo_employed",
            "unemployed_total",
        ]
        if column in columns
    ]
    return _collect_sorted(selected.select(keep), ["date_sort", "location_id"])


def _country_rows(dataset: SavegameDataset, playthrough_id: str | None) -> list[dict[str, Any]]:
    countries = dataset.scan("countries", playthrough_id)
    columns = _columns(countries)
    if {"snapshot_id", "country_id"}.issubset(columns):
        keep = [
            column
            for column in [
                *_snapshot_group_columns(columns),
                "country_id",
                "country_tag",
                "country_name",
                "rank",
                "population",
                "gold",
                "stability",
                "prestige",
                "expense",
                "loan_capacity",
                "owned_locations_count",
            ]
            if column in columns
        ]
        return _collect_sorted(countries.select(keep), ["date_sort", "country_tag"])

    locations = dataset.scan("locations", playthrough_id)
    loc_columns = _columns(locations)
    if not {"snapshot_id", "country_tag"}.issubset(loc_columns):
        return []
    group = [*_snapshot_group_columns(loc_columns), "country_tag"]
    return _collect_sorted(
        locations.group_by(group).agg(
            [
                _sum_or_zero(loc_columns, "total_population").alias("population"),
                _sum_or_zero(loc_columns, "development"),
                _sum_or_zero(loc_columns, "tax"),
                _sum_or_zero(loc_columns, "possible_tax"),
                pl.len().alias("locations"),
            ]
        ),
        ["date_sort", "country_tag"],
    )


def _location_aggregate_rows(
    dataset: SavegameDataset,
    playthrough_id: str | None,
    *,
    top_n: int,
) -> list[dict[str, Any]]:
    locations = dataset.scan("locations", playthrough_id)
    columns = _columns(locations)
    metrics = [
        ("total_population", "pops", "sum"),
        ("development", "development", "sum"),
        ("tax", "tax", "sum"),
        ("possible_tax", "possible_tax", "sum"),
        ("rgo_employed", "rgo_employed", "sum"),
        ("unemployed_total", "unemployed_total", "sum"),
    ]
    dimensions = [
        "province_slug",
        "area",
        "region",
        "macro_region",
        "super_region",
        "country_tag",
        "owner_country_id",
        "market_id",
        "rank",
        "raw_material",
    ]
    return _aggregate_rows(
        dataset,
        locations,
        columns,
        dimensions=dimensions,
        metrics=metrics,
        source="locations",
        playthrough_id=playthrough_id,
        top_n=top_n,
        sort_metric="pops",
    )


def _country_aggregate_rows(
    dataset: SavegameDataset,
    playthrough_id: str | None,
    *,
    top_n: int,
) -> list[dict[str, Any]]:
    locations = dataset.scan("locations", playthrough_id)
    columns = _columns(locations)
    return _aggregate_rows(
        dataset,
        locations,
        columns,
        dimensions=["country_tag", "owner_country_id"],
        metrics=[
            ("total_population", "pops", "sum"),
            ("development", "development", "sum"),
            ("tax", "tax", "sum"),
            ("possible_tax", "possible_tax", "sum"),
            ("location_id", "locations", "count"),
        ],
        source="countries",
        playthrough_id=playthrough_id,
        top_n=top_n,
        sort_metric="pops",
    )


def _market_aggregate_rows(
    dataset: SavegameDataset,
    playthrough_id: str | None,
    *,
    top_n: int,
) -> list[dict[str, Any]]:
    food = dataset.scan("market_food", playthrough_id)
    columns = _columns(food)
    return _aggregate_rows(
        dataset,
        food,
        columns,
        dimensions=["market_center_slug", "market_id"],
        metrics=[
            ("food", "food", "sum"),
            ("food_max", "food_max", "sum"),
            ("food_balance", "food_balance", "sum"),
            ("missing", "missing_food", "sum"),
            ("population", "market_population", "sum"),
            ("capacity", "capacity", "sum"),
        ],
        source="markets",
        playthrough_id=playthrough_id,
        top_n=top_n,
        sort_metric="food",
    )


def _goods_aggregate_rows(
    dataset: SavegameDataset,
    playthrough_id: str | None,
    *,
    top_n: int,
) -> list[dict[str, Any]]:
    goods = dataset.scan("market_goods", playthrough_id)
    columns = _columns(goods)
    return _aggregate_rows(
        dataset,
        goods,
        columns,
        dimensions=["good_id", "market_center_slug", "market_id", "goods_category"],
        metrics=[
            ("supply", "supply", "sum"),
            ("demand", "demand", "sum"),
            ("net", "net", "sum"),
            ("stockpile", "stockpile", "sum"),
            ("price", "avg_price", "mean"),
            ("supplied_Production", "production_supply", "sum"),
            ("demanded_Building", "building_demand", "sum"),
        ],
        source="goods",
        playthrough_id=playthrough_id,
        top_n=top_n,
        sort_metric="supply",
    )


def _building_aggregate_rows(
    dataset: SavegameDataset,
    playthrough_id: str | None,
    *,
    top_n: int,
) -> list[dict[str, Any]]:
    buildings = _with_location_context(dataset, "buildings", playthrough_id)
    columns = _columns(buildings)
    return _aggregate_rows(
        dataset,
        buildings,
        columns,
        dimensions=[
            "building_type",
            "country_tag",
            "region",
            "market_id",
            "market_center_slug",
            "province_slug",
        ],
        metrics=[
            ("building_id", "building_count", "count"),
            ("level", "level_sum", "sum"),
            ("employed", "employed", "sum"),
            ("employment", "employment", "sum"),
            ("last_months_profit", "profit", "sum"),
        ],
        source="buildings",
        playthrough_id=playthrough_id,
        top_n=top_n,
        sort_metric="level_sum",
    )


def _method_aggregate_rows(
    dataset: SavegameDataset,
    playthrough_id: str | None,
    *,
    top_n: int,
) -> list[dict[str, Any]]:
    methods = _with_location_context(dataset, "building_methods", playthrough_id)
    columns = _columns(methods)
    return _aggregate_rows(
        dataset,
        methods,
        columns,
        dimensions=[
            "production_method",
            "building_type",
            "country_tag",
            "region",
            "market_id",
            "market_center_slug",
        ],
        metrics=[
            ("production_method", "method_count", "count"),
            ("building_id", "building_count", "n_unique"),
        ],
        source="methods",
        playthrough_id=playthrough_id,
        top_n=top_n,
        sort_metric="method_count",
    )


def _aggregate_rows(
    dataset: SavegameDataset,
    lf: pl.LazyFrame,
    columns: set[str],
    *,
    dimensions: list[str],
    metrics: list[tuple[str, str, str]],
    source: str,
    playthrough_id: str | None,
    top_n: int,
    sort_metric: str,
) -> list[dict[str, Any]]:
    if "snapshot_id" not in columns:
        return []
    latest = _latest_snapshot_id(dataset, playthrough_id)
    if latest is None:
        return []
    rows: list[dict[str, Any]] = []
    snapshot_columns = _snapshot_group_columns(columns)
    for dimension in dimensions:
        if dimension not in columns:
            continue
        metric_exprs = _aggregate_metric_exprs(columns, metrics)
        if not metric_exprs:
            continue
        dim_col = pl.col(dimension).cast(pl.String)
        grouped = (
            lf.filter(pl.col(dimension).is_not_null())
            .with_columns(dim_col.alias("_aggregate_key"))
            .filter(pl.col("_aggregate_key") != "")
            .group_by([*snapshot_columns, "_aggregate_key"])
            .agg(metric_exprs)
        )
        grouped_columns = _columns(grouped)
        if sort_metric not in grouped_columns:
            continue
        top_keys_frame = (
            grouped.filter(pl.col("snapshot_id") == latest)
            .sort(sort_metric, descending=True)
            .select("_aggregate_key")
            .limit(top_n)
            .collect()
        )
        if top_keys_frame.is_empty():
            continue
        top_keys = top_keys_frame["_aggregate_key"].to_list()
        selected = grouped.filter(pl.col("_aggregate_key").is_in(top_keys)).with_columns(
            [
                pl.lit(source).alias("source"),
                pl.lit(dimension).alias("dimension"),
                pl.col("_aggregate_key").alias("key"),
            ]
        )
        keep = [
            column
            for column in [
                "source",
                "dimension",
                "key",
                *snapshot_columns,
                *(alias for _, alias, _ in metrics),
            ]
            if column in _columns(selected)
        ]
        rows.extend(_collect_sorted(selected.select(keep), ["dimension", "key", "date_sort"]))
    return rows


def _aggregate_metric_exprs(
    columns: set[str],
    metrics: list[tuple[str, str, str]],
) -> list[pl.Expr]:
    exprs: list[pl.Expr] = []
    for column, alias, kind in metrics:
        if kind == "count":
            exprs.append(pl.len().alias(alias))
        elif kind == "n_unique" and column in columns:
            exprs.append(pl.col(column).n_unique().alias(alias))
        elif kind == "sum" and column in columns:
            exprs.append(pl.col(column).fill_null(0).sum().alias(alias))
        elif kind == "mean" and column in columns:
            exprs.append(pl.col(column).mean().alias(alias))
    return exprs


def _with_location_context(
    dataset: SavegameDataset,
    table: str,
    playthrough_id: str | None,
) -> pl.LazyFrame:
    fact = dataset.scan(table, playthrough_id)
    fact_columns = _columns(fact)
    locations = dataset.scan("locations", playthrough_id)
    loc_columns = _columns(locations)
    if not {"snapshot_id", "location_id"}.issubset(fact_columns) or not {
        "snapshot_id",
        "location_id",
    }.issubset(loc_columns):
        return fact
    keep = [
        column
        for column in [
            "snapshot_id",
            "location_id",
            "country_tag",
            "owner_name",
            "owner_country_id",
            "province_slug",
            "area",
            "region",
            "macro_region",
            "super_region",
        ]
        if column in loc_columns
    ]
    if len(keep) <= 2:
        return fact
    enriched = fact.join(
        locations.select(keep).unique(subset=["snapshot_id", "location_id"]),
        on=["snapshot_id", "location_id"],
        how="left",
        suffix="_location",
    )
    market_food = dataset.scan("market_food", playthrough_id)
    food_columns = _columns(market_food)
    if {"snapshot_id", "market_id"}.issubset(fact_columns) and {
        "snapshot_id",
        "market_id",
        "market_center_slug",
    }.issubset(food_columns):
        enriched = enriched.join(
            market_food.select(["snapshot_id", "market_id", "market_center_slug"]).unique(
                subset=["snapshot_id", "market_id"]
            ),
            on=["snapshot_id", "market_id"],
            how="left",
            suffix="_market",
        )
    return enriched


def _delta_rows(
    dataset: SavegameDataset,
    playthrough_id: str | None,
    *,
    top_n: int,
) -> dict[str, list[dict[str, Any]]]:
    snapshots = dataset.snapshots(playthrough_id)
    if snapshots.height < 2:
        return {"goods": [], "buildings": [], "locations": []}
    sorted_snapshots = _sort_manifest(snapshots)
    previous = sorted_snapshots.item(sorted_snapshots.height - 2, "snapshot_id")
    latest = sorted_snapshots.item(sorted_snapshots.height - 1, "snapshot_id")
    return {
        "goods": _delta_for_table(
            dataset,
            playthrough_id,
            table="market_goods",
            key="good_id",
            metric="net",
            previous=previous,
            latest=latest,
            top_n=top_n,
        ),
        "buildings": _delta_for_table(
            dataset,
            playthrough_id,
            table="buildings",
            key="building_type",
            metric="level",
            previous=previous,
            latest=latest,
            top_n=top_n,
        ),
        "locations": _delta_for_table(
            dataset,
            playthrough_id,
            table="locations",
            key="slug",
            metric="development",
            previous=previous,
            latest=latest,
            top_n=top_n,
        ),
    }


def _delta_for_table(
    dataset: SavegameDataset,
    playthrough_id: str | None,
    *,
    table: str,
    key: str,
    metric: str,
    previous: str,
    latest: str,
    top_n: int,
) -> list[dict[str, Any]]:
    lf = dataset.scan(table, playthrough_id)
    columns = _columns(lf)
    if not {"snapshot_id", key, metric}.issubset(columns):
        return []
    base = (
        lf.filter(pl.col("snapshot_id").is_in([previous, latest]))
        .group_by(["snapshot_id", key])
        .agg(pl.col(metric).fill_null(0).sum().alias(metric))
        .collect()
    )
    if base.is_empty():
        return []
    pivoted = base.pivot(index=key, on="snapshot_id", values=metric).fill_null(0)
    if previous not in pivoted.columns:
        pivoted = pivoted.with_columns(pl.lit(0.0).alias(previous))
    if latest not in pivoted.columns:
        pivoted = pivoted.with_columns(pl.lit(0.0).alias(latest))
    return (
        pivoted.with_columns((pl.col(latest) - pl.col(previous)).alias("delta"))
        .sort("delta", descending=True)
        .head(top_n)
        .rename({previous: "previous", latest: "latest"})
        .to_dicts()
    )


def _columns(lf: pl.LazyFrame) -> set[str]:
    try:
        return set(lf.collect_schema().names())
    except Exception:
        return set()


def _snapshot_group_columns(columns: set[str]) -> list[str]:
    return [
        column
        for column in ["snapshot_id", "playthrough_id", "date", "year", "month", "day", "date_sort"]
        if column in columns
    ]


def _sum_or_zero(columns: set[str], column: str, alias: str | None = None) -> pl.Expr:
    output = alias or column
    if column not in columns:
        return pl.lit(0.0).alias(output)
    return pl.col(column).fill_null(0).sum().alias(output)


def _mean_or_zero(columns: set[str], column: str, alias: str | None = None) -> pl.Expr:
    output = alias or column
    if column not in columns:
        return pl.lit(0.0).alias(output)
    return pl.col(column).mean().alias(output)


def _max_or_zero(columns: set[str], column: str, alias: str | None = None) -> pl.Expr:
    output = alias or column
    if column not in columns:
        return pl.lit(0.0).alias(output)
    return pl.col(column).max().alias(output)


def _sum_metric_expr(columns: set[str], candidates: list[str]) -> pl.Expr:
    expr = pl.lit(0.0)
    for column in candidates:
        if column in columns:
            expr = expr + pl.col(column).fill_null(0)
    return expr


def _collect_sorted(lf: pl.LazyFrame, sort: list[str] | None = None) -> list[dict[str, Any]]:
    columns = _columns(lf)
    sort_columns = [
        column for column in (sort or ["date_sort", "snapshot_id"]) if column in columns
    ]
    if sort_columns:
        lf = lf.sort(sort_columns)
    return lf.collect().to_dicts()


def _latest_snapshot_id(dataset: SavegameDataset, playthrough_id: str | None) -> str | None:
    snapshots = dataset.snapshots(playthrough_id)
    if snapshots.is_empty():
        return None
    snapshots = _sort_manifest(snapshots)
    return snapshots.item(snapshots.height - 1, "snapshot_id")


def _empty_manifest() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "manifest_version": pl.Int64,
            "snapshot_id": pl.String,
            "playthrough_id": pl.String,
            "save_playthrough_id": pl.String,
            "playthrough_name": pl.String,
            "save_label": pl.String,
            "date": pl.String,
            "year": pl.Int64,
            "month": pl.Int64,
            "day": pl.Int64,
            "date_sort": pl.Int64,
            "path": pl.String,
            "mtime": pl.Float64,
            "mtime_ns": pl.Int64,
            "size": pl.Int64,
            "partial_hash": pl.String,
            "state_key": pl.String,
            "source_format": pl.String,
            "parser_profile": pl.String,
            "processed_at": pl.String,
            "parse_seconds": pl.Float64,
            "row_counts_json": pl.String,
        }
    )


def _sort_manifest(manifest: pl.DataFrame) -> pl.DataFrame:
    if manifest.is_empty():
        return manifest
    sort_columns = [
        column
        for column in ["playthrough_id", "date_sort", "mtime", "snapshot_id"]
        if column in manifest.columns
    ]
    return manifest.sort(sort_columns) if sort_columns else manifest


def _safe_id(value: Any) -> str:
    text = str(value or "unknown").strip().replace("-", "_")
    text = re.sub(r"[^A-Za-z0-9_]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "unknown"


def _snapshot_id(
    state: SaveFileState,
    partial_hash: str,
    *,
    year: int | None,
    month: int | None,
    day: int | None,
) -> str:
    if year is not None and month is not None and day is not None:
        stem = f"{year:04d}_{month:02d}_{day:02d}"
    else:
        stem = datetime.fromtimestamp(state.mtime).strftime("%Y%m%d_%H%M%S")
    return f"{stem}_{partial_hash[:12]}"


def _date_sort(year: int | None, month: int | None, day: int | None) -> int | None:
    if year is None or month is None or day is None:
        return None
    return year * 10000 + month * 100 + day


def _latest_mtime(paths: list[Path]) -> float:
    return max((path.stat().st_mtime for path in paths), default=0.0)


def _directory_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(file.stat().st_size for file in path.rglob("*") if file.is_file())


def _combined_row_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for row in rows:
        counts = json.loads(row.get("row_counts_json") or "{}")
        for table, count in counts.items():
            totals[table] = totals.get(table, 0) + int(count)
    return totals


def _legacy_pickle_size(playthrough_id: str | None) -> int | None:
    if playthrough_id is None:
        return None
    root = Path(r"C:\Development\ProsperPerishCalcs\analysis\savegame\notebooks\save_game_temp")
    if not root.exists():
        return None
    candidates = [root / playthrough_id, root / playthrough_id.replace("_", "-")]
    for candidate in candidates:
        if candidate.exists():
            size = sum(path.stat().st_size for path in candidate.rglob("*.pkl") if path.is_file())
            return size or None
    return None


class _MemorySampler:
    def __init__(self, interval_seconds: float = 0.5, *, include_children: bool = False):
        self.interval_seconds = interval_seconds
        self.include_children = include_children
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.peak_rss = 0

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> int:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
        return self.peak_rss

    def _run(self) -> None:
        try:
            import psutil
        except ModuleNotFoundError:
            return
        process = psutil.Process()
        while not self._stop.is_set():
            rss = process.memory_info().rss
            if self.include_children:
                for child in process.children(recursive=True):
                    try:
                        rss += child.memory_info().rss
                    except psutil.Error:
                        continue
            self.peak_rss = max(self.peak_rss, rss)
            self._stop.wait(self.interval_seconds)
