from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from eu5gameparser.domain.buildings import load_building_data
from eu5gameparser.load_order import DEFAULT_LOAD_ORDER_PATH, DataProfile
from eu5gameparser.savegame.notebook_labels import (
    NotebookLabelResolver,
    enrich_notebook_dimensions,
)

NOTEBOOK_SCHEMA_VERSION = 1
FACT_TABLES = (
    "locations",
    "population",
    "market_goods",
    "market_food",
    "market_good_bucket_flows",
    "production_method_good_flows",
    "rgo_flows",
    "buildings",
    "building_methods",
)
GOOD_PARTITIONED_FACTS = frozenset(
    {
        "market_good_bucket_flows",
        "production_method_good_flows",
        "rgo_flows",
    }
)
COMMON_COLUMNS = (
    "snapshot_id",
    "playthrough_id",
    "source_path",
    "date",
    "year",
    "month",
    "day",
)
SNAPSHOT_SCHEMA: dict[str, pl.DataType] = {
    "snapshot_id": pl.String,
    "playthrough_id": pl.String,
    "date": pl.String,
    "year": pl.UInt16,
    "month": pl.UInt8,
    "day": pl.UInt8,
    "date_sort": pl.UInt32,
    "path": pl.String,
    "source_path": pl.String,
    "mtime_ns": pl.Int64,
    "size": pl.Int64,
}
TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "locations": (
        *COMMON_COLUMNS,
        "location_id",
        "slug",
        "province_slug",
        "area",
        "region",
        "macro_region",
        "super_region",
        "country_tag",
        "country_name",
        "market_id",
        "market_center_slug",
        "development",
        "tax",
        "control",
        "population",
        "total_population",
        "pops",
        "population_capacity",
        "peasants",
        "burghers",
        "clergy",
        "nobles",
        "slaves",
        "tribesmen",
    ),
    "population": (
        *COMMON_COLUMNS,
        "location_id",
        "pop_type",
        "culture",
        "religion",
        "country_tag",
        "size",
        "workforce",
        "dependents",
        "literacy",
        "wealth",
    ),
    "market_goods": (
        *COMMON_COLUMNS,
        "market_id",
        "market_center_slug",
        "market_name",
        "good_id",
        "good_name",
        "goods_category",
        "category",
        "designation",
        "price",
        "default_price",
        "supply",
        "demand",
        "net",
        "stockpile",
        "stockpile_limit",
    ),
    "market_food": (
        *COMMON_COLUMNS,
        "market_id",
        "center_location_id",
        "market_center_slug",
        "market_name",
        "food",
        "food_max",
        "food_fill_percent",
        "food_price",
        "food_supply",
        "food_consumption",
        "food_balance",
        "food_not_traded",
        "missing",
        "population",
        "capacity",
        "food_per_population",
        "months_of_food",
    ),
    "market_good_bucket_flows": (
        *COMMON_COLUMNS,
        "market_id",
        "market_center_slug",
        "good_id",
        "good_name",
        "goods_category",
        "direction",
        "bucket",
        "save_column",
        "amount",
    ),
    "production_method_good_flows": (
        *COMMON_COLUMNS,
        "market_id",
        "market_center_slug",
        "good_id",
        "good_name",
        "goods_category",
        "production_method",
        "building_id",
        "building_type",
        "location_id",
        "direction",
        "nominal_amount",
        "allocated_amount",
        "allocation_factor",
        "building_count",
        "level_sum",
    ),
    "rgo_flows": (
        *COMMON_COLUMNS,
        "market_id",
        "market_center_slug",
        "good_id",
        "good_name",
        "goods_category",
        "location_id",
        "raw_material",
        "max_workers",
        "rgo_employed",
        "direction",
        "save_side",
        "nominal_amount",
        "allocated_amount",
        "allocation_factor",
    ),
    "buildings": (
        *COMMON_COLUMNS,
        "building_id",
        "building_type",
        "location_id",
        "market_id",
        "owner_tag",
        "level",
        "max_level",
        "employed",
        "employment",
        "open",
        "subsidized",
        "upkeep",
        "last_months_profit",
        "active_method_ids",
    ),
    "building_methods": (
        *COMMON_COLUMNS,
        "building_id",
        "building_type",
        "location_id",
        "market_id",
        "production_method",
    ),
}
TABLE_PREFIXES: dict[str, tuple[str, ...]] = {
    "locations": ("pop_", "population_", "pops_"),
    "market_goods": ("demanded_", "supplied_", "taken_", "produced_"),
}
DIMENSION_SPECS = {
    "goods": {
        "key": "good_id",
        "code": "good_code",
        "columns": ("good_id", "good_name", "goods_category", "category", "designation"),
    },
    "markets": {
        "key": "market_id",
        "code": "market_code",
        "columns": ("market_id", "center_location_id", "market_center_slug", "market_name"),
    },
    "locations": {
        "key": "location_id",
        "code": "location_code",
        "columns": (
            "location_id",
            "slug",
            "province_slug",
            "area",
            "region",
            "macro_region",
            "super_region",
            "country_tag",
            "country_name",
        ),
    },
    "countries": {
        "key": "country_tag",
        "code": "country_code",
        "columns": ("country_tag", "country_name"),
    },
    "areas": {"key": "area", "code": "area_code", "columns": ("area",)},
    "regions": {"key": "region", "code": "region_code", "columns": ("region",)},
    "macro_regions": {
        "key": "macro_region",
        "code": "macro_region_code",
        "columns": ("macro_region",),
    },
    "super_regions": {
        "key": "super_region",
        "code": "super_region_code",
        "columns": ("super_region",),
    },
    "building_types": {
        "key": "building_type",
        "code": "building_type_code",
        "columns": ("building_type",),
    },
    "buildings": {
        "key": "building_id",
        "code": "building_code",
        "columns": ("building_id", "building_type"),
    },
    "production_methods": {
        "key": "production_method",
        "code": "production_method_code",
        "columns": ("production_method",),
    },
}
ENCODE_SPECS = (
    ("good_id", "goods", "good_code"),
    ("market_id", "markets", "market_code"),
    ("location_id", "locations", "location_code"),
    ("country_tag", "countries", "country_code"),
    ("area", "areas", "area_code"),
    ("region", "regions", "region_code"),
    ("macro_region", "macro_regions", "macro_region_code"),
    ("super_region", "super_regions", "super_region_code"),
    ("building_type", "building_types", "building_type_code"),
    ("building_id", "buildings", "building_code"),
    ("production_method", "production_methods", "production_method_code"),
    ("raw_material", "goods", "raw_material_code"),
)
LABEL_COLUMNS_TO_DROP = {
    "good_code": ("good_id", "good_name", "goods_category", "category", "designation"),
    "market_code": ("market_id", "center_location_id", "market_center_slug", "market_name"),
    "location_code": (
        "location_id",
        "slug",
        "province_slug",
        "area",
        "region",
        "macro_region",
        "super_region",
        "country_tag",
        "country_name",
    ),
    "building_type_code": ("building_type",),
    "building_code": ("building_id",),
    "production_method_code": ("production_method",),
    "country_code": ("country_tag",),
    "area_code": ("area",),
    "region_code": ("region",),
    "macro_region_code": ("macro_region",),
    "super_region_code": ("super_region",),
    "raw_material_code": ("raw_material",),
}
IDENTIFIER_COLUMNS = {
    "snapshot_id",
    "playthrough_id",
    "source_path",
    "path",
    "date",
    "quick_hash",
    "mtime_ns",
    "size",
    "manifest_version",
    "building_id",
    "location_id",
    "market_id",
    "center_location_id",
    "active_method_ids",
    "production_method_group_index",
    "direction",
    "bucket",
    "save_column",
    "save_side",
    "pop_type",
    "culture",
    "religion",
    "owner_tag",
}


@dataclass(frozen=True)
class NotebookBuildResult:
    source: Path
    output: Path
    facts: dict[str, int]
    dimensions: dict[str, int]
    snapshots: int
    elapsed_seconds: float
    status: str = "rebuilt"
    stale_snapshots_ignored: int = 0
    manifest_fingerprint: str | None = None
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class _ManifestSelection:
    frame: pl.DataFrame
    total_snapshots: int
    stale_snapshots_ignored: int
    active_save_dir: Path | None


class SavegameNotebookDataset:
    def __init__(
        self,
        root: str | Path,
        *,
        profile: str | DataProfile | None = None,
        load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
        active_save_dir: str | Path | None = None,
    ):
        self.root = Path(root)
        self.profile = profile
        self.load_order_path = load_order_path
        self.active_save_dir = active_save_dir
        self._dimension_cache: dict[str, pl.DataFrame] | None = None

    @property
    def facts_root(self) -> Path:
        return self.root / "facts"

    @property
    def raw_tables_root(self) -> Path:
        return self.root / "tables"

    @property
    def dims_root(self) -> Path:
        return self.root / "dims"

    @property
    def snapshots_path(self) -> Path:
        return self.root / "snapshots.parquet"

    @property
    def manifest_path(self) -> Path:
        return self.root / "manifest.parquet"

    @property
    def is_raw(self) -> bool:
        return self.manifest_path.is_file() and self.raw_tables_root.is_dir()

    def snapshots(self) -> pl.DataFrame:
        if self.is_raw:
            manifest = self._raw_manifest().frame
            if manifest.is_empty():
                return _empty_snapshots()
            return _snapshot_frame(manifest)
        if not self.snapshots_path.exists():
            return _empty_snapshots()
        frame = pl.read_parquet(self.snapshots_path)
        if not frame.columns:
            return _empty_snapshots()
        missing = [
            pl.lit(None, dtype=dtype).alias(column)
            for column, dtype in SNAPSHOT_SCHEMA.items()
            if column not in frame.columns
        ]
        if missing:
            frame = frame.with_columns(missing)
        return frame

    def latest_playthrough(self) -> str | None:
        snapshots = self.snapshots()
        if snapshots.is_empty() or "playthrough_id" not in snapshots.columns:
            return None
        sort_columns = [
            column
            for column in ("mtime_ns", "date_sort", "year", "month", "day", "snapshot_id")
            if column in snapshots.columns
        ]
        if sort_columns:
            snapshots = snapshots.sort(sort_columns)
        return snapshots.item(-1, "playthrough_id")

    def dim(self, name: str) -> pl.DataFrame:
        path = self.dims_root / f"{name}.parquet"
        if path.exists():
            return pl.read_parquet(path)
        if self.is_raw:
            return self._raw_dimensions().get(name, pl.DataFrame())
        return pl.DataFrame()

    def scan_fact(
        self,
        table: str,
        *,
        playthrough_id: str | None = None,
        good_id: str | None = None,
    ) -> pl.LazyFrame:
        files = self.fact_files(
            table,
            playthrough_id=playthrough_id,
            good_id=None if self.is_raw else good_id,
        )
        if not files:
            return pl.DataFrame().lazy()
        frame = pl.scan_parquet(
            [str(path) for path in files],
            hive_partitioning=False,
            missing_columns="insert",
            extra_columns="ignore",
        )
        if self.is_raw:
            frame = _prepare_raw_fact_frame(table, frame, self._raw_dimensions())
        if good_id is not None:
            good_code = self.code_for("goods", key_value=good_id)
            if good_code is None:
                return pl.DataFrame().lazy()
            if "good_code" in frame.collect_schema().names():
                frame = frame.filter(pl.col("good_code") == good_code)
        return frame

    def fact_files(
        self,
        table: str,
        *,
        playthrough_id: str | None = None,
        good_id: str | None = None,
    ) -> list[Path]:
        root = (self.raw_tables_root if self.is_raw else self.facts_root) / table
        if playthrough_id is not None:
            root = root / f"playthrough_id={_safe_id(playthrough_id)}"
        if good_id is not None and not self.is_raw:
            good_code = self.code_for("goods", key_value=good_id)
            if good_code is None:
                return []
            if table in GOOD_PARTITIONED_FACTS:
                root = root / f"good_code={good_code}"
        if not root.exists():
            return []
        files = sorted(root.rglob("*.parquet"))
        if self.is_raw:
            snapshot_ids = self._raw_snapshot_ids()
            if snapshot_ids is None:
                return files
            return [path for path in files if path.stem in snapshot_ids]
        if good_id is None or table in GOOD_PARTITIONED_FACTS:
            return files
        good_code = self.code_for("goods", key_value=good_id)
        if good_code is None:
            return []
        return [
            path
            for path in files
            if "good_code" in pl.read_parquet_schema(path)
            and pl.scan_parquet(str(path))
            .filter(pl.col("good_code") == good_code)
            .limit(1)
            .collect()
            .height
        ]

    def code_for(self, dimension: str, *, key_value: Any) -> int | None:
        spec = DIMENSION_SPECS[dimension]
        dim = self.dim(dimension)
        if dim.is_empty():
            return None
        rows = dim.filter(pl.col(spec["key"]) == key_value)
        if rows.is_empty():
            return None
        return int(rows.item(0, spec["code"]))

    def with_dimension(self, frame: pl.LazyFrame, dimension: str) -> pl.LazyFrame:
        spec = DIMENSION_SPECS[dimension]
        dim = self.dim(dimension)
        if dim.is_empty() or spec["code"] not in frame.collect_schema().names():
            return frame
        return frame.join(dim.lazy(), on=spec["code"], how="left")

    def _raw_manifest(self) -> _ManifestSelection:
        return _select_manifest(self.root, active_save_dir=self.active_save_dir)

    def _raw_snapshot_ids(self) -> set[str] | None:
        return _snapshot_ids(self._raw_manifest().frame)

    def _raw_dimensions(self) -> dict[str, pl.DataFrame]:
        if self._dimension_cache is None:
            manifest = self._raw_manifest()
            active_snapshot_ids = _snapshot_ids(manifest.frame)
            table_files = {
                table: _filter_table_files_by_snapshots(
                    sorted((self.raw_tables_root / table).rglob("*.parquet")),
                    active_snapshot_ids,
                )
                for table in FACT_TABLES
                if (self.raw_tables_root / table).exists()
            }
            dimensions = _build_dimensions(self.root, table_files, manifest.frame)
            dimensions = _enrich_production_method_metadata(
                dimensions,
                profile=self.profile,
                load_order_path=self.load_order_path,
            )
            dimensions = enrich_notebook_dimensions(
                dimensions,
                resolver=_notebook_label_resolver(
                    profile=self.profile,
                    load_order_path=self.load_order_path,
                ),
            )
            self._dimension_cache = dimensions
        return self._dimension_cache

    def rank_groups(
        self,
        table: str,
        *,
        group_by: str | Sequence[str],
        metric: str,
        statistic: str = "sum",
        playthrough_id: str | None = None,
        limit: int | None = None,
        descending: bool = True,
    ) -> pl.LazyFrame:
        return rank_groups(
            self.scan_fact(table, playthrough_id=playthrough_id),
            group_by=group_by,
            metric=metric,
            statistic=statistic,
            limit=limit,
            descending=descending,
        )

    def scan_good_sources(
        self,
        good_id: str,
        *,
        playthrough_id: str | None = None,
    ) -> pl.LazyFrame:
        rgo = self.scan_fact("rgo_flows", playthrough_id=playthrough_id, good_id=good_id)
        pm = self.scan_fact(
            "production_method_good_flows",
            playthrough_id=playthrough_id,
            good_id=good_id,
        )
        return _concat_non_empty(
            [
                _filter_direction(rgo, {"source", "supply", "output", "produces"}).with_columns(
                    pl.lit("rgo").alias("flow_table")
                ),
                _filter_direction(pm, {"source", "supply", "output", "produces"}).with_columns(
                    pl.lit("production_method").alias("flow_table")
                ),
            ]
        )

    def scan_good_sinks(
        self,
        good_id: str,
        *,
        playthrough_id: str | None = None,
    ) -> pl.LazyFrame:
        buckets = self.scan_fact(
            "market_good_bucket_flows",
            playthrough_id=playthrough_id,
            good_id=good_id,
        )
        pm = self.scan_fact(
            "production_method_good_flows",
            playthrough_id=playthrough_id,
            good_id=good_id,
        )
        return _concat_non_empty(
            [
                _filter_direction(buckets, {"sink", "demand", "input", "consumes"}).with_columns(
                    pl.lit("market_bucket").alias("flow_table")
                ),
                _filter_direction(pm, {"sink", "demand", "input", "consumes"}).with_columns(
                    pl.lit("production_method").alias("flow_table")
                ),
            ]
        )


def build_savegame_notebook_dataset(
    dataset: str | Path,
    output: str | Path,
    *,
    overwrite: bool = True,
    skip_if_current: bool = False,
    active_save_dir: str | Path | None = None,
    profile: str | DataProfile | None = None,
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
) -> NotebookBuildResult:
    source = Path(dataset)
    target = Path(output)
    if not source.exists():
        raise FileNotFoundError(f"Savegame parquet dataset not found: {source}")
    if source.resolve() == target.resolve():
        raise ValueError("Notebook output must be different from the raw savegame dataset path.")

    started = time.perf_counter()
    manifest = _select_manifest(source, active_save_dir=active_save_dir)
    active_snapshot_ids = _snapshot_ids(manifest.frame)
    table_files = {
        table: _filter_table_files_by_snapshots(
            sorted((source / "tables" / table).rglob("*.parquet")),
            active_snapshot_ids,
        )
        for table in FACT_TABLES
        if (source / "tables" / table).exists()
    }
    manifest_fingerprint = _manifest_fingerprint(manifest.frame)
    build_metadata = {
        "schema_version": NOTEBOOK_SCHEMA_VERSION,
        "source": str(source),
        "manifest_fingerprint": manifest_fingerprint,
        "profile": _profile_name(profile),
        "load_order": str(load_order_path),
        "active_save_dir": str(manifest.active_save_dir) if manifest.active_save_dir else None,
    }
    if skip_if_current and _target_matches_metadata(target, build_metadata):
        metadata = _read_metadata(target)
        return NotebookBuildResult(
            source=source,
            output=target,
            facts=dict(metadata.get("facts", {})),
            dimensions=dict(metadata.get("dimensions", {})),
            snapshots=int(metadata.get("snapshots", 0)),
            elapsed_seconds=time.perf_counter() - started,
            status="up-to-date",
            stale_snapshots_ignored=manifest.stale_snapshots_ignored,
            manifest_fingerprint=manifest_fingerprint,
        )

    build_root = _prepare_build_root(target, overwrite=overwrite)
    warnings: list[str] = []
    try:
        build_root.mkdir(parents=True, exist_ok=True)
        (build_root / "facts").mkdir(exist_ok=True)
        (build_root / "dims").mkdir(exist_ok=True)

        snapshots = _write_snapshots(manifest.frame, build_root)
        dimensions = _build_dimensions(source, table_files, manifest.frame)
        dimensions = _enrich_production_method_metadata(
            dimensions,
            profile=profile,
            load_order_path=load_order_path,
        )
        dimensions = enrich_notebook_dimensions(
            dimensions,
            resolver=_notebook_label_resolver(
                profile=profile,
                load_order_path=load_order_path,
            ),
        )
        dimension_counts = _write_dimensions(build_root, dimensions)
        fact_counts = _write_facts(build_root, table_files, dimensions)
        metadata = {
            **build_metadata,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "facts": fact_counts,
            "dimensions": dimension_counts,
            "snapshots": snapshots,
            "total_source_snapshots": manifest.total_snapshots,
            "stale_snapshots_ignored": manifest.stale_snapshots_ignored,
        }
        (build_root / "metadata.json").write_text(
            json.dumps(metadata, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        if overwrite:
            warnings.extend(_replace_directory(target, build_root))
    except Exception:
        if overwrite:
            _cleanup_directory_best_effort(build_root)
        raise
    return NotebookBuildResult(
        source=source,
        output=target,
        facts=fact_counts,
        dimensions=dimension_counts,
        snapshots=snapshots,
        elapsed_seconds=time.perf_counter() - started,
        status="rebuilt",
        stale_snapshots_ignored=manifest.stale_snapshots_ignored,
        manifest_fingerprint=manifest_fingerprint,
        warnings=tuple(warnings),
    )


def rank_groups(
    frame: pl.LazyFrame,
    *,
    group_by: str | Sequence[str],
    metric: str,
    statistic: str = "sum",
    limit: int | None = None,
    descending: bool = True,
) -> pl.LazyFrame:
    if statistic == "sum":
        expression = pl.col(metric).sum()
    elif statistic == "mean":
        expression = pl.col(metric).mean()
    elif statistic == "median":
        expression = pl.col(metric).median()
    else:
        raise ValueError("statistic must be one of: sum, mean, median")
    grouped = (
        frame.group_by(group_by)
        .agg(expression.alias(metric))
        .sort(
            metric,
            descending=descending,
        )
    )
    if limit is not None:
        grouped = grouped.limit(limit)
    return grouped


def _select_manifest(
    source: Path,
    *,
    active_save_dir: str | Path | None,
) -> _ManifestSelection:
    manifest_path = source / "manifest.parquet"
    if manifest_path.exists():
        frame = pl.read_parquet(manifest_path)
    else:
        frame = pl.DataFrame()
    total_snapshots = frame.height
    active_dir = (
        Path(active_save_dir).expanduser().resolve() if active_save_dir is not None else None
    )
    if active_dir is None or frame.is_empty():
        return _ManifestSelection(
            frame=frame,
            total_snapshots=total_snapshots,
            stale_snapshots_ignored=0,
            active_save_dir=active_dir,
        )

    path_column = (
        "path"
        if "path" in frame.columns
        else "source_path"
        if "source_path" in frame.columns
        else None
    )
    if path_column is None:
        return _ManifestSelection(
            frame=frame,
            total_snapshots=total_snapshots,
            stale_snapshots_ignored=0,
            active_save_dir=active_dir,
        )

    keep = [
        _save_path_is_active(value, active_dir) for value in frame.get_column(path_column).to_list()
    ]
    filtered = frame.filter(pl.Series("_active_save", keep))
    return _ManifestSelection(
        frame=filtered,
        total_snapshots=total_snapshots,
        stale_snapshots_ignored=total_snapshots - filtered.height,
        active_save_dir=active_dir,
    )


def _save_path_is_active(value: object, active_dir: Path) -> bool:
    if value is None:
        return False
    path = Path(str(value)).expanduser()
    try:
        resolved = path.resolve()
    except OSError:
        return False
    if not resolved.is_file():
        return False
    try:
        resolved.relative_to(active_dir)
    except ValueError:
        return False
    return True


def _snapshot_ids(manifest: pl.DataFrame) -> set[str] | None:
    if "snapshot_id" not in manifest.columns:
        return None
    return {str(value) for value in manifest.get_column("snapshot_id").drop_nulls().to_list()}


def _filter_table_files_by_snapshots(
    files: list[Path], snapshot_ids: set[str] | None
) -> list[Path]:
    if snapshot_ids is None:
        return files
    return [path for path in files if path.stem in snapshot_ids]


def _manifest_fingerprint(manifest: pl.DataFrame) -> str:
    columns = [
        column
        for column in (
            "snapshot_id",
            "playthrough_id",
            "path",
            "source_path",
            "mtime_ns",
            "size",
            "partial_hash",
            "quick_hash",
            "state_key",
            "parser_profile",
            "row_counts_json",
        )
        if column in manifest.columns
    ]
    if columns:
        frame = manifest.select(columns)
        sort_columns = [
            column for column in ("playthrough_id", "snapshot_id", "path") if column in columns
        ]
        if sort_columns:
            frame = frame.sort(sort_columns)
        payload: object = frame.to_dicts()
    else:
        payload = []
    encoded = json.dumps(payload, default=str, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _profile_name(profile: str | DataProfile | None) -> str | None:
    if profile is None:
        return None
    if isinstance(profile, DataProfile):
        return profile.name
    return str(profile)


def _read_metadata(target: Path) -> dict[str, Any]:
    try:
        return json.loads((target / "metadata.json").read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _target_matches_metadata(target: Path, expected: dict[str, Any]) -> bool:
    if not (target / "metadata.json").is_file() or not (target / "snapshots.parquet").is_file():
        return False
    if not (target / "facts").is_dir() or not (target / "dims").is_dir():
        return False
    metadata = _read_metadata(target)
    return bool(metadata) and all(metadata.get(key) == value for key, value in expected.items())


def _prepare_build_root(target: Path, *, overwrite: bool) -> Path:
    if not overwrite:
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    return target.parent / f".{target.name}.tmp.{os.getpid()}.{time.time_ns()}"


def _replace_directory(target: Path, build_root: Path) -> list[str]:
    if build_root == target:
        return []
    backup: Path | None = None
    if target.exists():
        backup = target.parent / f".{target.name}.old.{os.getpid()}.{time.time_ns()}"
        try:
            _rename_with_retries(target, backup)
        except OSError as exc:
            raise OSError(
                f"Could not replace notebook output at {target}. Close open notebooks or Windows "
                "Explorer views using that directory, then rerun the build."
            ) from exc
    try:
        _rename_with_retries(build_root, target)
    except OSError as exc:
        if backup is not None and backup.exists() and not target.exists():
            try:
                _rename_with_retries(backup, target)
            except OSError:
                pass
        raise OSError(f"Could not move staged notebook output into place at {target}.") from exc

    warnings: list[str] = []
    if backup is not None:
        warning = _cleanup_directory_best_effort(backup)
        if warning:
            warnings.append(warning)
    return warnings


def _rename_with_retries(source: Path, target: Path) -> None:
    last_error: OSError | None = None
    for attempt in range(8):
        try:
            source.rename(target)
            return
        except OSError as exc:
            last_error = exc
            time.sleep(0.25 * (attempt + 1))
    if last_error is not None:
        raise last_error


def _cleanup_directory_best_effort(path: Path) -> str | None:
    last_error: OSError | None = None
    for attempt in range(4):
        try:
            if path.is_dir():
                shutil.rmtree(path)
            elif path.exists():
                path.unlink()
            return None
        except OSError as exc:
            last_error = exc
            time.sleep(0.1 * (attempt + 1))
    if last_error is None:
        return None
    return (
        f"Could not remove old notebook output {path}: {last_error}. The new dataset is already "
        "in place; close any open notebooks or Windows Explorer views and delete that old "
        "directory later."
    )


def _snapshot_frame(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return _empty_snapshots()
    if "source_path" not in frame.columns and "path" in frame.columns:
        frame = frame.with_columns(pl.col("path").cast(pl.String).alias("source_path"))
    missing = [
        pl.lit(None, dtype=dtype).alias(column)
        for column, dtype in SNAPSHOT_SCHEMA.items()
        if column not in frame.columns
    ]
    if missing:
        frame = frame.with_columns(missing)
    return _with_date_sort(_compact_frame(frame))


def _write_snapshots(manifest: pl.DataFrame, target: Path) -> int:
    if manifest.is_empty() or "snapshot_id" not in manifest.columns:
        _empty_snapshots().write_parquet(target / "snapshots.parquet", compression="zstd")
        return 0
    frame = _snapshot_frame(manifest)
    frame.write_parquet(target / "snapshots.parquet", compression="zstd")
    return frame.height


def _empty_snapshots() -> pl.DataFrame:
    return pl.DataFrame(schema=SNAPSHOT_SCHEMA)


def _build_dimensions(
    source: Path,
    table_files: dict[str, list[Path]],
    manifest_frame: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    dimensions: dict[str, pl.DataFrame] = {}
    if "playthrough_id" in manifest_frame.columns:
        dimensions["playthroughs"] = _dimension_with_code(
            manifest_frame.select("playthrough_id").unique().drop_nulls("playthrough_id"),
            key="playthrough_id",
            code="playthrough_code",
        )
    for name, spec in DIMENSION_SPECS.items():
        rows: list[pl.DataFrame] = []
        requested = tuple(spec["columns"])
        key = str(spec["key"])
        for files in table_files.values():
            for path in files:
                schema = pl.read_parquet_schema(path)
                if key not in schema:
                    continue
                columns = [column for column in requested if column in schema]
                if key not in columns:
                    columns.append(key)
                rows.append(_read_columns(path, columns))
        if not rows:
            continue
        frame = _coalesce_dimension_rows(
            pl.concat(rows, how="diagonal_relaxed").drop_nulls(key),
            key=key,
        )
        dimensions[name] = _dimension_with_code(frame, key=key, code=str(spec["code"]))
    return dimensions


def _coalesce_dimension_rows(frame: pl.DataFrame, *, key: str) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    value_columns = [column for column in frame.columns if column != key]
    if not value_columns:
        return frame.unique(subset=[key], keep="first", maintain_order=True)
    return frame.group_by(key, maintain_order=True).agg(
        pl.col(column).drop_nulls().first().alias(column) for column in value_columns
    )


def _enrich_production_method_metadata(
    dimensions: dict[str, pl.DataFrame],
    *,
    profile: str | DataProfile | None,
    load_order_path: str | Path,
) -> dict[str, pl.DataFrame]:
    methods = dimensions.get("production_methods")
    if methods is None or methods.is_empty() or "production_method" not in methods.columns:
        return dimensions

    metadata = _production_method_metadata(profile=profile, load_order_path=load_order_path)
    enriched = methods
    if not metadata.is_empty():
        enriched = enriched.join(
            metadata,
            left_on="production_method",
            right_on="name",
            how="left",
        )
    dimensions = dict(dimensions)
    dimensions["production_methods"] = _with_production_method_slot_defaults(enriched)
    return dimensions


def _production_method_metadata(
    *,
    profile: str | DataProfile | None,
    load_order_path: str | Path,
) -> pl.DataFrame:
    schema = {
        "name": pl.String,
        "production_method_building": pl.String,
        "production_method_group": pl.String,
        "production_method_group_index": pl.Int64,
    }
    if profile is None:
        return pl.DataFrame(schema=schema)
    try:
        data = load_building_data(profile=profile, load_order_path=load_order_path)
    except (FileNotFoundError, KeyError, OSError):
        return pl.DataFrame(schema=schema)
    methods = data.production_methods
    if methods.is_empty() or "name" not in methods.columns:
        return pl.DataFrame(schema=schema)
    expressions = [
        pl.col("name"),
        (
            pl.col("building").alias("production_method_building")
            if "building" in methods.columns
            else pl.lit(None, dtype=pl.String).alias("production_method_building")
        ),
        (
            pl.col("production_method_group")
            if "production_method_group" in methods.columns
            else pl.lit(None, dtype=pl.String).alias("production_method_group")
        ),
        (
            pl.col("production_method_group_index")
            if "production_method_group_index" in methods.columns
            else pl.lit(None, dtype=pl.Int64).alias("production_method_group_index")
        ),
    ]
    return methods.select(expressions).unique("name", keep="first", maintain_order=True)


def _notebook_label_resolver(
    *,
    profile: str | DataProfile | None,
    load_order_path: str | Path,
) -> NotebookLabelResolver:
    try:
        return NotebookLabelResolver.from_profile(
            profile=profile,
            load_order_path=load_order_path,
        )
    except (FileNotFoundError, KeyError, OSError):
        return NotebookLabelResolver()


def _with_production_method_slot_defaults(frame: pl.DataFrame) -> pl.DataFrame:
    additions: list[pl.Expr] = []
    for column, dtype in {
        "production_method_building": pl.String,
        "production_method_group": pl.String,
        "production_method_group_index": pl.Int64,
    }.items():
        if column not in frame.columns:
            additions.append(pl.lit(None, dtype=dtype).alias(column))
    if additions:
        frame = frame.with_columns(additions)
    return frame.with_columns(
        pl.when(pl.col("production_method_group_index").is_not_null())
        .then(
            pl.concat_str(
                [
                    pl.lit("Slot "),
                    (pl.col("production_method_group_index").cast(pl.Int64) + 1).cast(pl.String),
                ]
            )
        )
        .otherwise(pl.lit("Unslotted"))
        .alias("slot_label")
    )


def _write_dimensions(target: Path, dimensions: dict[str, pl.DataFrame]) -> dict[str, int]:
    counts: dict[str, int] = {}
    dims_root = target / "dims"
    for name, frame in dimensions.items():
        compact = _compact_frame(frame)
        compact.write_parquet(dims_root / f"{name}.parquet", compression="zstd")
        counts[name] = compact.height
    return counts


def _write_facts(
    target: Path,
    table_files: dict[str, list[Path]],
    dimensions: dict[str, pl.DataFrame],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table, files in table_files.items():
        table_count = 0
        for path in files:
            frame = _read_table_fact(path, table)
            if frame.is_empty():
                continue
            frame = _encode_dimensions(frame, dimensions)
            frame = _with_date_sort(_compact_frame(frame))
            table_count += frame.height
            _write_fact_frame(target, table, path, frame)
        if table_count:
            counts[table] = table_count
    return counts


def _read_table_fact(path: Path, table: str) -> pl.DataFrame:
    schema = pl.read_parquet_schema(path)
    requested = list(TABLE_COLUMNS.get(table, COMMON_COLUMNS))
    prefixes = TABLE_PREFIXES.get(table, ())
    columns = [
        column
        for column in schema
        if column in requested or any(column.startswith(prefix) for prefix in prefixes)
    ]
    if not columns:
        return pl.DataFrame()
    return _read_columns(path, columns)


def _prepare_raw_fact_frame(
    table: str,
    frame: pl.LazyFrame,
    dimensions: dict[str, pl.DataFrame],
) -> pl.LazyFrame:
    schema = frame.collect_schema()
    requested = set(TABLE_COLUMNS.get(table, COMMON_COLUMNS))
    prefixes = TABLE_PREFIXES.get(table, ())
    columns = [
        column
        for column in schema.names()
        if column == "date_sort"
        or column in requested
        or any(column.startswith(prefix) for prefix in prefixes)
    ]
    if not columns:
        return pl.DataFrame().lazy()
    frame = frame.select(list(dict.fromkeys(columns)))
    frame = _encode_dimensions_lazy(frame, dimensions)
    frame = _with_date_sort_lazy(frame)
    return _compact_lazy_frame(frame)


def _read_columns(path: Path, columns: Sequence[str]) -> pl.DataFrame:
    return pl.read_parquet(path, columns=list(dict.fromkeys(columns)))


def _encode_dimensions(
    frame: pl.DataFrame,
    dimensions: dict[str, pl.DataFrame],
) -> pl.DataFrame:
    for fact_column, dimension_name, code_column in ENCODE_SPECS:
        if fact_column not in frame.columns:
            continue
        dimension = dimensions.get(dimension_name)
        if dimension is None or dimension.is_empty():
            continue
        spec = DIMENSION_SPECS[dimension_name]
        key_column = str(spec["key"])
        if key_column not in dimension.columns or code_column not in dimension.columns:
            continue
        mapping = dimension.select(key_column, code_column)
        frame = frame.join(mapping, left_on=fact_column, right_on=key_column, how="left")
    drop_columns: list[str] = []
    for code_column, label_columns in LABEL_COLUMNS_TO_DROP.items():
        if code_column in frame.columns:
            drop_columns.extend(column for column in label_columns if column in frame.columns)
    if drop_columns:
        frame = frame.drop(*sorted(set(drop_columns)))
    return frame


def _encode_dimensions_lazy(
    frame: pl.LazyFrame,
    dimensions: dict[str, pl.DataFrame],
) -> pl.LazyFrame:
    for fact_column, dimension_name, code_column in ENCODE_SPECS:
        if fact_column not in frame.collect_schema().names():
            continue
        dimension = dimensions.get(dimension_name)
        if dimension is None or dimension.is_empty():
            continue
        spec = DIMENSION_SPECS[dimension_name]
        key_column = str(spec["key"])
        dimension_code = str(spec["code"])
        if key_column not in dimension.columns:
            continue
        code_source = code_column if code_column in dimension.columns else dimension_code
        if code_source not in dimension.columns:
            continue
        mapping = dimension.select(
            pl.col(key_column),
            pl.col(code_source).alias(code_column),
        )
        frame = frame.join(mapping.lazy(), left_on=fact_column, right_on=key_column, how="left")
    drop_columns: list[str] = []
    names = set(frame.collect_schema().names())
    for code_column, label_columns in LABEL_COLUMNS_TO_DROP.items():
        if code_column in names:
            drop_columns.extend(column for column in label_columns if column in names)
    if drop_columns:
        frame = frame.drop(*sorted(set(drop_columns)))
    return frame


def _write_fact_frame(target: Path, table: str, source_path: Path, frame: pl.DataFrame) -> None:
    playthrough_id = _frame_value(frame, "playthrough_id")
    snapshot_id = _frame_value(frame, "snapshot_id") or source_path.stem
    table_root = target / "facts" / table / f"playthrough_id={_safe_id(playthrough_id)}"
    if table in GOOD_PARTITIONED_FACTS and "good_code" in frame.columns:
        for key, partition in frame.partition_by(
            "good_code", as_dict=True, maintain_order=True
        ).items():
            good_code = key[0] if isinstance(key, tuple) else key
            path = table_root / f"good_code={good_code}" / f"{snapshot_id}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            partition.write_parquet(path, compression="zstd")
        return
    path = table_root / f"{snapshot_id}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path, compression="zstd")


def _dimension_with_code(frame: pl.DataFrame, *, key: str, code: str) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    columns = [key, *[column for column in frame.columns if column != key]]
    return (
        frame.select(columns)
        .sort(key)
        .unique(subset=[key], keep="first", maintain_order=True)
        .with_row_index(code)
        .with_columns(pl.col(code).cast(pl.UInt32))
    )


def _compact_frame(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    expressions: list[pl.Expr] = []
    for name, dtype in zip(frame.columns, frame.dtypes, strict=True):
        expression = pl.col(name)
        if name == "year":
            expression = expression.cast(pl.UInt16, strict=False)
        elif name in {"month", "day"}:
            expression = expression.cast(pl.UInt8, strict=False)
        elif name == "date_sort":
            expression = expression.cast(pl.UInt32, strict=False)
        elif name.endswith("_code"):
            expression = expression.cast(pl.UInt32, strict=False)
        elif name in IDENTIFIER_COLUMNS:
            pass
        elif dtype.is_integer():
            expression = expression.cast(pl.Float32, strict=False)
        elif dtype.is_float():
            expression = expression.cast(pl.Float32, strict=False)
        expressions.append(expression.alias(name))
    return frame.with_columns(expressions)


def _compact_lazy_frame(frame: pl.LazyFrame) -> pl.LazyFrame:
    schema = frame.collect_schema()
    expressions: list[pl.Expr] = []
    for name, dtype in schema.items():
        expression = pl.col(name)
        if name == "year":
            expression = expression.cast(pl.UInt16, strict=False)
        elif name in {"month", "day"}:
            expression = expression.cast(pl.UInt8, strict=False)
        elif name == "date_sort":
            expression = expression.cast(pl.UInt32, strict=False)
        elif name.endswith("_code"):
            expression = expression.cast(pl.UInt32, strict=False)
        elif name in IDENTIFIER_COLUMNS:
            pass
        elif dtype.is_integer():
            expression = expression.cast(pl.Float32, strict=False)
        elif dtype.is_float():
            expression = expression.cast(pl.Float32, strict=False)
        expressions.append(expression.alias(name))
    return frame.with_columns(expressions)


def _with_date_sort(frame: pl.DataFrame) -> pl.DataFrame:
    if {"year", "month", "day"}.issubset(frame.columns):
        computed = (
            pl.col("year").cast(pl.UInt32, strict=False) * 10_000
            + pl.col("month").cast(pl.UInt32, strict=False) * 100
            + pl.col("day").cast(pl.UInt32, strict=False)
        )
        if "date_sort" in frame.columns:
            return frame.with_columns(
                pl.when(pl.col("date_sort").is_null())
                .then(computed)
                .otherwise(pl.col("date_sort"))
                .alias("date_sort")
            )
        return frame.with_columns(computed.alias("date_sort"))
    return frame


def _with_date_sort_lazy(frame: pl.LazyFrame) -> pl.LazyFrame:
    names = frame.collect_schema().names()
    if {"year", "month", "day"}.issubset(names):
        computed = (
            pl.col("year").cast(pl.UInt32, strict=False) * 10_000
            + pl.col("month").cast(pl.UInt32, strict=False) * 100
            + pl.col("day").cast(pl.UInt32, strict=False)
        )
        if "date_sort" in names:
            return frame.with_columns(
                pl.when(pl.col("date_sort").is_null())
                .then(computed)
                .otherwise(pl.col("date_sort"))
                .alias("date_sort")
            )
        return frame.with_columns(computed.alias("date_sort"))
    return frame


def _frame_value(frame: pl.DataFrame, column: str) -> str:
    if column not in frame.columns or frame.is_empty():
        return "unknown"
    value = frame.item(0, column)
    if value is None:
        return "unknown"
    return str(value)


def _filter_direction(frame: pl.LazyFrame, allowed: set[str]) -> pl.LazyFrame:
    if "direction" not in frame.collect_schema().names():
        return frame
    return frame.filter(pl.col("direction").cast(pl.String).str.to_lowercase().is_in(allowed))


def _concat_non_empty(frames: Iterable[pl.LazyFrame]) -> pl.LazyFrame:
    items = [frame for frame in frames if frame.collect_schema().names()]
    if not items:
        return pl.DataFrame().lazy()
    return pl.concat(items, how="diagonal_relaxed")


def _safe_id(value: Any) -> str:
    text = str(value or "unknown")
    return "".join(
        character if character.isalnum() or character in {"_", "-"} else "_" for character in text
    )
