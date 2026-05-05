from __future__ import annotations

import json
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


class SavegameNotebookDataset:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    @property
    def facts_root(self) -> Path:
        return self.root / "facts"

    @property
    def dims_root(self) -> Path:
        return self.root / "dims"

    @property
    def snapshots_path(self) -> Path:
        return self.root / "snapshots.parquet"

    def snapshots(self) -> pl.DataFrame:
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
        if not path.exists():
            return pl.DataFrame()
        return pl.read_parquet(path)

    def scan_fact(
        self,
        table: str,
        *,
        playthrough_id: str | None = None,
        good_id: str | None = None,
    ) -> pl.LazyFrame:
        files = self.fact_files(table, playthrough_id=playthrough_id, good_id=good_id)
        if not files:
            return pl.DataFrame().lazy()
        return pl.scan_parquet(
            [str(path) for path in files],
            hive_partitioning=False,
            missing_columns="insert",
            extra_columns="ignore",
        )

    def fact_files(
        self,
        table: str,
        *,
        playthrough_id: str | None = None,
        good_id: str | None = None,
    ) -> list[Path]:
        root = self.facts_root / table
        if playthrough_id is not None:
            root = root / f"playthrough_id={_safe_id(playthrough_id)}"
        if good_id is not None:
            good_code = self.code_for("goods", key_value=good_id)
            if good_code is None:
                return []
            if table in GOOD_PARTITIONED_FACTS:
                root = root / f"good_code={good_code}"
        if not root.exists():
            return []
        files = sorted(root.rglob("*.parquet"))
        if good_id is None or table in GOOD_PARTITIONED_FACTS:
            return files
        good_code = self.code_for("goods", key_value=good_id)
        if good_code is None:
            return []
        return [
            path
            for path in files
            if "good_code" in pl.read_parquet_schema(path)
            and pl.scan_parquet(str(path)).filter(pl.col("good_code") == good_code).limit(1).collect().height
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
    table_files = {
        table: sorted((source / "tables" / table).rglob("*.parquet"))
        for table in FACT_TABLES
        if (source / "tables" / table).exists()
    }
    if overwrite and target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)
    (target / "facts").mkdir(exist_ok=True)
    (target / "dims").mkdir(exist_ok=True)

    snapshots = _write_snapshots(source, target)
    dimensions = _build_dimensions(source, table_files)
    dimensions = _enrich_production_method_metadata(
        dimensions,
        profile=profile,
        load_order_path=load_order_path,
    )
    dimensions = enrich_notebook_dimensions(
        dimensions,
        resolver=NotebookLabelResolver.from_profile(
            profile=profile,
            load_order_path=load_order_path,
        ),
    )
    dimension_counts = _write_dimensions(target, dimensions)
    fact_counts = _write_facts(target, table_files, dimensions)
    metadata = {
        "schema_version": NOTEBOOK_SCHEMA_VERSION,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": str(source),
        "facts": fact_counts,
        "dimensions": dimension_counts,
        "snapshots": snapshots,
    }
    (target / "metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return NotebookBuildResult(
        source=source,
        output=target,
        facts=fact_counts,
        dimensions=dimension_counts,
        snapshots=snapshots,
        elapsed_seconds=time.perf_counter() - started,
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
    grouped = frame.group_by(group_by).agg(expression.alias(metric)).sort(
        metric,
        descending=descending,
    )
    if limit is not None:
        grouped = grouped.limit(limit)
    return grouped


def _write_snapshots(source: Path, target: Path) -> int:
    manifest = source / "manifest.parquet"
    if not manifest.exists():
        _empty_snapshots().write_parquet(target / "snapshots.parquet", compression="zstd")
        return 0
    frame = pl.read_parquet(manifest)
    frame = _with_date_sort(_compact_frame(frame))
    frame.write_parquet(target / "snapshots.parquet", compression="zstd")
    return frame.height


def _empty_snapshots() -> pl.DataFrame:
    return pl.DataFrame(schema=SNAPSHOT_SCHEMA)


def _build_dimensions(
    source: Path,
    table_files: dict[str, list[Path]],
) -> dict[str, pl.DataFrame]:
    dimensions: dict[str, pl.DataFrame] = {}
    manifest = source / "manifest.parquet"
    if manifest.exists():
        manifest_frame = pl.read_parquet(manifest)
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


def _write_fact_frame(target: Path, table: str, source_path: Path, frame: pl.DataFrame) -> None:
    playthrough_id = _frame_value(frame, "playthrough_id")
    snapshot_id = _frame_value(frame, "snapshot_id") or source_path.stem
    table_root = target / "facts" / table / f"playthrough_id={_safe_id(playthrough_id)}"
    if table in GOOD_PARTITIONED_FACTS and "good_code" in frame.columns:
        for key, partition in frame.partition_by("good_code", as_dict=True, maintain_order=True).items():
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


def _with_date_sort(frame: pl.DataFrame) -> pl.DataFrame:
    if {"year", "month", "day"}.issubset(frame.columns) and "date_sort" not in frame.columns:
        return frame.with_columns(
            (
                pl.col("year").cast(pl.UInt32, strict=False) * 10_000
                + pl.col("month").cast(pl.UInt32, strict=False) * 100
                + pl.col("day").cast(pl.UInt32, strict=False)
            ).alias("date_sort")
        )
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
    return "".join(character if character.isalnum() or character in {"_", "-"} else "_" for character in text)
