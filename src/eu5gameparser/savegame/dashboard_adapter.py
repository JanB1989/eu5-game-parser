from __future__ import annotations

import json
import math
import shutil
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from eu5gameparser.domain.buildings import BuildingData, load_building_data
from eu5gameparser.load_order import DEFAULT_LOAD_ORDER_PATH
from eu5gameparser.savegame.dataset import SavegameDataset
from eu5gameparser.savegame.exporter import POP_TOTAL_COLUMNS

SNAPSHOT_COLUMNS = (
    "snapshot_id",
    "playthrough_id",
    "date",
    "year",
    "month",
    "day",
    "date_sort",
)
DASHBOARD_CACHE_SCHEMA_VERSION = 1
DASHBOARD_CACHE_TOP_N = 40
DASHBOARD_CACHE_DIR = "dashboard_cache"
_OVERVIEW_SERIES_KEYS = {
    "popsByType": "pops_by_type",
    "employment": "employment",
    "development": "development",
    "tax": "tax",
    "food": "food",
}
_FILTER_DIMENSIONS = {
    "good_id": "good_id",
    "goods_category": "goods_category",
    "goods_designation": "goods_designation",
    "market_center_slug": "market_center_slug",
    "building_type": "building_type",
    "production_method": "production_method",
    "country_tag": "country_tag",
    "pop_type": "pop_type",
}
_EMPTY_OVERVIEW = {
    "pops_by_type": [],
    "employment": [],
    "development": [],
    "tax": [],
    "food": [],
}


@dataclass(frozen=True)
class DashboardQueryResult:
    rows: list[dict[str, Any]]
    ranking: list[dict[str, Any]]
    metric: dict[str, str]
    dimension: dict[str, Any]


@dataclass(frozen=True)
class TemplateQueryResult:
    panels: dict[str, dict[str, Any]]
    ranking: list[dict[str, Any]]
    metric: dict[str, Any]
    scope: dict[str, Any]
    chips: list[str]
    empty_message: str | None = None


@dataclass(frozen=True)
class TemplateMetric:
    key: str
    domain: str
    metric: str
    label: str
    unit: str
    formatter: str
    valid_scopes: tuple[str, ...]
    default_scope: str

    def to_dict(self, available_scopes: list[str]) -> dict[str, Any]:
        return {
            "key": self.key,
            "domain": self.domain,
            "metric": self.metric,
            "label": self.label,
            "unit": self.unit,
            "formatter": self.formatter,
            "validScopes": available_scopes,
            "defaultScope": self.default_scope
            if self.default_scope in available_scopes
            else available_scopes[0],
        }


@dataclass(frozen=True)
class TemplateScope:
    key: str
    label: str
    group: str
    order: int

    def to_dict(self) -> dict[str, Any]:
        return {"key": self.key, "label": self.label, "group": self.group, "order": self.order}


class SavegameDashboardAdapter:
    def __init__(
        self,
        dataset: str | Path,
        *,
        profile: str = "merged_default",
        load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
        asset_root: str | Path | None = None,
        cache_top_n: int = DASHBOARD_CACHE_TOP_N,
    ) -> None:
        self.dataset = SavegameDataset(dataset)
        self.root = self.dataset.root
        self.profile = profile
        self.load_order_path = Path(load_order_path)
        self.asset_root = Path(asset_root or self.root / "dashboard_assets")
        self.cache_top_n = max(1, int(cache_top_n))
        self.cache_root = self.root / DASHBOARD_CACHE_DIR
        self._explorer_cache = pl.DataFrame()
        self._overview_cache = pl.DataFrame()
        self._filter_values_cache = pl.DataFrame()
        self._cache_available_pairs: set[tuple[str, str, str]] = set()
        self._cache_lock = threading.Lock()
        self._loaded_manifest_fingerprint: tuple[int, int] | None = None
        self._ensure_dashboard_cache()
        self._load_dashboard_cache()
        self._loaded_manifest_fingerprint = self._manifest_fingerprint()

    def close(self) -> None:
        return None

    def cache_info(self) -> dict[str, Any]:
        self._refresh_if_manifest_changed()
        return {
            "root": str(self.cache_root),
            "schemaVersion": DASHBOARD_CACHE_SCHEMA_VERSION,
            "topN": self.cache_top_n,
            "explorerRows": self._explorer_cache.height,
            "overviewRows": self._overview_cache.height,
            "filterRows": self._filter_values_cache.height,
            "availablePairs": len(self._cache_available_pairs),
        }

    def _ensure_dashboard_cache(self) -> None:
        if self._dashboard_cache_valid():
            return
        payload = self.dataset.build_progression_cubes(top_n=self.cache_top_n)
        snapshots = payload.get("snapshots") or []
        explorer_rows = _normalise_cache_rows(
            payload.get("explorer", {}).get("rows") or [],
            snapshots=snapshots,
        )
        overview_rows = _flatten_overview_cache_rows(
            payload.get("overviewSeries") or {},
            snapshots=snapshots,
        )
        filter_rows = [
            *self._raw_filter_value_rows(),
            *_filter_value_rows_from_explorer(explorer_rows),
        ]

        self.cache_root.mkdir(parents=True, exist_ok=True)
        _dataframe_or_empty(explorer_rows, _explorer_cache_schema()).write_parquet(
            self._explorer_cache_path(),
            compression="zstd",
        )
        _dataframe_or_empty(overview_rows, _overview_cache_schema()).write_parquet(
            self._overview_cache_path(),
            compression="zstd",
        )
        filter_frame = _dataframe_or_empty(filter_rows, _filter_values_schema())
        if not filter_frame.is_empty():
            filter_frame = filter_frame.unique(
                subset=["filter_key", "playthrough_id", "value"],
                keep="first",
                maintain_order=True,
            ).sort(["filter_key", "label", "value"])
        filter_frame.write_parquet(self._filter_values_cache_path(), compression="zstd")
        self._cache_manifest_path().write_text(
            json.dumps(self._dashboard_cache_manifest(), indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def _dashboard_cache_valid(self) -> bool:
        paths = [
            self._cache_manifest_path(),
            self._explorer_cache_path(),
            self._overview_cache_path(),
            self._filter_values_cache_path(),
        ]
        if any(not path.exists() for path in paths):
            return False
        try:
            current = self._dashboard_cache_manifest()
            cached = json.loads(self._cache_manifest_path().read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return False
        return cached == current

    def _dashboard_cache_manifest(self) -> dict[str, Any]:
        return {
            "schemaVersion": DASHBOARD_CACHE_SCHEMA_VERSION,
            "topN": self.cache_top_n,
            "sourceState": _source_state_rows(self.dataset.read_manifest()),
        }

    def _load_dashboard_cache(self) -> None:
        self._explorer_cache = _read_parquet_or_empty(
            self._explorer_cache_path(),
            _explorer_cache_schema(),
        )
        self._overview_cache = _read_parquet_or_empty(
            self._overview_cache_path(),
            _overview_cache_schema(),
        )
        self._filter_values_cache = _read_parquet_or_empty(
            self._filter_values_cache_path(),
            _filter_values_schema(),
        )
        self._cache_available_pairs = set()
        if {"domain", "metric", "dimension"}.issubset(self._explorer_cache.columns):
            pairs = self._explorer_cache.select(["domain", "metric", "dimension"]).unique()
            for row in pairs.to_dicts():
                self._cache_available_pairs.add(
                    (str(row["domain"]), str(row["metric"]), str(row["dimension"]))
                )

    def _manifest_fingerprint(self) -> tuple[int, int] | None:
        try:
            stat = self.dataset.manifest_path.stat()
        except OSError:
            return None
        return stat.st_mtime_ns, stat.st_size

    def _dashboard_cache_files_exist(self) -> bool:
        return all(
            path.exists()
            for path in [
                self._cache_manifest_path(),
                self._explorer_cache_path(),
                self._overview_cache_path(),
                self._filter_values_cache_path(),
            ]
        )

    def _refresh_if_manifest_changed(self) -> None:
        current = self._manifest_fingerprint()
        if current == self._loaded_manifest_fingerprint and self._dashboard_cache_files_exist():
            return
        with self._cache_lock:
            current = self._manifest_fingerprint()
            if current == self._loaded_manifest_fingerprint and self._dashboard_cache_files_exist():
                return
            self._ensure_dashboard_cache()
            self._load_dashboard_cache()
            self._loaded_manifest_fingerprint = self._manifest_fingerprint()

    def _cache_manifest_path(self) -> Path:
        return self.cache_root / "manifest.json"

    def _explorer_cache_path(self) -> Path:
        return self.cache_root / "explorer_series.parquet"

    def _overview_cache_path(self) -> Path:
        return self.cache_root / "overview_series.parquet"

    def _filter_values_cache_path(self) -> Path:
        return self.cache_root / "filter_values.parquet"

    def _raw_filter_value_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        location_columns = self._columns("locations")
        for column in POP_TOTAL_COLUMNS:
            if column not in location_columns:
                continue
            pop_type = column.removeprefix("population_")
            rows.append(
                {
                    "filter_key": "pop_type",
                    "playthrough_id": None,
                    "value": pop_type,
                    "label": pop_type.replace("_", " ").title(),
                }
            )
        for filter_key in _FILTER_DIMENSIONS:
            if filter_key == "pop_type":
                continue
            rows.extend(self._raw_filter_values_for_key(filter_key))
        rows.extend(self._raw_filter_values_for_key("religion_name"))
        return rows

    def _raw_filter_values_for_key(self, filter_key: str) -> list[dict[str, Any]]:
        source_info = _filter_source(filter_key)
        if source_info is None:
            return []
        table, column = source_info
        source = self._table_sql(table)
        columns = self._columns(table)
        if source is None or column not in columns:
            return []
        playthrough_expr = (
            "CAST(playthrough_id AS VARCHAR)" if "playthrough_id" in columns else "NULL"
        )
        values = self._query(
            f"""
            SELECT DISTINCT
              {_sql_string(filter_key)} AS filter_key,
              {playthrough_expr} AS playthrough_id,
              CAST({column} AS VARCHAR) AS value,
              CAST({column} AS VARCHAR) AS label
            FROM {source}
            WHERE {column} IS NOT NULL AND CAST({column} AS VARCHAR) != ''
            ORDER BY label
            LIMIT 5000
            """
        )
        return values.to_dicts()

    def snapshots(self, playthrough_id: str | None = None) -> list[dict[str, Any]]:
        self._refresh_if_manifest_changed()
        manifest = self.dataset.snapshots(playthrough_id)
        if manifest.is_empty():
            return []
        return manifest.select(
            [column for column in SNAPSHOT_COLUMNS if column in manifest.columns]
        ).to_dicts()

    def playthrough_options(self) -> list[dict[str, str]]:
        self._refresh_if_manifest_changed()
        manifest = self.dataset.read_manifest()
        if manifest.is_empty() or "playthrough_id" not in manifest.columns:
            return []
        label_columns = [
            column
            for column in ("playthrough_name", "save_label")
            if column in manifest.columns
        ]
        grouped = (
            manifest.group_by("playthrough_id")
            .agg(
                [
                    pl.col("date_sort").max().alias("latest_date_sort"),
                    pl.col("date").sort_by("date_sort").last().alias("latest_date"),
                    *[
                        pl.col(column).drop_nulls().first().alias(column)
                        for column in label_columns
                    ],
                ]
            )
            .sort("latest_date_sort", descending=True)
        )
        options: list[dict[str, str]] = [{"label": "All playthroughs", "value": ""}]
        for row in grouped.to_dicts():
            playthrough_id = str(row["playthrough_id"])
            name = row.get("playthrough_name") or row.get("save_label") or playthrough_id
            latest = row.get("latest_date")
            label = f"{name} ({latest})" if latest else str(name)
            options.append({"label": label, "value": playthrough_id})
        return options

    def date_options(self, playthrough_id: str | None = None) -> list[dict[str, Any]]:
        return [
            {
                "label": str(row.get("date") or row.get("snapshot_id")),
                "value": row.get("date_sort"),
            }
            for row in self.snapshots(_blank_to_none(playthrough_id))
            if row.get("date_sort") is not None
        ]

    def normalize_date_range(
        self,
        *,
        playthrough_id: str | None = None,
        from_date_sort: int | None = None,
        to_date_sort: int | None = None,
    ) -> tuple[int | None, int | None]:
        options = self.date_options(playthrough_id)
        values = [int(option["value"]) for option in options if option.get("value") is not None]
        if not values:
            return None, None
        lower = values[0] if from_date_sort is None else _nearest_date_sort(values, from_date_sort)
        upper = values[-1] if to_date_sort is None else _nearest_date_sort(values, to_date_sort)
        if lower > upper:
            lower, upper = upper, lower
        return lower, upper

    def overview(
        self,
        *,
        playthrough_id: str | None = None,
        from_date_sort: int | None = None,
        to_date_sort: int | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        self._refresh_if_manifest_changed()
        playthrough_id = _blank_to_none(playthrough_id)
        from_date_sort, to_date_sort = self.normalize_date_range(
            playthrough_id=playthrough_id,
            from_date_sort=from_date_sort,
            to_date_sort=to_date_sort,
        )
        cached = self._cached_overview(
            playthrough_id=playthrough_id,
            from_date_sort=from_date_sort,
            to_date_sort=to_date_sort,
        )
        if cached is not None:
            return cached
        locations = self._table_sql("locations", playthrough_id)
        if locations is None:
            return _empty_overview()

        location_columns = self._columns("locations", playthrough_id)
        where = self._date_where("l", from_date_sort, to_date_sort)
        pop_columns = [column for column in POP_TOTAL_COLUMNS if column in location_columns]
        location_exprs = [
            self._sum_expr(location_columns, "total_population", "total_pops"),
            self._sum_expr(location_columns, "development", "development"),
            self._sum_expr(location_columns, "tax", "collected_tax"),
            self._sum_expr(location_columns, "possible_tax", "possible_tax"),
            self._sum_expr(location_columns, "rgo_employed", "rgo_employed"),
            self._sum_expr(location_columns, "unemployed_total", "unemployed_pops"),
            *[self._sum_expr(location_columns, column, column) for column in pop_columns],
        ]
        location_overview = self._query(
            f"""
            SELECT
              {self._snapshot_select("l")},
              {", ".join(location_exprs)}
            FROM {locations} AS l
            {where}
            GROUP BY {self._snapshot_group_by("l")}
            ORDER BY l.date_sort
            """
        )
        if location_overview.is_empty():
            return _empty_overview()

        building_employment = self._building_employment(
            playthrough_id,
            from_date_sort,
            to_date_sort,
        )
        if (
            building_employment.height
            and "snapshot_id" in location_overview.columns
            and "snapshot_id" in building_employment.columns
        ):
            location_overview = location_overview.join(
                building_employment,
                on="snapshot_id",
                how="left",
            )
        if "building_employed" not in location_overview.columns:
            location_overview = location_overview.with_columns(
                pl.lit(0.0).alias("building_employed")
            )
        location_overview = location_overview.with_columns(
            pl.col("building_employed").fill_null(0.0)
        )

        pops_by_type: list[dict[str, Any]] = []
        for row in location_overview.to_dicts():
            for column in pop_columns:
                value = _number(row.get(column))
                if value == 0:
                    continue
                pops_by_type.append(
                    {
                        **_snapshot_values(row),
                        "pop_type": column.removeprefix("population_"),
                        "value": value,
                    }
                )

        food = self._food_overview(playthrough_id, from_date_sort, to_date_sort)
        return {
            "pops_by_type": pops_by_type,
            "employment": [
                {
                    **_snapshot_values(row),
                    "total_pops": row.get("total_pops"),
                    "employed_pops": _number(row.get("rgo_employed"))
                    + _number(row.get("building_employed")),
                    "unemployed_pops": row.get("unemployed_pops"),
                }
                for row in location_overview.to_dicts()
            ],
            "development": [
                {**_snapshot_values(row), "development": row.get("development")}
                for row in location_overview.to_dicts()
            ],
            "tax": [
                {
                    **_snapshot_values(row),
                    "collected_tax": row.get("collected_tax"),
                    "uncollected_tax": max(
                        _number(row.get("possible_tax")) - _number(row.get("collected_tax")), 0.0
                    ),
                    "possible_tax": row.get("possible_tax"),
                }
                for row in location_overview.to_dicts()
            ],
            "food": food.to_dicts(),
        }

    def explorer_metadata(self) -> dict[str, list[dict[str, Any]] | list[str]]:
        self._refresh_if_manifest_changed()
        return {
            "domains": [
                {"label": "Population", "value": "population"},
                {"label": "Goods", "value": "goods"},
                {"label": "Food", "value": "food"},
                {"label": "Buildings", "value": "buildings"},
                {"label": "Production Methods", "value": "methods"},
            ],
            "metrics": _metrics(),
            "scopes": _scopes(),
            "dimensions": _dimensions(),
            "aggregations": ["sum", "mean", "median", "min", "max"],
        }

    def template_metadata(self) -> dict[str, Any]:
        self._refresh_if_manifest_changed()
        scopes_by_key = {scope.key: scope for scope in _template_scopes()}
        metrics: list[dict[str, Any]] = []
        available_scope_keys: set[str] = set()
        for metric in _template_metrics():
            valid_scopes = [
                scope
                for scope in self._available_template_scopes(metric)
                if scope in scopes_by_key
            ]
            if not valid_scopes:
                continue
            available_scope_keys.update(valid_scopes)
            metrics.append(metric.to_dict(valid_scopes))
        if not metrics:
            for metric in _template_metrics():
                valid_scopes = [
                    scope for scope in metric.valid_scopes if scope in scopes_by_key
                ]
                available_scope_keys.update(valid_scopes)
                metrics.append(metric.to_dict(valid_scopes))
        scopes = [
            scope.to_dict()
            for scope in sorted(scopes_by_key.values(), key=lambda item: item.order)
            if scope.key in available_scope_keys
        ]
        return {
            "domains": _template_domains(),
            "metrics": metrics,
            "scopes": scopes,
            "scopeGroups": _scope_groups(scopes),
            "filters": _template_filters(),
            "defaultMetric": "population:pops",
            "defaultScope": "super_region",
            "limits": [5, 10, 20, 40],
        }

    def template_filter_options(
        self,
        filter_key: str,
        *,
        playthrough_id: str | None = None,
    ) -> list[dict[str, str]]:
        self._refresh_if_manifest_changed()
        playthrough_id = _blank_to_none(playthrough_id)
        cached = self._cached_filter_options(filter_key, playthrough_id=playthrough_id)
        if cached:
            return cached
        if filter_key == "pop_type":
            return [
                {
                    "label": column.removeprefix("population_").replace("_", " ").title(),
                    "value": column.removeprefix("population_"),
                }
                for column in POP_TOTAL_COLUMNS
                if column in self._columns("locations", playthrough_id)
            ]
        source_info = _filter_source(filter_key)
        if source_info is None:
            return []
        table, column = source_info
        source = self._table_sql(table, playthrough_id)
        if source is None or column not in self._columns(table, playthrough_id):
            return []
        rows = self._query(
            f"""
            SELECT DISTINCT CAST({column} AS VARCHAR) AS value
            FROM {source}
            WHERE {column} IS NOT NULL AND CAST({column} AS VARCHAR) != ''
            ORDER BY value
            LIMIT 5000
            """
        )
        return [{"label": str(row["value"]), "value": str(row["value"])} for row in rows.to_dicts()]

    def template_query(
        self,
        *,
        metric_key: str = "population:pops",
        scope: str = "super_region",
        limit: int = 5,
        filters: dict[str, str | None] | None = None,
        playthrough_id: str | None = None,
        from_date_sort: int | None = None,
        to_date_sort: int | None = None,
    ) -> TemplateQueryResult:
        self._refresh_if_manifest_changed()
        metric = _template_metric_by_key(metric_key)
        available_scopes = self._available_template_scopes(metric)
        if (
            scope not in available_scopes
            and scope in metric.valid_scopes
            and self._template_metric_scope_available(metric, scope)
        ):
            available_scopes = [*available_scopes, scope]
        if not available_scopes:
            available_scopes = [
                scope if scope in metric.valid_scopes else metric.default_scope
            ]
        if scope not in available_scopes:
            scope = (
                metric.default_scope
                if metric.default_scope in available_scopes
                else available_scopes[0]
            )
        playthrough_id = _blank_to_none(playthrough_id)
        from_date_sort, to_date_sort = self.normalize_date_range(
            playthrough_id=playthrough_id,
            from_date_sort=from_date_sort,
            to_date_sort=to_date_sort,
        )
        clean_filters = _clean_filters(filters)
        rows = self._cached_template_grouped_rows(
            metric,
            scope,
            playthrough_id=playthrough_id,
            from_date_sort=from_date_sort,
            to_date_sort=to_date_sort,
        )
        if rows is None or clean_filters:
            rows = self._template_grouped_rows(
                metric,
                scope,
                filters=clean_filters,
                playthrough_id=playthrough_id,
                from_date_sort=from_date_sort,
                to_date_sort=to_date_sort,
            )
        ranking = _template_ranking(rows)
        panels = _template_panels(rows, ranking, limit=max(1, int(limit)))
        scope_dict = _template_scope_by_key(scope).to_dict()
        date_label = self._date_range_label(
            playthrough_id=playthrough_id,
            from_date_sort=from_date_sort,
            to_date_sort=to_date_sort,
        )
        empty_message = None
        if rows.is_empty():
            empty_message = (
                f"No rows for {metric.label} grouped by {scope_dict['label']} "
                f"between {date_label}."
            )
        return TemplateQueryResult(
            panels=panels,
            ranking=ranking.to_dicts() if not ranking.is_empty() else [],
            metric=metric.to_dict(available_scopes),
            scope=scope_dict,
            chips=[
                metric.label,
                f"Group by {scope_dict['label']}",
                *_filter_chips(clean_filters, metadata_filters=_template_filters()),
                date_label,
                f"N={max(1, int(limit))}",
            ],
            empty_message=empty_message,
        )

    def _cached_overview(
        self,
        *,
        playthrough_id: str | None,
        from_date_sort: int | None,
        to_date_sort: int | None,
    ) -> dict[str, list[dict[str, Any]]] | None:
        if self._overview_cache.is_empty() or "series" not in self._overview_cache.columns:
            return None
        frame = self._filter_cached_frame(
            self._overview_cache,
            playthrough_id=playthrough_id,
            from_date_sort=from_date_sort,
            to_date_sort=to_date_sort,
        )
        output = _empty_overview()
        for cache_key, result_key in _OVERVIEW_SERIES_KEYS.items():
            rows = frame.filter(pl.col("series") == cache_key).drop("series").to_dicts()
            output[result_key] = rows
        if not output["pops_by_type"]:
            output["pops_by_type"] = self._cached_pops_by_type_overview(
                playthrough_id=playthrough_id,
                from_date_sort=from_date_sort,
                to_date_sort=to_date_sort,
            )
        return output

    def _cached_pops_by_type_overview(
        self,
        *,
        playthrough_id: str | None,
        from_date_sort: int | None,
        to_date_sort: int | None,
    ) -> list[dict[str, Any]]:
        if ("population", "pops", "pop_type") not in self._cache_available_pairs:
            return []
        frame = self._filter_cached_frame(
            self._explorer_cache,
            playthrough_id=playthrough_id,
            from_date_sort=from_date_sort,
            to_date_sort=to_date_sort,
        )
        frame = frame.filter(
            (pl.col("domain") == "population")
            & (pl.col("metric") == "pops")
            & (pl.col("dimension") == "pop_type")
        )
        if frame.is_empty():
            return []
        keep = [
            column
            for column in [*SNAPSHOT_COLUMNS, "entity_key", "value"]
            if column in frame.columns
        ]
        return (
            frame.select(keep)
            .rename({"entity_key": "pop_type"})
            .sort(["date_sort", "pop_type"])
            .to_dicts()
        )

    def _available_template_scopes(self, metric: TemplateMetric) -> list[str]:
        available = [
            scope
            for scope in metric.valid_scopes
            if (metric.domain, metric.metric, scope) in self._cache_available_pairs
        ]
        if (
            metric.key == "population:pops"
            and "religion_name" in metric.valid_scopes
            and "religion_name" not in available
            and self._cached_filter_options("religion_name", playthrough_id=None)
        ):
            available.append("religion_name")
        if available:
            return available
        return [
            item
            for item in metric.valid_scopes
            if self._template_metric_scope_available(metric, item)
        ]

    def _cached_filter_options(
        self,
        filter_key: str,
        *,
        playthrough_id: str | None,
    ) -> list[dict[str, str]]:
        if self._filter_values_cache.is_empty():
            return []
        required = {"filter_key", "value", "label"}
        if not required.issubset(self._filter_values_cache.columns):
            return []
        frame = self._filter_values_cache.filter(pl.col("filter_key") == filter_key)
        if playthrough_id and "playthrough_id" in frame.columns:
            frame = frame.filter(
                pl.col("playthrough_id").is_null()
                | (pl.col("playthrough_id") == playthrough_id)
            )
        if frame.is_empty():
            return []
        frame = frame.unique(subset=["value"], keep="first", maintain_order=True).sort("label")
        return [
            {"label": str(row.get("label") or row["value"]), "value": str(row["value"])}
            for row in frame.select(["label", "value"]).to_dicts()
        ]

    def _cached_template_grouped_rows(
        self,
        metric: TemplateMetric,
        scope: str,
        *,
        playthrough_id: str | None,
        from_date_sort: int | None,
        to_date_sort: int | None,
    ) -> pl.DataFrame | None:
        if (metric.domain, metric.metric, scope) not in self._cache_available_pairs:
            return None
        frame = self._filter_cached_frame(
            self._explorer_cache,
            playthrough_id=playthrough_id,
            from_date_sort=from_date_sort,
            to_date_sort=to_date_sort,
        )
        frame = frame.filter(
            (pl.col("domain") == metric.domain)
            & (pl.col("metric") == metric.metric)
            & (pl.col("dimension") == scope)
        )
        if frame.is_empty():
            return pl.DataFrame()
        keep = [
            column
            for column in [
                *SNAPSHOT_COLUMNS,
                "entity_key",
                "entity_label",
                "value",
            ]
            if column in frame.columns
        ]
        return frame.select(keep).sort(["date_sort", "entity_label"])

    def _filter_cached_frame(
        self,
        frame: pl.DataFrame,
        *,
        playthrough_id: str | None,
        from_date_sort: int | None,
        to_date_sort: int | None,
    ) -> pl.DataFrame:
        output = frame
        if playthrough_id and "playthrough_id" in output.columns:
            output = output.filter(pl.col("playthrough_id") == playthrough_id)
        if from_date_sort is not None and "date_sort" in output.columns:
            output = output.filter(pl.col("date_sort") >= int(from_date_sort))
        if to_date_sort is not None and "date_sort" in output.columns:
            output = output.filter(pl.col("date_sort") <= int(to_date_sort))
        return output

    def explorer_query(
        self,
        *,
        domain: str = "population",
        metric: str = "pops",
        dimension: str = "super_region",
        aggregation: str = "sum",
        rank: str = "top",
        limit: int = 5,
        playthrough_id: str | None = None,
        from_date_sort: int | None = None,
        to_date_sort: int | None = None,
    ) -> DashboardQueryResult:
        self._refresh_if_manifest_changed()
        playthrough_id = _blank_to_none(playthrough_id)
        metric_def = _metric_by_key(domain, metric)
        dimension_def = _dimension_by_key(dimension)
        grouped = self._explorer_grouped_rows(
            domain=domain,
            metric=metric,
            dimension=dimension,
            playthrough_id=playthrough_id,
            from_date_sort=from_date_sort,
            to_date_sort=to_date_sort,
        )
        if grouped.is_empty():
            return DashboardQueryResult([], [], metric_def, dimension_def)

        ranking = _rank_entities(grouped, aggregation=aggregation, rank=rank, limit=limit)
        selected = {row["entity_key"] for row in ranking}
        rows = (
            grouped.filter(pl.col("entity_key").is_in(selected))
            .sort(["date_sort", "entity_label"])
            .to_dicts()
        )
        return DashboardQueryResult(rows, ranking, metric_def, dimension_def)

    def building_references(
        self,
        *,
        limit: int = 200,
        search: str | None = None,
    ) -> list[dict[str, Any]]:
        self._refresh_if_manifest_changed()
        data = load_building_data(profile=self.profile, load_order_path=self.load_order_path)
        rows = data.buildings.sort("name").to_dicts()
        if search:
            needle = search.lower()
            rows = [
                row
                for row in rows
                if needle in str(row.get("name") or "").lower()
                or needle in str(row.get("category") or "").lower()
                or needle in str(row.get("pop_type") or "").lower()
            ]
        resolver = BuildingIconResolver(
            profile=self.profile,
            load_order_path=self.load_order_path,
            asset_root=self.asset_root,
        )
        output: list[dict[str, Any]] = []
        for row in rows[:limit]:
            icon_url = resolver.icon_url(row)
            output.append(
                {
                    "name": row.get("name"),
                    "category": row.get("category"),
                    "effective_price_gold": row.get("effective_price_gold"),
                    "price_kind": row.get("price_kind"),
                    "pop_type": row.get("pop_type"),
                    "employment_size": row.get("employment_size"),
                    "icon_url": icon_url,
                }
            )
        return output

    def _explorer_grouped_rows(
        self,
        *,
        domain: str,
        metric: str,
        dimension: str,
        filters: dict[str, str] | None = None,
        playthrough_id: str | None,
        from_date_sort: int | None,
        to_date_sort: int | None,
    ) -> pl.DataFrame:
        if domain == "population":
            return self._population_rows(
                metric,
                dimension,
                playthrough_id,
                from_date_sort,
                to_date_sort,
                filters=filters,
            )
        if domain == "goods":
            return self._simple_rows(
                "market_goods",
                metric,
                dimension,
                _GOODS_METRICS,
                playthrough_id,
                from_date_sort,
                to_date_sort,
                filters=filters,
            )
        if domain == "food":
            return self._simple_rows(
                "market_food",
                metric,
                dimension,
                _FOOD_METRICS,
                playthrough_id,
                from_date_sort,
                to_date_sort,
                filters=filters,
            )
        if domain == "buildings":
            return self._joined_location_rows(
                "buildings",
                metric,
                dimension,
                _BUILDING_METRICS,
                playthrough_id,
                from_date_sort,
                to_date_sort,
                filters=filters,
            )
        if domain == "methods":
            return self._joined_location_rows(
                "building_methods",
                metric,
                dimension,
                _METHOD_METRICS,
                playthrough_id,
                from_date_sort,
                to_date_sort,
                filters=filters,
            )
        return pl.DataFrame()

    def _template_grouped_rows(
        self,
        metric: TemplateMetric,
        scope: str,
        *,
        filters: dict[str, str] | None = None,
        playthrough_id: str | None,
        from_date_sort: int | None,
        to_date_sort: int | None,
    ) -> pl.DataFrame:
        if metric.key == "population:pops" and scope == "pop_type":
            return self._population_pop_type_rows(
                playthrough_id,
                from_date_sort,
                to_date_sort,
                filters=filters,
            )
        if metric.key == "population:pops" and scope == "religion_name":
            return self._simple_rows(
                "population",
                "pops",
                "religion_name",
                {"pops": ("size", "sum")},
                playthrough_id,
                from_date_sort,
                to_date_sort,
                filters=filters,
            )
        return self._explorer_grouped_rows(
            domain=metric.domain,
            metric=metric.metric,
            dimension=scope,
            filters=filters,
            playthrough_id=playthrough_id,
            from_date_sort=from_date_sort,
            to_date_sort=to_date_sort,
        )

    def _population_pop_type_rows(
        self,
        playthrough_id: str | None,
        from_date_sort: int | None,
        to_date_sort: int | None,
        *,
        filters: dict[str, str] | None = None,
    ) -> pl.DataFrame:
        source = self._table_sql("locations", playthrough_id)
        if source is None:
            return pl.DataFrame()
        columns = self._columns("locations", playthrough_id)
        pop_columns = [column for column in POP_TOTAL_COLUMNS if column in columns]
        if not pop_columns:
            return pl.DataFrame()
        where = self._date_where("l", from_date_sort, to_date_sort)
        queries = []
        selected_pop_type = (filters or {}).get("pop_type")
        for column in pop_columns:
            pop_type = column.removeprefix("population_")
            if selected_pop_type and selected_pop_type != pop_type:
                continue
            label = pop_type.replace("_", " ").title()
            queries.append(
                f"""
                SELECT
                  {self._snapshot_select("l")},
                  {_sql_string(pop_type)} AS entity_key,
                  {_sql_string(label)} AS entity_label,
                  COALESCE(SUM(l.{column}), 0) AS value
                FROM {source} AS l
                {where}
                GROUP BY {self._snapshot_group_by("l")}
                """
            )
        if not queries:
            return pl.DataFrame()
        return self._query(" UNION ALL ".join(queries))

    def _template_metric_scope_available(self, metric: TemplateMetric, scope: str) -> bool:
        if scope == "global":
            return True
        if scope == "religion_name":
            return metric.key == "population:pops" and self._population_religion_available()
        if scope == "pop_type":
            return metric.key == "population:pops" and self._population_pop_type_available()
        if metric.domain == "population":
            return self._scope_column_available("locations", scope)
        if metric.domain == "goods":
            return self._scope_column_available("market_goods", scope)
        if metric.domain == "food":
            return self._scope_column_available("market_food", scope)
        if metric.domain == "buildings":
            return self._scope_column_available(
                "buildings", scope
            ) or self._scope_column_available("locations", scope)
        if metric.domain == "methods":
            return self._scope_column_available(
                "building_methods", scope
            ) or self._scope_column_available("locations", scope)
        return False

    def _scope_column_available(self, table: str, column: str) -> bool:
        columns = self._columns(table)
        if column not in columns:
            return False
        return self._column_has_non_empty_value(table, column)

    def _population_pop_type_available(self) -> bool:
        columns = self._columns("locations")
        return any(column in columns for column in POP_TOTAL_COLUMNS)

    def _population_religion_available(self) -> bool:
        source = self._table_sql("population")
        if source is None:
            return False
        columns = self._columns("population")
        if not {"religion_name", "size"}.issubset(columns):
            return False
        count = self._query(f"SELECT COUNT(*) AS row_count FROM {source} WHERE size IS NOT NULL")
        return bool(count.height and count.item(0, "row_count"))

    def _date_range_label(
        self,
        *,
        playthrough_id: str | None,
        from_date_sort: int | None,
        to_date_sort: int | None,
    ) -> str:
        snapshots = self.snapshots(playthrough_id)
        if not snapshots:
            return "No dates"
        by_sort = {row.get("date_sort"): row.get("date") for row in snapshots}
        from_label = by_sort.get(from_date_sort) if from_date_sort is not None else None
        to_label = by_sort.get(to_date_sort) if to_date_sort is not None else None
        return f"{from_label or snapshots[0].get('date')} - {to_label or snapshots[-1].get('date')}"

    def _population_rows(
        self,
        metric: str,
        dimension: str,
        playthrough_id: str | None,
        from_date_sort: int | None,
        to_date_sort: int | None,
        *,
        filters: dict[str, str] | None = None,
    ) -> pl.DataFrame:
        if metric == "pops" and (filters or {}).get("pop_type") and dimension != "pop_type":
            return self._population_filtered_pop_type_rows(
                dimension,
                str((filters or {})["pop_type"]),
                playthrough_id,
                from_date_sort,
                to_date_sort,
            )
        if metric == "employed":
            locations = self._simple_rows(
                "locations",
                "employed",
                dimension,
                {"employed": ("rgo_employed", "sum")},
                playthrough_id,
                from_date_sort,
                to_date_sort,
                filters=filters,
            )
            buildings = self._joined_location_rows(
                "buildings",
                "employed",
                dimension,
                {"employed": ("employed", "sum")},
                playthrough_id,
                from_date_sort,
                to_date_sort,
                filters=filters,
            )
            return _merge_grouped_values(locations, buildings)
        if metric == "uncollected_tax":
            collected = self._population_rows(
                "collected_tax",
                dimension,
                playthrough_id,
                from_date_sort,
                to_date_sort,
                filters=filters,
            )
            possible = self._population_rows(
                "possible_tax",
                dimension,
                playthrough_id,
                from_date_sort,
                to_date_sort,
                filters=filters,
            )
            return _derive_uncollected_tax(collected, possible)
        return self._simple_rows(
            "locations",
            metric,
            dimension,
            _POPULATION_METRICS,
            playthrough_id,
            from_date_sort,
            to_date_sort,
            filters=filters,
        )

    def _population_filtered_pop_type_rows(
        self,
        dimension: str,
        pop_type: str,
        playthrough_id: str | None,
        from_date_sort: int | None,
        to_date_sort: int | None,
    ) -> pl.DataFrame:
        source = self._table_sql("locations", playthrough_id)
        if source is None:
            return pl.DataFrame()
        columns = self._columns("locations", playthrough_id)
        value_column = f"population_{pop_type}"
        if value_column not in columns:
            return pl.DataFrame()
        dimension_sql = self._dimension_sql(dimension, "t", columns)
        if dimension_sql is None:
            return pl.DataFrame()
        where = self._date_where("t", from_date_sort, to_date_sort)
        return self._query(
            f"""
            SELECT
              {self._snapshot_select("t")},
              {dimension_sql[0]} AS entity_key,
              {dimension_sql[1]} AS entity_label,
              COALESCE(SUM(t.{value_column}), 0) AS value
            FROM {source} AS t
            {where}
            GROUP BY {self._snapshot_group_by("t")}, entity_key, entity_label
            ORDER BY t.date_sort, entity_label
            """
        )

    def _simple_rows(
        self,
        table: str,
        metric: str,
        dimension: str,
        metric_map: dict[str, tuple[str, str]],
        playthrough_id: str | None,
        from_date_sort: int | None,
        to_date_sort: int | None,
        *,
        filters: dict[str, str] | None = None,
    ) -> pl.DataFrame:
        source = self._table_sql(table, playthrough_id)
        if source is None or metric not in metric_map:
            return pl.DataFrame()
        columns = self._columns(table, playthrough_id)
        value_column, aggregator = metric_map[metric]
        if value_column not in columns and aggregator != "count":
            return pl.DataFrame()
        dimension_sql = self._dimension_sql(dimension, "t", columns)
        if dimension_sql is None:
            return pl.DataFrame()
        where = self._where_clause(
            "t",
            columns,
            from_date_sort,
            to_date_sort,
            filters=filters,
        )
        value_expr = self._aggregate_sql(value_column, aggregator, columns)
        return self._query(
            f"""
            SELECT
              {self._snapshot_select("t")},
              {dimension_sql[0]} AS entity_key,
              {dimension_sql[1]} AS entity_label,
              {value_expr} AS value
            FROM {source} AS t
            {where}
            GROUP BY {self._snapshot_group_by("t")}, entity_key, entity_label
            ORDER BY t.date_sort, entity_label
            """
        )

    def _joined_location_rows(
        self,
        table: str,
        metric: str,
        dimension: str,
        metric_map: dict[str, tuple[str, str]],
        playthrough_id: str | None,
        from_date_sort: int | None,
        to_date_sort: int | None,
        *,
        filters: dict[str, str] | None = None,
    ) -> pl.DataFrame:
        source = self._table_sql(table, playthrough_id)
        locations = self._table_sql("locations", playthrough_id)
        if source is None or locations is None or metric not in metric_map:
            return pl.DataFrame()
        source_columns = self._columns(table, playthrough_id)
        location_columns = self._columns("locations", playthrough_id)
        columns = source_columns | location_columns
        value_column, aggregator = metric_map[metric]
        dimension_sql = self._dimension_sql(
            dimension,
            "x",
            source_columns,
            fallback_alias="l",
            fallback_columns=location_columns,
        )
        if dimension_sql is None:
            return pl.DataFrame()
        where = self._where_clause(
            "x",
            source_columns,
            from_date_sort,
            to_date_sort,
            filters=filters,
            fallback_alias="l",
            fallback_columns=location_columns,
        )
        value_expr = self._aggregate_sql(value_column, aggregator, columns, alias="x")
        return self._query(
            f"""
            SELECT
              {self._snapshot_select("x")},
              {dimension_sql[0]} AS entity_key,
              {dimension_sql[1]} AS entity_label,
              {value_expr} AS value
            FROM {source} AS x
            LEFT JOIN {locations} AS l
              ON x.snapshot_id = l.snapshot_id AND x.location_id = l.location_id
            {where}
            GROUP BY {self._snapshot_group_by("x")}, entity_key, entity_label
            ORDER BY x.date_sort, entity_label
            """
        )

    def _building_employment(
        self,
        playthrough_id: str | None,
        from_date_sort: int | None,
        to_date_sort: int | None,
    ) -> pl.DataFrame:
        buildings = self._table_sql("buildings", playthrough_id)
        if buildings is None:
            return pl.DataFrame({"snapshot_id": [], "building_employed": []})
        columns = self._columns("buildings", playthrough_id)
        if "employed" not in columns:
            return pl.DataFrame({"snapshot_id": [], "building_employed": []})
        where = self._date_where("b", from_date_sort, to_date_sort)
        return self._query(
            f"""
            SELECT b.snapshot_id, COALESCE(SUM(b.employed), 0) AS building_employed
            FROM {buildings} AS b
            {where}
            GROUP BY b.snapshot_id
            """
        )

    def _food_overview(
        self,
        playthrough_id: str | None,
        from_date_sort: int | None,
        to_date_sort: int | None,
    ) -> pl.DataFrame:
        food = self._table_sql("market_food", playthrough_id)
        if food is None:
            return pl.DataFrame()
        columns = self._columns("market_food", playthrough_id)
        exprs = [
            self._sum_expr(columns, "food", "food"),
            self._sum_expr(columns, "food_max", "food_max"),
            self._sum_expr(columns, "food_supply", "food_supply"),
            self._sum_expr(columns, "food_consumption", "food_consumption"),
            self._sum_expr(columns, "food_balance", "food_balance"),
            self._sum_expr(columns, "missing", "missing"),
        ]
        where = self._date_where("f", from_date_sort, to_date_sort)
        return self._query(
            f"""
            SELECT
              {self._snapshot_select("f")},
              {", ".join(exprs)}
            FROM {food} AS f
            {where}
            GROUP BY {self._snapshot_group_by("f")}
            ORDER BY f.date_sort
            """
        )

    def _columns(self, table: str, playthrough_id: str | None = None) -> set[str]:
        source = self._table_sql(table, playthrough_id)
        if source is None:
            return set()
        try:
            with duckdb.connect(database=":memory:") as connection:
                result = connection.execute(f"SELECT * FROM {source} LIMIT 0")
                return {column[0] for column in result.description or []}
        except duckdb.Error:
            return set()

    def _column_has_non_empty_value(
        self,
        table: str,
        column: str,
        playthrough_id: str | None = None,
    ) -> bool:
        source = self._table_sql(table, playthrough_id)
        if source is None or column not in self._columns(table, playthrough_id):
            return False
        try:
            with duckdb.connect(database=":memory:") as connection:
                result = connection.execute(
                    f"""
                    SELECT 1
                    FROM {source}
                    WHERE {column} IS NOT NULL AND CAST({column} AS VARCHAR) != ''
                    LIMIT 1
                    """
                )
                return result.fetchone() is not None
        except duckdb.Error:
            return False

    def _table_sql(self, table: str, playthrough_id: str | None = None) -> str | None:
        files = self.dataset.table_files(table, playthrough_id=playthrough_id)
        if not files:
            return None
        quoted = ", ".join(_sql_string(_path_for_duckdb(path)) for path in files)
        return f"read_parquet([{quoted}], union_by_name=true, hive_partitioning=false)"

    def _query(self, sql: str) -> pl.DataFrame:
        with duckdb.connect(database=":memory:") as connection:
            result = connection.execute(sql)
            columns = [column[0] for column in result.description or []]
            rows = result.fetchall()
            if not columns:
                return pl.DataFrame()
            if not rows:
                return pl.DataFrame(schema={column: pl.Null for column in columns})
            return pl.DataFrame(rows, schema=columns, orient="row", infer_schema_length=None)

    def _sum_expr(self, columns: set[str], column: str, alias: str | None = None) -> str:
        alias = alias or column
        if column not in columns:
            return f"CAST(0 AS DOUBLE) AS {alias}"
        return f"COALESCE(SUM({column}), 0) AS {alias}"

    def _aggregate_sql(
        self,
        column: str,
        aggregator: str,
        columns: set[str],
        *,
        alias: str = "t",
    ) -> str:
        if aggregator == "count":
            return "COUNT(*)"
        target = f"{alias}.{column}"
        if column not in columns:
            return "CAST(0 AS DOUBLE)"
        if aggregator == "n_unique":
            return f"COUNT(DISTINCT {target})"
        if aggregator == "mean":
            return f"AVG({target})"
        return f"COALESCE(SUM({target}), 0)"

    def _dimension_sql(
        self,
        dimension: str,
        alias: str,
        columns: set[str],
        *,
        fallback_alias: str | None = None,
        fallback_columns: set[str] | None = None,
    ) -> tuple[str, str] | None:
        if dimension == "global":
            return ("'world'", "'World'")
        if dimension in columns:
            expr = f"CAST({alias}.{dimension} AS VARCHAR)"
            return (
                f"COALESCE(NULLIF({expr}, ''), 'unknown')",
                f"COALESCE(NULLIF({expr}, ''), 'Unknown')",
            )
        if fallback_alias and fallback_columns and dimension in fallback_columns:
            expr = f"CAST({fallback_alias}.{dimension} AS VARCHAR)"
            return (
                f"COALESCE(NULLIF({expr}, ''), 'unknown')",
                f"COALESCE(NULLIF({expr}, ''), 'Unknown')",
            )
        return None

    def _date_where(
        self,
        alias: str,
        from_date_sort: int | None,
        to_date_sort: int | None,
    ) -> str:
        clauses = self._date_clauses(alias, from_date_sort, to_date_sort)
        return "" if not clauses else "WHERE " + " AND ".join(clauses)

    def _where_clause(
        self,
        alias: str,
        columns: set[str],
        from_date_sort: int | None,
        to_date_sort: int | None,
        *,
        filters: dict[str, str] | None = None,
        fallback_alias: str | None = None,
        fallback_columns: set[str] | None = None,
    ) -> str:
        clauses = self._date_clauses(alias, from_date_sort, to_date_sort)
        for key, value in (filters or {}).items():
            if not value:
                continue
            if key in columns:
                clauses.append(f"CAST({alias}.{key} AS VARCHAR) = {_sql_string(str(value))}")
                continue
            if fallback_alias and fallback_columns and key in fallback_columns:
                clauses.append(
                    f"CAST({fallback_alias}.{key} AS VARCHAR) = {_sql_string(str(value))}"
                )
        return "" if not clauses else "WHERE " + " AND ".join(clauses)

    def _date_clauses(
        self,
        alias: str,
        from_date_sort: int | None,
        to_date_sort: int | None,
    ) -> list[str]:
        clauses = []
        if from_date_sort is not None:
            clauses.append(f"{alias}.date_sort >= {int(from_date_sort)}")
        if to_date_sort is not None:
            clauses.append(f"{alias}.date_sort <= {int(to_date_sort)}")
        return clauses

    def _snapshot_select(self, alias: str) -> str:
        return ", ".join(f"{alias}.{column}" for column in SNAPSHOT_COLUMNS)

    def _snapshot_group_by(self, alias: str) -> str:
        return ", ".join(f"{alias}.{column}" for column in SNAPSHOT_COLUMNS)


class BuildingIconResolver:
    def __init__(
        self,
        *,
        profile: str = "merged_default",
        load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
        asset_root: str | Path,
    ) -> None:
        self.profile = profile
        self.load_order_path = Path(load_order_path)
        self.asset_root = Path(asset_root)

    def icon_url(self, building: dict[str, Any]) -> str | None:
        from eu5gameparser.graphs.goods_flow import _building_icon_source, _profile_game_roots

        roots = _profile_game_roots(self.profile, self.load_order_path)
        source = _building_icon_source(building, profile_roots=roots)
        if source is None or not source.is_file():
            return None
        cache_dir = self.asset_root / "building_icons"
        cache_dir.mkdir(parents=True, exist_ok=True)
        target = cache_dir / f"{source.stem}.png"
        suffix = source.suffix.lower()
        if suffix == ".dds":
            if not target.exists() or source.stat().st_mtime > target.stat().st_mtime:
                if not _convert_dds_preview(source, target):
                    return None
        elif suffix in {".png", ".jpg", ".jpeg", ".webp", ".gif"}:
            target = cache_dir / f"{source.stem}{source.suffix.lower()}"
            if not target.exists() or source.stat().st_mtime > target.stat().st_mtime:
                shutil.copy2(source, target)
        else:
            return None
        try:
            relative = target.resolve().relative_to(self.asset_root.resolve())
        except ValueError:
            return None
        return f"/assets/{relative.as_posix()}"


def build_building_icon_lookup(
    data: BuildingData,
    *,
    profile: str = "merged_default",
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
    asset_root: str | Path,
) -> dict[str, str | None]:
    resolver = BuildingIconResolver(
        profile=profile,
        load_order_path=load_order_path,
        asset_root=asset_root,
    )
    return {row["name"]: resolver.icon_url(row) for row in data.buildings.to_dicts()}


_POPULATION_METRICS = {
    "pops": ("total_population", "sum"),
    "unemployed": ("unemployed_total", "sum"),
    "development": ("development", "sum"),
    "collected_tax": ("tax", "sum"),
    "possible_tax": ("possible_tax", "sum"),
}
_GOODS_METRICS = {
    "supply": ("supply", "sum"),
    "demand": ("demand", "sum"),
    "net": ("net", "sum"),
    "stockpile": ("stockpile", "sum"),
    "avg_price": ("price", "mean"),
    "production_supply": ("supplied_Production", "sum"),
    "building_demand": ("demanded_Building", "sum"),
}
_FOOD_METRICS = {
    "food": ("food", "sum"),
    "capacity": ("food_max", "sum"),
    "supply": ("food_supply", "sum"),
    "demand": ("food_consumption", "sum"),
    "balance": ("food_balance", "sum"),
    "missing_food": ("missing", "sum"),
    "fill_percent": ("food_fill_percent", "mean"),
    "months_of_food": ("months_of_food", "mean"),
}
_BUILDING_METRICS = {
    "building_count": ("building_id", "count"),
    "level_sum": ("level", "sum"),
    "employed": ("employed", "sum"),
    "employment_capacity": ("employment", "sum"),
    "profit": ("last_months_profit", "sum"),
}
_METHOD_METRICS = {
    "method_count": ("production_method", "count"),
    "building_count": ("building_id", "n_unique"),
}


def _metrics() -> list[dict[str, str]]:
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


def _metric(domain: str, key: str, label: str, unit: str, formatter: str) -> dict[str, str]:
    return {"domain": domain, "key": key, "label": label, "unit": unit, "formatter": formatter}


def _scopes() -> list[dict[str, str]]:
    return [
        {"label": "World", "value": "world"},
        {"label": "Geography", "value": "geography"},
        {"label": "Political", "value": "political"},
        {"label": "Markets", "value": "markets"},
        {"label": "Goods", "value": "goods"},
        {"label": "Production", "value": "production"},
        {"label": "Population", "value": "population"},
    ]


def _dimensions() -> list[dict[str, Any]]:
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


def _dimension(key: str, label: str, scope: str, order: int) -> dict[str, Any]:
    return {"key": key, "label": label, "scope": scope, "order": order}


def _template_metrics() -> list[TemplateMetric]:
    world = ("global",)
    geography = ("super_region", "macro_region", "region", "area", "province_slug")
    political = ("country_tag",)
    goods = (*world, "good_id", "goods_category", "goods_designation", "market_center_slug")
    market = (*world, "market_center_slug")
    production = (*world, "building_type")
    methods = (*world, "production_method", "building_type", "country_tag")
    population_scopes = (*world, *geography, *political, "pop_type", "religion_name")
    location_scopes = (*world, *geography, *political)
    return [
        TemplateMetric(
            "population:pops",
            "population",
            "pops",
            "Pops",
            "Pops",
            "whole",
            population_scopes,
            "super_region",
        ),
        TemplateMetric(
            "population:employed",
            "population",
            "employed",
            "Employed",
            "Pops",
            "whole",
            location_scopes,
            "super_region",
        ),
        TemplateMetric(
            "population:unemployed",
            "population",
            "unemployed",
            "Unemployed",
            "Pops",
            "whole",
            location_scopes,
            "super_region",
        ),
        TemplateMetric(
            "population:development",
            "population",
            "development",
            "Development",
            "Development",
            "whole",
            location_scopes,
            "super_region",
        ),
        TemplateMetric(
            "population:collected_tax",
            "population",
            "collected_tax",
            "Collected Tax",
            "Gold",
            "money",
            location_scopes,
            "super_region",
        ),
        TemplateMetric(
            "population:uncollected_tax",
            "population",
            "uncollected_tax",
            "Uncollected Tax",
            "Gold",
            "money",
            location_scopes,
            "super_region",
        ),
        TemplateMetric(
            "goods:supply",
            "goods",
            "supply",
            "Goods Supply",
            "Goods",
            "decimal",
            goods,
            "good_id",
        ),
        TemplateMetric(
            "goods:demand",
            "goods",
            "demand",
            "Goods Demand",
            "Goods",
            "decimal",
            goods,
            "good_id",
        ),
        TemplateMetric(
            "goods:net",
            "goods",
            "net",
            "Goods Net",
            "Goods",
            "decimal",
            goods,
            "good_id",
        ),
        TemplateMetric(
            "goods:avg_price",
            "goods",
            "avg_price",
            "Average Price",
            "Gold",
            "money",
            goods,
            "good_id",
        ),
        TemplateMetric(
            "food:food",
            "food",
            "food",
            "Food Stockpile",
            "Food",
            "whole",
            market,
            "market_center_slug",
        ),
        TemplateMetric(
            "food:balance",
            "food",
            "balance",
            "Food Balance",
            "Food/month",
            "decimal",
            market,
            "market_center_slug",
        ),
        TemplateMetric(
            "buildings:building_count",
            "buildings",
            "building_count",
            "Building Count",
            "Buildings",
            "whole",
            (*production, *location_scopes),
            "building_type",
        ),
        TemplateMetric(
            "buildings:level_sum",
            "buildings",
            "level_sum",
            "Building Levels",
            "Levels",
            "whole",
            (*production, *location_scopes),
            "building_type",
        ),
        TemplateMetric(
            "buildings:profit",
            "buildings",
            "profit",
            "Building Profit",
            "Gold",
            "money",
            (*production, *location_scopes),
            "building_type",
        ),
        TemplateMetric(
            "methods:method_count",
            "methods",
            "method_count",
            "Production Method Uses",
            "Uses",
            "whole",
            methods,
            "production_method",
        ),
    ]


def _template_scopes() -> list[TemplateScope]:
    return [
        TemplateScope("global", "World", "World", 0),
        TemplateScope("super_region", "Super Region", "Geography", 10),
        TemplateScope("macro_region", "Macro Region", "Geography", 20),
        TemplateScope("region", "Region", "Geography", 30),
        TemplateScope("area", "Area", "Geography", 40),
        TemplateScope("province_slug", "Province", "Geography", 50),
        TemplateScope("country_tag", "Country", "Political", 60),
        TemplateScope("pop_type", "Pop Type", "Population", 70),
        TemplateScope("religion_name", "Religion", "Population", 80),
        TemplateScope("market_center_slug", "Market", "Markets", 90),
        TemplateScope("goods_category", "Goods Category", "Goods", 100),
        TemplateScope("goods_designation", "Goods Designation", "Goods", 110),
        TemplateScope("good_id", "Good", "Goods", 120),
        TemplateScope("building_type", "Building", "Production", 130),
        TemplateScope("production_method", "Production Method", "Production", 140),
    ]


def _template_metric_by_key(key: str) -> TemplateMetric:
    for metric in _template_metrics():
        if metric.key == key:
            return metric
    raise ValueError(f"Unknown template metric: {key}")


def _template_scope_by_key(key: str) -> TemplateScope:
    for scope in _template_scopes():
        if scope.key == key:
            return scope
    raise ValueError(f"Unknown template scope: {key}")


def _template_domains() -> list[dict[str, str]]:
    return [
        {"label": "Population", "value": "population"},
        {"label": "Goods", "value": "goods"},
        {"label": "Food", "value": "food"},
        {"label": "Buildings", "value": "buildings"},
        {"label": "Production Methods", "value": "methods"},
    ]


def _scope_groups(scopes: list[dict[str, Any]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    output: list[dict[str, str]] = []
    for scope in scopes:
        group = str(scope["group"])
        if group in seen:
            continue
        seen.add(group)
        output.append({"label": group, "value": group})
    return output


def _template_filters() -> list[dict[str, Any]]:
    return [
        _template_filter("good_id", "Good", ("goods",)),
        _template_filter("goods_category", "Goods Category", ("goods",)),
        _template_filter("goods_designation", "Goods Designation", ("goods",)),
        _template_filter("market_center_slug", "Market", ("goods", "food")),
        _template_filter("building_type", "Building", ("buildings", "methods")),
        _template_filter("production_method", "Production Method", ("methods",)),
        _template_filter("country_tag", "Country", ("population", "buildings", "methods")),
        _template_filter("pop_type", "Pop Type", ("population",)),
        _template_filter("religion_name", "Religion", ("population",)),
    ]


def _template_filter(key: str, label: str, domains: tuple[str, ...]) -> dict[str, Any]:
    return {"key": key, "label": label, "domains": list(domains)}


def _filter_source(filter_key: str) -> tuple[str, str] | None:
    sources = {
        "good_id": ("market_goods", "good_id"),
        "goods_category": ("market_goods", "goods_category"),
        "goods_designation": ("market_goods", "goods_designation"),
        "market_center_slug": ("market_goods", "market_center_slug"),
        "building_type": ("buildings", "building_type"),
        "production_method": ("building_methods", "production_method"),
        "country_tag": ("locations", "country_tag"),
        "religion_name": ("population", "religion_name"),
    }
    return sources.get(filter_key)


def _clean_filters(filters: dict[str, str | None] | None) -> dict[str, str]:
    return {key: str(value) for key, value in (filters or {}).items() if value not in {None, ""}}


def _filter_chips(
    filters: dict[str, str],
    *,
    metadata_filters: list[dict[str, Any]],
) -> list[str]:
    labels = {item["key"]: item["label"] for item in metadata_filters}
    return [f"{labels.get(key, key)}: {value}" for key, value in filters.items()]


def _metric_by_key(domain: str, metric: str) -> dict[str, str]:
    for item in _metrics():
        if item["domain"] == domain and item["key"] == metric:
            return item
    return _metric(domain, metric, metric, "", "decimal")


def _dimension_by_key(dimension: str) -> dict[str, Any]:
    for item in _dimensions():
        if item["key"] == dimension:
            return item
    return _dimension(dimension, dimension, "unknown", 0)


def _rank_entities(
    rows: pl.DataFrame,
    *,
    aggregation: str,
    rank: str,
    limit: int,
) -> list[dict[str, Any]]:
    if rows.is_empty():
        return []
    grouped = rows.group_by(["entity_key", "entity_label"]).agg(
        [
            _aggregation_expr(aggregation).alias("aggregate"),
            pl.col("value").sort_by("date_sort").first().alias("first"),
            pl.col("value").sort_by("date_sort").last().alias("last"),
            pl.col("value").min().alias("min"),
            pl.col("value").mean().alias("mean"),
            pl.col("value").median().alias("median"),
            pl.col("value").max().alias("max"),
        ]
    )
    grouped = grouped.with_columns((pl.col("last") - pl.col("first")).alias("delta"))
    descending = rank != "bottom"
    return (
        grouped.sort("aggregate", descending=descending)
        .head(max(1, int(limit)))
        .to_dicts()
    )


def _template_ranking(rows: pl.DataFrame) -> pl.DataFrame:
    if rows.is_empty():
        return pl.DataFrame()
    stats = rows.group_by(["entity_key", "entity_label"]).agg(
        [
            pl.col("value").sum().alias("sum"),
            pl.col("value").mean().alias("mean"),
            pl.col("value").sort_by("date_sort").first().alias("first"),
            pl.col("value").sort_by("date_sort").last().alias("last"),
            pl.col("value").min().alias("min"),
            pl.col("value").max().alias("max"),
        ]
    )
    return stats.with_columns(
        [
            (pl.col("last") - pl.col("first")).alias("absolute_change"),
            pl.when(pl.col("first").abs() > 1e-9)
            .then(((pl.col("last") - pl.col("first")) / pl.col("first").abs()) * 100.0)
            .otherwise(None)
            .alias("percent_change"),
        ]
    ).sort("sum", descending=True)


def _template_panels(
    rows: pl.DataFrame,
    ranking: pl.DataFrame,
    *,
    limit: int,
) -> dict[str, dict[str, Any]]:
    panel_specs = [
        ("top_sum", "Top N by Sum", "sum", True),
        ("bottom_sum", "Bottom N by Sum", "sum", False),
        ("top_mean", "Top N by Mean", "mean", True),
        ("bottom_mean", "Bottom N by Mean", "mean", False),
        ("top_change", "Top N by Absolute Change", "absolute_change", True),
        ("bottom_change", "Bottom N by Absolute Change", "absolute_change", False),
    ]
    if rows.is_empty() or ranking.is_empty():
        return {
            key: {
                "key": key,
                "title": title,
                "rankBasis": basis,
                "rows": [],
                "ranking": [],
            }
            for key, title, basis, _ in panel_specs
        }
    output: dict[str, dict[str, Any]] = {}
    for key, title, basis, descending in panel_specs:
        ranked = ranking.sort(basis, descending=descending).head(limit)
        selected = ranked.get_column("entity_key").to_list()
        panel_rows = (
            rows.filter(pl.col("entity_key").is_in(selected))
            .sort(["date_sort", "entity_label"])
            .to_dicts()
        )
        output[key] = {
            "key": key,
            "title": title,
            "rankBasis": basis,
            "rows": panel_rows,
            "ranking": ranked.to_dicts(),
        }
    return output


def _aggregation_expr(kind: str) -> pl.Expr:
    if kind == "mean":
        return pl.col("value").mean()
    if kind == "median":
        return pl.col("value").median()
    if kind == "min":
        return pl.col("value").min()
    if kind == "max":
        return pl.col("value").max()
    return pl.col("value").sum()


def _merge_grouped_values(left: pl.DataFrame, right: pl.DataFrame) -> pl.DataFrame:
    if left.is_empty():
        return right
    if right.is_empty():
        return left
    columns = [
        "snapshot_id",
        "playthrough_id",
        "date",
        "year",
        "month",
        "day",
        "date_sort",
        "entity_key",
        "entity_label",
    ]
    merged = left.join(right, on=columns, how="full", coalesce=True, suffix="_right")
    return merged.with_columns(
        (pl.col("value").fill_null(0.0) + pl.col("value_right").fill_null(0.0)).alias("value")
    ).select([*columns, "value"])


def _derive_uncollected_tax(collected: pl.DataFrame, possible: pl.DataFrame) -> pl.DataFrame:
    if possible.is_empty():
        return pl.DataFrame()
    columns = [
        "snapshot_id",
        "playthrough_id",
        "date",
        "year",
        "month",
        "day",
        "date_sort",
        "entity_key",
        "entity_label",
    ]
    merged = possible.join(collected, on=columns, how="left", suffix="_collected")
    return merged.with_columns(
        (pl.col("value") - pl.col("value_collected").fill_null(0.0)).clip(0).alias("value")
    ).select([*columns, "value"])


def _snapshot_values(row: dict[str, Any]) -> dict[str, Any]:
    return {key: row.get(key) for key in SNAPSHOT_COLUMNS if key in row}


def _empty_overview() -> dict[str, list[dict[str, Any]]]:
    return {key: list(value) for key, value in _EMPTY_OVERVIEW.items()}


def _source_state_rows(manifest: pl.DataFrame) -> list[dict[str, Any]]:
    if manifest.is_empty() or "snapshot_id" not in manifest.columns:
        return []
    columns = [
        column
        for column in [
            "snapshot_id",
            "playthrough_id",
            "date_sort",
            "state_key",
            "mtime_ns",
            "size",
            "partial_hash",
        ]
        if column in manifest.columns
    ]
    sort_columns = [
        column for column in ["playthrough_id", "date_sort", "snapshot_id"] if column in columns
    ]
    frame = manifest.select(columns)
    if sort_columns:
        frame = frame.sort(sort_columns)
    return frame.to_dicts()


def _normalise_cache_rows(
    rows: list[dict[str, Any]],
    *,
    snapshots: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    snapshots_by_id = {row.get("snapshot_id"): row for row in snapshots}
    output: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        snapshot = snapshots_by_id.get(item.get("snapshot_id"), {})
        for column in SNAPSHOT_COLUMNS:
            if item.get(column) is None and snapshot.get(column) is not None:
                item[column] = snapshot.get(column)
        for column in ["domain", "metric", "dimension", "entity_key", "entity_label"]:
            if item.get(column) is not None:
                item[column] = str(item[column])
        item["value"] = _number(item.get("value"))
        output.append(item)
    return output


def _flatten_overview_cache_rows(
    overview: dict[str, list[dict[str, Any]]],
    *,
    snapshots: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    snapshots_by_id = {row.get("snapshot_id"): row for row in snapshots}
    output: list[dict[str, Any]] = []
    for series, rows in overview.items():
        if series not in _OVERVIEW_SERIES_KEYS:
            continue
        for row in rows:
            item = {"series": series, **dict(row)}
            snapshot = snapshots_by_id.get(item.get("snapshot_id"), {})
            for column in SNAPSHOT_COLUMNS:
                if item.get(column) is None and snapshot.get(column) is not None:
                    item[column] = snapshot.get(column)
            output.append(item)
    return output


def _filter_value_rows_from_explorer(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for row in rows:
        dimension = str(row.get("dimension") or "")
        for filter_key, filter_dimension in _FILTER_DIMENSIONS.items():
            if dimension != filter_dimension:
                continue
            value = str(row.get("entity_key") or "")
            if not value:
                continue
            label = str(row.get("entity_label") or value)
            if filter_key == "pop_type":
                label = value.replace("_", " ").title()
            key = (filter_key, row.get("playthrough_id"), value)
            if key in seen:
                continue
            seen.add(key)
            output.append(
                {
                    "filter_key": filter_key,
                    "playthrough_id": row.get("playthrough_id"),
                    "value": value,
                    "label": label,
                }
            )
    return output


def _dataframe_or_empty(
    rows: list[dict[str, Any]],
    schema: dict[str, pl.DataType],
) -> pl.DataFrame:
    if rows:
        frame = pl.DataFrame(rows, infer_schema_length=None)
    else:
        frame = pl.DataFrame(schema=schema)
    for column, dtype in schema.items():
        if column not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=dtype).alias(column))
            continue
        frame = frame.with_columns(pl.col(column).cast(dtype, strict=False))
    return frame


def _read_parquet_or_empty(path: Path, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    try:
        if path.exists():
            return pl.read_parquet(path)
    except Exception:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(schema=schema)


def _explorer_cache_schema() -> dict[str, pl.DataType]:
    return {
        "snapshot_id": pl.String,
        "playthrough_id": pl.String,
        "date": pl.String,
        "year": pl.Int64,
        "month": pl.Int64,
        "day": pl.Int64,
        "date_sort": pl.Int64,
        "domain": pl.String,
        "metric": pl.String,
        "dimension": pl.String,
        "entity_key": pl.String,
        "entity_label": pl.String,
        "value": pl.Float64,
    }


def _overview_cache_schema() -> dict[str, pl.DataType]:
    return {
        "series": pl.String,
        "snapshot_id": pl.String,
        "playthrough_id": pl.String,
        "date": pl.String,
        "year": pl.Int64,
        "month": pl.Int64,
        "day": pl.Int64,
        "date_sort": pl.Int64,
    }


def _filter_values_schema() -> dict[str, pl.DataType]:
    return {
        "filter_key": pl.String,
        "playthrough_id": pl.String,
        "value": pl.String,
        "label": pl.String,
    }


def _number(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if math.isnan(number) else number


def _path_for_duckdb(path: Path) -> str:
    return path.resolve().as_posix()


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _blank_to_none(value: str | None) -> str | None:
    return None if value in {None, ""} else value


def _nearest_date_sort(values: list[int], requested: int | None) -> int:
    if requested is None:
        return values[0]
    target = int(requested)
    if target <= values[0]:
        return values[0]
    if target >= values[-1]:
        return values[-1]
    return min(values, key=lambda value: (abs(value - target), value))


def _convert_dds_preview(source: Path, target: Path) -> bool:
    try:
        from PIL import Image

        with Image.open(source) as image:
            image.thumbnail((64, 64))
            image.convert("RGBA").save(target)
        return True
    except Exception:
        target.unlink(missing_ok=True)
        return False
