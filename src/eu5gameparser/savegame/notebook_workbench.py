from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns
from matplotlib.ticker import MaxNLocator

from eu5gameparser.domain.buildings import load_building_data
from eu5gameparser.savegame import notebook_analysis as ana
from eu5gameparser.savegame.notebook_labels import NotebookLabelResolver
from eu5gameparser.savegame.notebook_dataset import SavegameNotebookDataset


@dataclass(frozen=True)
class WorkbenchConfig:
    data_root: Path | None = None
    profile: str | None = "constructor"
    load_order_path: Path | None = None
    playthrough: str | None = None
    start_date: int | None = None
    end_date: int | None = None
    snapshot_date: int | None = None
    good_search: str | None = "wheat"
    market_search: str | None = None
    building_search: str | None = "cookery"
    pm_search: str | None = None
    pm_drilldown_search: str | None = None
    country_search: str | None = "england"
    group_by: str = "region"
    building_scope: str = "macro_region"
    flow_group_by: tuple[str, ...] = ("flow_table", "market")
    consumption_group_by: str = "bucket"
    imbalance_sort: str = "mean_flow"
    agg: str = "sum"
    top_n: int = 25
    bucket_years: int = 25
    start_year: int = 1337
    population_metric: str = "total_population"
    food_rank_by: str = "food_fill_ratio"
    building_metric: str = "level"

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> WorkbenchConfig:
        flow_group_by = values.get("FLOW_GROUP_BY", cls.flow_group_by)
        if isinstance(flow_group_by, str):
            flow_group_by = (flow_group_by,)
        return cls(
            data_root=values.get("DATA_ROOT"),
            profile=values.get("PROFILE", cls.profile),
            load_order_path=values.get("LOAD_ORDER_PATH"),
            playthrough=values.get("PLAYTHROUGH"),
            start_date=values.get("START_DATE"),
            end_date=values.get("END_DATE"),
            snapshot_date=values.get("SNAPSHOT_DATE"),
            good_search=values.get("GOOD_SEARCH", cls.good_search),
            market_search=values.get("MARKET_SEARCH", cls.market_search),
            building_search=values.get("BUILDING_SEARCH", cls.building_search),
            pm_search=values.get("PM_SEARCH", cls.pm_search),
            pm_drilldown_search=values.get(
                "PM_DRILLDOWN_SEARCH",
                values.get("PM_SEARCH", cls.pm_drilldown_search),
            ),
            country_search=values.get("COUNTRY_SEARCH", cls.country_search),
            group_by=values.get("GROUP_BY", cls.group_by),
            building_scope=values.get("BUILDING_SCOPE", cls.building_scope),
            flow_group_by=tuple(flow_group_by),
            consumption_group_by=values.get("CONSUMPTION_GROUP_BY", cls.consumption_group_by),
            imbalance_sort=values.get("IMBALANCE_SORT", cls.imbalance_sort),
            agg=values.get("AGG", cls.agg),
            top_n=int(values.get("TOP_N", cls.top_n)),
            bucket_years=int(values.get("BUCKET_YEARS", cls.bucket_years)),
            start_year=int(values.get("START_YEAR", cls.start_year)),
            population_metric=values.get("POPULATION_METRIC", cls.population_metric),
            food_rank_by=values.get("FOOD_RANK_BY", cls.food_rank_by),
            building_metric=values.get("BUILDING_METRIC", cls.building_metric),
        )


@dataclass(frozen=True)
class PopulationResults:
    latest: pl.DataFrame
    delta: pl.DataFrame
    time_series: pl.DataFrame
    top_time_series: pl.DataFrame
    global_time_series: pl.DataFrame


@dataclass(frozen=True)
class GoodsResults:
    global_time_series: pl.DataFrame
    scarcity: pl.DataFrame
    imbalance: pl.DataFrame
    flow_value: pl.DataFrame

    @property
    def goods_imbalance_all(self) -> pl.DataFrame:
        return self.imbalance


@dataclass(frozen=True)
class FlowResults:
    source_breakdown: pl.DataFrame
    sink_breakdown: pl.DataFrame
    source_time_series: pl.DataFrame
    sink_time_series: pl.DataFrame
    good_consumption_latest: pl.DataFrame
    good_consumption_over_time: pl.DataFrame


@dataclass(frozen=True)
class FoodResults:
    rank: pl.DataFrame
    global_time_series: pl.DataFrame
    price_distribution: pl.DataFrame
    delta: pl.DataFrame


@dataclass(frozen=True)
class BuildingResults:
    latest: pl.DataFrame
    time_series: pl.DataFrame
    pm_adoption: pl.DataFrame
    pm_preferences: pl.DataFrame
    pm_slot_latest: pl.DataFrame
    pm_slot_time_series: pl.DataFrame
    pm_flow_time_series: pl.DataFrame
    pm_values: pl.DataFrame

    @property
    def pm_usage_by_slot_over_time(self) -> pl.DataFrame:
        return self.pm_slot_time_series

    @property
    def pm_latest_distribution_by_slot(self) -> pl.DataFrame:
        return self.pm_slot_latest

    @property
    def pm_regional_preferences_by_slot(self) -> pl.DataFrame:
        return self.pm_preferences


def open_workbench(config: WorkbenchConfig | None = None) -> SavegameWorkbench:
    sns.set_theme(style="whitegrid")
    pl.Config.set_tbl_rows(40)
    pl.Config.set_tbl_cols(40)
    return SavegameWorkbench(config or WorkbenchConfig())


class SavegameWorkbench:
    def __init__(self, config: WorkbenchConfig):
        self.config = config
        self.repo = _portable_path(_find_repo_root())
        self.data_root = (
            _portable_path(config.data_root)
            if config.data_root
            else self.repo / "graphs" / "dataset"
        )
        load_order_path = (
            _portable_path(config.load_order_path)
            if config.load_order_path is not None
            else self.repo / "constructor.load_order.toml"
        )
        profile = config.profile if load_order_path.is_file() else None
        self.dataset = SavegameNotebookDataset(
            self.data_root,
            profile=profile,
            load_order_path=load_order_path,
        )
        self.snapshots = self.dataset.snapshots()
        if self.snapshots.is_empty():
            raise RuntimeError(
                "No raw savegame dataset found. Run `uv run ppc savegame-notebooks build` "
                "from the constructor repo, then restart this kernel."
            )
        self.playthrough = config.playthrough or self.dataset.latest_playthrough()
        self.goods_matches = self.search("goods", config.good_search)
        self.market_matches = self.search("markets", config.market_search)
        self.building_matches = self.search("building_types", config.building_search)
        self.pm_matches = self.search("production_methods", config.pm_drilldown_search)
        self.country_matches = self.search("countries", config.country_search)
        self.good_id = _first_value(self.goods_matches, "good_id")
        self.good_label = _first_value(self.goods_matches, "good_label") or self.good_id
        self.market_query = _query_or_none(config.market_search)
        self.building_query = _query_or_none(config.building_search)
        self.pm_query = _query_or_none(config.pm_drilldown_search)
        self.building_label = _first_value(self.building_matches, "building_label") or self.building_query or "selected building"
        self.group_col = ana.normalize_group_by(config.group_by)[0]

    def search(self, dimension: str, query: object | None) -> pl.DataFrame:
        if query is None or str(query).strip() == "":
            return ana.search_dimension(self.dataset, dimension, limit=self.config.top_n)
        return ana.search_dimension(self.dataset, dimension, query, limit=self.config.top_n)

    @property
    def goods_filter(self) -> list[str] | None:
        if _query_or_none(self.config.good_search) is None:
            return None
        return [str(self.good_id)] if self.good_id else ["__no_good_match__"]

    @property
    def active_good(self) -> str | None:
        return str(self.good_id) if self.good_id else None

    @property
    def active_good_label(self) -> str:
        return str(self.good_label or "selected good")

    @property
    def snapshot_summary(self) -> pl.DataFrame:
        return (
            self.snapshots.group_by("playthrough_id")
            .agg(
                pl.len().alias("snapshots"),
                pl.min("year").alias("first_year"),
                pl.max("year").alias("last_year"),
                pl.max("date_sort").alias("_last_date_sort"),
            )
            .sort("_last_date_sort", descending=True)
            .drop("_last_date_sort")
        )

    def print_selection(self) -> None:
        print(f"repo: {self.repo}")
        print(f"data: {self.data_root}")
        print(f"data mode: {'raw' if self.dataset.is_raw else 'optimized'}")
        print(f"playthrough: {self.playthrough}")
        print(f"good: {self.active_good_label} ({self.active_good})")
        print(f"market search: {self.market_query or 'all markets'}")
        print(f"building search: {self.building_query or 'all buildings'}")
        print(f"pm drilldown search: {self.pm_query or 'none'}")

    def preview(self) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        return (
            self.goods_matches,
            self.market_matches,
            self.building_matches,
            self.pm_matches,
            self.country_matches,
            self.snapshot_summary,
        )

    def population(self) -> PopulationResults:
        cfg = self.config
        latest = ana.location_latest_rank(
            self.dataset,
            playthrough_id=self.playthrough,
            group_by=cfg.group_by,
            metric=cfg.population_metric,
            statistic=cfg.agg,
            start_date=cfg.start_date,
            end_date=cfg.end_date,
            snapshot_date=cfg.snapshot_date,
            limit=cfg.top_n,
        ).collect()
        delta = ana.location_first_last_delta(
            self.dataset,
            playthrough_id=self.playthrough,
            group_by=cfg.group_by,
            metric=cfg.population_metric,
            statistic=cfg.agg,
            start_date=cfg.start_date,
            end_date=cfg.end_date,
        ).collect().head(cfg.top_n)
        time_series = ana.location_time_series(
            self.dataset,
            playthrough_id=self.playthrough,
            group_by=cfg.group_by,
            metric=cfg.population_metric,
            statistic=cfg.agg,
            start_date=cfg.start_date,
            end_date=cfg.end_date,
        ).collect()
        global_time_series = ana.location_global_time_series(
            self.dataset,
            playthrough_id=self.playthrough,
            start_date=cfg.start_date,
            end_date=cfg.end_date,
        ).collect()
        top_groups = latest[self.group_col].head(10).to_list() if self.group_col in latest.columns else []
        top_time_series = (
            time_series.filter(pl.col(self.group_col).is_in(top_groups))
            if top_groups and self.group_col in time_series.columns
            else time_series
        )
        return PopulationResults(latest, delta, time_series, top_time_series, global_time_series)

    def goods(self) -> GoodsResults:
        cfg = self.config
        return GoodsResults(
            global_time_series=ana.goods_global_time_series(
                self.dataset,
                playthrough_id=self.playthrough,
                goods=self.goods_filter,
                start_date=cfg.start_date,
                end_date=cfg.end_date,
            ).collect(),
            scarcity=ana.market_shortage_glut(
                self.dataset,
                playthrough_id=self.playthrough,
                goods=self.goods_filter,
                snapshot_date=cfg.snapshot_date,
            ).collect(),
            imbalance=ana.goods_imbalance_buckets(
                self.dataset,
                playthrough_id=self.playthrough,
                bucket_years=cfg.bucket_years,
                sort_by=cfg.imbalance_sort,
                start_year=cfg.start_year,
                start_date=cfg.start_date,
                end_date=cfg.end_date,
            ).collect(),
            flow_value=ana.market_flow_proxy(
                self.dataset,
                playthrough_id=self.playthrough,
                group_by="good",
                start_date=cfg.start_date,
                end_date=cfg.end_date,
            ).collect(),
        )

    def flows(self) -> FlowResults:
        cfg = self.config
        return FlowResults(
            source_breakdown=ana.good_flow_breakdown(
                self.dataset,
                good=self.active_good,
                direction="source",
                playthrough_id=self.playthrough,
                group_by=cfg.flow_group_by,
                market_query=self.market_query,
                start_date=cfg.start_date,
                end_date=cfg.end_date,
                snapshot_date=cfg.snapshot_date,
                limit=cfg.top_n,
            ).collect(),
            sink_breakdown=ana.good_flow_breakdown(
                self.dataset,
                good=self.active_good,
                direction="sink",
                playthrough_id=self.playthrough,
                group_by=cfg.flow_group_by,
                market_query=self.market_query,
                start_date=cfg.start_date,
                end_date=cfg.end_date,
                snapshot_date=cfg.snapshot_date,
                limit=cfg.top_n,
            ).collect(),
            source_time_series=ana.good_flow_time_series(
                self.dataset,
                good=self.active_good,
                direction="source",
                playthrough_id=self.playthrough,
                group_by="flow_table",
                market_query=self.market_query,
                start_date=cfg.start_date,
                end_date=cfg.end_date,
            ).collect(),
            sink_time_series=ana.good_flow_time_series(
                self.dataset,
                good=self.active_good,
                direction="sink",
                playthrough_id=self.playthrough,
                group_by="flow_table",
                market_query=self.market_query,
                start_date=cfg.start_date,
                end_date=cfg.end_date,
            ).collect(),
            good_consumption_latest=ana.good_consumption_latest(
                self.dataset,
                good=self.active_good,
                playthrough_id=self.playthrough,
                group_by=cfg.consumption_group_by,
                market_query=self.market_query,
                start_date=cfg.start_date,
                end_date=cfg.end_date,
                snapshot_date=cfg.snapshot_date,
                limit=cfg.top_n,
            ).collect(),
            good_consumption_over_time=ana.good_consumption_over_time(
                self.dataset,
                good=self.active_good,
                playthrough_id=self.playthrough,
                group_by=cfg.consumption_group_by,
                market_query=self.market_query,
                start_date=cfg.start_date,
                end_date=cfg.end_date,
            ).collect(),
        )

    def food(self) -> FoodResults:
        cfg = self.config
        return FoodResults(
            rank=ana.food_market_rank(
                self.dataset,
                playthrough_id=self.playthrough,
                rank_by=cfg.food_rank_by,
                market_query=self.market_query,
                snapshot_date=cfg.snapshot_date,
                largest=False,
                limit=cfg.top_n,
            ).collect(),
            global_time_series=ana.food_global_stockpile(
                self.dataset,
                playthrough_id=self.playthrough,
                start_date=cfg.start_date,
                end_date=cfg.end_date,
            ).collect(),
            price_distribution=ana.food_price_distribution(
                self.dataset,
                playthrough_id=self.playthrough,
                start_date=cfg.start_date,
                end_date=cfg.end_date,
            ).collect(),
            delta=ana.food_first_last_delta(
                self.dataset,
                playthrough_id=self.playthrough,
                metric=cfg.food_rank_by,
                group_by="market",
                start_date=cfg.start_date,
                end_date=cfg.end_date,
            ).collect().head(cfg.top_n),
        )

    def buildings(self) -> BuildingResults:
        cfg = self.config
        return BuildingResults(
            latest=ana.building_latest_rank(
                self.dataset,
                playthrough_id=self.playthrough,
                metric=cfg.building_metric,
                group_by=["building", cfg.group_by],
                statistic=cfg.agg,
                building_query=self.building_query,
                snapshot_date=cfg.snapshot_date,
                limit=cfg.top_n,
            ).collect(),
            time_series=ana.building_metric_time_series(
                self.dataset,
                playthrough_id=self.playthrough,
                metric=cfg.building_metric,
                group_by=["building", cfg.group_by],
                statistic=cfg.agg,
                building_query=self.building_query,
                start_date=cfg.start_date,
                end_date=cfg.end_date,
            ).collect(),
            pm_adoption=ana.pm_adoption_over_time(
                self.dataset,
                playthrough_id=self.playthrough,
                building_query=self.building_query,
                start_date=cfg.start_date,
                end_date=cfg.end_date,
            ).collect(),
            pm_preferences=ana.pm_regional_preferences(
                self.dataset,
                playthrough_id=self.playthrough,
                building_query=self.building_query,
                group_by=cfg.building_scope,
                snapshot_date=cfg.snapshot_date,
            ).collect(),
            pm_slot_latest=ana.pm_slot_distribution_latest(
                self.dataset,
                playthrough_id=self.playthrough,
                building_query=self.building_query,
                snapshot_date=cfg.snapshot_date,
            ).collect(),
            pm_slot_time_series=ana.pm_slot_distribution_over_time(
                self.dataset,
                playthrough_id=self.playthrough,
                building_query=self.building_query,
                start_date=cfg.start_date,
                end_date=cfg.end_date,
            ).collect(),
            pm_flow_time_series=ana.pm_good_flow_time_series(
                self.dataset,
                playthrough_id=self.playthrough,
                good=self.active_good,
                building_query=self.building_query,
                pm_query=self.pm_query,
                start_date=cfg.start_date,
                end_date=cfg.end_date,
            ).collect(),
            pm_values=ana.pm_value_mix(
                self.dataset,
                playthrough_id=self.playthrough,
                good=self.active_good,
                building_query=self.building_query,
                pm_query=self.pm_query,
                snapshot_date=cfg.snapshot_date,
            ).collect(),
        )

    def plot_population(self, result: PopulationResults) -> None:
        cfg = self.config
        barh(result.latest, cfg.population_metric, self.group_col, title=f"Latest {cfg.population_metric} by {self.group_col}", top_n=cfg.top_n)
        line_plot(result.top_time_series, _time_axis(result.top_time_series), cfg.population_metric, hue=self.group_col, title=f"{cfg.population_metric} over time")
        line_plot(result.global_time_series, _time_axis(result.global_time_series), "total_population", title="Global population over time")

    def plot_goods(self, result: GoodsResults) -> None:
        combined_line_plot(result.global_time_series, ("supply", "demand", "net"), hue="good_label", title="Selected goods supply, demand, and net")
        band_plot(result.global_time_series, _time_axis(result.global_time_series), "price_p10", "median_price", "price_p90", title="Selected goods price distribution")
        heatmap(result.imbalance, "good_label", "bucket_start", "imbalance_percent", title="All-goods imbalance by year bucket")

    def plot_flows(self, result: FlowResults) -> None:
        flow_group = ana.normalize_group_by(self.config.flow_group_by)[-1]
        barh(result.source_breakdown, "amount", flow_group, title=f"Where {self.active_good_label} comes from", top_n=self.config.top_n)
        barh(result.sink_breakdown, "amount", flow_group, title=f"Where {self.active_good_label} goes", top_n=self.config.top_n)
        consumption_group = ana.normalize_group_by(self.config.consumption_group_by)[0]
        if self.config.consumption_group_by == "bucket":
            consumption_group = "consumption_label"
        barh(result.good_consumption_latest, "amount", consumption_group, title=f"{self.active_good_label} consumption breakdown", top_n=self.config.top_n)
        line_plot(result.source_time_series, _time_axis(result.source_time_series), "amount", hue="flow_table", title=f"{self.active_good_label} sources over time")
        line_plot(result.sink_time_series, _time_axis(result.sink_time_series), "amount", hue="flow_table", title=f"{self.active_good_label} sinks over time")
        line_plot(result.good_consumption_over_time, _time_axis(result.good_consumption_over_time), "amount", hue=consumption_group, title=f"{self.active_good_label} consumption over time")

    def plot_food(self, result: FoodResults) -> None:
        barh(result.rank, self.config.food_rank_by, "market_label", title=f"Food markets ranked by {self.config.food_rank_by}", top_n=self.config.top_n)
        combined_line_plot(result.global_time_series, ("food", "food_max"), title="Global food stockpile and capacity")
        band_plot(result.price_distribution, _time_axis(result.price_distribution), "price_p10", "price_p50", "price_p90", title="Food price distribution")

    def plot_buildings(self, result: BuildingResults) -> None:
        frame = _with_plot_slot_metadata(result.pm_slot_time_series, self.dataset)
        return slot_line_plots(
            frame,
            "share",
            slot=("building_label", "slot_label"),
            title=f"{self.building_label} production method share over time",
        )

    def plot_building_slot(self, result: BuildingResults, slot: int | str) -> None:
        frame = _filter_plot_slot(_slot_plot_frame(result, self.dataset), slot)
        if frame.is_empty():
            try:
                frame = _filter_plot_slot(_slot_plot_frame(self.buildings(), self.dataset), slot)
            except Exception:
                frame = frame.head(0)
        if frame.is_empty():
            return
        return slot_line_plots(
            frame,
            "share",
            slot=("building_label", "slot_label"),
            title=f"{self.building_label} production method share over time",
        )


def _slot_plot_frame(
    result: BuildingResults,
    dataset: SavegameNotebookDataset,
) -> pl.DataFrame:
    return _with_plot_slot_metadata(result.pm_slot_time_series, dataset)


def _with_plot_slot_metadata(
    frame: pl.DataFrame,
    dataset: SavegameNotebookDataset,
) -> pl.DataFrame:
    if frame.is_empty() or not {"building_label", "slot_label"}.issubset(frame.columns):
        return frame
    metadata = _plot_slot_metadata(dataset)
    join_key = _plot_slot_join_key(frame, metadata)
    if join_key is None:
        return _with_plot_slot_share(frame)

    joined = frame.join(
        metadata.rename(
            {
                "slot_label": "_slot_label",
                "production_method_group_index": "_production_method_group_index",
            }
        ),
        on=join_key,
        how="left",
    )
    updates: list[pl.Expr] = []
    if "_slot_label" in joined.columns:
        updates.append(
            pl.when(
                pl.col("_slot_label").is_not_null()
                & (pl.col("_slot_label") != "Unslotted")
            )
            .then(pl.col("_slot_label"))
            .otherwise(pl.col("slot_label"))
            .alias("slot_label")
        )
    if "_production_method_group_index" in joined.columns:
        if "production_method_group_index" in joined.columns:
            updates.append(
                pl.coalesce(
                    "production_method_group_index",
                    "_production_method_group_index",
                ).alias("production_method_group_index")
            )
        else:
            updates.append(
                pl.col("_production_method_group_index").alias(
                    "production_method_group_index"
                )
            )
    if updates:
        joined = joined.with_columns(updates)
    drop_columns = [
        column
        for column in ("_slot_label", "_production_method_group_index")
        if column in joined.columns
    ]
    if drop_columns:
        joined = joined.drop(drop_columns)
    return _with_plot_slot_share(joined)


def _filter_plot_slot(frame: pl.DataFrame, slot: int | str) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    if isinstance(slot, int):
        if "production_method_group_index" in frame.columns:
            return frame.filter(pl.col("production_method_group_index") == slot)
        if "slot_label" in frame.columns:
            return frame.filter(pl.col("slot_label") == f"Slot {slot + 1}")
        return frame.head(0)

    text = str(slot).strip().lower()
    if not text or "slot_label" not in frame.columns:
        return frame.head(0)
    return frame.filter(pl.col("slot_label").cast(pl.String).str.to_lowercase() == text)


def _plot_slot_join_key(frame: pl.DataFrame, metadata: pl.DataFrame) -> str | None:
    for column in ("production_method", "production_method_label"):
        if column in frame.columns and column in metadata.columns:
            return column
    return None


def _with_plot_slot_share(frame: pl.DataFrame) -> pl.DataFrame:
    if "share" not in frame.columns or "buildings" not in frame.columns:
        return frame
    date_column = _time_axis(frame)
    denominator = [
        column
        for column in (date_column, "building_label", "slot_label")
        if column in frame.columns
    ]
    if not denominator:
        return frame
    return frame.with_columns(
        (pl.col("buildings") / pl.sum("buildings").over(denominator)).alias("share")
    )


def _plot_slot_metadata(dataset: SavegameNotebookDataset) -> pl.DataFrame:
    from_building_data = _plot_slot_metadata_from_building_data(dataset)
    if not from_building_data.is_empty():
        return from_building_data
    return _plot_slot_metadata_from_dimensions(dataset)


def _plot_slot_metadata_from_dimensions(dataset: SavegameNotebookDataset) -> pl.DataFrame:
    try:
        methods = dataset.dim("production_methods")
    except (FileNotFoundError, KeyError, OSError, pl.exceptions.PolarsError):
        return _empty_plot_slot_metadata()
    required = {"production_method", "slot_label", "production_method_group_index"}
    if methods.is_empty() or not required.issubset(methods.columns):
        return _empty_plot_slot_metadata()
    frame = methods
    if "production_method_label" not in frame.columns:
        frame = frame.with_columns(
            pl.col("production_method")
            .map_elements(lambda value: _label_from_key(value), return_dtype=pl.String)
            .alias("production_method_label")
        )
    return frame.select(
        [
            "production_method",
            "production_method_label",
            "slot_label",
            "production_method_group_index",
        ]
    ).unique("production_method", keep="first", maintain_order=True)


def _plot_slot_metadata_from_building_data(dataset: SavegameNotebookDataset) -> pl.DataFrame:
    if dataset.profile is None:
        return _empty_plot_slot_metadata()
    try:
        data = load_building_data(
            profile=dataset.profile,
            load_order_path=dataset.load_order_path,
        )
        resolver = NotebookLabelResolver.from_profile(
            profile=dataset.profile,
            load_order_path=dataset.load_order_path,
        )
    except (FileNotFoundError, KeyError, OSError):
        return _empty_plot_slot_metadata()
    methods = data.production_methods
    if methods.is_empty() or "name" not in methods.columns:
        return _empty_plot_slot_metadata()
    frame = methods.select(
        [
            pl.col("name").alias("production_method"),
            (
                pl.col("building")
                if "building" in methods.columns
                else pl.lit(None, dtype=pl.String)
            ).alias("production_method_building"),
            (
                pl.col("production_method_group_index")
                if "production_method_group_index" in methods.columns
                else pl.lit(None, dtype=pl.Int64)
            ).alias("production_method_group_index"),
        ]
    )
    return frame.with_columns(
        pl.col("production_method")
        .map_elements(lambda value: resolver.label(value), return_dtype=pl.String)
        .alias("production_method_label"),
        pl.struct(["production_method_building", "production_method_group_index"])
        .map_elements(
            lambda row: _slot_label_from_metadata(row, resolver),
            return_dtype=pl.String,
        )
        .alias("slot_label"),
    ).select(
        [
            "production_method",
            "production_method_label",
            "slot_label",
            "production_method_group_index",
        ]
    ).unique("production_method", keep="first", maintain_order=True)


def _empty_plot_slot_metadata() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "production_method": pl.String,
            "production_method_label": pl.String,
            "slot_label": pl.String,
            "production_method_group_index": pl.Int64,
        }
    )


def _slot_label_from_metadata(
    row: dict[str, object],
    resolver: NotebookLabelResolver,
) -> str:
    group_index = row.get("production_method_group_index")
    if group_index is None:
        return "Unslotted"
    index = int(group_index)
    fallback = f"Slot {index + 1}"
    building = row.get("production_method_building")
    if building is None or str(building).strip() == "":
        return fallback
    return resolver.label(f"{building}_slot_{index}", fallback=fallback)


def _label_from_key(value: object) -> str:
    text = "" if value is None else str(value)
    return " ".join(part for part in text.replace("-", "_").split("_") if part).title()


def line_plot(frame: pl.DataFrame, x: str, y: str, *, hue: str | None = None, title: str = "") -> None:
    if frame.is_empty() or x not in frame.columns or y not in frame.columns:
        print("No rows")
        return
    fig, ax = plt.subplots(figsize=(11, 4.5))
    if hue and hue in frame.columns:
        for key, part in frame.sort(x).partition_by(hue, as_dict=True).items():
            ax.plot(part[x].to_list(), part[y].to_list(), marker="o", linewidth=1.6, label=_label(key))
        if frame[hue].n_unique() <= 14:
            ax.legend(loc="best", fontsize=8)
    else:
        ordered = frame.sort(x)
        ax.plot(ordered[x].to_list(), ordered[y].to_list(), marker="o", linewidth=1.8)
    _finish_line_axis(ax, x, title or y, x, y)


def slot_line_plots(
    frame: pl.DataFrame,
    y: str,
    *,
    slot: str | Sequence[str] = "slot_label",
    hue: str = "production_method_label",
    title: str = "",
) -> list[Any]:
    x = _time_axis(frame)
    slot_columns = _column_list(slot)
    required = {x, y, hue, *slot_columns}
    if frame.is_empty() or not required.issubset(frame.columns):
        print("No rows")
        return []
    slot_partitions = frame.sort(
        _slot_sort_columns(frame, slot_columns, x, hue)
    ).partition_by(slot_columns, as_dict=True)
    figures: list[Any] = []
    for slot_key, part in slot_partitions.items():
        fig, ax = plt.subplots(figsize=(11, 4.5))
        for key, series in part.sort(x).partition_by(hue, as_dict=True).items():
            ax.plot(series[x].to_list(), series[y].to_list(), marker="o", linewidth=1.6, label=_label(key))
        ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=8)
        _finish_line_axis(ax, x, f"{title} - {_label(slot_key)}" if title else _label(slot_key), x, y)
        figures.append(fig)
    return figures


def slot_barh_plots(
    frame: pl.DataFrame,
    x: str,
    y: str,
    *,
    slot: str = "slot_label",
    title: str = "",
) -> None:
    required = {x, y, slot}
    if frame.is_empty() or not required.issubset(frame.columns):
        print("No rows")
        return
    for slot_key, part in frame.sort([slot, x], descending=[False, True]).partition_by(slot, as_dict=True).items():
        shown = part.filter(pl.col(x).is_not_null()).sort(x, descending=True)
        if shown.is_empty():
            continue
        fig, ax = plt.subplots(figsize=(10, max(3.5, shown.height * 0.35)))
        ax.barh(shown[y].cast(str).to_list(), shown[x].to_list())
        ax.invert_yaxis()
        ax.set_title(f"{title} - {_label(slot_key)}" if title else _label(slot_key))
        ax.set_xlabel(x)
        ax.set_ylabel(y)
        plt.tight_layout()
        plt.show()


def combined_line_plot(
    frame: pl.DataFrame,
    metrics: Sequence[str],
    *,
    hue: str | None = None,
    title: str = "",
) -> None:
    x = _time_axis(frame)
    if frame.is_empty() or x not in frame.columns or not set(metrics).issubset(frame.columns):
        print("No rows")
        return
    fig, ax = plt.subplots(figsize=(11, 4.5))
    partitions = frame.sort(x).partition_by(hue, as_dict=True) if hue and hue in frame.columns else {None: frame.sort(x)}
    for key, part in partitions.items():
        for metric in metrics:
            label = metric if key is None else f"{_label(key)} / {metric}"
            ax.plot(part[x].to_list(), part[metric].to_list(), marker="o", linewidth=1.5, label=label)
    if len(partitions) * len(metrics) <= 16:
        ax.legend(loc="best", fontsize=8)
    _finish_line_axis(ax, x, title or ", ".join(metrics), x, "value")


def band_plot(frame: pl.DataFrame, x: str, low: str, mid: str, high: str, *, title: str = "") -> None:
    required = {x, low, mid, high}
    if frame.is_empty() or not required.issubset(frame.columns):
        print("No rows")
        return
    ordered = frame.sort(x)
    fig, ax = plt.subplots(figsize=(11, 4.5))
    ax.fill_between(ordered[x].to_list(), ordered[low].to_list(), ordered[high].to_list(), alpha=0.18)
    ax.plot(ordered[x].to_list(), ordered[mid].to_list(), marker="o", linewidth=1.8)
    _finish_line_axis(ax, x, title or mid, x, mid)


def barh(frame: pl.DataFrame, x: str, y: str, *, title: str = "", top_n: int = 25) -> None:
    if frame.is_empty() or x not in frame.columns or y not in frame.columns:
        print("No rows")
        return
    shown = frame.filter(pl.col(x).is_not_null()).head(top_n)
    if shown.is_empty():
        print("No rows")
        return
    fig, ax = plt.subplots(figsize=(10, max(4, min(12, shown.height * 0.35))))
    ax.barh(shown[y].cast(str).to_list(), shown[x].to_list())
    ax.invert_yaxis()
    ax.set_title(title or x)
    ax.set_xlabel(x)
    ax.set_ylabel(y)
    plt.tight_layout()
    _display_figure(fig)


def heatmap(frame: pl.DataFrame, index: str, columns: str, values: str, *, title: str = "") -> None:
    if frame.is_empty() or not {index, columns, values}.issubset(frame.columns):
        print("No rows")
        return
    pivot = frame.pivot(index=index, on=columns, values=values, aggregate_function="first").fill_null(0)
    value_columns = [column for column in pivot.columns if column != index]
    if not value_columns:
        print("No rows")
        return
    fig, ax = plt.subplots(figsize=(max(8, len(value_columns) * 0.6), max(5, pivot.height * 0.25)))
    sns.heatmap(pivot.select(value_columns).to_numpy(), ax=ax, cmap="coolwarm", center=0)
    ax.set_title(title or values)
    ax.set_xticklabels(value_columns, rotation=45, ha="right")
    ax.set_yticklabels(pivot[index].cast(str).to_list(), rotation=0)
    ax.set_xlabel(columns)
    ax.set_ylabel(index)
    plt.tight_layout()
    _display_figure(fig)


def _find_repo_root(start: Path | None = None) -> Path:
    current = _portable_path(start or Path.cwd()).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "constructor.toml").is_file():
            return candidate
    raise FileNotFoundError("Could not find constructor.toml; run this notebook from the constructor repo.")


def _portable_path(value: str | Path) -> Path:
    path = Path(value)
    if path.exists():
        return path
    text = str(value).replace("\\", "/")
    wsl_match = re.match(r"^/mnt/([A-Za-z])/(.*)$", text)
    if wsl_match:
        drive, rest = wsl_match.groups()
        return Path(f"{drive.upper()}:/{rest}")
    windows_match = re.match(r"^([A-Za-z]):/(.*)$", text)
    if windows_match:
        drive, rest = windows_match.groups()
        candidate = Path("/mnt") / drive.lower() / rest
        if candidate.exists():
            return candidate
    return path


def _first_value(frame: pl.DataFrame, column: str) -> object | None:
    if frame.is_empty() or column not in frame.columns:
        return None
    return frame.item(0, column)


def _query_or_none(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _time_axis(frame: pl.DataFrame) -> str:
    return "year" if "year" in frame.columns else "date_sort"


def _column_list(columns: str | Sequence[str]) -> list[str]:
    if isinstance(columns, str):
        return [columns]
    return list(columns)


def _slot_sort_columns(
    frame: pl.DataFrame,
    slot_columns: Sequence[str],
    x: str,
    hue: str,
) -> list[str]:
    names = set(frame.columns)
    columns: list[str] = []
    for column in slot_columns:
        if (
            column == "slot_label"
            and "production_method_group_index" in names
            and "production_method_group_index" not in slot_columns
        ):
            columns.append("production_method_group_index")
        columns.append(column)
    columns.extend([x, hue])
    unique: list[str] = []
    for column in columns:
        if column in names and column not in unique:
            unique.append(column)
    return unique


def _label(value: object) -> str:
    if isinstance(value, tuple):
        return " / ".join(str(part) for part in value)
    return str(value)


def _finish_line_axis(ax: Any, x: str, title: str, xlabel: str, ylabel: str) -> None:
    if x == "year":
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    plt.tight_layout()
    _display_figure(ax.figure)


def _display_figure(fig: Any) -> None:
    try:
        from IPython.display import display
    except ImportError:
        plt.show()
        return
    display(fig)
    plt.close(fig)
