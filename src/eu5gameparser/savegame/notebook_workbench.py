from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns
from matplotlib.ticker import MaxNLocator

from eu5gameparser.savegame import notebook_analysis as ana
from eu5gameparser.savegame.notebook_dataset import SavegameNotebookDataset


@dataclass(frozen=True)
class WorkbenchConfig:
    data_root: Path | None = None
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
        self.repo = _find_repo_root()
        self.data_root = Path(config.data_root) if config.data_root else self.repo / "graphs" / "savegame_notebooks" / "data"
        self.dataset = SavegameNotebookDataset(self.data_root)
        self.snapshots = self.dataset.snapshots()
        if self.snapshots.is_empty():
            raise RuntimeError(
                "No optimized savegame notebook data found. Run `uv run ppc savegame-notebooks build` "
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
        slot_line_plots(result.pm_slot_time_series, "share", title=f"{self.building_label} production method share over time")
        slot_barh_plots(result.pm_slot_latest, "share", "production_method_label", title=f"Latest {self.building_label} production method distribution")
        line_plot(result.pm_flow_time_series, _time_axis(result.pm_flow_time_series), "amount", hue="direction", title=f"{self.active_good_label} PM flows for {self.building_label}")


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
    slot: str = "slot_label",
    hue: str = "production_method_label",
    title: str = "",
) -> None:
    x = _time_axis(frame)
    required = {x, y, slot, hue}
    if frame.is_empty() or not required.issubset(frame.columns):
        print("No rows")
        return
    for slot_key, part in frame.sort([slot, x, hue]).partition_by(slot, as_dict=True).items():
        fig, ax = plt.subplots(figsize=(11, 4.5))
        for key, series in part.sort(x).partition_by(hue, as_dict=True).items():
            ax.plot(series[x].to_list(), series[y].to_list(), marker="o", linewidth=1.6, label=_label(key))
        ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=8)
        _finish_line_axis(ax, x, f"{title} - {_label(slot_key)}" if title else _label(slot_key), x, y)


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
    plt.show()


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
    plt.show()


def _find_repo_root(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "constructor.toml").is_file():
            return candidate
    raise FileNotFoundError("Could not find constructor.toml; run this notebook from the constructor repo.")


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
    plt.show()
