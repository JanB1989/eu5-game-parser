from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import polars as pl

from eu5gameparser.savegame.notebook_dataset import (
    DIMENSION_SPECS,
    SavegameNotebookDataset,
)


GROUP_ALIASES = {
    "area": "area_label",
    "building": "building_label",
    "building_type": "building_label",
    "consumption": "consumption_label",
    "consumption_bucket": "consumption_label",
    "country": "country_label",
    "country_tag": "country_label",
    "good": "good_label",
    "location": "location_label",
    "macro_region": "macro_region_label",
    "market": "market_label",
    "pm": "production_method_label",
    "production_method": "production_method_label",
    "region": "region_label",
    "super_region": "super_region_label",
}
STANDARD_DIMENSIONS = (
    "goods",
    "markets",
    "locations",
    "building_types",
    "production_methods",
)


def schema_names(frame: pl.LazyFrame) -> set[str]:
    return set(frame.collect_schema().names())


def has_columns(frame: pl.LazyFrame, *columns: str) -> bool:
    return set(columns).issubset(schema_names(frame))


def empty_frame(schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return pl.DataFrame(schema=schema)


def empty_lazy(schema: dict[str, pl.DataType]) -> pl.LazyFrame:
    return empty_frame(schema).lazy()


def search_dimension(
    dataset: SavegameNotebookDataset,
    dimension: str,
    query: object | None = None,
    *,
    limit: int = 50,
) -> pl.DataFrame:
    frame = dataset.dim(dimension)
    if frame.is_empty() or not frame.columns:
        return frame
    if query is None or str(query).strip() == "":
        return frame.head(limit)

    text = str(query).strip().lower()
    searchable = _searchable_columns(frame)
    if not searchable:
        return frame.head(0)
    lowered = [pl.col(column).cast(pl.String).str.to_lowercase().fill_null("") for column in searchable]
    exact = pl.any_horizontal([column == text for column in lowered])
    prefix = pl.any_horizontal([column.str.starts_with(text) for column in lowered])
    contains = pl.any_horizontal([column.str.contains(text, literal=True) for column in lowered])
    return (
        frame.with_columns(
            pl.when(exact)
            .then(pl.lit(0))
            .when(prefix)
            .then(pl.lit(1))
            .when(contains)
            .then(pl.lit(2))
            .otherwise(pl.lit(99))
            .alias("_match_rank")
        )
        .filter(pl.col("_match_rank") < 99)
        .sort(["_match_rank", searchable[0]])
        .drop("_match_rank")
        .head(limit)
    )


def resolve_codes(
    dataset: SavegameNotebookDataset,
    dimension: str,
    values: object | Sequence[object] | None = None,
    *,
    query: object | None = None,
    limit: int | None = None,
) -> list[int]:
    spec = DIMENSION_SPECS[dimension]
    code_column = str(spec["code"])
    if values is not None:
        frame = dataset.dim(dimension)
        candidates = _as_list(values)
        if frame.is_empty() or code_column not in frame.columns:
            return []
        searchable = _searchable_columns(frame)
        exact_terms = {str(value).strip().lower() for value in candidates if str(value).strip()}
        if not exact_terms:
            return []
        predicates = [
            pl.col(column).cast(pl.String).str.to_lowercase().is_in(exact_terms)
            for column in searchable
        ]
        if not predicates:
            return []
        matches = frame.filter(pl.any_horizontal(predicates))
    else:
        matches = search_dimension(dataset, dimension, query, limit=limit or 10_000)
    if limit is not None:
        matches = matches.head(limit)
    if matches.is_empty() or code_column not in matches.columns:
        return []
    return [int(value) for value in matches[code_column].drop_nulls().to_list()]


def window(
    frame: pl.LazyFrame,
    *,
    playthrough_id: str | None = None,
    start_date: int | None = None,
    end_date: int | None = None,
    snapshot_date: int | None = None,
) -> pl.LazyFrame:
    names = schema_names(frame)
    if playthrough_id is not None and "playthrough_id" in names:
        frame = frame.filter(pl.col("playthrough_id") == playthrough_id)
    if snapshot_date is not None and "date_sort" in names:
        frame = frame.filter(pl.col("date_sort") == snapshot_date)
    if start_date is not None and "date_sort" in names:
        frame = frame.filter(pl.col("date_sort") >= start_date)
    if end_date is not None and "date_sort" in names:
        frame = frame.filter(pl.col("date_sort") <= end_date)
    return frame


def with_dimensions(
    dataset: SavegameNotebookDataset,
    frame: pl.LazyFrame,
    dimensions: Sequence[str] = STANDARD_DIMENSIONS,
) -> pl.LazyFrame:
    for dimension in dimensions:
        frame = dataset.with_dimension(frame, dimension)
    return frame


def normalize_group_by(group_by: str | Sequence[str]) -> list[str]:
    return [GROUP_ALIASES.get(group, group) for group in _as_str_list(group_by)]


def aggregate_raw(
    frame: pl.LazyFrame,
    *,
    group_by: str | Sequence[str],
    metric: str,
    statistic: str = "sum",
    value_name: str | None = None,
    limit: int | None = None,
    descending: bool = True,
) -> pl.LazyFrame:
    groups = normalize_group_by(group_by)
    output_name = value_name or metric
    if not has_columns(frame, metric, *groups):
        return empty_lazy(_schema_for_groups(groups, output_name))
    grouped = frame.group_by(groups).agg(_stat_expr(metric, statistic).alias(output_name)).sort(
        output_name,
        descending=descending,
    )
    if limit is not None:
        grouped = grouped.limit(limit)
    return grouped


def time_series(
    frame: pl.LazyFrame,
    *,
    group_by: str | Sequence[str],
    metric: str,
    statistic: str = "sum",
    value_name: str | None = None,
    date_column: str = "date_sort",
) -> pl.LazyFrame:
    groups = normalize_group_by(group_by)
    output_name = value_name or metric
    if not has_columns(frame, date_column, metric, *groups):
        return empty_lazy({date_column: pl.UInt32, **_schema_for_groups(groups, output_name)})
    aggregations = [_stat_expr(metric, statistic).alias(output_name)]
    if "year" in schema_names(frame) and date_column != "year":
        aggregations.insert(0, pl.first("year").alias("year"))
    return (
        frame.group_by([date_column, *groups])
        .agg(aggregations)
        .sort([date_column, *groups])
    )


def latest_snapshot(
    frame: pl.LazyFrame,
    *,
    snapshot_date: int | None = None,
    date_column: str = "date_sort",
) -> pl.LazyFrame:
    if snapshot_date is not None:
        return window(frame, snapshot_date=snapshot_date)
    if date_column not in schema_names(frame):
        return frame
    latest = frame.select(pl.max(date_column).alias("_latest")).collect().item(0, "_latest")
    if latest is None:
        return frame.filter(pl.lit(False))
    return frame.filter(pl.col(date_column) == latest)


def latest_rank(
    frame: pl.LazyFrame,
    *,
    group_by: str | Sequence[str],
    metric: str,
    statistic: str = "sum",
    snapshot_date: int | None = None,
    limit: int | None = None,
    descending: bool = True,
) -> pl.LazyFrame:
    return aggregate_raw(
        latest_snapshot(frame, snapshot_date=snapshot_date),
        group_by=group_by,
        metric=metric,
        statistic=statistic,
        limit=limit,
        descending=descending,
    )


def first_last_delta(
    frame: pl.LazyFrame,
    *,
    group_by: str | Sequence[str],
    metric: str,
    statistic: str = "sum",
    date_column: str = "date_sort",
) -> pl.LazyFrame:
    groups = normalize_group_by(group_by)
    if not has_columns(frame, date_column, metric, *groups):
        return empty_lazy(
            {
                **_schema_for_groups(groups, metric),
                "first": pl.Float32,
                "last": pl.Float32,
                "delta": pl.Float32,
                "abs_delta": pl.Float32,
            }
        )
    dates = frame.select(
        pl.min(date_column).alias("_first"),
        pl.max(date_column).alias("_last"),
    ).collect()
    first = dates.item(0, "_first")
    last = dates.item(0, "_last")
    if first is None or last is None:
        return empty_lazy(
            {
                **_schema_for_groups(groups, metric),
                "first": pl.Float32,
                "last": pl.Float32,
                "delta": pl.Float32,
                "abs_delta": pl.Float32,
            }
        )
    first_frame = aggregate_raw(
        frame.filter(pl.col(date_column) == first),
        group_by=groups,
        metric=metric,
        statistic=statistic,
        value_name="first",
        descending=True,
    )
    last_frame = aggregate_raw(
        frame.filter(pl.col(date_column) == last),
        group_by=groups,
        metric=metric,
        statistic=statistic,
        value_name="last",
        descending=True,
    )
    return (
        first_frame.join(last_frame, on=groups, how="full", coalesce=True)
        .with_columns(
            pl.col("first").fill_null(0.0),
            pl.col("last").fill_null(0.0),
        )
        .with_columns(
            (pl.col("last") - pl.col("first")).alias("delta"),
            (pl.col("last") - pl.col("first")).abs().alias("abs_delta"),
        )
        .sort("abs_delta", descending=True)
    )


def add_year_bucket(
    frame: pl.LazyFrame,
    *,
    bucket_years: int,
    start_year: int = 1337,
    column: str = "bucket_start",
) -> pl.LazyFrame:
    if "year" not in schema_names(frame):
        return frame
    return frame.with_columns(
        (
            ((pl.col("year").cast(pl.Int32) - start_year) // bucket_years) * bucket_years
            + start_year
        )
        .cast(pl.Int32)
        .alias(column)
    )


def location_frame(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None = None,
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    frame = dataset.scan_fact("locations", playthrough_id=playthrough_id)
    frame = window(frame, start_date=start_date, end_date=end_date)
    return with_dimensions(dataset, frame, ("locations", "markets"))


def location_latest_rank(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    group_by: str | Sequence[str],
    metric: str,
    statistic: str = "sum",
    start_date: int | None = None,
    end_date: int | None = None,
    snapshot_date: int | None = None,
    limit: int | None = 25,
) -> pl.LazyFrame:
    return latest_rank(
        location_frame(dataset, playthrough_id=playthrough_id, start_date=start_date, end_date=end_date),
        group_by=group_by,
        metric=metric,
        statistic=statistic,
        snapshot_date=snapshot_date,
        limit=limit,
    )


def location_time_series(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    group_by: str | Sequence[str],
    metric: str,
    statistic: str = "sum",
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    return time_series(
        location_frame(dataset, playthrough_id=playthrough_id, start_date=start_date, end_date=end_date),
        group_by=group_by,
        metric=metric,
        statistic=statistic,
    )


def location_global_time_series(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    frame = location_frame(dataset, playthrough_id=playthrough_id, start_date=start_date, end_date=end_date)
    required = {"date_sort", "total_population", "development", "tax", "control"}
    if not required.issubset(schema_names(frame)):
        return empty_lazy(
            {
                "date_sort": pl.UInt32,
                "total_population": pl.Float32,
                "development": pl.Float32,
                "tax": pl.Float32,
                "mean_control": pl.Float32,
            }
        )
    return (
        frame.group_by("date_sort")
        .agg(
            pl.first("year").alias("year") if "year" in schema_names(frame) else pl.lit(None).alias("year"),
            pl.sum("total_population").alias("total_population"),
            pl.sum("development").alias("development"),
            pl.sum("tax").alias("tax"),
            pl.mean("control").alias("mean_control"),
        )
        .sort("date_sort")
    )


def location_first_last_delta(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    group_by: str | Sequence[str],
    metric: str,
    statistic: str = "sum",
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    return first_last_delta(
        location_frame(dataset, playthrough_id=playthrough_id, start_date=start_date, end_date=end_date),
        group_by=group_by,
        metric=metric,
        statistic=statistic,
    )


def market_goods_frame(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None = None,
    goods: object | Sequence[object] | None = None,
    good_query: object | None = None,
    market_ids: object | Sequence[object] | None = None,
    market_query: object | None = None,
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    frame = dataset.scan_fact("market_goods", playthrough_id=playthrough_id)
    frame = window(frame, start_date=start_date, end_date=end_date)
    good_codes = _codes_for_filter(dataset, "goods", values=goods, query=good_query)
    if good_codes is not None and "good_code" in schema_names(frame):
        frame = frame.filter(pl.col("good_code").is_in(good_codes))
    market_codes = _codes_for_filter(dataset, "markets", values=market_ids, query=market_query)
    if market_codes is not None and "market_code" in schema_names(frame):
        frame = frame.filter(pl.col("market_code").is_in(market_codes))
    return with_dimensions(dataset, frame, ("goods", "markets"))


def goods_global_time_series(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    goods: object | Sequence[object] | None = None,
    good_query: object | None = None,
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    frame = market_goods_frame(
        dataset,
        playthrough_id=playthrough_id,
        goods=goods,
        good_query=good_query,
        start_date=start_date,
        end_date=end_date,
    )
    if not has_columns(frame, "date_sort", "good_label", "supply", "demand", "price"):
        return empty_lazy(
            {
                "date_sort": pl.UInt32,
                "year": pl.UInt16,
                "good_id": pl.String,
                "good_label": pl.String,
                "supply": pl.Float32,
                "demand": pl.Float32,
                "net": pl.Float32,
                "stockpile": pl.Float32,
                "mean_price": pl.Float32,
                "median_price": pl.Float32,
                "price_p10": pl.Float32,
                "price_p90": pl.Float32,
            }
        )
    stockpile_expr = (
        pl.sum("stockpile").alias("stockpile")
        if "stockpile" in schema_names(frame)
        else pl.lit(None, dtype=pl.Float32).alias("stockpile")
    )
    return (
        frame.group_by("date_sort", "good_id", "good_label")
        .agg(
            pl.first("year").alias("year") if "year" in schema_names(frame) else pl.lit(None).alias("year"),
            pl.sum("supply").alias("supply"),
            pl.sum("demand").alias("demand"),
            stockpile_expr,
            pl.mean("price").alias("mean_price"),
            pl.median("price").alias("median_price"),
            pl.col("price").quantile(0.10).alias("price_p10"),
            pl.col("price").quantile(0.90).alias("price_p90"),
        )
        .with_columns((pl.col("supply") - pl.col("demand")).alias("net"))
        .sort("date_sort", "good_id")
    )


def market_shortage_glut(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    goods: object | Sequence[object] | None = None,
    good_query: object | None = None,
    snapshot_date: int | None = None,
    min_flow: float = 0.01,
) -> pl.LazyFrame:
    frame = latest_snapshot(
        market_goods_frame(dataset, playthrough_id=playthrough_id, goods=goods, good_query=good_query),
        snapshot_date=snapshot_date,
    )
    groups = [
        column
        for column in ("good_id", "good_label", "market_id", "market_label")
        if column in schema_names(frame)
    ]
    if not groups or not has_columns(frame, "supply", "demand"):
        return empty_lazy(
            {
                "good_id": pl.String,
                "good_label": pl.String,
                "market_id": pl.Int64,
                "market_label": pl.String,
                "supply": pl.Float32,
                "demand": pl.Float32,
                "net": pl.Float32,
                "supply_demand_ratio": pl.Float32,
                "scarcity_rank": pl.UInt32,
            }
        )
    return (
        frame.group_by(groups)
        .agg(
            pl.sum("supply").alias("supply"),
            pl.sum("demand").alias("demand"),
            pl.mean("price").alias("price") if "price" in schema_names(frame) else pl.lit(None).alias("price"),
        )
        .filter((pl.col("supply").fill_null(0) + pl.col("demand").fill_null(0)) >= min_flow)
        .with_columns(
            (pl.col("supply") - pl.col("demand")).alias("net"),
            pl.when(pl.col("demand") > 0)
            .then(pl.col("supply") / pl.col("demand"))
            .otherwise(None)
            .alias("supply_demand_ratio"),
        )
        .with_columns(pl.col("supply_demand_ratio").rank("ordinal").over("good_id").alias("scarcity_rank"))
        .sort(["good_id", "supply_demand_ratio"], nulls_last=True)
    )


def goods_imbalance_buckets(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    bucket_years: int,
    sort_by: str = "mean_flow",
    start_year: int = 1337,
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    frame = add_year_bucket(
        market_goods_frame(
            dataset,
            playthrough_id=playthrough_id,
            start_date=start_date,
            end_date=end_date,
        ),
        bucket_years=bucket_years,
        start_year=start_year,
    )
    if not has_columns(frame, "bucket_start", "good_label", "supply", "demand"):
        return empty_lazy(
            {
                "bucket_start": pl.Int32,
                "good_id": pl.String,
                "good_label": pl.String,
                "supply": pl.Float32,
                "demand": pl.Float32,
                "net": pl.Float32,
                "imbalance_percent": pl.Float32,
                "flow": pl.Float32,
                "market_cap": pl.Float32,
                "mean_flow": pl.Float32,
                "total_flow": pl.Float32,
                "mean_market_cap": pl.Float32,
                "total_market_cap": pl.Float32,
            }
        )
    price_expr = (
        ((pl.col("supply").fill_null(0) + pl.col("demand").fill_null(0)) * pl.col("price").fill_null(0)).sum()
        if "price" in schema_names(frame)
        else pl.lit(0.0, dtype=pl.Float32)
    )
    bucketed = (
        frame.group_by("bucket_start", "good_id", "good_label")
        .agg(
            pl.sum("supply").alias("supply"),
            pl.sum("demand").alias("demand"),
            price_expr.alias("market_cap"),
        )
        .with_columns(
            (pl.col("supply") - pl.col("demand")).alias("net"),
            (pl.col("supply").abs() + pl.col("demand").abs()).alias("flow"),
            pl.when((pl.col("supply") + pl.col("demand")) > 0)
            .then(100 * (pl.col("supply") - pl.col("demand")) / (pl.col("supply") + pl.col("demand")))
            .otherwise(None)
            .alias("imbalance_percent"),
        )
    )
    stats = bucketed.group_by("good_id").agg(
        pl.mean("flow").alias("mean_flow"),
        pl.sum("flow").alias("total_flow"),
        pl.mean("market_cap").alias("mean_market_cap"),
        pl.sum("market_cap").alias("total_market_cap"),
    )
    sort_column = sort_by if sort_by in {"mean_flow", "total_flow", "mean_market_cap", "total_market_cap"} else "mean_flow"
    return (
        bucketed.join(stats, on="good_id", how="left")
        .sort([sort_column, "good_label", "bucket_start"], descending=[True, False, False])
    )


def market_flow_proxy(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    group_by: str | Sequence[str] = "good_id",
    start_date: int | None = None,
    end_date: int | None = None,
    limit: int | None = None,
) -> pl.LazyFrame:
    groups = normalize_group_by(group_by)
    frame = market_goods_frame(
        dataset,
        playthrough_id=playthrough_id,
        start_date=start_date,
        end_date=end_date,
    )
    if not has_columns(frame, "date_sort", "supply", "demand", "price", *groups):
        return empty_lazy({"date_sort": pl.UInt32, **_schema_for_groups(groups, "flow_value")})
    proxy = frame.with_columns(
        pl.when(pl.col("supply").fill_null(0) < pl.col("demand").fill_null(0))
        .then(pl.col("supply").fill_null(0))
        .otherwise(pl.col("demand").fill_null(0))
        .alias("_cleared_flow")
    ).with_columns((pl.col("_cleared_flow") * pl.col("price").fill_null(0)).alias("_flow_value"))
    result = (
        proxy.group_by(["date_sort", *groups])
        .agg(
            pl.first("year").alias("year") if "year" in schema_names(proxy) else pl.lit(None).alias("year"),
            pl.sum("_cleared_flow").alias("cleared_flow"),
            pl.sum("_flow_value").alias("flow_value"),
        )
        .sort("flow_value", descending=True)
    )
    if limit is not None:
        result = result.limit(limit)
    return result


def good_flow_frame(
    dataset: SavegameNotebookDataset,
    *,
    good: object | None,
    direction: str,
    playthrough_id: str | None = None,
    market_query: object | None = None,
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    if good is None:
        return pl.DataFrame().lazy()
    if direction == "source":
        frame = dataset.scan_good_sources(str(good), playthrough_id=playthrough_id)
    elif direction == "sink":
        frame = dataset.scan_good_sinks(str(good), playthrough_id=playthrough_id)
    else:
        raise ValueError("direction must be one of: source, sink")
    frame = window(frame, start_date=start_date, end_date=end_date)
    frame = with_dimensions(dataset, frame)
    amount_columns = [column for column in ("amount", "allocated_amount") if column in schema_names(frame)]
    if amount_columns:
        frame = frame.with_columns(
            pl.sum_horizontal(*[pl.col(column).fill_null(0) for column in amount_columns]).alias("flow_amount")
        )
    else:
        frame = frame.with_columns(pl.lit(0.0, dtype=pl.Float32).alias("flow_amount"))
    market_codes = _codes_for_filter(dataset, "markets", query=market_query)
    if market_codes is not None and "market_code" in schema_names(frame):
        frame = frame.filter(pl.col("market_code").is_in(market_codes))
    return frame


def good_flow_breakdown(
    dataset: SavegameNotebookDataset,
    *,
    good: object | None,
    direction: str,
    playthrough_id: str | None,
    group_by: str | Sequence[str],
    market_query: object | None = None,
    start_date: int | None = None,
    end_date: int | None = None,
    snapshot_date: int | None = None,
    limit: int | None = 25,
) -> pl.LazyFrame:
    groups = normalize_group_by(group_by)
    frame = latest_snapshot(
        good_flow_frame(
            dataset,
            good=good,
            direction=direction,
            playthrough_id=playthrough_id,
            market_query=market_query,
            start_date=start_date,
            end_date=end_date,
        ),
        snapshot_date=snapshot_date,
    )
    if not has_columns(frame, "flow_amount", *groups):
        return empty_lazy(_schema_for_groups(groups, "amount"))
    result = frame.group_by(groups).agg(pl.sum("flow_amount").alias("amount")).sort(
        "amount",
        descending=True,
    )
    if limit is not None:
        result = result.limit(limit)
    return result


def good_flow_time_series(
    dataset: SavegameNotebookDataset,
    *,
    good: object | None,
    direction: str,
    playthrough_id: str | None,
    group_by: str | Sequence[str],
    market_query: object | None = None,
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    return time_series(
        good_flow_frame(
            dataset,
            good=good,
            direction=direction,
            playthrough_id=playthrough_id,
            market_query=market_query,
            start_date=start_date,
            end_date=end_date,
        ),
        group_by=group_by,
        metric="flow_amount",
        statistic="sum",
        value_name="amount",
    )


def good_consumption_latest(
    dataset: SavegameNotebookDataset,
    *,
    good: object | None,
    playthrough_id: str | None,
    group_by: str | Sequence[str] = "consumption",
    market_query: object | None = None,
    start_date: int | None = None,
    end_date: int | None = None,
    snapshot_date: int | None = None,
    limit: int | None = 25,
) -> pl.LazyFrame:
    groups = _consumption_groups(group_by)
    frame = latest_snapshot(
        good_consumption_frame(
            dataset,
            good=good,
            playthrough_id=playthrough_id,
            market_query=market_query,
            start_date=start_date,
            end_date=end_date,
        ),
        snapshot_date=snapshot_date,
    )
    if not has_columns(frame, "amount", *groups):
        return empty_lazy({**_schema_for_groups(groups, "amount"), "share": pl.Float32})
    grouped = frame.group_by(groups).agg(pl.sum("amount").alias("amount"))
    total = grouped.select(pl.sum("amount").alias("_total_amount"))
    result = (
        grouped.join(total, how="cross")
        .with_columns(
            pl.when(pl.col("_total_amount") > 0)
            .then(pl.col("amount") / pl.col("_total_amount"))
            .otherwise(None)
            .alias("share")
        )
        .drop("_total_amount")
        .sort("amount", descending=True)
    )
    if limit is not None:
        result = result.limit(limit)
    return result


def good_consumption_over_time(
    dataset: SavegameNotebookDataset,
    *,
    good: object | None,
    playthrough_id: str | None,
    group_by: str | Sequence[str] = "consumption",
    market_query: object | None = None,
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    groups = _consumption_groups(group_by)
    frame = good_consumption_frame(
        dataset,
        good=good,
        playthrough_id=playthrough_id,
        market_query=market_query,
        start_date=start_date,
        end_date=end_date,
    )
    if not has_columns(frame, "date_sort", "amount", *groups):
        return empty_lazy({"date_sort": pl.UInt32, "year": pl.UInt16, **_schema_for_groups(groups, "amount")})
    return (
        frame.group_by("date_sort", *groups)
        .agg(
            pl.first("year").alias("year") if "year" in schema_names(frame) else pl.lit(None).alias("year"),
            pl.sum("amount").alias("amount"),
        )
        .sort(["date_sort", *groups])
    )


def good_consumption_frame(
    dataset: SavegameNotebookDataset,
    *,
    good: object | None,
    playthrough_id: str | None,
    market_query: object | None = None,
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    frame = good_flow_frame(
        dataset,
        good=good,
        direction="sink",
        playthrough_id=playthrough_id,
        market_query=market_query,
        start_date=start_date,
        end_date=end_date,
    )
    if not schema_names(frame):
        return empty_lazy(
            {
                "date_sort": pl.UInt32,
                "year": pl.UInt16,
                "flow_table": pl.String,
                "consumption_label": pl.String,
                "amount": pl.Float32,
            }
        )
    names = schema_names(frame)
    amount = pl.col("flow_amount").fill_null(0) if "flow_amount" in names else pl.lit(0.0, dtype=pl.Float32)
    bucket = pl.col("bucket").cast(pl.String) if "bucket" in names else pl.lit(None, dtype=pl.String)
    pm = (
        pl.col("production_method_label").cast(pl.String)
        if "production_method_label" in names
        else pl.lit(None, dtype=pl.String)
    )
    building = pl.col("building_label").cast(pl.String) if "building_label" in names else pl.lit(None, dtype=pl.String)
    fallback = (
        pl.when(pm.is_not_null())
        .then(pl.concat_str([pl.lit("PM input: "), pm]))
        .when(building.is_not_null())
        .then(pl.concat_str([pl.lit("Building input: "), building]))
        .otherwise(pl.lit("Unclassified consumption"))
    )
    return frame.with_columns(
        amount.alias("amount"),
        pl.when(bucket.is_not_null())
        .then(bucket)
        .otherwise(fallback)
        .alias("consumption_label"),
    )


def food_frame(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None = None,
    market_query: object | None = None,
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    frame = dataset.scan_fact("market_food", playthrough_id=playthrough_id)
    frame = window(frame, start_date=start_date, end_date=end_date)
    market_codes = _codes_for_filter(dataset, "markets", query=market_query)
    if market_codes is not None and "market_code" in schema_names(frame):
        frame = frame.filter(pl.col("market_code").is_in(market_codes))
    frame = with_dimensions(dataset, frame, ("markets",))
    names = schema_names(frame)
    if {"food", "food_max"}.issubset(names):
        fill_expr = pl.when(pl.col("food_max") > 0).then(pl.col("food") / pl.col("food_max")).otherwise(None)
    elif "food_fill_percent" in names:
        fill_expr = (
            pl.when(pl.col("food_fill_percent") > 1)
            .then(pl.col("food_fill_percent") / 100)
            .otherwise(pl.col("food_fill_percent"))
        )
    else:
        fill_expr = pl.lit(None, dtype=pl.Float32)
    return frame.with_columns(fill_expr.alias("food_fill_ratio"))


def food_market_rank(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    rank_by: str,
    market_query: object | None = None,
    snapshot_date: int | None = None,
    largest: bool = False,
    limit: int | None = 25,
) -> pl.LazyFrame:
    frame = latest_snapshot(
        food_frame(dataset, playthrough_id=playthrough_id, market_query=market_query),
        snapshot_date=snapshot_date,
    )
    groups = [column for column in ("market_id", "market_label") if column in schema_names(frame)]
    if rank_by not in schema_names(frame) or not groups:
        return empty_lazy({"market_id": pl.Int64, "market_label": pl.String, rank_by: pl.Float32})
    sum_metrics = [column for column in ("food", "food_max", "food_balance", "population", "capacity") if column in schema_names(frame)]
    mean_metrics = [column for column in ("food_price", "food_fill_ratio") if column in schema_names(frame)]
    result = frame.group_by(groups).agg(
        [pl.sum(column).alias(column) for column in sum_metrics]
        + [pl.mean(column).alias(column) for column in mean_metrics]
    )
    if rank_by in schema_names(result):
        result = result.sort(rank_by, descending=largest)
    if limit is not None:
        result = result.limit(limit)
    return result


def food_global_stockpile(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    frame = food_frame(dataset, playthrough_id=playthrough_id, start_date=start_date, end_date=end_date)
    if not has_columns(frame, "date_sort", "food", "food_max"):
        return empty_lazy({"date_sort": pl.UInt32, "food": pl.Float32, "food_max": pl.Float32, "food_fill_ratio": pl.Float32})
    return (
        frame.group_by("date_sort")
        .agg(
            pl.first("year").alias("year") if "year" in schema_names(frame) else pl.lit(None).alias("year"),
            pl.sum("food").alias("food"),
            pl.sum("food_max").alias("food_max"),
            pl.sum("food_balance").alias("food_balance") if "food_balance" in schema_names(frame) else pl.lit(None).alias("food_balance"),
        )
        .with_columns(
            pl.when(pl.col("food_max") > 0).then(pl.col("food") / pl.col("food_max")).otherwise(None).alias("food_fill_ratio")
        )
        .sort("date_sort")
    )


def food_price_distribution(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    frame = food_frame(dataset, playthrough_id=playthrough_id, start_date=start_date, end_date=end_date)
    if not has_columns(frame, "date_sort", "food_price"):
        return empty_lazy({"date_sort": pl.UInt32, "price_p10": pl.Float32, "price_p50": pl.Float32, "price_p90": pl.Float32})
    return (
        frame.group_by("date_sort")
        .agg(
            pl.first("year").alias("year") if "year" in schema_names(frame) else pl.lit(None).alias("year"),
            pl.col("food_price").quantile(0.10).alias("price_p10"),
            pl.median("food_price").alias("price_p50"),
            pl.col("food_price").quantile(0.90).alias("price_p90"),
        )
        .sort("date_sort")
    )


def food_first_last_delta(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    metric: str,
    group_by: str | Sequence[str] = "market_id",
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    return first_last_delta(
        food_frame(dataset, playthrough_id=playthrough_id, start_date=start_date, end_date=end_date),
        group_by=group_by,
        metric=metric,
        statistic="mean" if metric in {"food_price", "food_fill_ratio"} else "sum",
    )


def buildings_frame(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None = None,
    building_query: object | None = None,
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    frame = dataset.scan_fact("buildings", playthrough_id=playthrough_id)
    frame = window(frame, start_date=start_date, end_date=end_date)
    building_codes = _codes_for_filter(dataset, "building_types", query=building_query)
    if building_codes is not None and "building_type_code" in schema_names(frame):
        frame = frame.filter(pl.col("building_type_code").is_in(building_codes))
    return with_dimensions(dataset, frame, ("building_types", "locations", "markets"))


def building_metric_time_series(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    metric: str,
    group_by: str | Sequence[str],
    statistic: str = "sum",
    building_query: object | None = None,
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    return time_series(
        buildings_frame(
            dataset,
            playthrough_id=playthrough_id,
            building_query=building_query,
            start_date=start_date,
            end_date=end_date,
        ),
        group_by=group_by,
        metric=metric,
        statistic=statistic,
    )


def building_latest_rank(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    metric: str,
    group_by: str | Sequence[str],
    statistic: str = "sum",
    building_query: object | None = None,
    snapshot_date: int | None = None,
    limit: int | None = 25,
) -> pl.LazyFrame:
    return latest_rank(
        buildings_frame(dataset, playthrough_id=playthrough_id, building_query=building_query),
        group_by=group_by,
        metric=metric,
        statistic=statistic,
        snapshot_date=snapshot_date,
        limit=limit,
    )


def building_methods_frame(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None = None,
    building_query: object | None = None,
    pm_query: object | None = None,
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    frame = dataset.scan_fact("building_methods", playthrough_id=playthrough_id)
    frame = window(frame, start_date=start_date, end_date=end_date)
    building_codes = _codes_for_filter(dataset, "building_types", query=building_query)
    if building_codes is not None and "building_type_code" in schema_names(frame):
        frame = frame.filter(pl.col("building_type_code").is_in(building_codes))
    pm_codes = _codes_for_filter(dataset, "production_methods", query=pm_query)
    if pm_codes is not None and "production_method_code" in schema_names(frame):
        frame = frame.filter(pl.col("production_method_code").is_in(pm_codes))
    return with_dimensions(dataset, frame, ("building_types", "production_methods", "locations"))


def pm_adoption_over_time(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    building_query: object | None = None,
    pm_query: object | None = None,
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    frame = building_methods_frame(
        dataset,
        playthrough_id=playthrough_id,
        building_query=building_query,
        pm_query=pm_query,
        start_date=start_date,
        end_date=end_date,
    )
    if not has_columns(frame, "date_sort", "building_label", "production_method_label"):
        return empty_lazy({"date_sort": pl.UInt32, "year": pl.UInt16, "building_label": pl.String, "production_method_label": pl.String, "buildings": pl.UInt32, "share": pl.Float32})
    return (
        frame.group_by("date_sort", "building_label", "production_method_label")
        .agg(pl.len().alias("buildings"))
        .join(
            frame.group_by("date_sort").agg(
                pl.first("year").alias("year") if "year" in schema_names(frame) else pl.lit(None).alias("year")
            ),
            on="date_sort",
            how="left",
        )
        .with_columns((pl.col("buildings") / pl.sum("buildings").over("date_sort", "building_label")).alias("share"))
        .sort("date_sort", "building_label", "production_method_label")
    )


def pm_regional_preferences(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    building_query: object | None = None,
    pm_query: object | None = None,
    group_by: str | Sequence[str] = "region",
    snapshot_date: int | None = None,
    limit: int | None = None,
) -> pl.LazyFrame:
    scope_groups = normalize_group_by(group_by)
    frame = latest_snapshot(
        building_methods_frame(
            dataset,
            playthrough_id=playthrough_id,
            building_query=building_query,
            pm_query=pm_query,
        ),
        snapshot_date=snapshot_date,
    )
    groups = [
        *scope_groups,
        *[
            column
            for column in (
                "building_label",
                "slot_label",
                "production_method_label",
                "production_method_group_index",
            )
            if column in schema_names(frame)
        ],
    ]
    required = {"building_label", "production_method_label", *scope_groups}
    if not groups or not required.issubset(schema_names(frame)):
        return empty_lazy(
            {
                **_schema_for_groups(scope_groups, "buildings"),
                "building_label": pl.String,
                "slot_label": pl.String,
                "production_method_label": pl.String,
                "production_method_group_index": pl.Int64,
                "share": pl.Float32,
            }
        )
    denominator_groups = [
        column for column in [*scope_groups, "building_label", "slot_label"] if column in groups
    ]
    result = (
        frame.group_by(groups)
        .agg(pl.len().alias("buildings"))
        .with_columns((pl.col("buildings") / pl.sum("buildings").over(denominator_groups)).alias("share"))
        .sort([*denominator_groups, "share", "production_method_label"], descending=[False] * len(denominator_groups) + [True, False])
    )
    if limit is not None:
        result = result.limit(limit)
    return result


def pm_slot_distribution_latest(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    building_query: object | None = None,
    pm_query: object | None = None,
    snapshot_date: int | None = None,
    limit: int | None = None,
) -> pl.LazyFrame:
    frame = latest_snapshot(
        building_methods_frame(
            dataset,
            playthrough_id=playthrough_id,
            building_query=building_query,
            pm_query=pm_query,
        ),
        snapshot_date=snapshot_date,
    )
    groups = _pm_slot_groups(frame)
    if not groups:
        return _empty_pm_slot_distribution()
    sort_columns = _pm_slot_sort_columns(groups)
    result = (
        frame.group_by(groups)
        .agg(
            pl.first("year").alias("year") if "year" in schema_names(frame) else pl.lit(None).alias("year"),
            pl.len().alias("buildings"),
        )
        .with_columns(
            (
                pl.col("buildings")
                / pl.sum("buildings").over("building_label", "slot_label")
            ).alias("share")
        )
        .sort(sort_columns, descending=[column == "share" for column in sort_columns])
    )
    if limit is not None:
        result = result.limit(limit)
    return result


def pm_slot_distribution_over_time(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    building_query: object | None = None,
    pm_query: object | None = None,
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    frame = building_methods_frame(
        dataset,
        playthrough_id=playthrough_id,
        building_query=building_query,
        pm_query=pm_query,
        start_date=start_date,
        end_date=end_date,
    )
    groups = _pm_slot_groups(frame)
    if not groups or not has_columns(frame, "date_sort"):
        return _empty_pm_slot_distribution(include_date=True)
    return (
        frame.group_by("date_sort", *groups)
        .agg(
            pl.first("year").alias("year") if "year" in schema_names(frame) else pl.lit(None).alias("year"),
            pl.len().alias("buildings"),
        )
        .with_columns(
            (
                pl.col("buildings")
                / pl.sum("buildings").over("date_sort", "building_label", "slot_label")
            ).alias("share")
        )
        .sort(["date_sort", *_pm_slot_sort_columns(groups)])
    )


def pm_good_flow_time_series(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    good: object | None = None,
    building_query: object | None = None,
    pm_query: object | None = None,
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    frame = _pm_flow_frame(
        dataset,
        playthrough_id=playthrough_id,
        good=good,
        building_query=building_query,
        pm_query=pm_query,
        start_date=start_date,
        end_date=end_date,
    )
    if not has_columns(frame, "date_sort", "direction", "good_label", "building_label", "production_method_label", "allocated_amount"):
        return empty_lazy(
            {
                "date_sort": pl.UInt32,
                "year": pl.UInt16,
                "direction": pl.String,
                "good_label": pl.String,
                "building_label": pl.String,
                "production_method_label": pl.String,
                "amount": pl.Float32,
            }
        )
    return (
        frame.group_by("date_sort", "direction", "good_label", "building_label", "production_method_label")
        .agg(
            pl.first("year").alias("year") if "year" in schema_names(frame) else pl.lit(None).alias("year"),
            pl.sum("allocated_amount").alias("amount"),
            pl.sum("level_sum").alias("level_sum") if "level_sum" in schema_names(frame) else pl.lit(None).alias("level_sum"),
        )
        .sort("date_sort", "direction", "good_label", "building_label", "production_method_label")
    )


def pm_value_mix(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    good: object | None = None,
    building_query: object | None = None,
    pm_query: object | None = None,
    snapshot_date: int | None = None,
) -> pl.LazyFrame:
    flows = latest_snapshot(
        _pm_flow_frame(
            dataset,
            playthrough_id=playthrough_id,
            good=good,
            building_query=building_query,
            pm_query=pm_query,
        ),
        snapshot_date=snapshot_date,
    )
    if not has_columns(flows, "snapshot_id", "playthrough_id", "market_code", "good_code", "allocated_amount"):
        return empty_lazy(
            {
                "direction": pl.String,
                "good_label": pl.String,
                "building_label": pl.String,
                "production_method_label": pl.String,
                "amount": pl.Float32,
                "market_value": pl.Float32,
                "default_value": pl.Float32,
            }
        )
    prices = dataset.scan_fact("market_goods", playthrough_id=playthrough_id)
    good_codes = _codes_for_filter(dataset, "goods", values=good)
    if good_codes is not None and "good_code" in schema_names(prices):
        prices = prices.filter(pl.col("good_code").is_in(good_codes))
    prices = latest_snapshot(prices, snapshot_date=snapshot_date).select(
        "snapshot_id",
        "playthrough_id",
        "market_code",
        "good_code",
        "price",
        "default_price",
    )
    joined = flows.join(
        prices,
        on=["snapshot_id", "playthrough_id", "market_code", "good_code"],
        how="left",
    )
    groups = [
        column
        for column in ("direction", "good_label", "building_label", "production_method_label")
        if column in schema_names(joined)
    ]
    return (
        joined.with_columns(
            pl.col("allocated_amount").fill_null(0).alias("_amount"),
            (pl.col("allocated_amount").fill_null(0) * pl.col("price").fill_null(0)).alias("_market_value"),
            (pl.col("allocated_amount").fill_null(0) * pl.col("default_price").fill_null(0)).alias("_default_value"),
        )
        .group_by(groups)
        .agg(
            pl.sum("_amount").alias("amount"),
            pl.sum("_market_value").alias("market_value"),
            pl.sum("_default_value").alias("default_value"),
        )
        .sort("market_value", descending=True)
    )


def _pm_flow_frame(
    dataset: SavegameNotebookDataset,
    *,
    playthrough_id: str | None,
    good: object | None = None,
    building_query: object | None = None,
    pm_query: object | None = None,
    start_date: int | None = None,
    end_date: int | None = None,
) -> pl.LazyFrame:
    frame = dataset.scan_fact(
        "production_method_good_flows",
        playthrough_id=playthrough_id,
        good_id=str(good) if isinstance(good, str) else None,
    )
    frame = window(frame, start_date=start_date, end_date=end_date)
    good_codes = _codes_for_filter(dataset, "goods", values=good)
    if good_codes is not None and "good_code" in schema_names(frame):
        frame = frame.filter(pl.col("good_code").is_in(good_codes))
    building_codes = _codes_for_filter(dataset, "building_types", query=building_query)
    if building_codes is not None and "building_type_code" in schema_names(frame):
        frame = frame.filter(pl.col("building_type_code").is_in(building_codes))
    pm_codes = _codes_for_filter(dataset, "production_methods", query=pm_query)
    if pm_codes is not None and "production_method_code" in schema_names(frame):
        frame = frame.filter(pl.col("production_method_code").is_in(pm_codes))
    return with_dimensions(dataset, frame)


def _stat_expr(metric: str, statistic: str) -> pl.Expr:
    if statistic == "sum":
        return pl.sum(metric)
    if statistic == "mean":
        return pl.mean(metric)
    if statistic == "median":
        return pl.median(metric)
    raise ValueError("statistic must be one of: sum, mean, median")


def _schema_for_groups(groups: Sequence[str], metric: str) -> dict[str, pl.DataType]:
    schema: dict[str, pl.DataType] = {group: pl.String for group in groups}
    schema[metric] = pl.Float32
    return schema


def _codes_for_filter(
    dataset: SavegameNotebookDataset,
    dimension: str,
    *,
    values: object | Sequence[object] | None = None,
    query: object | None = None,
) -> list[int] | None:
    if values is None and (query is None or str(query).strip() == ""):
        return None
    return resolve_codes(dataset, dimension, values, query=query)


def _searchable_columns(frame: pl.DataFrame) -> list[str]:
    labels = [column for column in frame.columns if column.endswith("_label")]
    rest = [
        column
        for column in frame.columns
        if column not in labels and not column.endswith("_code")
    ]
    return [*labels, *rest]


def _pm_slot_groups(frame: pl.LazyFrame) -> list[str]:
    names = schema_names(frame)
    required = {"building_label", "slot_label", "production_method_label"}
    if not required.issubset(names):
        return []
    optional = [
        column
        for column in (
            "building_type",
            "production_method_group_index",
            "production_method",
        )
        if column in names
    ]
    return [
        "building_label",
        "slot_label",
        "production_method_label",
        *optional,
    ]


def _pm_slot_sort_columns(groups: Sequence[str]) -> list[str]:
    return [
        column
        for column in (
            "building_label",
            "production_method_group_index",
            "slot_label",
            "share",
            "production_method_label",
        )
        if column in groups or column == "share"
    ]


def _consumption_groups(group_by: str | Sequence[str]) -> list[str]:
    groups = []
    for group in _as_str_list(group_by):
        if group == "bucket":
            groups.append("consumption_label")
        else:
            groups.extend(normalize_group_by(group))
    return list(dict.fromkeys(groups))


def _empty_pm_slot_distribution(*, include_date: bool = False) -> pl.LazyFrame:
    schema: dict[str, pl.DataType] = {
        "building_label": pl.String,
        "slot_label": pl.String,
        "production_method_label": pl.String,
        "production_method_group_index": pl.Int64,
        "buildings": pl.UInt32,
        "share": pl.Float32,
    }
    if include_date:
        schema = {"date_sort": pl.UInt32, "year": pl.UInt16, **schema}
    else:
        schema = {"year": pl.UInt16, **schema}
    return empty_lazy(schema)


def _as_list(value: object | Sequence[object]) -> list[object]:
    if isinstance(value, str) or not isinstance(value, Sequence):
        return [value]
    return list(value)


def _as_str_list(value: str | Sequence[str]) -> list[str]:
    if isinstance(value, str):
        return [value]
    return list(value)
