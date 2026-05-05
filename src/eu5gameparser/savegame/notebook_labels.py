from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from eu5gameparser.load_order import (
    DEFAULT_LOAD_ORDER_PATH,
    DataProfile,
    GameLayer,
    load_profile,
)


_LOCALIZATION_RE = re.compile(
    r'^\s*([A-Za-z0-9_.:-]+):(?:\d+)?\s+"((?:[^"\\]|\\.)*)"', re.MULTILINE
)
_LOCALIZATION_REFERENCE_RE = re.compile(r"\$([^$]+)\$")


@dataclass(frozen=True)
class NotebookLabelResolver:
    localization: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_profile(
        cls,
        *,
        profile: str | DataProfile | None = None,
        load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
    ) -> NotebookLabelResolver:
        if profile is None:
            return cls()
        data_profile = profile if isinstance(profile, DataProfile) else load_profile(profile, load_order_path)
        localization: dict[str, str] = {}
        for layer in data_profile.layers:
            localization.update(_load_layer_localization(layer))
        return cls(localization=localization)

    def label(self, key: object, *, fallback: object | None = None) -> str:
        text = "" if key is None else str(key)
        if text in self.localization:
            return self._localized_text(text)
        fallback_text = "" if fallback is None else str(fallback)
        if fallback_text and fallback_text in self.localization:
            return self._localized_text(fallback_text)
        return _titleize(fallback_text or text)

    def _localized_text(self, key: str, seen: frozenset[str] = frozenset()) -> str:
        raw = self.localization.get(key)
        if raw is None:
            return _titleize(key)
        if key in seen:
            return _titleize(key)
        return _clean_display(
            _LOCALIZATION_REFERENCE_RE.sub(
                lambda match: self._localized_text(match.group(1), seen | {key}),
                raw,
            )
        )


def enrich_notebook_dimensions(
    dimensions: dict[str, pl.DataFrame],
    *,
    resolver: NotebookLabelResolver,
) -> dict[str, pl.DataFrame]:
    result = dict(dimensions)
    for name, frame in list(result.items()):
        if frame.is_empty():
            continue
        if name == "goods":
            result[name] = _with_label(frame, "good_id", "good_label", resolver)
        elif name == "building_types":
            result[name] = _with_label(frame, "building_type", "building_label", resolver)
        elif name == "production_methods":
            result[name] = _with_label(
                frame,
                "production_method",
                "production_method_label",
                resolver,
            )
        elif name == "countries":
            result[name] = _with_label(
                frame,
                "country_tag",
                "country_label",
                resolver,
                fallback_column="country_name",
            )
        elif name in {"areas", "regions", "macro_regions", "super_regions"}:
            key = str(name[:-1] if name.endswith("s") else name)
            result[name] = _with_label(frame, key, f"{key}_label", resolver)
        elif name == "locations":
            result[name] = _with_location_labels(frame, resolver)

    if "markets" in result and not result["markets"].is_empty():
        result["markets"] = _with_market_labels(
            result["markets"],
            result.get("locations", pl.DataFrame()),
            resolver,
        )
    return result


def _with_label(
    frame: pl.DataFrame,
    key_column: str,
    label_column: str,
    resolver: NotebookLabelResolver,
    *,
    fallback_column: str | None = None,
) -> pl.DataFrame:
    if key_column not in frame.columns:
        return frame
    fallback = fallback_column if fallback_column in frame.columns else key_column
    if fallback == key_column:
        return frame.with_columns(
            pl.col(key_column)
            .map_elements(lambda value: resolver.label(value), return_dtype=pl.String)
            .alias(label_column)
        )
    return frame.with_columns(
        pl.struct([key_column, fallback])
        .map_elements(
            lambda row: resolver.label(row.get(key_column), fallback=row.get(fallback)),
            return_dtype=pl.String,
        )
        .alias(label_column)
    )


def _with_location_labels(frame: pl.DataFrame, resolver: NotebookLabelResolver) -> pl.DataFrame:
    additions: list[pl.Expr] = []
    if "slug" in frame.columns:
        additions.append(
            pl.col("slug")
            .map_elements(lambda value: resolver.label(value), return_dtype=pl.String)
            .alias("location_label")
        )
    for column in ("area", "region", "macro_region", "super_region"):
        if column in frame.columns:
            additions.append(
                pl.col(column)
                .map_elements(lambda value: resolver.label(value), return_dtype=pl.String)
                .alias(f"{column}_label")
            )
    if "country_tag" in frame.columns:
        fallback = "country_name" if "country_name" in frame.columns else "country_tag"
        if fallback == "country_tag":
            additions.append(
                pl.col("country_tag")
                .map_elements(lambda value: resolver.label(value), return_dtype=pl.String)
                .alias("country_label")
            )
        else:
            additions.append(
                pl.struct(["country_tag", fallback])
                .map_elements(
                    lambda row: resolver.label(row.get("country_tag"), fallback=row.get(fallback)),
                    return_dtype=pl.String,
                )
                .alias("country_label")
            )
    return frame.with_columns(additions) if additions else frame


def _with_market_labels(
    markets: pl.DataFrame,
    locations: pl.DataFrame,
    resolver: NotebookLabelResolver,
) -> pl.DataFrame:
    if "market_code" not in markets.columns:
        return markets
    location_labels_by_slug = pl.DataFrame()
    if {"slug", "location_label"}.issubset(locations.columns):
        location_labels_by_slug = locations.select("slug", "location_label").unique("slug")
    location_labels_by_id = pl.DataFrame()
    if {"location_id", "location_label"}.issubset(locations.columns):
        location_labels_by_id = locations.select(
            "location_id",
            pl.col("location_label").alias("center_location_label"),
        ).unique("location_id")
    frame = markets
    if (
        not location_labels_by_slug.is_empty()
        and "market_center_slug" in markets.columns
        and "slug" in location_labels_by_slug.columns
    ):
        frame = frame.join(
            location_labels_by_slug,
            left_on="market_center_slug",
            right_on="slug",
            how="left",
        )
    if (
        not location_labels_by_id.is_empty()
        and "center_location_id" in markets.columns
        and "location_id" in location_labels_by_id.columns
    ):
        frame = frame.join(
            location_labels_by_id,
            left_on="center_location_id",
            right_on="location_id",
            how="left",
        )
    if "location_label" not in frame.columns:
        frame = frame.with_columns(pl.lit(None, dtype=pl.String).alias("location_label"))
    if "center_location_label" not in frame.columns:
        frame = frame.with_columns(pl.lit(None, dtype=pl.String).alias("center_location_label"))
    if "market_center_slug" not in frame.columns:
        frame = frame.with_columns(pl.lit(None, dtype=pl.String).alias("market_center_slug"))
    return frame.with_columns(
        pl.struct(["market_id", "market_center_slug", "location_label", "center_location_label"])
        .map_elements(
            lambda row: _market_label(row, resolver),
            return_dtype=pl.String,
        )
        .alias("market_label")
    ).drop("location_label", "center_location_label")


def _market_label(row: dict[str, object], resolver: NotebookLabelResolver) -> str:
    location_label = row.get("location_label")
    if location_label:
        return str(location_label)
    center_location_label = row.get("center_location_label")
    if center_location_label:
        return str(center_location_label)
    center = row.get("market_center_slug")
    if center:
        return resolver.label(center)
    market_id = row.get("market_id")
    return f"Market #{market_id}" if market_id is not None else "Market"


def _load_layer_localization(layer: GameLayer) -> dict[str, str]:
    result: dict[str, str] = {}
    for root in _localization_roots(layer):
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*.yml")):
            result.update(_parse_localization_file(path))
    return result


def _localization_roots(layer: GameLayer) -> tuple[Path, ...]:
    if layer.kind == "vanilla":
        return (
            layer.root / "game" / "in_game" / "localization" / "english",
            layer.root / "game" / "main_menu" / "localization" / "english",
            layer.root / "game" / "localization" / "english",
        )
    return (
        layer.root / "in_game" / "localization" / "english",
        layer.root / "main_menu" / "localization" / "english",
        layer.root / "localization" / "english",
    )


def _parse_localization_file(path: Path) -> dict[str, str]:
    try:
        text = path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError:
        text = path.read_text(encoding="cp1252")
    except OSError:
        return {}
    return {key: _clean_localization(value) for key, value in _LOCALIZATION_RE.findall(text)}


def _clean_localization(value: str) -> str:
    return _clean_display(value.replace(r"\"", '"').replace(r"\n", " "))


def _clean_display(value: str) -> str:
    text = value.strip()
    if text.startswith("'") and not text.endswith("'"):
        text = text[1:].strip()
    return text


def _titleize(value: str) -> str:
    text = value.strip()
    if not text:
        return ""
    return " ".join(part for part in text.replace("-", "_").split("_") if part).title()
