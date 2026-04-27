from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl

from eu5gameparser.clausewitz.syntax import CList, Scalar, Value
from eu5gameparser.config import ParserConfig
from eu5gameparser.load_order import (
    DEFAULT_LOAD_ORDER_PATH,
    DataProfile,
    GameLayer,
    MergedDirectory,
    MergedEntry,
    load_merged_directory,
    load_profile,
)

ADVANCEMENT_METADATA_KEYS = {
    "age",
    "ai_preference_tags",
    "ai_weight",
    "allow",
    "allow_children",
    "depth",
    "icon",
    "potential",
    "requires",
}


@dataclass(frozen=True)
class Advancement:
    name: str
    age: str | None
    icon: str | None
    requires: list[str]
    has_potential: bool
    unlocks: dict[str, list[str]]
    modifiers: dict[str, float]
    data: dict[str, Any]
    source_file: str
    source_line: int
    source_layer: str
    source_mod: str | None
    source_mode: str
    source_history: str


@dataclass(frozen=True)
class AdvancementData:
    advancements: pl.DataFrame
    warnings: list[str] = field(default_factory=list)


def load_advancement_data(
    config: ParserConfig | None = None,
    *,
    profile: str | DataProfile | None = None,
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
) -> AdvancementData:
    profile_config = _resolve_profile(config, profile, load_order_path)
    advances_dir = load_merged_directory(profile_config, "advances")
    advancements = _load_advancements(advances_dir)
    return AdvancementData(
        advancements=pl.DataFrame(
            [_advancement_row(advancement) for advancement in advancements],
            schema=_advancement_schema(),
        ),
        warnings=advances_dir.warnings,
    )


def _resolve_profile(
    config: ParserConfig | None,
    profile: str | DataProfile | None,
    load_order_path: str | Path,
) -> DataProfile:
    if isinstance(profile, DataProfile):
        return profile
    if isinstance(profile, str):
        return load_profile(profile, load_order_path)
    config = config or ParserConfig.from_env()
    return DataProfile(
        name="vanilla",
        layers=(GameLayer(id="vanilla", name="Vanilla", root=config.game_root, kind="vanilla"),),
    )


def _load_advancements(directory: MergedDirectory) -> list[Advancement]:
    return [_advancement_from_entry(entry) for entry in directory.entries]


def _advancement_from_entry(entry: MergedEntry) -> Advancement:
    block = entry.value
    unlocks = _unlock_lists(block)
    return Advancement(
        name=entry.key,
        age=_scalar_string(_last(block, "age")),
        icon=_scalar_string(_last(block, "icon")),
        requires=_all_scalar_strings(block, "requires"),
        has_potential=_last(block, "potential") is not None,
        unlocks=unlocks,
        modifiers=_numeric_modifiers(block),
        data=_to_python(block),
        source_file=entry.source_file,
        source_line=entry.source_line,
        source_layer=entry.source_layer,
        source_mod=entry.source_mod,
        source_mode=entry.source_mode,
        source_history=entry.source_history_json(),
    )


def _advancement_row(advancement: Advancement) -> dict[str, Any]:
    return {
        "name": advancement.name,
        "age": advancement.age,
        "icon": advancement.icon,
        "requires": advancement.requires,
        "has_potential": advancement.has_potential,
        "unlock_production_method": advancement.unlocks.get("unlock_production_method", []),
        "unlock_building": advancement.unlocks.get("unlock_building", []),
        "unlock_unit": advancement.unlocks.get("unlock_unit", []),
        "unlock_law": advancement.unlocks.get("unlock_law", []),
        "unlock_government_reform": advancement.unlocks.get("unlock_government_reform", []),
        "unlock_policy": advancement.unlocks.get("unlock_policy", []),
        "unlocks": _json(advancement.unlocks),
        "modifiers": _json(advancement.modifiers),
        "data": _json(advancement.data),
        "source_file": advancement.source_file,
        "source_line": advancement.source_line,
        "source_layer": advancement.source_layer,
        "source_mod": advancement.source_mod,
        "source_mode": advancement.source_mode,
        "source_history": advancement.source_history,
    }


def _advancement_schema() -> dict[str, Any]:
    return {
        "name": pl.String,
        "age": pl.String,
        "icon": pl.String,
        "requires": pl.List(pl.String),
        "has_potential": pl.Boolean,
        "unlock_production_method": pl.List(pl.String),
        "unlock_building": pl.List(pl.String),
        "unlock_unit": pl.List(pl.String),
        "unlock_law": pl.List(pl.String),
        "unlock_government_reform": pl.List(pl.String),
        "unlock_policy": pl.List(pl.String),
        "unlocks": pl.String,
        "modifiers": pl.String,
        "data": pl.String,
        "source_file": pl.String,
        "source_line": pl.Int64,
        "source_layer": pl.String,
        "source_mod": pl.String,
        "source_mode": pl.String,
        "source_history": pl.String,
    }


def _unlock_lists(block: CList) -> dict[str, list[str]]:
    unlocks: dict[str, list[str]] = {}
    for entry in block.entries:
        if not entry.key.startswith("unlock_"):
            continue
        value = _scalar(entry.value)
        if value is not None:
            unlocks.setdefault(entry.key, []).append(str(value))
    return unlocks


def _numeric_modifiers(block: CList) -> dict[str, float]:
    modifiers: dict[str, float] = {}
    for entry in block.entries:
        if entry.key in ADVANCEMENT_METADATA_KEYS or entry.key.startswith("unlock_"):
            continue
        scalar = _scalar(entry.value)
        if _is_number(scalar):
            modifiers[entry.key] = modifiers.get(entry.key, 0.0) + float(scalar)
    return modifiers


def _last(block: CList, key: str, default: Value | None = None) -> Value | None:
    values = block.values(key)
    return values[-1] if values else default


def _all_scalar_strings(block: CList, key: str) -> list[str]:
    result: list[str] = []
    for value in block.values(key):
        scalar = _scalar(value)
        if scalar is not None:
            result.append(str(scalar))
    return result


def _scalar(value: Value | None) -> Scalar | None:
    if isinstance(value, CList):
        return None
    return value


def _scalar_string(value: Value | None) -> str | None:
    scalar = _scalar(value)
    return None if scalar is None else str(scalar)


def _is_number(value: Scalar | None) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def _to_python(value: Value | None) -> Any:
    if isinstance(value, CList):
        return {
            "entries": [
                {"key": entry.key, "op": entry.op, "value": _to_python(entry.value)}
                for entry in value.entries
            ],
            "items": [_to_python(item) for item in value.items],
        }
    return value


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))
