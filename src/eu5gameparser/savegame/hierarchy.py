from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

from eu5gameparser.clausewitz.parser import parse_file
from eu5gameparser.clausewitz.syntax import CList
from eu5gameparser.load_order import DEFAULT_LOAD_ORDER_PATH, GameLayer, load_profile

HierarchyRow = dict[str, str | None]


def load_location_hierarchy(
    profile: str = "merged_default",
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
) -> dict[str, HierarchyRow]:
    return _load_location_hierarchy_cached(profile, str(Path(load_order_path)))


@lru_cache(maxsize=16)
def _load_location_hierarchy_cached(
    profile: str,
    load_order_path: str,
) -> dict[str, HierarchyRow]:
    data_profile = load_profile(profile, load_order_path)
    hierarchy: dict[str, HierarchyRow] = {}
    for layer in data_profile.layers:
        path = _definitions_path(layer)
        if not path.is_file():
            continue
        hierarchy.update(parse_location_hierarchy(path))
    return hierarchy


def parse_location_hierarchy(path: str | Path) -> dict[str, HierarchyRow]:
    document = parse_file(Path(path))
    rows: dict[str, HierarchyRow] = {}
    for super_region in document.entries:
        super_block = _as_block(super_region.value)
        if super_block is None:
            continue
        _walk_level(
            super_block,
            rows,
            super_region=super_region.key,
            macro_region=None,
            region=None,
            area=None,
        )
    return rows


def _walk_level(
    block: CList,
    rows: dict[str, HierarchyRow],
    *,
    super_region: str | None,
    macro_region: str | None,
    region: str | None,
    area: str | None,
) -> None:
    for entry in block.entries:
        child = _as_block(entry.value)
        if child is None:
            continue
        if child.entries:
            if macro_region is None:
                _walk_level(
                    child,
                    rows,
                    super_region=super_region,
                    macro_region=entry.key,
                    region=region,
                    area=area,
                )
            elif region is None:
                _walk_level(
                    child,
                    rows,
                    super_region=super_region,
                    macro_region=macro_region,
                    region=entry.key,
                    area=area,
                )
            elif area is None:
                _walk_level(
                    child,
                    rows,
                    super_region=super_region,
                    macro_region=macro_region,
                    region=region,
                    area=entry.key,
                )
            else:
                _register_province(
                    rows,
                    province=entry.key,
                    locations=[str(item) for item in child.items],
                    super_region=super_region,
                    macro_region=macro_region,
                    region=region,
                    area=area,
                )
            continue
        _register_province(
            rows,
            province=entry.key,
            locations=[str(item) for item in child.items],
            super_region=super_region,
            macro_region=macro_region,
            region=region,
            area=area,
        )


def _register_province(
    rows: dict[str, HierarchyRow],
    *,
    province: str,
    locations: list[str],
    super_region: str | None,
    macro_region: str | None,
    region: str | None,
    area: str | None,
) -> None:
    for location in locations:
        rows[location] = {
            "province_slug": province,
            "area": area,
            "region": region,
            "macro_region": macro_region,
            "super_region": super_region,
        }


def _definitions_path(layer: GameLayer) -> Path:
    if layer.kind == "vanilla":
        return layer.root / "game" / "in_game" / "map_data" / "definitions.txt"
    return layer.root / "in_game" / "map_data" / "definitions.txt"


def _as_block(value: Any) -> CList | None:
    return value if isinstance(value, CList) else None
