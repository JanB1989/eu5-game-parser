from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class SaveReferenceMaps:
    population_sizes: dict[int, float] = field(default_factory=dict)
    population: dict[int, dict[str, str | float | None]] = field(default_factory=dict)
    countries: dict[int, dict[str, str | None]] = field(default_factory=dict)
    provinces: dict[int, str | None] = field(default_factory=dict)


REFERENCE_TARGET_SECTIONS = frozenset({"population", "countries", "provinces"})
_DATABASE_RE = re.compile(r"\bdatabase\s*=\s*\{")
_ID_BLOCK_RE = re.compile(r"(?m)(?:^|\s)(\d+)\s*=\s*\{")
_SIZE_RE = re.compile(r"(?m)(?:^|\s)size\s*=\s*([-+]?\d+(?:\.\d+)?)")
_SCALAR_RE = re.compile(r'(?m)(?:^|\s){key}\s*=\s*("[^"]*"|[^\s{{}}#=]+)')


def extract_reference_maps(raw_sections: dict[str, str]) -> SaveReferenceMaps:
    population = _population_from_raw(raw_sections.get("population"))
    return SaveReferenceMaps(
        population_sizes={
            item_id: data["size"]
            for item_id, data in population.items()
            if isinstance(data.get("size"), float)
        },
        population=population,
        countries=_countries_from_raw(raw_sections.get("countries")),
        provinces=_provinces_from_raw(raw_sections.get("provinces")),
    )


def reference_maps_from_root(
    *,
    population_sizes: dict[int, float],
    population: dict[int, dict[str, str | float | None]] | None = None,
    countries: dict[int, dict[str, str | None]],
    provinces: dict[int, str | None],
) -> SaveReferenceMaps:
    return SaveReferenceMaps(
        population_sizes=population_sizes,
        population=population or {},
        countries=countries,
        provinces=provinces,
    )


def _population_sizes_from_raw(section: str | None) -> dict[int, float]:
    return {
        item_id: data["size"]
        for item_id, data in _population_from_raw(section).items()
        if isinstance(data.get("size"), float)
    }


def _population_from_raw(section: str | None) -> dict[int, dict[str, str | float | None]]:
    rows: dict[int, dict[str, str | float | None]] = {}
    database = _database_body(section)
    if database is None:
        return rows
    for item_id, block in _iter_id_blocks(database):
        size = _scalar_float(_first_scalar(block, "size"))
        pop_type = _first_scalar(block, "type")
        rows[item_id] = {"type": pop_type, "size": size}
    return rows


def _countries_from_raw(section: str | None) -> dict[int, dict[str, str | None]]:
    rows: dict[int, dict[str, str | None]] = {}
    database = _database_body(section)
    if database is None:
        return rows
    for item_id, block in _iter_id_blocks(database):
        tag = (
            _first_scalar(block, "definition")
            or _first_scalar(block, "tag")
            or _first_scalar(block, "country_name")
            or _first_scalar(block, "name")
        )
        name = _first_scalar(block, "country_name") or _first_scalar(block, "name") or tag
        rows[item_id] = {"tag": tag, "name": name}
    return rows


def _provinces_from_raw(section: str | None) -> dict[int, str | None]:
    rows: dict[int, str | None] = {}
    database = _database_body(section)
    if database is None:
        return rows
    for item_id, block in _iter_id_blocks(database):
        rows[item_id] = _first_scalar(block, "province_definition")
    return rows


def _database_body(section: str | None) -> str | None:
    if not section:
        return None
    match = _DATABASE_RE.search(section)
    if not match:
        return None
    open_index = section.find("{", match.start())
    if open_index < 0:
        return None
    try:
        close_index = _matching_brace(section, open_index)
    except ValueError:
        return None
    return section[open_index + 1 : close_index]


def _iter_id_blocks(text: str) -> list[tuple[int, str]]:
    result: list[tuple[int, str]] = []
    index = 0
    while True:
        match = _ID_BLOCK_RE.search(text, index)
        if match is None:
            return result
        open_index = text.find("{", match.start())
        if open_index < 0:
            return result
        try:
            close_index = _matching_brace(text, open_index)
        except ValueError:
            index = match.end()
            continue
        result.append((int(match.group(1)), text[open_index + 1 : close_index]))
        index = close_index + 1


def _first_scalar(text: str, key: str) -> str | None:
    match = re.compile(_SCALAR_RE.pattern.format(key=re.escape(key))).search(text)
    if match is None:
        return None
    value = match.group(1).strip()
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        return value[1:-1]
    return value


def _scalar_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _matching_brace(text: str, open_index: int) -> int:
    depth = 0
    index = open_index
    in_string = False
    while index < len(text):
        char = text[index]
        if in_string:
            if char == "\\":
                index += 2
                continue
            if char == '"':
                in_string = False
            index += 1
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return index
        index += 1
    raise ValueError(f"Unterminated block at offset {open_index}.")
