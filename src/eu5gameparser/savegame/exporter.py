from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from eu5gameparser.clausewitz.parser import parse_text
from eu5gameparser.clausewitz.syntax import CEntry, CList, Value
from eu5gameparser.domain.buildings import BuildingData
from eu5gameparser.domain.eu5 import Eu5Data, load_eu5_data
from eu5gameparser.load_order import DEFAULT_LOAD_ORDER_PATH

DEFAULT_SAVE_GAMES_DIR = Path(
    r"C:\Users\Anwender\Documents\Paradox Interactive\Europa Universalis V\save games"
)
TARGET_SECTIONS = frozenset({"metadata", "market_manager", "locations", "building_manager"})
FLOAT_TOLERANCE = 1e-6
POP_TYPES = (
    "nobles",
    "clergy",
    "burghers",
    "laborers",
    "soldiers",
    "peasants",
    "slaves",
    "tribesmen",
)
POP_EMPLOYED_COLUMNS = tuple(f"employed_{pop_type}" for pop_type in POP_TYPES)
POP_UNEMPLOYED_COLUMNS = tuple(f"unemployed_{pop_type}" for pop_type in POP_TYPES)


@dataclass(frozen=True)
class SavegameTables:
    save_metadata: pl.DataFrame
    markets: pl.DataFrame
    market_goods: pl.DataFrame
    market_good_bucket_flows: pl.DataFrame
    locations: pl.DataFrame
    buildings: pl.DataFrame
    building_methods: pl.DataFrame
    rgo_flows: pl.DataFrame
    production_method_good_flows: pl.DataFrame
    production_method_population_flows: pl.DataFrame
    market_population_pools: pl.DataFrame
    accounting_checks: pl.DataFrame

    def as_dict(self) -> dict[str, pl.DataFrame]:
        return {
            "save_metadata": self.save_metadata,
            "markets": self.markets,
            "market_goods": self.market_goods,
            "market_good_bucket_flows": self.market_good_bucket_flows,
            "locations": self.locations,
            "buildings": self.buildings,
            "building_methods": self.building_methods,
            "rgo_flows": self.rgo_flows,
            "production_method_good_flows": self.production_method_good_flows,
            "production_method_population_flows": self.production_method_population_flows,
            "market_population_pools": self.market_population_pools,
            "accounting_checks": self.accounting_checks,
        }


def latest_save_path(save_dir: str | Path = DEFAULT_SAVE_GAMES_DIR) -> Path | None:
    directory = Path(save_dir)
    if not directory.is_dir():
        return None
    saves = [path for path in directory.glob("*.eu5") if path.is_file()]
    if not saves:
        return None
    return max(saves, key=lambda path: path.stat().st_mtime).resolve()


def is_text_save(path: str | Path) -> bool:
    save_path = Path(path)
    try:
        head = save_path.read_bytes()[:256]
    except OSError:
        return False
    if not head.startswith(b"SAV") or b"\n" not in head:
        return False
    body = head.split(b"\n", 1)[1].lstrip()
    return body.startswith(b"metadata=") or body.startswith(b"metadata =")


def write_savegame_parquet(
    output: str | Path,
    *,
    save_path: str | Path | None = None,
    save_dir: str | Path = DEFAULT_SAVE_GAMES_DIR,
    profile: str = "merged_default",
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
    force_rakaly: bool = False,
    eu5_data: Eu5Data | None = None,
) -> SavegameTables:
    tables = load_savegame_tables(
        save_path=save_path,
        save_dir=save_dir,
        profile=profile,
        load_order_path=load_order_path,
        force_rakaly=force_rakaly,
        eu5_data=eu5_data,
    )
    output_path = Path(output)
    output_path.mkdir(parents=True, exist_ok=True)
    for name, table in tables.as_dict().items():
        table.write_parquet(output_path / f"{name}.parquet")
    return tables


def load_savegame_tables(
    *,
    save_path: str | Path | None = None,
    save_dir: str | Path = DEFAULT_SAVE_GAMES_DIR,
    profile: str = "merged_default",
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
    force_rakaly: bool = False,
    eu5_data: Eu5Data | None = None,
) -> SavegameTables:
    resolved_save = _resolve_save_path(save_path, save_dir)
    if force_rakaly or not is_text_save(resolved_save):
        root = _load_save_with_rakaly(resolved_save)
    else:
        root = _load_text_save_sections(resolved_save)

    eu5_data = eu5_data or load_eu5_data(profile=profile, load_order_path=load_order_path)
    return _tables_from_root(resolved_save, root, eu5_data)


def _resolve_save_path(save_path: str | Path | None, save_dir: str | Path) -> Path:
    if save_path is not None:
        resolved = Path(save_path)
        if not resolved.is_file():
            raise FileNotFoundError(f"Save file does not exist: {resolved}")
        return resolved.resolve()
    latest = latest_save_path(save_dir)
    if latest is None:
        raise FileNotFoundError(f"No .eu5 save files found in {Path(save_dir)}")
    return latest


def _load_text_save_sections(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8-sig", errors="replace")
    if "\n" not in text:
        raise ValueError(f"Text save {path} has no SAV header terminator.")
    body = text.split("\n", 1)[1]
    raw_sections = _extract_top_level_sections(body, TARGET_SECTIONS)
    missing = sorted(TARGET_SECTIONS - raw_sections.keys())
    if missing:
        raise ValueError(f"Save {path} is missing required sections: {', '.join(missing)}")
    return {key: _document_value(key, value, path) for key, value in raw_sections.items()}


def _load_save_with_rakaly(path: Path) -> dict[str, Any]:
    try:
        from eu5 import Save  # type: ignore[import-not-found]
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Rakaly fallback requires the optional `eu5` Python package. "
            "Use text-format saves or install eu5>=0.0.6 in this environment."
        ) from exc
    save = Save(str(path))
    data = getattr(save, "_data", None)
    if not isinstance(data, dict):
        raise RuntimeError("Rakaly fallback did not return a dict-like save payload.")
    return _normalize_rakaly_root(data)


def _normalize_rakaly_root(data: dict[str, Any]) -> dict[str, Any]:
    if isinstance(data.get("metadata"), dict):
        return data
    first = next(iter(data.values()), None)
    if isinstance(first, dict) and isinstance(first.get("metadata"), dict):
        return first
    return data


def _extract_top_level_sections(text: str, keys: frozenset[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    index = 0
    length = len(text)
    while index < length and len(result) < len(keys):
        index = _skip_space_and_comments(text, index)
        if index >= length:
            break
        key_start = index
        key, index = _read_atom(text, index)
        index = _skip_space_and_comments(text, index)
        if index >= length or text[index] != "=":
            index = max(index + 1, key_start + 1)
            continue
        index += 1
        index = _skip_space_and_comments(text, index)
        if index >= length:
            break
        if text[index] == "{":
            value_end = _matching_brace(text, index)
            value = text[index : value_end + 1]
            index = value_end + 1
        else:
            value, index = _read_atom(text, index)
        if key in keys:
            result[key] = value
    return result


def _skip_space_and_comments(text: str, index: int) -> int:
    length = len(text)
    while index < length:
        char = text[index]
        if char.isspace():
            index += 1
            continue
        if char == "#":
            newline = text.find("\n", index)
            if newline == -1:
                return length
            index = newline + 1
            continue
        break
    return index


def _read_atom(text: str, index: int) -> tuple[str, int]:
    if text[index] == '"':
        index += 1
        value: list[str] = []
        while index < len(text):
            char = text[index]
            if char == "\\" and index + 1 < len(text):
                value.append(text[index + 1])
                index += 2
                continue
            if char == '"':
                return "".join(value), index + 1
            value.append(char)
            index += 1
        raise ValueError("Unterminated quoted atom in save.")
    start = index
    while index < len(text):
        char = text[index]
        if char.isspace() or char in "{}#=<>!":
            break
        index += 1
    if start == index:
        raise ValueError(f"Expected atom at offset {index}.")
    return text[start:index], index


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


def _document_value(key: str, value: str, path: Path) -> CList:
    document = parse_text(f"{key}={value}", path)
    if not document.entries or not isinstance(document.entries[0].value, CList):
        raise ValueError(f"Section {key!r} in {path} is not a block.")
    return document.entries[0].value


def _tables_from_root(path: Path, root: dict[str, Any], eu5_data: Eu5Data) -> SavegameTables:
    metadata = _metadata_row(path, root.get("metadata"))
    location_slugs = _location_slugs(root.get("metadata"))
    locations = _locations_table(root.get("locations"), location_slugs)
    location_market = _location_market_map(locations)
    location_slug = _location_slug_map(locations, location_slugs)
    markets, market_goods = _market_tables(
        root.get("market_manager"),
        location_slug,
        _default_prices(eu5_data),
    )
    buildings, building_methods = _building_tables(
        root.get("building_manager"),
        location_market,
        location_slug,
        eu5_data.building_data,
    )
    bucket_flows = _market_good_bucket_flows(market_goods)
    population_flows = _population_flow_table(
        eu5_data.building_data,
        locations,
        buildings,
        building_methods,
        market_goods,
    )
    market_population_pools = _market_population_pool_table(
        markets,
        locations,
        population_flows,
    )
    flows, rgo_flows, checks = _flow_tables(
        eu5_data.building_data,
        locations,
        buildings,
        building_methods,
        market_goods,
        bucket_flows,
    )
    return SavegameTables(
        save_metadata=pl.DataFrame([metadata], schema=_metadata_schema()),
        markets=markets,
        market_goods=market_goods,
        market_good_bucket_flows=bucket_flows,
        locations=locations,
        buildings=buildings,
        building_methods=building_methods,
        rgo_flows=rgo_flows,
        production_method_good_flows=flows,
        production_method_population_flows=population_flows,
        market_population_pools=market_population_pools,
        accounting_checks=checks,
    )


def _metadata_row(path: Path, metadata: Any) -> dict[str, Any]:
    stat = path.stat()
    block = _as_block(metadata)
    return {
        "path": str(path),
        "mtime": float(stat.st_mtime),
        "size": int(stat.st_size),
        "date": _scalar_string(_first(block, "date")),
        "playthrough_name": _scalar_string(_first(block, "playthrough_name")),
        "save_label": _scalar_string(_first(block, "save_label")),
        "playthrough_id": _scalar_string(_first(block, "playthrough_id")),
    }


def _location_slugs(metadata: Any) -> dict[int, str]:
    block = _as_block(metadata)
    compatibility = _as_block(_first(block, "compatibility"))
    locations = _as_block(_first(compatibility, "locations"))
    if locations is None:
        return {}
    return {index + 1: str(value) for index, value in enumerate(_list_scalars(locations))}


def _locations_table(locations_root: Any, location_slugs: dict[int, str]) -> pl.DataFrame:
    root = _as_block(locations_root)
    locations = _as_block(_first(root, "locations"))
    rows: list[dict[str, Any]] = []
    if locations is not None:
        for entry in locations.entries:
            data = _as_block(entry.value)
            if data is None:
                continue
            location_id = _to_int(entry.key)
            if location_id is None:
                continue
            rgo_employed_by_pop = _rgo_employed_by_pop(data)
            unemployed_by_pop = _unemployed_by_pop(data)
            rows.append(
                {
                    "location_id": location_id,
                    "slug": location_slugs.get(location_id),
                    "owner": _to_int(_first(data, "owner")),
                    "controller": _to_int(_first(data, "controller")),
                    "market_id": _to_int(_first(data, "market")),
                    "second_best_market_id": _to_int(_first(data, "second_best_market")),
                    "province": _to_int(_first(data, "province")),
                    "development": _to_float(_first(data, "development")),
                    "control": _to_float(_first(data, "control")),
                    "rank": _scalar_string(_first(data, "rank")),
                    "raw_material": _scalar_string(_first(data, "raw_material")),
                    "max_raw_material_workers": _to_float(
                        _first(data, "max_raw_material_workers")
                    ),
                    "rgo_employed": sum(rgo_employed_by_pop.values()),
                    "unemployed_total": sum(unemployed_by_pop.values()),
                    **_pop_columns(rgo_employed_by_pop),
                    **_unemployed_pop_columns(unemployed_by_pop),
                }
            )
    return pl.DataFrame(rows, schema=_locations_schema())


def _market_tables(
    market_manager: Any,
    location_slug: dict[int, str],
    default_prices: dict[str, float],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    root = _as_block(market_manager)
    database = _as_block(_first(root, "database"))
    market_rows: list[dict[str, Any]] = []
    good_rows: list[dict[str, Any]] = []
    category_columns: set[str] = set()
    if database is not None:
        for market_entry in database.entries:
            market = _as_block(market_entry.value)
            market_id = _to_int(market_entry.key)
            if market is None or market_id is None:
                continue
            center = _to_int(_first(market, "center"))
            market_rows.append(
                {
                    "market_id": market_id,
                    "center_location_id": center,
                    "market_center_slug": location_slug.get(center or -1),
                    "food": _to_float(_first(market, "food")),
                    "food_max": _to_float(_first(market, "max")),
                    "price": _to_float(_first(market, "price")),
                    "food_consumption": _to_float(_first(market, "food_consumption")),
                    "food_supply": _to_float(_first(market, "food_supply")),
                    "food_not_traded": _to_float(_first(market, "food_not_traded")),
                    "missing": _to_float(_first(market, "missing")),
                    "population": _to_float(_first(market, "population")),
                    "capacity": _to_float(_first(market, "capacity")),
                    "average_migration_attraction": _to_float(
                        _first(market, "average_migration_attraction")
                    ),
                }
            )
            goods = _as_block(_first(market, "goods"))
            if goods is None:
                continue
            for good_entry in goods.entries:
                good_data = _as_block(good_entry.value)
                if good_data is None:
                    continue
                good_id = good_entry.key
                supply = _to_float(_first(good_data, "supply"))
                demand = _to_float(_first(good_data, "demand"))
                row = {
                    "market_id": market_id,
                    "market_center_slug": location_slug.get(center or -1),
                    "good_id": good_id,
                    "price": _to_float(_first(good_data, "price")),
                    "default_price": default_prices.get(good_id),
                    "supply": supply,
                    "demand": demand,
                    "net": _net(supply, demand),
                    "stockpile": _to_float(_first(good_data, "stockpile")),
                    "total_taken": _to_float(_first(good_data, "total_taken")),
                    "possible": _to_float(_first(good_data, "possible")),
                    "impact": _to_float(_first(good_data, "impact")),
                    "priority": _to_float(_first(good_data, "priority")),
                    "allowed_export_amount": _to_float(_first(good_data, "allowed_export_amount")),
                    "last_month": _to_float(_first(good_data, "last_month")),
                    "locations_with_this_as_raw_material": _to_float(
                        _first(good_data, "locations_with_this_as_raw_material")
                    ),
                }
                for prefix, key in (
                    ("supplied", "supplied"),
                    ("demanded", "demanded"),
                    ("taken", "taken"),
                ):
                    bucket = _as_block(_first(good_data, key))
                    if bucket is None:
                        continue
                    for category in bucket.entries:
                        column = f"{prefix}_{category.key}"
                        row[column] = _to_float(category.value)
                        category_columns.add(column)
                good_rows.append(row)

    market_goods_schema = _market_goods_schema(sorted(category_columns))
    return (
        pl.DataFrame(market_rows, schema=_markets_schema()),
        pl.DataFrame(good_rows, schema=market_goods_schema),
    )


def _building_tables(
    building_manager: Any,
    location_market: dict[int, int],
    location_slug: dict[int, str],
    building_data: BuildingData,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    root = _as_block(building_manager)
    database = _as_block(_first(root, "database"))
    known_methods = set(building_data.production_methods["name"].to_list())
    rows: list[dict[str, Any]] = []
    method_rows: list[dict[str, Any]] = []
    if database is not None:
        for entry in database.entries:
            data = _as_block(entry.value)
            building_id = _to_int(entry.key)
            if data is None or building_id is None:
                continue
            location_id = _to_int(_first(data, "location"))
            building_type = _scalar_string(_first(data, "type")) or _scalar_string(
                _first(data, "building")
            )
            methods = _active_method_ids(data, known_methods)
            row = {
                "building_id": building_id,
                "building_type": building_type,
                "location_id": location_id,
                "location_slug": location_slug.get(location_id or -1),
                "market_id": location_market.get(location_id or -1),
                "owner": _to_int(_first(data, "owner")),
                "level": _to_float(_first(data, "level")) or 0.0,
                "max_level": _to_float(_first(data, "max_level")),
                "employed": _to_float(_first(data, "employed")),
                "employment": _to_float(_first(data, "employment")),
                "open": _to_bool(_first(data, "open")),
                "subsidized": _to_bool(_first(data, "subsidized")),
                "upkeep": _to_float(_first(data, "upkeep")),
                "last_months_profit": _to_float(_first(data, "last_months_profit")),
                "active_method_ids": methods,
            }
            rows.append(row)
            for method_id in methods:
                method_rows.append(
                    {
                        "building_id": building_id,
                        "building_type": building_type,
                        "location_id": location_id,
                        "location_slug": location_slug.get(location_id or -1),
                        "market_id": location_market.get(location_id or -1),
                        "production_method": method_id,
                    }
                )
    return (
        pl.DataFrame(rows, schema=_buildings_schema()),
        pl.DataFrame(method_rows, schema=_building_methods_schema()),
    )


def _market_good_bucket_flows(market_goods: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    if not market_goods.is_empty():
        bucket_columns = [
            column
            for column in market_goods.columns
            if column.startswith("supplied_") or column.startswith("demanded_")
        ]
        for good_row in market_goods.to_dicts():
            for column in bucket_columns:
                amount = good_row.get(column) or 0.0
                if abs(amount) <= FLOAT_TOLERANCE:
                    continue
                prefix, bucket = column.split("_", 1)
                rows.append(
                    {
                        "market_id": good_row["market_id"],
                        "market_center_slug": good_row.get("market_center_slug"),
                        "good_id": good_row["good_id"],
                        "direction": "supply" if prefix == "supplied" else "demand",
                        "bucket": bucket,
                        "save_column": column,
                        "amount": amount,
                    }
                )
    return pl.DataFrame(rows, schema=_bucket_flow_schema())


def _flow_tables(
    building_data: BuildingData,
    locations: pl.DataFrame,
    buildings: pl.DataFrame,
    building_methods: pl.DataFrame,
    market_goods: pl.DataFrame,
    bucket_flows: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    raw_flow_rows = _nominal_flow_rows(building_data, buildings, building_methods)
    rgo_rows = _rgo_nominal_flow_rows(locations)
    raw_flow_rows_by_key = _index_flow_rows(raw_flow_rows)
    rgo_rows_by_key = _index_flow_rows(rgo_rows)
    target_rows = _flow_target_rows(market_goods)

    flow_rows: list[dict[str, Any]] = []
    rgo_flow_rows: list[dict[str, Any]] = []
    allocated_totals: dict[tuple[int, str, str], float] = {}

    for row in target_rows:
        key = (row["market_id"], row["good_id"], row["direction"])
        expected = row["expected_total"] or 0.0
        detail_rows = raw_flow_rows_by_key.get(key, [])
        if row["direction"] == "output":
            rgo_detail_rows = rgo_rows_by_key.get(key, [])
            allocated = _allocate_output_rows(
                row,
                expected,
                detail_rows,
                rgo_detail_rows,
                flow_rows,
                rgo_flow_rows,
            )
        else:
            allocated = _allocate_scaled_rows(expected, detail_rows, flow_rows)
        allocated_totals[key] = allocated

        delta = expected - allocated
        if abs(delta) > FLOAT_TOLERANCE:
            flow_rows.append(_unattributed_flow_row(row, delta))
            allocated_totals[key] = allocated_totals.get(key, 0.0) + delta

    check_rows = _accounting_check_rows(market_goods, bucket_flows, allocated_totals)

    return (
        pl.DataFrame(flow_rows, schema=_flow_schema()),
        pl.DataFrame(rgo_flow_rows, schema=_rgo_flow_schema()),
        pl.DataFrame(check_rows, schema=_accounting_schema()),
    )


def _index_flow_rows(
    rows: list[dict[str, Any]],
) -> dict[tuple[int, str, str], list[dict[str, Any]]]:
    rows_by_key: dict[tuple[int, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        rows_by_key.setdefault(_flow_key(row), []).append(row)
    return rows_by_key


def _flow_key(row: dict[str, Any]) -> tuple[int, str, str]:
    return (row["market_id"], row["good_id"], row["direction"])


def _allocate_output_rows(
    target: dict[str, Any],
    expected: float,
    method_rows: list[dict[str, Any]],
    rgo_rows: list[dict[str, Any]],
    flow_rows: list[dict[str, Any]],
    rgo_flow_rows: list[dict[str, Any]],
) -> float:
    method_nominal = sum(row["nominal_amount"] or 0.0 for row in method_rows)
    if rgo_rows:
        method_factor = 0.0
        if method_nominal > 0:
            method_factor = min(1.0, expected / method_nominal)
    else:
        method_factor = 0.0 if method_nominal == 0 else expected / method_nominal
    method_allocated = _append_allocated_rows(method_rows, method_factor, flow_rows)
    remainder = max(expected - method_allocated, 0.0)
    rgo_nominal = sum(row["nominal_amount"] or 0.0 for row in rgo_rows)
    rgo_factor = 0.0 if rgo_nominal == 0 else remainder / rgo_nominal
    rgo_allocated = 0.0
    for row in rgo_rows:
        allocated = (row["nominal_amount"] or 0.0) * rgo_factor
        rgo_row = dict(row)
        rgo_row["market_center_slug"] = target["market_center_slug"]
        rgo_row["allocated_amount"] = allocated
        rgo_row["allocation_factor"] = rgo_factor
        rgo_flow_rows.append(rgo_row)
        rgo_allocated += allocated
    return method_allocated + rgo_allocated


def _allocate_scaled_rows(
    expected: float,
    detail_rows: list[dict[str, Any]],
    flow_rows: list[dict[str, Any]],
) -> float:
    nominal = sum(row["nominal_amount"] or 0.0 for row in detail_rows)
    factor = 0.0 if nominal == 0 else expected / nominal
    return _append_allocated_rows(detail_rows, factor, flow_rows)


def _append_allocated_rows(
    detail_rows: list[dict[str, Any]],
    factor: float,
    flow_rows: list[dict[str, Any]],
) -> float:
    allocated_total = 0.0
    for row in detail_rows:
        allocated = (row["nominal_amount"] or 0.0) * factor
        flow_row = dict(row)
        flow_row["allocation_factor"] = factor
        flow_row["allocated_amount"] = allocated
        flow_rows.append(flow_row)
        allocated_total += allocated
    return allocated_total


def _unattributed_flow_row(row: dict[str, Any], amount: float) -> dict[str, Any]:
    is_output = row["direction"] == "output"
    return {
        "market_id": row["market_id"],
        "market_center_slug": row["market_center_slug"],
        "good_id": row["good_id"],
        "production_method": "unattributed production"
        if is_output
        else "unattributed building demand",
        "building_id": None,
        "building_type": None,
        "location_id": None,
        "location_slug": None,
        "direction": row["direction"],
        "save_side": row["save_side"],
        "nominal_amount": 0.0,
        "allocated_amount": amount,
        "allocation_factor": None,
        "building_count": 0,
        "level_sum": 0.0,
    }


def _rgo_nominal_flow_rows(locations: pl.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if locations.is_empty():
        return rows
    for location in locations.to_dicts():
        good = location.get("raw_material")
        market_id = location.get("market_id")
        if not good or market_id is None:
            continue
        employed = location.get("rgo_employed") or 0.0
        max_workers = location.get("max_raw_material_workers") or 0.0
        nominal = employed if employed > 0 else max_workers
        if nominal <= 0:
            nominal = 1.0
        rows.append(
            {
                "market_id": market_id,
                "market_center_slug": None,
                "good_id": good,
                "location_id": location.get("location_id"),
                "location_slug": location.get("slug"),
                "raw_material": good,
                "max_raw_material_workers": max_workers,
                "rgo_employed": employed,
                "direction": "output",
                "save_side": "supplied_Production",
                "nominal_amount": nominal,
                "allocated_amount": None,
                "allocation_factor": None,
            }
        )
    return rows


def _population_flow_table(
    building_data: BuildingData,
    locations: pl.DataFrame,
    buildings: pl.DataFrame,
    building_methods: pl.DataFrame,
    market_goods: pl.DataFrame,
) -> pl.DataFrame:
    rows = [
        *_building_population_flow_rows(
            building_data,
            buildings,
            building_methods,
            market_goods,
        ),
        *_rgo_population_flow_rows(locations),
    ]
    return pl.DataFrame(rows, schema=_population_flow_schema())


def _building_population_flow_rows(
    building_data: BuildingData,
    buildings: pl.DataFrame,
    building_methods: pl.DataFrame,
    market_goods: pl.DataFrame,
) -> list[dict[str, Any]]:
    if buildings.is_empty() or building_methods.is_empty():
        return []
    building_by_id = {row["building_id"]: row for row in buildings.to_dicts()}
    methods_by_building: dict[int, list[dict[str, Any]]] = {}
    for active in building_methods.to_dicts():
        methods_by_building.setdefault(active["building_id"], []).append(active)
    method_by_name = {row["name"]: row for row in building_data.production_methods.to_dicts()}
    pop_type_by_building = {
        row["name"]: row.get("pop_type") for row in building_data.buildings.to_dicts()
    }
    market_price, default_price = _market_price_maps(market_goods)

    rows: list[dict[str, Any]] = []
    for building_id, active_methods in methods_by_building.items():
        building = building_by_id.get(building_id)
        if building is None or building.get("market_id") is None:
            continue
        methods = [
            (active, method_by_name.get(active["production_method"]))
            for active in active_methods
            if method_by_name.get(active["production_method"]) is not None
        ]
        if not methods:
            continue
        employed_total = _building_basis(building)
        if abs(employed_total) <= FLOAT_TOLERANCE:
            continue
        weights = [
            _method_population_weight(
                method,
                building["market_id"],
                market_price,
                default_price,
            )
            for _, method in methods
        ]
        total_weight = sum(weights)
        if total_weight > FLOAT_TOLERANCE:
            shares = [weight / total_weight for weight in weights]
            basis = "output_value"
        else:
            shares = [1.0 / len(methods)] * len(methods)
            basis = "equal_fallback"

        pop_type = pop_type_by_building.get(building.get("building_type"))
        for (active, method), share, weight in zip(methods, shares, weights, strict=False):
            amount = employed_total * share
            rows.append(
                {
                    "market_id": building["market_id"],
                    "market_center_slug": None,
                    "good_id": method.get("produced"),
                    "production_method": active["production_method"],
                    "building_id": active["building_id"],
                    "building_type": active["building_type"],
                    "location_id": active["location_id"],
                    "location_slug": active["location_slug"],
                    "source_kind": "building",
                    "allocation_basis": basis,
                    "allocation_weight": weight,
                    "employment_share": share,
                    "employed_total": amount,
                    **_single_pop_columns(pop_type, amount),
                }
            )
    return rows


def _method_population_weight(
    method: dict[str, Any],
    market_id: int,
    market_price: dict[tuple[int, str], float],
    default_price: dict[str, float],
) -> float:
    produced = method.get("produced")
    output = method.get("output")
    if produced is None or output is None:
        return 0.0
    price = market_price.get((market_id, produced))
    if price is None:
        price = default_price.get(produced)
    if price is None:
        price = method.get("output_value")
        if output:
            price = (price or 0.0) / float(output)
    if price is None:
        return 0.0
    return max(float(output) * float(price), 0.0)


def _market_price_maps(
    market_goods: pl.DataFrame,
) -> tuple[dict[tuple[int, str], float], dict[str, float]]:
    market_price: dict[tuple[int, str], float] = {}
    default_price: dict[str, float] = {}
    if market_goods.is_empty():
        return market_price, default_price
    for row in market_goods.to_dicts():
        market_id = row.get("market_id")
        good = row.get("good_id")
        if market_id is None or good is None:
            continue
        if row.get("price") is not None:
            market_price[(market_id, good)] = float(row["price"])
        if row.get("default_price") is not None:
            default_price[good] = float(row["default_price"])
    return market_price, default_price


def _rgo_population_flow_rows(locations: pl.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if locations.is_empty():
        return rows
    for location in locations.to_dicts():
        good = location.get("raw_material")
        market_id = location.get("market_id")
        if not good or market_id is None:
            continue
        pop_amounts = {
            pop_type: location.get(f"employed_{pop_type}") or 0.0 for pop_type in POP_TYPES
        }
        employed_total = sum(pop_amounts.values())
        if abs(employed_total) <= FLOAT_TOLERANCE:
            continue
        rows.append(
            {
                "market_id": market_id,
                "market_center_slug": None,
                "good_id": good,
                "production_method": f"rgo_{good}",
                "building_id": None,
                "building_type": None,
                "location_id": location.get("location_id"),
                "location_slug": location.get("slug"),
                "source_kind": "rgo",
                "allocation_basis": "employed_in_rgo",
                "allocation_weight": employed_total,
                "employment_share": 1.0,
                "employed_total": employed_total,
                **_pop_columns(pop_amounts),
            }
        )
    return rows


def _market_population_pool_table(
    markets: pl.DataFrame,
    locations: pl.DataFrame,
    population_flows: pl.DataFrame,
) -> pl.DataFrame:
    market_labels = _market_center_slug_by_id(markets)
    all_market_ids = set(market_labels)
    employed_by_market = _population_pool_by_market(
        population_flows,
        ["employed_total", *POP_EMPLOYED_COLUMNS],
    )
    unemployed_by_market = _population_pool_by_market(
        locations,
        ["unemployed_total", *POP_UNEMPLOYED_COLUMNS],
    )
    all_market_ids.update(employed_by_market)
    all_market_ids.update(unemployed_by_market)

    rows = [
        _population_pool_row(
            market_id,
            market_labels.get(market_id),
            employed_by_market.get(market_id, {}),
            unemployed_by_market.get(market_id, {}),
        )
        for market_id in sorted(market_id for market_id in all_market_ids if market_id is not None)
    ]
    rows.insert(0, _global_population_pool_row(rows))
    return pl.DataFrame(rows, schema=_market_population_pool_schema())


def _market_center_slug_by_id(markets: pl.DataFrame) -> dict[int, str | None]:
    if markets.is_empty():
        return {}
    return {
        row["market_id"]: row.get("market_center_slug")
        for row in markets.select(["market_id", "market_center_slug"]).to_dicts()
        if row["market_id"] is not None
    }


def _population_pool_by_market(
    table: pl.DataFrame,
    columns: list[str],
) -> dict[int, dict[str, float]]:
    if table.is_empty() or "market_id" not in table.columns:
        return {}
    selected = ["market_id", *[column for column in columns if column in table.columns]]
    if len(selected) == 1:
        return {}
    grouped = (
        table.select(selected)
        .filter(pl.col("market_id").is_not_null())
        .group_by("market_id")
        .agg(
            [
                pl.col(column).fill_null(0).sum().alias(column)
                for column in selected
                if column != "market_id"
            ]
        )
    )
    return {
        row["market_id"]: {
            column: float(row.get(column) or 0.0)
            for column in columns
            if column in grouped.columns
        }
        for row in grouped.to_dicts()
    }


def _population_pool_row(
    market_id: int | None,
    market_center_slug: str | None,
    employed: dict[str, float],
    unemployed: dict[str, float],
) -> dict[str, Any]:
    return {
        "market_id": market_id,
        "market_center_slug": market_center_slug,
        "employed_total": float(employed.get("employed_total", 0.0)),
        **{
            column: float(employed.get(column, 0.0))
            for column in POP_EMPLOYED_COLUMNS
        },
        "unemployed_total": float(unemployed.get("unemployed_total", 0.0)),
        **{
            column: float(unemployed.get(column, 0.0))
            for column in POP_UNEMPLOYED_COLUMNS
        },
    }


def _global_population_pool_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    columns = [
        "employed_total",
        *POP_EMPLOYED_COLUMNS,
        "unemployed_total",
        *POP_UNEMPLOYED_COLUMNS,
    ]
    return {
        "market_id": None,
        "market_center_slug": "Global",
        **{column: sum(row.get(column, 0.0) or 0.0 for row in rows) for column in columns},
    }


def _accounting_check_rows(
    market_goods: pl.DataFrame,
    bucket_flows: pl.DataFrame,
    allocated_totals: dict[tuple[int, str, str], float],
) -> list[dict[str, Any]]:
    bucket_totals: dict[tuple[int, str, str], float] = {}
    for row in bucket_flows.to_dicts():
        key = (row["market_id"], row["good_id"], row["direction"])
        bucket_totals[key] = bucket_totals.get(key, 0.0) + (row["amount"] or 0.0)

    rows: list[dict[str, Any]] = []
    for row in market_goods.to_dicts():
        market_id = row["market_id"]
        good_id = row["good_id"]
        supply = row.get("supply") or 0.0
        demand = row.get("demand") or 0.0
        supply_bucket_sum = bucket_totals.get((market_id, good_id, "supply"), 0.0)
        demand_bucket_sum = bucket_totals.get((market_id, good_id, "demand"), 0.0)
        supply_graph_total = supply_bucket_sum
        demand_graph_total = demand_bucket_sum
        supply_delta = supply_graph_total - supply
        demand_delta = demand_graph_total - demand
        production_detail_delta = allocated_totals.get((market_id, good_id, "output"), 0.0) - (
            row.get("supplied_Production") or 0.0
        )
        building_detail_delta = allocated_totals.get((market_id, good_id, "input"), 0.0) - (
            row.get("demanded_Building") or 0.0
        )
        max_delta = max(
            abs(supply_delta),
            abs(demand_delta),
            abs(production_detail_delta),
            abs(building_detail_delta),
        )
        rows.append(
            {
                "market_id": market_id,
                "market_center_slug": row.get("market_center_slug"),
                "good_id": good_id,
                "side": "market_good",
                "expected_total": None,
                "exported_total": None,
                "delta": max_delta,
                "supply": supply,
                "supply_bucket_sum": supply_bucket_sum,
                "supply_graph_total": supply_graph_total,
                "supply_delta": supply_delta,
                "demand": demand,
                "demand_bucket_sum": demand_bucket_sum,
                "demand_graph_total": demand_graph_total,
                "demand_delta": demand_delta,
                "production_detail_delta": production_detail_delta,
                "building_detail_delta": building_detail_delta,
                "status": "ok" if max_delta <= FLOAT_TOLERANCE else "mismatch",
            }
        )
    return rows


def _nominal_flow_rows(
    building_data: BuildingData,
    buildings: pl.DataFrame,
    building_methods: pl.DataFrame,
) -> list[dict[str, Any]]:
    if buildings.is_empty() or building_methods.is_empty():
        return []
    building_by_id = {row["building_id"]: row for row in buildings.to_dicts()}
    method_by_name = {row["name"]: row for row in building_data.production_methods.to_dicts()}
    rows: list[dict[str, Any]] = []
    for active in building_methods.to_dicts():
        building = building_by_id.get(active["building_id"])
        method = method_by_name.get(active["production_method"])
        if building is None or method is None or building.get("market_id") is None:
            continue
        basis = _building_basis(building)
        level_sum = float(building.get("level") or 0.0)
        common = {
            "market_id": building["market_id"],
            "market_center_slug": None,
            "production_method": active["production_method"],
            "building_id": active["building_id"],
            "building_type": active["building_type"],
            "location_id": active["location_id"],
            "location_slug": active["location_slug"],
            "building_count": 1,
            "level_sum": level_sum,
        }
        if method.get("produced") and method.get("output") is not None:
            rows.append(
                {
                    **common,
                    "good_id": method["produced"],
                    "direction": "output",
                    "save_side": "supplied_Production",
                    "nominal_amount": float(method["output"]) * basis,
                    "allocated_amount": None,
                    "allocation_factor": None,
                }
            )
        for good, amount in zip(
            method.get("input_goods") or [],
            method.get("input_amounts") or [],
            strict=False,
        ):
            rows.append(
                {
                    **common,
                    "good_id": good,
                    "direction": "input",
                    "save_side": "demanded_Building",
                    "nominal_amount": float(amount) * basis,
                    "allocated_amount": None,
                    "allocation_factor": None,
                }
            )
    return rows


def _building_basis(building: dict[str, Any]) -> float:
    level = building.get("level") or 0.0
    employment = building.get("employed")
    if employment is None:
        employment = building.get("employment")
    if employment is None:
        employment = 1.0
    return float(level) * float(employment)


def _rgo_employed_by_pop(location: CList) -> dict[str, float]:
    return _pop_stat_by_pop(location, "employed_in_rgo")


def _unemployed_by_pop(location: CList) -> dict[str, float]:
    return _pop_stat_by_pop(location, "unemployed")


def _pop_stat_by_pop(location: CList, field: str) -> dict[str, float]:
    population = _as_block(_first(location, "population"))
    pop_stats = _as_block(_first(population, "pop_stats")) if population is not None else None
    if pop_stats is None:
        return {}
    amounts: dict[str, float] = {}
    for entry in pop_stats.entries:
        stats = _as_block(entry.value)
        if stats is not None:
            amounts[entry.key] = _to_float(_first(stats, field)) or 0.0
    return amounts


def _pop_columns(amounts: dict[str, float]) -> dict[str, float]:
    return {f"employed_{pop_type}": float(amounts.get(pop_type, 0.0)) for pop_type in POP_TYPES}


def _unemployed_pop_columns(amounts: dict[str, float]) -> dict[str, float]:
    return {
        f"unemployed_{pop_type}": float(amounts.get(pop_type, 0.0))
        for pop_type in POP_TYPES
    }


def _single_pop_columns(pop_type: str | None, amount: float) -> dict[str, float]:
    amounts = {pop_type: amount} if pop_type in POP_TYPES else {}
    return _pop_columns(amounts)


def _flow_target_rows(market_goods: pl.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if market_goods.is_empty():
        return rows
    for row in market_goods.to_dicts():
        for direction, side in (("output", "supplied_Production"), ("input", "demanded_Building")):
            rows.append(
                {
                    "market_id": row["market_id"],
                    "market_center_slug": row.get("market_center_slug"),
                    "good_id": row["good_id"],
                    "direction": direction,
                    "save_side": side,
                    "expected_total": row.get(side) or 0.0,
                }
            )
    return rows


def _active_method_ids(block: CList, known_methods: set[str]) -> list[str]:
    methods: list[str] = []
    for entry in block.entries:
        if entry.key in _RESERVED_BUILDING_KEYS:
            continue
        if entry.key in known_methods:
            methods.append(entry.key)
    return sorted(set(methods))


_RESERVED_BUILDING_KEYS = frozenset(
    {
        "type",
        "building",
        "location",
        "level",
        "max_level",
        "employed",
        "employment",
        "employment_requirement",
        "employment_requirement_status",
        "name",
        "owner",
        "open",
        "subsidized",
        "upkeep",
        "last_months_profit",
        "building_id",
        "id",
    }
)


def _default_prices(eu5_data: Eu5Data) -> dict[str, float]:
    return {
        row["name"]: row["default_market_price"]
        for row in eu5_data.goods.to_dicts()
        if row.get("default_market_price") is not None
    }


def _location_market_map(locations: pl.DataFrame) -> dict[int, int]:
    if locations.is_empty():
        return {}
    return {
        row["location_id"]: row["market_id"]
        for row in locations.select(["location_id", "market_id"]).to_dicts()
        if row["location_id"] is not None and row["market_id"] is not None
    }


def _location_slug_map(locations: pl.DataFrame, fallback: dict[int, str]) -> dict[int, str]:
    result = dict(fallback)
    if locations.is_empty():
        return result
    for row in locations.select(["location_id", "slug"]).to_dicts():
        if row["location_id"] is not None and row["slug"] is not None:
            result[row["location_id"]] = row["slug"]
    return result


def _as_block(value: Any) -> CList | None:
    if isinstance(value, CList):
        return value
    if isinstance(value, dict):
        return _dict_to_block(value)
    return None


def _dict_to_block(raw: dict[str, Any]) -> CList:
    return CList(
        entries=[
            CEntry(key=str(key), op="=", value=_to_clausewitz(value), location=_empty_location())
            for key, value in raw.items()
        ]
    )


def _to_clausewitz(value: Any) -> Value:
    if isinstance(value, dict):
        return _dict_to_block(value)
    if isinstance(value, list):
        return CList(items=[_to_clausewitz(item) for item in value])
    if isinstance(value, str | int | float | bool):
        return value
    return str(value)


def _empty_location():
    from eu5gameparser.clausewitz.syntax import SourceLocation

    return SourceLocation(None, 0, 0)


def _first(block: CList | None, key: str) -> Value | None:
    if block is None:
        return None
    return block.first(key)


def _list_scalars(block: CList) -> list[Any]:
    return [item for item in block.items if not isinstance(item, CList)]


def _scalar_string(value: Any) -> str | None:
    if value is None or isinstance(value, CList):
        return None
    if isinstance(value, bool):
        return "yes" if value else "no"
    return str(value)


def _to_int(value: Any) -> int | None:
    if value is None or isinstance(value, CList):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    if value is None or isinstance(value, CList):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.lower()
        if lowered == "yes":
            return True
        if lowered == "no":
            return False
    return None


def _net(supply: float | None, demand: float | None) -> float | None:
    if supply is None or demand is None:
        return None
    return supply - demand


def _metadata_schema() -> dict[str, Any]:
    return {
        "path": pl.String,
        "mtime": pl.Float64,
        "size": pl.Int64,
        "date": pl.String,
        "playthrough_name": pl.String,
        "save_label": pl.String,
        "playthrough_id": pl.String,
    }


def _markets_schema() -> dict[str, Any]:
    return {
        "market_id": pl.Int64,
        "center_location_id": pl.Int64,
        "market_center_slug": pl.String,
        "food": pl.Float64,
        "food_max": pl.Float64,
        "price": pl.Float64,
        "food_consumption": pl.Float64,
        "food_supply": pl.Float64,
        "food_not_traded": pl.Float64,
        "missing": pl.Float64,
        "population": pl.Float64,
        "capacity": pl.Float64,
        "average_migration_attraction": pl.Float64,
    }


def _market_goods_schema(category_columns: list[str]) -> dict[str, Any]:
    schema = {
        "market_id": pl.Int64,
        "market_center_slug": pl.String,
        "good_id": pl.String,
        "price": pl.Float64,
        "default_price": pl.Float64,
        "supply": pl.Float64,
        "demand": pl.Float64,
        "net": pl.Float64,
        "stockpile": pl.Float64,
        "total_taken": pl.Float64,
        "possible": pl.Float64,
        "impact": pl.Float64,
        "priority": pl.Float64,
        "allowed_export_amount": pl.Float64,
        "last_month": pl.Float64,
        "locations_with_this_as_raw_material": pl.Float64,
    }
    schema.update({column: pl.Float64 for column in category_columns})
    return schema


def _locations_schema() -> dict[str, Any]:
    schema = {
        "location_id": pl.Int64,
        "slug": pl.String,
        "owner": pl.Int64,
        "controller": pl.Int64,
        "market_id": pl.Int64,
        "second_best_market_id": pl.Int64,
        "province": pl.Int64,
        "development": pl.Float64,
        "control": pl.Float64,
        "rank": pl.String,
        "raw_material": pl.String,
        "max_raw_material_workers": pl.Float64,
        "rgo_employed": pl.Float64,
        "unemployed_total": pl.Float64,
    }
    schema.update({column: pl.Float64 for column in POP_EMPLOYED_COLUMNS})
    schema.update({column: pl.Float64 for column in POP_UNEMPLOYED_COLUMNS})
    return schema


def _buildings_schema() -> dict[str, Any]:
    return {
        "building_id": pl.Int64,
        "building_type": pl.String,
        "location_id": pl.Int64,
        "location_slug": pl.String,
        "market_id": pl.Int64,
        "owner": pl.Int64,
        "level": pl.Float64,
        "max_level": pl.Float64,
        "employed": pl.Float64,
        "employment": pl.Float64,
        "open": pl.Boolean,
        "subsidized": pl.Boolean,
        "upkeep": pl.Float64,
        "last_months_profit": pl.Float64,
        "active_method_ids": pl.List(pl.String),
    }


def _building_methods_schema() -> dict[str, Any]:
    return {
        "building_id": pl.Int64,
        "building_type": pl.String,
        "location_id": pl.Int64,
        "location_slug": pl.String,
        "market_id": pl.Int64,
        "production_method": pl.String,
    }


def _bucket_flow_schema() -> dict[str, Any]:
    return {
        "market_id": pl.Int64,
        "market_center_slug": pl.String,
        "good_id": pl.String,
        "direction": pl.String,
        "bucket": pl.String,
        "save_column": pl.String,
        "amount": pl.Float64,
    }


def _rgo_flow_schema() -> dict[str, Any]:
    return {
        "market_id": pl.Int64,
        "market_center_slug": pl.String,
        "good_id": pl.String,
        "location_id": pl.Int64,
        "location_slug": pl.String,
        "raw_material": pl.String,
        "max_raw_material_workers": pl.Float64,
        "rgo_employed": pl.Float64,
        "direction": pl.String,
        "save_side": pl.String,
        "nominal_amount": pl.Float64,
        "allocated_amount": pl.Float64,
        "allocation_factor": pl.Float64,
    }


def _flow_schema() -> dict[str, Any]:
    return {
        "market_id": pl.Int64,
        "market_center_slug": pl.String,
        "good_id": pl.String,
        "production_method": pl.String,
        "building_id": pl.Int64,
        "building_type": pl.String,
        "location_id": pl.Int64,
        "location_slug": pl.String,
        "direction": pl.String,
        "save_side": pl.String,
        "nominal_amount": pl.Float64,
        "allocated_amount": pl.Float64,
        "allocation_factor": pl.Float64,
        "building_count": pl.Int64,
        "level_sum": pl.Float64,
    }


def _population_flow_schema() -> dict[str, Any]:
    schema = {
        "market_id": pl.Int64,
        "market_center_slug": pl.String,
        "good_id": pl.String,
        "production_method": pl.String,
        "building_id": pl.Int64,
        "building_type": pl.String,
        "location_id": pl.Int64,
        "location_slug": pl.String,
        "source_kind": pl.String,
        "allocation_basis": pl.String,
        "allocation_weight": pl.Float64,
        "employment_share": pl.Float64,
        "employed_total": pl.Float64,
    }
    schema.update({column: pl.Float64 for column in POP_EMPLOYED_COLUMNS})
    return schema


def _market_population_pool_schema() -> dict[str, Any]:
    schema = {
        "market_id": pl.Int64,
        "market_center_slug": pl.String,
        "employed_total": pl.Float64,
    }
    schema.update({column: pl.Float64 for column in POP_EMPLOYED_COLUMNS})
    schema["unemployed_total"] = pl.Float64
    schema.update({column: pl.Float64 for column in POP_UNEMPLOYED_COLUMNS})
    return schema


def _accounting_schema() -> dict[str, Any]:
    return {
        "market_id": pl.Int64,
        "market_center_slug": pl.String,
        "good_id": pl.String,
        "side": pl.String,
        "expected_total": pl.Float64,
        "exported_total": pl.Float64,
        "delta": pl.Float64,
        "supply": pl.Float64,
        "supply_bucket_sum": pl.Float64,
        "supply_graph_total": pl.Float64,
        "supply_delta": pl.Float64,
        "demand": pl.Float64,
        "demand_bucket_sum": pl.Float64,
        "demand_graph_total": pl.Float64,
        "demand_delta": pl.Float64,
        "production_detail_delta": pl.Float64,
        "building_detail_delta": pl.Float64,
        "status": pl.String,
    }


def _json_debug(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)
