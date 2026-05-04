from __future__ import annotations

import json
from typing import Any

import polars as pl

from eu5gameparser.clausewitz.syntax import CList

EXTENDED_TARGET_SECTIONS = frozenset(
    {
        "character_db",
        "countries",
        "culture_manager",
        "dynasty_manager",
        "estate_manager",
        "loan_manager",
        "population",
        "provinces",
        "religion_manager",
    }
)

EXTENDED_SAVEGAME_TABLES = (
    "countries",
    "population",
    "provinces",
    "cultures",
    "religions",
    "estates",
    "loans",
    "characters",
    "dynasties",
)


def empty_extended_tables() -> dict[str, pl.DataFrame]:
    return {name: pl.DataFrame() for name in EXTENDED_SAVEGAME_TABLES}


def extended_tables_from_root(root: dict[str, Any]) -> dict[str, pl.DataFrame]:
    data = _root_to_python(root)
    return {
        "countries": _countries_table(data),
        "population": _population_table(data),
        "provinces": _provinces_table(data),
        "cultures": _cultures_table(data),
        "religions": _religions_table(data),
        "estates": _estates_table(data),
        "loans": _loans_table(data),
        "characters": _characters_table(data),
        "dynasties": _dynasties_table(data),
    }


def _root_to_python(root: dict[str, Any]) -> dict[str, Any]:
    return {key: _value_to_python(value) for key, value in root.items()}


def _value_to_python(value: Any) -> Any:
    if isinstance(value, CList):
        entries: dict[str, Any] = {}
        for entry in value.entries:
            converted = _value_to_python(entry.value)
            if entry.key in entries:
                existing = entries[entry.key]
                if isinstance(existing, list):
                    existing.append(converted)
                else:
                    entries[entry.key] = [existing, converted]
            else:
                entries[entry.key] = converted
        items = [_value_to_python(item) for item in value.items]
        if entries and items:
            entries["_items"] = items
            return entries
        if entries:
            return entries
        return items
    return value


def _database(data: dict[str, Any], section: str) -> dict[str, Any]:
    block = data.get(section)
    if not isinstance(block, dict):
        return {}
    database = block.get("database")
    return database if isinstance(database, dict) else {}


def _safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _country_tag(countries: dict[str, Any], country_id: Any) -> str | None:
    cid = _safe_int(country_id)
    if cid is None:
        return None
    record = countries.get(str(cid))
    if not isinstance(record, dict):
        return None
    return (
        record.get("definition")
        or record.get("tag")
        or record.get("country_name")
        or record.get("name")
    )


def _culture_name(cultures: dict[str, Any], culture_id: Any) -> str | None:
    cid = _safe_int(culture_id)
    if cid is None:
        return None
    record = cultures.get(str(cid))
    if not isinstance(record, dict):
        return None
    return record.get("name") or record.get("culture_definition")


def _religion_name(religions: dict[str, Any], religion_id: Any) -> str | None:
    rid = _safe_int(religion_id)
    if rid is None:
        return None
    record = religions.get(str(rid))
    if not isinstance(record, dict):
        return None
    return record.get("name") or record.get("key")


def _list_len_or_int(value: Any) -> int:
    if isinstance(value, list):
        return len(value)
    parsed = _safe_int(value)
    return parsed or 0


def _rows_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame()
    normalized = [_normalize_row(row) for row in rows]
    string_columns = {
        key
        for row in normalized
        for key, value in row.items()
        if isinstance(value, str)
    }
    for row in normalized:
        for key in string_columns:
            if key in row and row[key] is not None:
                row[key] = str(row[key])
    return pl.DataFrame(normalized, infer_schema_length=None)


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _normalize_cell(value) for key, value in row.items()}


def _normalize_cell(value: Any) -> Any:
    if isinstance(value, dict | list):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return value


def _countries_table(data: dict[str, Any]) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for country_id, country in _database(data, "countries").items():
        if country == "none" or not isinstance(country, dict):
            continue
        cid = _safe_int(country_id)
        if cid is None:
            continue
        currency = (
            country.get("currency_data") if isinstance(country.get("currency_data"), dict) else {}
        )
        economy = country.get("economy") if isinstance(country.get("economy"), dict) else {}
        score = country.get("score") if isinstance(country.get("score"), dict) else {}
        score_rating = (
            score.get("score_rating") if isinstance(score.get("score_rating"), dict) else {}
        )
        tax_rates = economy.get("tax_rates") if isinstance(economy.get("tax_rates"), dict) else {}
        maintenances = (
            economy.get("maintenances") if isinstance(economy.get("maintenances"), dict) else {}
        )
        row: dict[str, Any] = {
            "country_id": cid,
            "country_tag": _country_tag({str(cid): country}, cid) or "",
            "country_name": country.get("country_name") or country.get("name") or "",
            "country_type": country.get("country_type") or "",
            "rank": country.get("level") or "",
            "capital": _safe_int(country.get("capital")),
            "primary_culture": _safe_int(country.get("primary_culture")),
            "primary_religion": _safe_int(
                country.get("primary_religion") or country.get("religion")
            ),
            "population": _safe_float(country.get("population"), 0.0),
            "gold": _safe_float(currency.get("gold"), 0.0),
            "stability": _safe_float(currency.get("stability"), 0.0),
            "prestige": _safe_float(currency.get("prestige"), 0.0),
            "government_power": _safe_float(currency.get("government_power"), 0.0),
            "purity": _safe_float(currency.get("purity"), 0.0),
            "righteousness": _safe_float(currency.get("righteousness"), 0.0),
            "expense": _safe_float(economy.get("expense"), 0.0),
            "loan_capacity": _safe_float(economy.get("loan_capacity"), 0.0),
            "coin_minting": _safe_float(economy.get("coin_minting"), 0.0),
            "score_ADM": _safe_float(score_rating.get("ADM"), 0.0),
            "score_MIL": _safe_float(score_rating.get("MIL"), 0.0),
            "owned_locations_count": _list_len_or_int(country.get("owned_locations")),
            "controlled_locations_count": _list_len_or_int(country.get("controlled_locations")),
            "core_locations_count": _list_len_or_int(country.get("core_locations")),
        }
        for estate, rate in tax_rates.items():
            row[f"tax_{estate}"] = _safe_float(rate, 0.0)
        for maintenance in ("ArmyMaintenance", "NavyMaintenance", "FortMaintenance"):
            if maintenance in maintenances:
                row[f"maint_{maintenance.lower()}"] = _safe_float(maintenances[maintenance], 0.0)
        rows.append(row)
    return _rows_frame(rows)


def _population_table(data: dict[str, Any]) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    missing_columns: set[str] = set()
    cultures = _database(data, "culture_manager")
    religions = _database(data, "religion_manager")
    for pop_id, population in _database(data, "population").items():
        if population == "none" or not isinstance(population, dict):
            continue
        pid = _safe_int(pop_id)
        if pid is None:
            continue
        culture_id = _safe_int(population.get("culture"))
        religion_id = _safe_int(population.get("religion"))
        missing = population.get("missing") if isinstance(population.get("missing"), dict) else {}
        row: dict[str, Any] = {
            "pop_id": pid,
            "type": population.get("type") or "",
            "estate": population.get("estate") or "",
            "culture": culture_id,
            "culture_name": _culture_name(cultures, culture_id),
            "religion": religion_id,
            "religion_name": _religion_name(religions, religion_id),
            "status": population.get("status") or "",
            "size": _safe_float(population.get("size")),
            "satisfaction": _safe_float(population.get("satisfaction")),
            "literacy": _safe_float(population.get("literacy")),
            "goods": _safe_float(population.get("goods")),
            "price": _safe_float(population.get("price")),
        }
        for key, value in missing.items():
            if key == "demand":
                continue
            column = f"missing_{key}"
            row[column] = _safe_float(value)
            missing_columns.add(column)
        rows.append(row)
    for row in rows:
        for column in missing_columns:
            row.setdefault(column, None)
    return _rows_frame(rows)


def _provinces_table(data: dict[str, Any]) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    produced_columns: set[str] = set()
    countries = _database(data, "countries")
    for province_id, province in _database(data, "provinces").items():
        if province == "none" or not isinstance(province, dict):
            continue
        pid = _safe_int(province_id)
        if pid is None:
            continue
        food = province.get("food") if isinstance(province.get("food"), dict) else {}
        produced = (
            province.get("last_month_produced")
            if isinstance(province.get("last_month_produced"), dict)
            else {}
        )
        owner_id = _safe_int(province.get("owner"))
        row: dict[str, Any] = {
            "province_id": pid,
            "province_definition": province.get("province_definition") or "",
            "capital": _safe_int(province.get("capital")),
            "owner_id": owner_id,
            "owner_tag": _country_tag(countries, owner_id),
            "food_current": _safe_float(food.get("current")),
            "food_max": _safe_float(province.get("max_food_value")),
            "cached_food_change": _safe_float(province.get("cached_food_change")),
        }
        for good, amount in produced.items():
            column = f"produced_{good}"
            row[column] = _safe_float(amount)
            produced_columns.add(column)
        rows.append(row)
    for row in rows:
        for column in produced_columns:
            row.setdefault(column, None)
    return _rows_frame(rows)


def _cultures_table(data: dict[str, Any]) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for culture_id, culture in _database(data, "culture_manager").items():
        if culture == "none" or not isinstance(culture, dict):
            continue
        cid = _safe_int(culture_id)
        if cid is None:
            continue
        rgb = _rgb(culture)
        rows.append(
            {
                "culture_id": cid,
                "name": culture.get("name") or "",
                "culture_definition": culture.get("culture_definition") or "",
                "size": _safe_float(culture.get("size")),
                "language": culture.get("language") or "",
                "color_r": rgb[0],
                "color_g": rgb[1],
                "color_b": rgb[2],
            }
        )
    return _rows_frame(rows)


def _religions_table(data: dict[str, Any]) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for religion_id, religion in _database(data, "religion_manager").items():
        if religion == "none" or not isinstance(religion, dict):
            continue
        rid = _safe_int(religion_id)
        if rid is None:
            continue
        rgb = _rgb(religion)
        rows.append(
            {
                "religion_id": rid,
                "name": religion.get("name") or "",
                "key": religion.get("key") or "",
                "icon": religion.get("icon") or "",
                "group": religion.get("group") or "",
                "language": religion.get("language") or "",
                "has_karma": bool(religion.get("has_karma")),
                "color_r": rgb[0],
                "color_g": rgb[1],
                "color_b": rgb[2],
            }
        )
    return _rows_frame(rows)


def _rgb(record: dict[str, Any]) -> tuple[int | None, int | None, int | None]:
    color = record.get("color") if isinstance(record.get("color"), dict) else {}
    rgb = color.get("rgb")
    if not isinstance(rgb, list):
        return None, None, None
    values = [_safe_int(value) for value in rgb[:3]]
    return (
        values[0] if len(values) > 0 else None,
        values[1] if len(values) > 1 else None,
        values[2] if len(values) > 2 else None,
    )


def _estates_table(data: dict[str, Any]) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    countries = _database(data, "countries")
    for estate_id, estate in _database(data, "estate_manager").items():
        if estate == "none" or not isinstance(estate, dict):
            continue
        eid = _safe_int(estate_id)
        if eid is None:
            continue
        country_id = _safe_int(estate.get("country"))
        rows.append(
            {
                "estate_id": eid,
                "estate_type": estate.get("estate_type") or "",
                "country_id": country_id,
                "country_tag": _country_tag(countries, country_id),
                "wealth_impact": _safe_float(estate.get("wealth_impact")),
                "satisfaction": _safe_float(estate.get("satisfaction")),
                "existence": bool(estate.get("existence")),
            }
        )
    return _rows_frame(rows)


def _loans_table(data: dict[str, Any]) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    countries = _database(data, "countries")
    for loan_id, loan in _database(data, "loan_manager").items():
        if loan == "none" or not isinstance(loan, dict):
            continue
        lid = _safe_int(loan_id)
        if lid is None:
            continue
        borrower_id = _safe_int(loan.get("borrower"))
        rows.append(
            {
                "loan_id": lid,
                "borrower_country_id": borrower_id,
                "borrower_tag": _country_tag(countries, borrower_id),
                "amount": _safe_float(loan.get("amount")),
                "interest": _safe_float(loan.get("interest")),
                "month": _safe_int(loan.get("month")),
            }
        )
    return _rows_frame(rows)


def _characters_table(data: dict[str, Any]) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    countries = _database(data, "countries")
    cultures = _database(data, "culture_manager")
    religions = _database(data, "religion_manager")
    for character_id, character in _database(data, "character_db").items():
        if character == "none" or not isinstance(character, dict):
            continue
        cid = _safe_int(character_id)
        if cid is None:
            continue
        country_id = _safe_int(character.get("country"))
        culture_id = _safe_int(character.get("culture"))
        religion_id = _safe_int(character.get("religion"))
        death = character.get("death_data") if isinstance(character.get("death_data"), dict) else {}
        traits = character.get("traits")
        if isinstance(traits, list):
            traits_text = "|".join(str(trait) for trait in traits)
        elif traits is None:
            traits_text = ""
        else:
            traits_text = str(traits)
        rows.append(
            {
                "character_id": cid,
                "country_id": country_id,
                "country_tag": _country_tag(countries, country_id),
                "first_name": character.get("first_name") or "",
                "adm": _safe_int(character.get("adm")),
                "dip": _safe_int(character.get("dip")),
                "mil": _safe_int(character.get("mil")),
                "culture": culture_id,
                "culture_name": _culture_name(cultures, culture_id),
                "religion": religion_id,
                "religion_name": _religion_name(religions, religion_id),
                "estate": character.get("estate") or "",
                "dynasty": _safe_int(character.get("dynasty")),
                "birth_date": character.get("birth_date") or "",
                "birth_location_id": _safe_int(character.get("birth")),
                "death_date": death.get("death_date"),
                "traits": traits_text,
                "is_alive": not bool(death.get("death_date")),
            }
        )
    return _rows_frame(rows)


def _dynasties_table(data: dict[str, Any]) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for dynasty_id, dynasty in _database(data, "dynasty_manager").items():
        if dynasty == "none" or not isinstance(dynasty, dict):
            continue
        did = _safe_int(dynasty_id)
        if did is None:
            continue
        rows.append(
            {
                "dynasty_id": did,
                "key": dynasty.get("key") or "",
                "name": dynasty.get("name") or "",
                "home": _safe_int(dynasty.get("home")),
                "important": bool(dynasty.get("important")),
            }
        )
    return _rows_frame(rows)
