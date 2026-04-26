from __future__ import annotations

import json
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from eu5gameparser.clausewitz.parser import parse_file
from eu5gameparser.clausewitz.syntax import CEntry, CList, SourceLocation
from eu5gameparser.config import DEFAULT_GAME_ROOT
from eu5gameparser.scanner import iter_text_files

DEFAULT_LOAD_ORDER_PATH = Path("eu5_load_order.toml")
VANILLA_COMMON_RELATIVE = Path("game") / "in_game" / "common"
MOD_COMMON_RELATIVE = Path("in_game") / "common"
REPEATABLE_INJECT_KEYS = {"unique_production_methods", "obsolete"}


@dataclass(frozen=True)
class SourceRecord:
    layer_id: str
    layer_name: str
    mod_name: str | None
    mode: str
    file: str
    line: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "layer_id": self.layer_id,
            "layer_name": self.layer_name,
            "mod_name": self.mod_name,
            "mode": self.mode,
            "file": self.file,
            "line": self.line,
        }


@dataclass(frozen=True)
class GameLayer:
    id: str
    name: str
    root: Path
    kind: str

    @property
    def common_dir(self) -> Path:
        relative = VANILLA_COMMON_RELATIVE if self.kind == "vanilla" else MOD_COMMON_RELATIVE
        return self.root / relative


@dataclass(frozen=True)
class DataProfile:
    name: str
    layers: tuple[GameLayer, ...]


@dataclass(frozen=True)
class LoadOrderConfig:
    vanilla_root: Path
    mods: dict[str, GameLayer]
    profiles: dict[str, tuple[str, ...]]

    @classmethod
    def load(cls, path: str | Path = DEFAULT_LOAD_ORDER_PATH) -> LoadOrderConfig:
        config_path = _resolve_load_order_path(Path(path))
        data = tomllib.loads(config_path.read_text(encoding="utf-8"))
        vanilla_root = Path(data.get("paths", {}).get("vanilla_root", DEFAULT_GAME_ROOT))
        mods: dict[str, GameLayer] = {}
        for mod in data.get("mods", []):
            mod_id = str(mod["id"])
            mods[mod_id] = GameLayer(
                id=mod_id,
                name=str(mod.get("name") or mod_id),
                root=Path(mod["root"]),
                kind="mod",
            )
        profiles = {
            str(name): tuple(str(layer_id) for layer_id in layer_ids)
            for name, layer_ids in data.get("profiles", {}).items()
        }
        return cls(vanilla_root=vanilla_root, mods=mods, profiles=profiles)

    def profile(self, name: str = "merged_default") -> DataProfile:
        layer_ids = self.profiles.get(name)
        if layer_ids is None:
            raise KeyError(f"Unknown data profile {name!r}")
        layers = tuple(self.layer(layer_id) for layer_id in layer_ids)
        return DataProfile(name=name, layers=layers)

    def layer(self, layer_id: str) -> GameLayer:
        if layer_id == "vanilla":
            return GameLayer(
                id="vanilla",
                name="Vanilla",
                root=self.vanilla_root,
                kind="vanilla",
            )
        try:
            return self.mods[layer_id]
        except KeyError as exc:
            raise KeyError(f"Unknown data layer {layer_id!r}") from exc


@dataclass(frozen=True)
class MergedEntry:
    key: str
    value: CList
    location: SourceLocation
    source_layer: str
    source_mod: str | None
    source_mode: str
    source_history: tuple[SourceRecord, ...] = field(default_factory=tuple)

    @property
    def source_file(self) -> str:
        return str(self.location.path or "")

    @property
    def source_line(self) -> int:
        return self.location.line

    def source_history_json(self) -> str:
        return json.dumps(
            [record.to_dict() for record in self.source_history],
            sort_keys=True,
            separators=(",", ":"),
        )


@dataclass(frozen=True)
class MergedDirectory:
    entries: list[MergedEntry]
    warnings: list[str] = field(default_factory=list)


def load_profile(
    profile: str = "merged_default",
    load_order_path: str | Path = DEFAULT_LOAD_ORDER_PATH,
) -> DataProfile:
    return LoadOrderConfig.load(load_order_path).profile(profile)


def _resolve_load_order_path(path: Path) -> Path:
    if path.is_absolute() or path.exists():
        return path
    for base in (Path.cwd(), *Path.cwd().parents):
        candidate = base / path
        if candidate.exists():
            return candidate
    package_root = Path(__file__).resolve().parents[2]
    candidate = package_root / path
    if candidate.exists():
        return candidate
    return path


def load_merged_directory(profile: DataProfile, relative_dir: str | Path) -> MergedDirectory:
    relative = Path(relative_dir)
    effective: dict[str, MergedEntry] = {}
    file_objects: dict[Path, set[str]] = {}
    warnings: list[str] = []

    for layer in profile.layers:
        root = layer.common_dir / relative
        files = _effective_files_for_layer(root)
        for file in files:
            relative_file = file.relative_to(root)
            for key in file_objects.get(relative_file, set()):
                effective.pop(key, None)
            file_objects[relative_file] = set()
            for parsed_entry in parse_file(file).entries:
                if not isinstance(parsed_entry.value, CList):
                    continue
                mode, key = _entry_mode(parsed_entry.key)
                source = _source_record(layer, mode, parsed_entry)
                existing = effective.get(key)

                if mode in {"CREATE", "REPLACE", "REPLACE_OR_CREATE"}:
                    if mode == "REPLACE" and existing is None:
                        warnings.append(_missing_warning(mode, key, parsed_entry, layer))
                    effective[key] = _merged_entry(key, parsed_entry, layer, mode, (source,))
                    file_objects[relative_file].add(key)
                elif mode == "TRY_REPLACE":
                    if existing is not None:
                        effective[key] = _merged_entry(
                            key,
                            parsed_entry,
                            layer,
                            mode,
                            (*existing.source_history, source),
                        )
                        file_objects[relative_file].add(key)
                elif mode in {"INJECT", "TRY_INJECT", "INJECT_OR_CREATE"}:
                    if existing is None:
                        if mode == "INJECT":
                            warnings.append(_missing_warning(mode, key, parsed_entry, layer))
                        if mode == "INJECT_OR_CREATE":
                            effective[key] = _merged_entry(
                                key, parsed_entry, layer, mode, (source,)
                            )
                            file_objects[relative_file].add(key)
                        continue
                    effective[key] = MergedEntry(
                        key=key,
                        value=_inject_lists(existing.value, parsed_entry.value),
                        location=parsed_entry.location,
                        source_layer=layer.id,
                        source_mod=None if layer.kind == "vanilla" else layer.name,
                        source_mode=mode,
                        source_history=(*existing.source_history, source),
                    )
                    file_objects[relative_file].add(key)
                else:
                    warnings.append(f"Unsupported database entry mode {mode!r} for {key}")

    return MergedDirectory(entries=list(effective.values()), warnings=warnings)


def _effective_files_for_layer(root: Path) -> list[Path]:
    by_relative_path: dict[Path, Path] = {}
    for path in iter_text_files(root):
        by_relative_path[path.relative_to(root)] = path
    return [
        by_relative_path[key]
        for key in sorted(by_relative_path, key=lambda item: item.as_posix())
    ]


def _entry_mode(raw_key: str) -> tuple[str, str]:
    if ":" not in raw_key:
        return "CREATE", raw_key
    prefix, key = raw_key.split(":", 1)
    mode = prefix.strip().upper()
    return mode, key


def _source_record(layer: GameLayer, mode: str, entry: CEntry) -> SourceRecord:
    return SourceRecord(
        layer_id=layer.id,
        layer_name=layer.name,
        mod_name=None if layer.kind == "vanilla" else layer.name,
        mode=mode,
        file=str(entry.location.path or ""),
        line=entry.location.line,
    )


def _merged_entry(
    key: str,
    parsed_entry: CEntry,
    layer: GameLayer,
    mode: str,
    history: tuple[SourceRecord, ...],
) -> MergedEntry:
    return MergedEntry(
        key=key,
        value=parsed_entry.value,
        location=parsed_entry.location,
        source_layer=layer.id,
        source_mod=None if layer.kind == "vanilla" else layer.name,
        source_mode=mode,
        source_history=history,
    )


def _inject_lists(base: CList, patch: CList) -> CList:
    entries = list(base.entries)
    items = [*base.items, *patch.items]
    for patch_entry in patch.entries:
        if patch_entry.key in REPEATABLE_INJECT_KEYS:
            entries.append(patch_entry)
            continue
        match_index = _last_entry_index(entries, patch_entry.key)
        if (
            match_index is not None
            and isinstance(entries[match_index].value, CList)
            and isinstance(patch_entry.value, CList)
        ):
            existing = entries[match_index]
            entries[match_index] = CEntry(
                key=existing.key,
                op=patch_entry.op,
                value=_inject_lists(existing.value, patch_entry.value),
                location=patch_entry.location,
            )
        else:
            entries.append(patch_entry)
    return CList(entries=entries, items=items)


def _last_entry_index(entries: list[CEntry], key: str) -> int | None:
    for index in range(len(entries) - 1, -1, -1):
        if entries[index].key == key:
            return index
    return None


def _missing_warning(mode: str, key: str, entry: CEntry, layer: GameLayer) -> str:
    return (
        f"{mode}:{key} in {entry.location.path}:{entry.location.line} "
        f"has no existing object in layer {layer.id}"
    )
