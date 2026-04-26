from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_GAME_ROOT = Path(r"C:\Games\steamapps\common\Europa Universalis V")
COMMON_RELATIVE = Path("game") / "in_game" / "common"


@dataclass(frozen=True)
class ParserConfig:
    """Filesystem configuration for a patchable EU5 install or fixture tree."""

    game_root: Path = DEFAULT_GAME_ROOT
    common_relative: Path = COMMON_RELATIVE
    building_categories_relative: Path = Path("building_categories")
    building_types_relative: Path = Path("building_types")
    production_methods_relative: Path = Path("production_methods")

    @classmethod
    def from_env(cls, game_root: str | Path | None = None) -> ParserConfig:
        root = Path(game_root or os.environ.get("EU5_GAME_ROOT") or DEFAULT_GAME_ROOT)
        return cls(game_root=root)

    @property
    def common_dir(self) -> Path:
        return self.game_root / self.common_relative

    @property
    def building_categories_dir(self) -> Path:
        return self.common_dir / self.building_categories_relative

    @property
    def building_types_dir(self) -> Path:
        return self.common_dir / self.building_types_relative

    @property
    def production_methods_dir(self) -> Path:
        return self.common_dir / self.production_methods_relative

    def paths(self) -> dict[str, Path]:
        return {
            "game_root": self.game_root,
            "common": self.common_dir,
            "building_categories": self.building_categories_dir,
            "building_types": self.building_types_dir,
            "production_methods": self.production_methods_dir,
        }
