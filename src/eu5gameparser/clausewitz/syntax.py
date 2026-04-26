from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

type Scalar = str | int | float | bool
type Value = Scalar | CList


@dataclass(frozen=True)
class SourceLocation:
    path: Path | None
    line: int
    column: int


@dataclass(frozen=True)
class CEntry:
    key: str
    op: str
    value: Value
    location: SourceLocation


@dataclass(frozen=True)
class CList:
    entries: list[CEntry] = field(default_factory=list)
    items: list[Value] = field(default_factory=list)

    def values(self, key: str) -> list[Value]:
        return [entry.value for entry in self.entries if entry.key == key]

    def first(self, key: str, default: Value | None = None) -> Value | None:
        values = self.values(key)
        return values[0] if values else default


@dataclass(frozen=True)
class CDocument:
    entries: list[CEntry]
    path: Path | None = None

    def values(self, key: str) -> list[Value]:
        return [entry.value for entry in self.entries if entry.key == key]
