from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SearchHit:
    path: Path
    line_number: int
    line: str


def iter_text_files(root: Path, pattern: str = "*.txt") -> Iterable[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.rglob(pattern) if path.is_file())


def search_text(
    root: Path, needle: str, pattern: str = "*.txt", ignore_case: bool = True
) -> list[SearchHit]:
    hits: list[SearchHit] = []
    sought = needle.lower() if ignore_case else needle
    for path in iter_text_files(root, pattern):
        try:
            lines = path.read_text(encoding="utf-8-sig").splitlines()
        except UnicodeDecodeError:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        for line_number, line in enumerate(lines, start=1):
            haystack = line.lower() if ignore_case else line
            if sought in haystack:
                hits.append(SearchHit(path=path, line_number=line_number, line=line))
    return hits
