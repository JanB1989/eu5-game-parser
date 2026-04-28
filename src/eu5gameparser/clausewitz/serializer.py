from __future__ import annotations

from typing import Any

from eu5gameparser.clausewitz.parser import parse_text
from eu5gameparser.clausewitz.syntax import CDocument, CEntry, CList, Scalar, Value


def render_document(document: CDocument) -> str:
    return "\n".join(render_entry(entry) for entry in document.entries).rstrip() + "\n"


def render_entry(entry: CEntry, *, indent: int = 0) -> str:
    prefix = "\t" * indent
    return f"{prefix}{entry.key} {entry.op} {render_value(entry.value, indent=indent)}"


def render_value(value: Value, *, indent: int = 0) -> str:
    if isinstance(value, CList):
        return render_list(value, indent=indent)
    return render_scalar(value)


def render_list(block: CList, *, indent: int = 0) -> str:
    if not block.entries and not block.items:
        return "{}"
    lines = ["{"]
    for item in block.items:
        if isinstance(item, CList):
            lines.append(f"{'\t' * (indent + 1)}{render_list(item, indent=indent + 1)}")
        else:
            lines.append(f"{'\t' * (indent + 1)}{render_scalar(item)}")
    for entry in block.entries:
        lines.append(render_entry(entry, indent=indent + 1))
    lines.append(f"{'\t' * indent}}}")
    return "\n".join(lines)


def render_scalar(value: Scalar) -> str:
    if value is True:
        return "yes"
    if value is False:
        return "no"
    return str(value)


def normalized_document(document: CDocument) -> list[dict[str, Any]]:
    return [normalized_entry(entry) for entry in document.entries]


def normalized_entry(entry: CEntry) -> dict[str, Any]:
    return {"key": entry.key, "op": entry.op, "value": normalized_value(entry.value)}


def normalized_value(value: Value) -> Any:
    if isinstance(value, CList):
        return {
            "items": [normalized_value(item) for item in value.items],
            "entries": [normalized_entry(entry) for entry in value.entries],
        }
    return value


def normalized_text(text: str) -> list[dict[str, Any]]:
    return normalized_document(parse_text(text))
