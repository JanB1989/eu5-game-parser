from __future__ import annotations

from pathlib import Path

from eu5gameparser.clausewitz.syntax import CDocument, CEntry, CList, Scalar, SourceLocation, Value
from eu5gameparser.clausewitz.tokenizer import Token, tokenize


class ParserError(ValueError):
    pass


class _Parser:
    def __init__(self, tokens: list[Token], path: Path | None) -> None:
        self.tokens = tokens
        self.path = path
        self.position = 0

    def parse_document(self) -> CDocument:
        entries: list[CEntry] = []
        while not self._at("EOF"):
            entries.append(self._parse_entry())
        return CDocument(entries=entries, path=self.path)

    def _parse_entry(self) -> CEntry:
        key = self._expect("ATOM")
        op = self._expect("OP")
        value = self._parse_value()
        return CEntry(
            key=key.value,
            op=op.value,
            value=value,
            location=SourceLocation(self.path, key.line, key.column),
        )

    def _parse_value(self) -> Value:
        if self._at("LBRACE"):
            return self._parse_list()
        atom = self._expect("ATOM")
        return parse_scalar(atom.value)

    def _parse_list(self) -> CList:
        self._expect("LBRACE")
        entries: list[CEntry] = []
        items: list[Value] = []
        while not self._at("RBRACE"):
            if self._at("EOF"):
                raise self._error("Unexpected end of file inside block")
            if self._at("LBRACE"):
                items.append(self._parse_list())
                continue
            atom = self._expect("ATOM")
            if self._at("OP"):
                op = self._expect("OP")
                entries.append(
                    CEntry(
                        key=atom.value,
                        op=op.value,
                        value=self._parse_value(),
                        location=SourceLocation(self.path, atom.line, atom.column),
                    )
                )
            else:
                items.append(parse_scalar(atom.value))
        self._expect("RBRACE")
        return CList(entries=entries, items=items)

    def _at(self, kind: str) -> bool:
        return self.tokens[self.position].kind == kind

    def _expect(self, kind: str) -> Token:
        token = self.tokens[self.position]
        if token.kind != kind:
            raise self._error(f"Expected {kind}, got {token.kind}")
        self.position += 1
        return token

    def _error(self, message: str) -> ParserError:
        token = self.tokens[self.position]
        source = f" in {self.path}" if self.path else ""
        return ParserError(f"{message} at {token.line}:{token.column}{source}")


def parse_scalar(value: str) -> Scalar:
    lowered = value.lower()
    if lowered == "yes":
        return True
    if lowered == "no":
        return False
    try:
        if any(marker in value for marker in (".", "e", "E")):
            return float(value)
        return int(value)
    except ValueError:
        return value


def parse_text(text: str, path: str | Path | None = None) -> CDocument:
    parsed_path = Path(path) if path is not None else None
    return _Parser(tokenize(text, parsed_path), parsed_path).parse_document()


def parse_file(path: str | Path, encoding: str = "utf-8-sig") -> CDocument:
    parsed_path = Path(path)
    return parse_text(parsed_path.read_text(encoding=encoding), parsed_path)
