from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Token:
    kind: str
    value: str
    line: int
    column: int


class TokenizerError(ValueError):
    pass


OPERATORS = ("<=", ">=", "!=", "=", "<", ">")
SYMBOLS = {"{": "LBRACE", "}": "RBRACE"}


def tokenize(text: str, path: Path | None = None) -> list[Token]:
    tokens: list[Token] = []
    i = 0
    line = 1
    column = 1

    while i < len(text):
        char = text[i]

        if char in " \t\r":
            i += 1
            column += 1
            continue

        if char == "\n":
            i += 1
            line += 1
            column = 1
            continue

        if char == "#":
            while i < len(text) and text[i] != "\n":
                i += 1
                column += 1
            continue

        if char in SYMBOLS:
            tokens.append(Token(SYMBOLS[char], char, line, column))
            i += 1
            column += 1
            continue

        op = next((candidate for candidate in OPERATORS if text.startswith(candidate, i)), None)
        if op is not None:
            tokens.append(Token("OP", op, line, column))
            i += len(op)
            column += len(op)
            continue

        if char == '"':
            start_line = line
            start_column = column
            i += 1
            column += 1
            value: list[str] = []
            while i < len(text):
                char = text[i]
                if char == "\\" and i + 1 < len(text):
                    value.append(text[i + 1])
                    i += 2
                    column += 2
                    continue
                if char == '"':
                    i += 1
                    column += 1
                    tokens.append(Token("ATOM", "".join(value), start_line, start_column))
                    break
                if char == "\n":
                    line += 1
                    column = 1
                    value.append(char)
                    i += 1
                    continue
                value.append(char)
                i += 1
                column += 1
            else:
                source = f" in {path}" if path else ""
                raise TokenizerError(f"Unterminated string at {start_line}:{start_column}{source}")
            continue

        start = i
        start_column = column
        while i < len(text):
            char = text[i]
            if char.isspace() or char in '{}#<>=!"':
                break
            i += 1
            column += 1
        if start == i:
            source = f" in {path}" if path else ""
            raise TokenizerError(f"Unexpected character {char!r} at {line}:{column}{source}")
        tokens.append(Token("ATOM", text[start:i], line, start_column))

    tokens.append(Token("EOF", "", line, column))
    return tokens
