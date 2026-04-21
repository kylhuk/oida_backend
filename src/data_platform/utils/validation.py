from __future__ import annotations

import re
from dataclasses import dataclass

from data_platform.utils.sql import quote_clickhouse_identifier, slug_to_identifier

_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,119}$")
_MAX_IDENTIFIER_LENGTH = 255
_DANGEROUS_SQL_RE = re.compile(
    r"\b(ALTER|ATTACH|CALL|COPY|CREATE|DELETE|DETACH|DROP|EXPORT|INSTALL|INSERT|LOAD|MERGE|PRAGMA|SET|TRUNCATE|UPDATE|USE|VACUUM)\b",
    re.IGNORECASE,
)
_READ_ONLY_SQL_START_RE = re.compile(r"^\s*(WITH|SELECT)\b", re.IGNORECASE | re.DOTALL)
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DEFAULT_PARTITION_BY = "toYYYYMM(_ingested_at)"
_DEFAULT_ORDER_BY = ["_dataset_slug", "_ingested_at", "_ingestion_id"]


@dataclass(frozen=True)
class _IdentifierExpression:
    name: str


@dataclass(frozen=True)
class _FunctionCallExpression:
    name: str
    arguments: tuple["_Expression", ...]


_Expression = _IdentifierExpression | _FunctionCallExpression


def validate_slug(value: str, field_name: str = "slug") -> str:
    normalized = value.strip().lower()
    if not _SLUG_RE.match(normalized):
        raise ValueError(
            f"{field_name} must match {_SLUG_RE.pattern!r} and contain only lowercase letters, digits, underscores, or hyphens."
        )
    return normalized


def validate_optional_identifier(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    normalized = slug_to_identifier(value)
    if not normalized:
        raise ValueError(f"{field_name} cannot be empty.")
    if len(normalized) > _MAX_IDENTIFIER_LENGTH:
        raise ValueError(
            f"{field_name} must be at most {_MAX_IDENTIFIER_LENGTH} characters after normalization."
        )
    return normalized


def _mask_sql_non_code(query: str) -> tuple[str, str]:
    masked: list[str] = []
    index = 0
    state = "code"

    while index < len(query):
        char = query[index]
        next_char = query[index + 1] if index + 1 < len(query) else ""

        if state == "code":
            if char == "'":
                masked.append(" ")
                state = "single_quote"
            elif char == '"':
                masked.append(" ")
                state = "double_quote"
            elif char == "-" and next_char == "-":
                masked.extend([" ", " "])
                state = "line_comment"
                index += 1
            elif char == "/" and next_char == "*":
                masked.extend([" ", " "])
                state = "block_comment"
                index += 1
            else:
                masked.append(char)
        elif state == "single_quote":
            if char == "'" and next_char == "'":
                masked.extend([" ", " "])
                index += 1
            elif char == "'":
                masked.append(" ")
                state = "code"
            else:
                masked.append("\n" if char == "\n" else " ")
        elif state == "double_quote":
            if char == '"' and next_char == '"':
                masked.extend([" ", " "])
                index += 1
            elif char == '"':
                masked.append(" ")
                state = "code"
            else:
                masked.append("\n" if char == "\n" else " ")
        elif state == "line_comment":
            if char == "\n":
                masked.append("\n")
                state = "code"
            else:
                masked.append(" ")
        elif state == "block_comment":
            if char == "*" and next_char == "/":
                masked.extend([" ", " "])
                state = "code"
                index += 1
            else:
                masked.append("\n" if char == "\n" else " ")

        index += 1

    return "".join(masked), state


def _strip_terminal_semicolons(query: str, masked_query: str) -> tuple[str, str]:
    query_chars = list(query)
    masked_chars = list(masked_query)

    while True:
        last_code_index = next((idx for idx in range(len(masked_chars) - 1, -1, -1) if not masked_chars[idx].isspace()), -1)
        if last_code_index == -1 or masked_chars[last_code_index] != ";":
            break
        query_chars[last_code_index] = " "
        masked_chars[last_code_index] = " "

    return "".join(query_chars).strip(), "".join(masked_chars)


class _ExpressionParser:
    def __init__(self, value: str, field_name: str) -> None:
        self.value = value
        self.field_name = field_name
        self.index = 0

    def parse_expression(self) -> _Expression:
        identifier = self._parse_identifier()
        self._skip_whitespace()
        if self._peek() != "(":
            return _IdentifierExpression(identifier)

        self.index += 1
        self._skip_whitespace()
        arguments: list[_Expression] = []
        if self._peek() != ")":
            while True:
                arguments.append(self.parse_expression())
                self._skip_whitespace()
                token = self._peek()
                if token == ",":
                    self.index += 1
                    self._skip_whitespace()
                    continue
                if token == ")":
                    break
                raise self._error("Expected ',' or ')' in expression.")
        self._expect(")")
        return _FunctionCallExpression(identifier, tuple(arguments))

    def parse_expression_list(self, allow_parenthesized: bool = False) -> list[_Expression]:
        self._skip_whitespace()
        if allow_parenthesized and self._peek() == "(":
            self.index += 1
            self._skip_whitespace()
            expressions = self._parse_nonempty_expression_list(end_token=")")
            self._expect(")")
            self._skip_whitespace()
            if not self._at_end():
                raise self._error("Unexpected trailing content.")
            return expressions

        expressions = self._parse_nonempty_expression_list(end_token=None)
        self._skip_whitespace()
        if not self._at_end():
            raise self._error("Unexpected trailing content.")
        return expressions

    def parse_single_expression(self) -> _Expression:
        expression = self.parse_expression()
        self._skip_whitespace()
        if not self._at_end():
            raise self._error("Unexpected trailing content.")
        return expression

    def _parse_nonempty_expression_list(self, end_token: str | None) -> list[_Expression]:
        expressions = [self.parse_expression()]
        while True:
            self._skip_whitespace()
            token = self._peek()
            if token == ",":
                self.index += 1
                self._skip_whitespace()
                expressions.append(self.parse_expression())
                continue
            if end_token is not None and token == end_token:
                break
            if end_token is None or self._at_end():
                break
            raise self._error("Expected ',' between expressions.")
        return expressions

    def _parse_identifier(self) -> str:
        self._skip_whitespace()
        start = self.index
        while self.index < len(self.value) and (self.value[self.index].isalnum() or self.value[self.index] == "_"):
            self.index += 1
        identifier = self.value[start:self.index]
        if not identifier or not _IDENTIFIER_RE.match(identifier):
            if self._at_end():
                raise self._error("Expected identifier.")
            raise self._error(f"Unsupported token {self.value[self.index]!r}.")
        return identifier

    def _expect(self, expected: str) -> None:
        self._skip_whitespace()
        if self._peek() != expected:
            raise self._error(f"Expected {expected!r}.")
        self.index += 1

    def _skip_whitespace(self) -> None:
        while self.index < len(self.value) and self.value[self.index].isspace():
            self.index += 1

    def _peek(self) -> str | None:
        if self.index >= len(self.value):
            return None
        return self.value[self.index]

    def _at_end(self) -> bool:
        return self.index >= len(self.value)

    def _error(self, message: str) -> ValueError:
        return ValueError(f"{self.field_name} must be a safe ClickHouse expression. {message}")


def _render_expression(expression: _Expression) -> str:
    if isinstance(expression, _IdentifierExpression):
        return quote_clickhouse_identifier(expression.name)
    return expression.name + "(" + ", ".join(_render_expression(argument) for argument in expression.arguments) + ")"


def _parse_safe_expression(value: str, field_name: str) -> _Expression:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string.")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} cannot be empty.")
    return _ExpressionParser(normalized, field_name).parse_single_expression()


def render_clickhouse_partition_by(value: str | None) -> str:
    field_name = "serving_config.partition_by"
    expression = _parse_safe_expression(_DEFAULT_PARTITION_BY if value is None else value, field_name)
    return _render_expression(expression)


def render_clickhouse_order_by(value: str | list[str] | None) -> str:
    field_name = "serving_config.order_by"
    if value is None:
        expressions = [_parse_safe_expression(item, field_name) for item in _DEFAULT_ORDER_BY]
    elif isinstance(value, list):
        if not value:
            raise ValueError(f"{field_name} cannot be an empty list.")
        expressions = []
        for item in value:
            if not isinstance(item, str):
                raise ValueError(f"{field_name} list items must be strings.")
            expressions.append(_parse_safe_expression(item, field_name))
    elif isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            raise ValueError(f"{field_name} cannot be empty.")
        expressions = _ExpressionParser(normalized, field_name).parse_expression_list(allow_parenthesized=True)
    else:
        raise ValueError(f"{field_name} must be a string or list of strings.")

    if len(expressions) == 1:
        return _render_expression(expressions[0])
    return "(" + ", ".join(_render_expression(expression) for expression in expressions) + ")"


def validate_serving_config(serving_config: dict | None) -> dict:
    normalized = dict(serving_config or {})
    if "partition_by" in normalized:
        normalized["partition_by"] = render_clickhouse_partition_by(normalized["partition_by"])
    if "order_by" in normalized:
        normalized["order_by"] = render_clickhouse_order_by(normalized["order_by"])
    return normalized


def validate_read_only_sql(query: str | None, field_name: str) -> str | None:
    if query is None:
        return None

    sanitized = query.strip()
    if not sanitized:
        return None

    masked, terminal_state = _mask_sql_non_code(sanitized)
    if terminal_state in {"single_quote", "double_quote", "block_comment"}:
        raise ValueError(f"{field_name} contains an unterminated string literal or block comment.")

    sanitized, masked = _strip_terminal_semicolons(sanitized, masked)
    executable_sql = masked.strip()

    if ";" in executable_sql:
        raise ValueError(f"{field_name} must contain a single statement.")

    if not _READ_ONLY_SQL_START_RE.match(executable_sql):
        raise ValueError(f"{field_name} must start with SELECT or WITH.")

    if _DANGEROUS_SQL_RE.search(executable_sql):
        raise ValueError(f"{field_name} must be read-only SQL.")

    return sanitized
