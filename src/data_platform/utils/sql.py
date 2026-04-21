from __future__ import annotations

import re


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def slug_to_identifier(value: str) -> str:
    candidate = re.sub(r"[^A-Za-z0-9_]+", "_", value.strip().lower())
    candidate = re.sub(r"_+", "_", candidate).strip("_")
    if not candidate:
        raise ValueError("Identifier cannot be empty.")
    if candidate[0].isdigit():
        candidate = f"d_{candidate}"
    return candidate


def default_gold_table_name(dataset_slug: str) -> str:
    return f"dataset_{slug_to_identifier(dataset_slug)}"


def quote_duckdb_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def quote_clickhouse_identifier(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def assert_safe_identifier(name: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Unsafe identifier: {name}")
    return name


def sql_string_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"
