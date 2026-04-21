from __future__ import annotations

import hashlib
import json
import re
from collections import OrderedDict


_COMPLEX_TYPE_MARKERS = ("LIST", "STRUCT", "MAP", "UNION")
_DECIMAL_TYPE_RE = re.compile(r"^(?:DECIMAL|NUMERIC)\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)$")
_MAX_DECIMAL_PRECISION = 38


def _is_complex_duckdb_type(value: str) -> bool:
    return any(token in value for token in _COMPLEX_TYPE_MARKERS) or "[]" in value or "ARRAY" in value


def _parse_decimal_type(raw_type: str) -> tuple[int, int] | None:
    match = _DECIMAL_TYPE_RE.match(raw_type.upper().strip())
    if not match:
        return None

    precision = int(match.group(1))
    scale = int(match.group(2) or 0)
    if precision <= 0 or scale < 0 or scale > precision:
        return None
    return precision, scale


def _canonical_decimal_type(raw_type: str) -> str | None:
    parsed = _parse_decimal_type(raw_type)
    if parsed is None:
        return None
    precision, scale = parsed
    return f"DECIMAL({precision},{scale})"


def _is_decimal_compatible_numeric(value: str) -> bool:
    return value.startswith("DECIMAL") or value in {"BOOLEAN", "BIGINT"}


def normalize_duckdb_type(raw_type: str) -> str:
    value = raw_type.upper().strip()

    # DuckDB can expose nested/list-valued columns as LIST/STRUCT/MAP types or
    # scalar-looking array syntax such as INTEGER[] and TIMESTAMP[]. The platform
    # serves these conservatively as strings rather than misclassifying them as
    # scalars during schema evolution.
    if _is_complex_duckdb_type(value):
        return "VARCHAR"
    canonical_decimal = _canonical_decimal_type(value)
    if canonical_decimal is not None:
        return canonical_decimal
    if value.startswith("DECIMAL"):
        return value
    if any(token in value for token in ["VARCHAR", "TEXT", "STRING", "UUID", "BLOB", "JSON"]):
        return "VARCHAR"
    if value.startswith("BOOL"):
        return "BOOLEAN"
    if any(value.startswith(prefix) for prefix in ["DOUBLE", "FLOAT", "REAL"]):
        return "DOUBLE"
    if any(value.startswith(prefix) for prefix in ["TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT"]):
        return "BIGINT"
    if value.startswith("DATE"):
        return "DATE"
    if "TIMESTAMP" in value or value.startswith("DATETIME"):
        return "TIMESTAMP"
    return "VARCHAR"


def widen_duckdb_types(existing_type: str, new_type: str) -> str:
    left = normalize_duckdb_type(existing_type)
    right = normalize_duckdb_type(new_type)

    if left == right:
        return left
    if "VARCHAR" in {left, right}:
        return "VARCHAR"
    if {left, right} <= {"BIGINT", "DOUBLE"}:
        return "DOUBLE"
    if left.startswith("DECIMAL") or right.startswith("DECIMAL"):
        if "DOUBLE" in {left, right}:
            return "DOUBLE"
        if not (_is_decimal_compatible_numeric(left) and _is_decimal_compatible_numeric(right)):
            return "VARCHAR"

        decimal_scales = [parsed[1] for value in (left, right) if (parsed := _parse_decimal_type(value)) is not None]
        if not decimal_scales:
            return "DECIMAL(38,10)"
        return f"DECIMAL({_MAX_DECIMAL_PRECISION},{min(max(decimal_scales), _MAX_DECIMAL_PRECISION)})"
    if {left, right} <= {"DATE", "TIMESTAMP"}:
        return "TIMESTAMP"
    if {left, right} == {"BOOLEAN", "BIGINT"}:
        return "BIGINT"
    return "VARCHAR"


def merge_schemas(
    existing_schema: list[dict[str, str]] | None,
    new_schema: list[dict[str, str]],
) -> list[dict[str, str]]:
    merged: OrderedDict[str, str] = OrderedDict()

    for column in existing_schema or []:
        merged[column["name"]] = normalize_duckdb_type(column["type"])

    for column in new_schema:
        name = column["name"]
        column_type = normalize_duckdb_type(column["type"])
        if name in merged:
            merged[name] = widen_duckdb_types(merged[name], column_type)
        else:
            merged[name] = column_type

    return [{"name": name, "type": dtype} for name, dtype in merged.items()]



def diff_schemas(
    from_schema: list[dict[str, str]] | None,
    to_schema: list[dict[str, str]] | None,
) -> dict[str, object]:
    normalized_from = [
        {"name": item["name"], "type": normalize_duckdb_type(item["type"])}
        for item in (from_schema or [])
    ]
    normalized_to = [
        {"name": item["name"], "type": normalize_duckdb_type(item["type"])}
        for item in (to_schema or [])
    ]

    from_by_name = {item["name"]: item["type"] for item in normalized_from}
    to_by_name = {item["name"]: item["type"] for item in normalized_to}

    added_columns = [item for item in normalized_to if item["name"] not in from_by_name]
    removed_columns = [item for item in normalized_from if item["name"] not in to_by_name]

    changed_columns: list[dict[str, object]] = []
    for item in normalized_to:
        name = item["name"]
        if name not in from_by_name:
            continue

        previous_type = from_by_name[name]
        current_type = item["type"]
        if previous_type == current_type:
            continue

        widened_type = widen_duckdb_types(previous_type, current_type)
        compatible = widened_type == current_type
        changed_columns.append(
            {
                "name": name,
                "from_type": previous_type,
                "to_type": current_type,
                "compatible": compatible,
            }
        )

    breaking_changes = bool(removed_columns or changed_columns)
    return {
        "from_schema": normalized_from,
        "to_schema": normalized_to,
        "added_columns": added_columns,
        "removed_columns": removed_columns,
        "changed_columns": changed_columns,
        "breaking_changes": breaking_changes,
        "has_changes": bool(added_columns or removed_columns or changed_columns),
    }


def assess_schema_compatibility(
    current_schema: list[dict[str, str]] | None,
    candidate_schema: list[dict[str, str]] | None,
) -> dict[str, object]:
    diff = diff_schemas(current_schema, candidate_schema)
    merged_schema = merge_schemas(diff["from_schema"], diff["to_schema"])
    contract_compatible = not diff["removed_columns"] and all(
        bool(item["compatible"]) for item in diff["changed_columns"]
    )
    strict_mode_compatible = not bool(diff["has_changes"])

    return {
        "current_schema": diff["from_schema"],
        "candidate_schema": diff["to_schema"],
        "merged_schema": merged_schema,
        "added_columns": diff["added_columns"],
        "removed_columns": diff["removed_columns"],
        "changed_columns": diff["changed_columns"],
        "breaking_changes": bool(diff["breaking_changes"]),
        "has_changes": bool(diff["has_changes"]),
        "contract_compatible": contract_compatible,
        "strict_mode_compatible": strict_mode_compatible,
    }


def schema_fingerprint(schema: list[dict[str, str]]) -> str:
    normalized = [{"name": item["name"], "type": normalize_duckdb_type(item["type"])} for item in schema]
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def schemas_equal(left: list[dict[str, str]] | None, right: list[dict[str, str]] | None) -> bool:
    return (left or []) == (right or [])


def duckdb_to_clickhouse_type(duckdb_type: str) -> str:
    normalized = normalize_duckdb_type(duckdb_type)

    if normalized == "BOOLEAN":
        return "Nullable(Bool)"
    if normalized == "BIGINT":
        return "Nullable(Int64)"
    if normalized == "DOUBLE":
        return "Nullable(Float64)"
    if normalized.startswith("DECIMAL"):
        parsed = _parse_decimal_type(normalized)
        scale = 10 if parsed is None else min(parsed[1], _MAX_DECIMAL_PRECISION)
        return f"Nullable(Decimal({_MAX_DECIMAL_PRECISION},{scale}))"
    if normalized == "DATE":
        return "Nullable(Date)"
    if normalized == "TIMESTAMP":
        return "Nullable(DateTime64(3))"
    return "Nullable(String)"
