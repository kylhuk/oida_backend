from __future__ import annotations

from collections.abc import Iterable, Mapping
from copy import deepcopy
from typing import Any


def _read_value(source: Any, name: str, default: Any = None) -> Any:
    if isinstance(source, Mapping):
        return source.get(name, default)
    return getattr(source, name, default)


_VERSION_SNAPSHOT_FIELDS: tuple[str, ...] = (
    "slug",
    "name",
    "description",
    "table_name",
    "config",
    "is_default",
)


def build_data_product_version_snapshot(source: Any, *, version: int | None = None) -> dict[str, Any]:
    resolved_version = version if version is not None else _read_value(source, "current_version", 1)
    try:
        normalized_version = int(resolved_version)
    except (TypeError, ValueError):
        normalized_version = 1
    if normalized_version < 1:
        normalized_version = 1

    snapshot: dict[str, Any] = {
        "data_product_id": _read_value(source, "id"),
        "dataset_id": _read_value(source, "dataset_id"),
        "version": normalized_version,
    }
    for field_name in _VERSION_SNAPSHOT_FIELDS:
        value = _read_value(source, field_name)
        if field_name == "config":
            snapshot[field_name] = deepcopy(value) if isinstance(value, Mapping) else {}
        else:
            snapshot[field_name] = value
    snapshot["is_default"] = bool(snapshot["is_default"])
    return snapshot


def data_product_version_matches_current(current_source: Any, version_source: Any) -> bool:
    current = build_data_product_version_snapshot(current_source)
    version = build_data_product_version_snapshot(version_source)
    return {key: current[key] for key in _VERSION_SNAPSHOT_FIELDS} == {
        key: version[key] for key in _VERSION_SNAPSHOT_FIELDS
    }


def validate_contiguous_data_product_versions(versions: Iterable[int]) -> None:
    ordered = sorted(int(version) for version in versions)
    expected = list(range(1, len(ordered) + 1))
    if ordered != expected:
        raise ValueError("data product versions must use consecutive version numbers starting at 1.")
