from __future__ import annotations

import pytest

from data_platform.utils.data_product_versions import (
    build_data_product_version_snapshot,
    data_product_version_matches_current,
    validate_contiguous_data_product_versions,
)


class _Product:
    id = "prod-1"
    dataset_id = "ds-1"
    current_version = 3
    slug = "orders_gold"
    name = "Orders Gold"
    description = "Current"
    table_name = "orders_gold"
    config = {"freshness": {"minutes": 15}}
    is_default = True


def test_build_data_product_version_snapshot_normalizes_version_and_copies_config() -> None:
    snapshot = build_data_product_version_snapshot(_Product())

    assert snapshot["data_product_id"] == "prod-1"
    assert snapshot["dataset_id"] == "ds-1"
    assert snapshot["version"] == 3
    assert snapshot["slug"] == "orders_gold"
    assert snapshot["config"] == {"freshness": {"minutes": 15}}

    snapshot["config"]["freshness"]["minutes"] = 60
    assert _Product.config == {"freshness": {"minutes": 15}}


def test_data_product_version_matches_current_ignores_metadata_fields() -> None:
    current = {
        "id": "prod-1",
        "dataset_id": "ds-1",
        "current_version": 4,
        "slug": "orders_gold",
        "name": "Orders Gold",
        "description": "Current",
        "table_name": "orders_gold",
        "config": {"freshness": {"minutes": 15}},
        "is_default": True,
    }
    version = {
        "id": "ver-4",
        "data_product_id": "prod-1",
        "dataset_id": "ds-1",
        "version": 4,
        "slug": "orders_gold",
        "name": "Orders Gold",
        "description": "Current",
        "table_name": "orders_gold",
        "config": {"freshness": {"minutes": 15}},
        "is_default": True,
    }
    mismatched = dict(version)
    mismatched["name"] = "Orders Gold v4"

    assert data_product_version_matches_current(current, version) is True
    assert data_product_version_matches_current(current, mismatched) is False


def test_validate_contiguous_data_product_versions_rejects_gaps() -> None:
    validate_contiguous_data_product_versions([1, 2, 3])

    with pytest.raises(ValueError, match="consecutive version numbers"):
        validate_contiguous_data_product_versions([1, 3])
