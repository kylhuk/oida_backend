import pytest

from data_platform.utils.data_product_versions import (
    build_data_product_version_snapshot,
    data_product_version_matches_current,
    validate_contiguous_data_product_versions,
)


def test_build_data_product_version_snapshot_normalizes_version_and_copies_config() -> None:
    source = {
        "id": "product-1",
        "dataset_id": "dataset-1",
        "slug": "orders",
        "name": "Orders",
        "description": "Frontend orders table",
        "table_name": "orders_gold",
        "config": {"columns": ["id", "amount"]},
        "is_default": True,
        "current_version": 3,
    }

    snapshot = build_data_product_version_snapshot(source)
    assert snapshot["data_product_id"] == "product-1"
    assert snapshot["dataset_id"] == "dataset-1"
    assert snapshot["version"] == 3
    assert snapshot["config"] == {"columns": ["id", "amount"]}
    assert snapshot["config"] is not source["config"]


def test_data_product_version_matches_current_compares_snapshotted_fields_only() -> None:
    current = {
        "slug": "orders",
        "name": "Orders",
        "description": None,
        "table_name": "orders_gold",
        "config": {"columns": ["id"]},
        "is_default": False,
        "current_version": 4,
    }
    version = {
        "slug": "orders",
        "name": "Orders",
        "description": None,
        "table_name": "orders_gold",
        "config": {"columns": ["id"]},
        "is_default": False,
        "current_version": 2,
    }

    assert data_product_version_matches_current(current, version) is True


def test_validate_contiguous_data_product_versions_rejects_gaps() -> None:
    validate_contiguous_data_product_versions([1, 2, 3])

    with pytest.raises(ValueError, match="consecutive version numbers starting at 1"):
        validate_contiguous_data_product_versions([1, 3])
