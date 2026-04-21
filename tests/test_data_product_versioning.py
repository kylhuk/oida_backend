from __future__ import annotations

from data_platform.schemas.dataset import (
    CreateDataProductRequest,
    CreateDatasetRequest,
    UpdateDataProductRequest,
)
from data_platform.services.dataset_service import DatasetService


def test_data_product_versions_start_at_one_and_increment_on_update(db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    product = DatasetService.create_data_product(
        db_session,
        dataset,
        CreateDataProductRequest(slug="orders_dashboard", name="Orders Dashboard"),
    )

    assert product.current_version == 1

    updated = DatasetService.update_data_product(
        db_session,
        dataset,
        product,
        UpdateDataProductRequest(description="Dashboard product", config={"exposed": True}),
    )
    versions = DatasetService.list_data_product_versions(db_session, updated)

    assert updated.current_version == 2
    assert [item.version for item in versions] == [2, 1]
    assert versions[0].description == "Dashboard product"
    assert versions[0].config == {"exposed": True}
    assert versions[1].description is None


def test_switching_default_product_versions_the_old_default_too(db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    original_default = DatasetService.get_dataset_data_product(db_session, dataset.id, "orders")
    assert original_default is not None
    assert original_default.is_default is True
    assert original_default.current_version == 1

    new_default = DatasetService.create_data_product(
        db_session,
        dataset,
        CreateDataProductRequest(
            slug="orders_dashboard",
            name="Orders Dashboard",
            is_default=True,
        ),
    )

    refreshed_original = DatasetService.get_dataset_data_product(db_session, dataset.id, "orders")
    assert refreshed_original is not None
    assert refreshed_original.is_default is False
    assert refreshed_original.current_version == 2
    assert new_default.is_default is True
    assert new_default.current_version == 1

    original_versions = DatasetService.list_data_product_versions(db_session, refreshed_original)
    assert [item.version for item in original_versions] == [2, 1]
    assert original_versions[0].is_default is False
    assert original_versions[1].is_default is True
