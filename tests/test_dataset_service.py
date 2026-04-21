from __future__ import annotations

from data_platform.models.ingestion import IngestionJob
from data_platform.schemas.dataset import CreateDataProductRequest, CreateDatasetRequest, UpdateDatasetRequest
from data_platform.services.dataset_service import DatasetService


def test_create_dataset_and_default_product(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders", name="Orders"),
    )

    products = DatasetService.list_data_products(db_session, dataset)
    assert dataset.gold_table_name == "dataset_orders"
    assert len(products) == 1
    assert products[0].is_default is True
    assert products[0].table_name == dataset.gold_table_name


def test_update_dataset_changes_transform_and_tags(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders", name="Orders"),
    )

    updated = DatasetService.update_dataset(
        db_session,
        dataset,
        UpdateDatasetRequest(gold_sql="SELECT id FROM source", tags=["Finance", "finance", "Ops"]),
    )

    assert updated.gold_sql == "SELECT id FROM source"
    assert updated.tags == ["finance", "ops"]


def test_save_schema_snapshot_is_deduplicated(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders", name="Orders"),
    )

    schema = [{"name": "id", "type": "BIGINT"}]
    first = DatasetService.save_schema_snapshot(db_session, dataset, "gold", schema)
    second = DatasetService.save_schema_snapshot(db_session, dataset, "gold", schema)
    snapshots = DatasetService.list_schema_snapshots(db_session, dataset.id)

    assert first.id == second.id
    assert len(snapshots) == 1
    assert snapshots[0].version == 1


def test_create_data_product_can_point_to_custom_table(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders", name="Orders"),
    )

    product = DatasetService.create_data_product(
        db_session,
        dataset,
        CreateDataProductRequest(
            slug="orders_dashboard",
            name="Orders Dashboard",
            table_name="orders_dashboard_gold",
            is_default=True,
        ),
    )

    products = DatasetService.list_data_products(db_session, dataset)
    defaults = [item for item in products if item.is_default]
    assert product.table_name == "orders_dashboard_gold"
    assert len(defaults) == 1
    assert defaults[0].slug == "orders_dashboard"


def test_dataset_stats_aggregate_ingestions(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders", name="Orders"),
    )
    db_session.add_all(
        [
            IngestionJob(dataset_id=dataset.id, source_type="upload", status="succeeded", job_metadata={}),
            IngestionJob(dataset_id=dataset.id, source_type="upload", status="failed", job_metadata={}),
        ]
    )
    db_session.commit()

    stats = DatasetService.get_dataset_stats(db_session, dataset)
    assert stats["ingestion_status_counts"] == {"failed": 1, "succeeded": 1}
    assert stats["data_product_count"] == 1
    assert stats["schema_versions"]["gold"] == 0



def test_build_schema_diff_defaults_to_previous_snapshot(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders", name="Orders"),
    )

    DatasetService.save_schema_snapshot(db_session, dataset, "gold", [{"name": "id", "type": "BIGINT"}])
    DatasetService.save_schema_snapshot(
        db_session,
        dataset,
        "gold",
        [{"name": "id", "type": "BIGINT"}, {"name": "amount", "type": "DOUBLE"}],
    )

    diff = DatasetService.build_schema_diff(db_session, dataset, layer="gold")

    assert diff["from_version"] == 1
    assert diff["to_version"] == 2
    assert diff["added_columns"] == [{"name": "amount", "type": "DOUBLE"}]
    assert diff["removed_columns"] == []
    assert diff["changed_columns"] == []
    assert diff["breaking_changes"] is False


def test_build_schema_diff_can_compare_against_empty_baseline(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders", name="Orders"),
    )

    first = DatasetService.save_schema_snapshot(db_session, dataset, "silver", [{"name": "id", "type": "BIGINT"}])

    diff = DatasetService.build_schema_diff(db_session, dataset, layer="silver", from_version=0, to_version=first.version)

    assert diff["from_version"] == 0
    assert diff["to_version"] == 1
    assert diff["added_columns"] == [{"name": "id", "type": "BIGINT"}]
    assert diff["breaking_changes"] is False


def test_build_schema_compatibility_uses_latest_snapshot_by_default(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders", name="Orders"),
    )

    latest = DatasetService.save_schema_snapshot(db_session, dataset, "gold", [{"name": "id", "type": "BIGINT"}])

    report = DatasetService.build_schema_compatibility(
        db_session,
        dataset,
        layer="gold",
        candidate_schema=[{"name": "id", "type": "BIGINT"}, {"name": "amount", "type": "DOUBLE"}],
    )

    assert report["against_version"] == latest.version
    assert report["against_fingerprint"] == latest.fingerprint
    assert report["contract_compatible"] is True
    assert report["strict_mode_compatible"] is False
    assert report["added_columns"] == [{"name": "amount", "type": "DOUBLE"}]



def test_build_schema_compatibility_allows_empty_baseline(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders", name="Orders"),
    )

    report = DatasetService.build_schema_compatibility(
        db_session,
        dataset,
        layer="silver",
        candidate_schema=[{"name": "id", "type": "BIGINT"}],
        against_version=0,
    )

    assert report["against_version"] == 0
    assert report["against_fingerprint"] is None
    assert report["current_schema"] == []
    assert report["candidate_schema"] == [{"name": "id", "type": "BIGINT"}]
    assert report["contract_compatible"] is True
