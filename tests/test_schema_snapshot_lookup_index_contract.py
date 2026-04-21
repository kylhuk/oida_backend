from __future__ import annotations

from pathlib import Path


def test_schema_snapshot_model_declares_historical_lookup_index() -> None:
    source = Path("src/data_platform/models/dataset.py").read_text()

    assert "ix_schema_snapshots_dataset_layer_created" in source
    assert (
        'sa.Index("ix_schema_snapshots_dataset_layer_created", "dataset_id", "layer", "created_at")'
        in source
    )


def test_migration_registry_includes_schema_snapshot_lookup_index_upgrade() -> None:
    source = Path("src/data_platform/migrations/manager.py").read_text()

    assert "def _add_schema_snapshot_lookup_index" in source
    assert 'index_name="ix_schema_snapshots_dataset_layer_created"' in source
    assert 'columns=("dataset_id", "layer", "created_at")' in source
    assert 'version=3' in source
    assert 'name="0003_schema_snapshot_lookup_index"' in source
