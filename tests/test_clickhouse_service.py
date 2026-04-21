from __future__ import annotations

import importlib.util
import types
from pathlib import Path

import pytest


class DummySettings:
    def __init__(self) -> None:
        self.clickhouse_host = "clickhouse"
        self.clickhouse_port = 8123
        self.clickhouse_username = "default"
        self.clickhouse_password = "clickhouse"
        self.clickhouse_database = "gold"
        self.s3_endpoint = "http://minio:9000"
        self.s3_access_key = "minioadmin"
        self.s3_secret_key = "minioadmin"


class DummyResult:
    def __init__(self, result_rows, column_names=None):
        self.result_rows = result_rows
        self.column_names = column_names or []


class DummyClient:
    def __init__(self):
        self.commands: list[str] = []
        self.describe_rows = [["id", "Nullable(Int64)"], ["_ingestion_id", "String"]]

    def command(self, sql: str):
        self.commands.append(sql)
        return 1

    def query(self, sql: str):
        if sql.startswith("DESCRIBE TABLE"):
            return DummyResult(self.describe_rows)
        if sql.startswith("SELECT total_rows FROM system.tables"):
            return DummyResult([[1234]])
        if sql.startswith("SELECT * FROM"):
            return DummyResult([[1, "job-1"]], column_names=["id", "_ingestion_id"])
        if sql.startswith("SELECT count()"):
            return DummyResult([[999]])
        if sql.startswith("EXISTS TABLE"):
            return DummyResult([[1]])
        raise AssertionError(f"Unexpected query: {sql}")


def _load_clickhouse_service_module(module_name: str):
    module_path = Path(__file__).resolve().parents[1] / "src" / "data_platform" / "services" / "clickhouse_service.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def clickhouse_service_module():
    return _load_clickhouse_service_module("_isolated_clickhouse_service_test_module")


@pytest.fixture()
def service(clickhouse_service_module):
    return clickhouse_service_module.ClickHouseService(settings=DummySettings(), client=DummyClient())


def test_clickhouse_service_module_imports_without_optional_dependencies(clickhouse_service_module):
    assert clickhouse_service_module.ClickHouseService.__name__ == "ClickHouseService"


def test_clickhouse_service_raises_clear_error_when_settings_loader_is_unavailable(clickhouse_service_module, monkeypatch):
    real_import_module = clickhouse_service_module.importlib.import_module

    def fake_import_module(name: str):
        if name == "data_platform.settings":
            return types.SimpleNamespace()
        return real_import_module(name)

    monkeypatch.setattr(clickhouse_service_module.importlib, "import_module", fake_import_module)

    service = clickhouse_service_module.ClickHouseService()
    with pytest.raises(RuntimeError, match="get_settings"):
        _ = service.settings


def test_clickhouse_service_raises_clear_error_when_driver_is_unavailable(clickhouse_service_module, monkeypatch):
    real_import_module = clickhouse_service_module.importlib.import_module

    def fake_import_module(name: str):
        if name == "data_platform.settings":
            return types.SimpleNamespace(get_settings=lambda: DummySettings())
        if name == "clickhouse_connect":
            raise ModuleNotFoundError("No module named 'clickhouse_connect'")
        return real_import_module(name)

    monkeypatch.setattr(clickhouse_service_module.importlib, "import_module", fake_import_module)

    service = clickhouse_service_module.ClickHouseService()
    with pytest.raises(RuntimeError, match="clickhouse-connect"):
        _ = service.client


def test_ensure_gold_table_modifies_changed_column_types(service):
    service.ensure_gold_table("dataset_orders", [{"name": "id", "type": "DOUBLE"}], {})

    commands = "\n".join(service.client.commands)
    assert "CREATE TABLE IF NOT EXISTS" in commands
    assert "MODIFY COLUMN `id` Nullable(Float64)" in commands


def test_delete_rows_for_ingestion_is_idempotency_primitive(service):
    service.delete_rows_for_ingestion("dataset_orders", "job-123")
    assert any("DELETE WHERE _ingestion_id = 'job-123'" in command for command in service.client.commands)


def test_insert_parquet_from_object_uri_url_encodes_object_key(service):
    service.insert_parquet_from_object_uri(
        "dataset_orders",
        [{"name": "id", "type": "BIGINT"}],
        "s3://gold/orders%20archive/part%20%2301%3F.parquet",
    )

    command = service.client.commands[-1]
    assert "http://minio:9000/gold/orders%20archive/part%20%2301%3F.parquet" in command


def test_preview_table_uses_approximate_total_by_default(service):
    columns, rows, total, total_is_estimate = service.preview_table("dataset_orders")

    assert columns == ["id", "_ingestion_id"]
    assert rows == [{"id": 1, "_ingestion_id": "job-1"}]
    assert total == 1234
    assert total_is_estimate is True


def test_ensure_gold_table_quotes_list_order_by_identifiers(service):
    service.ensure_gold_table(
        "dataset_orders",
        [{"name": "id", "type": "BIGINT"}],
        {"order_by": ["id", "_ingested_at"]},
    )

    create_sql = service.client.commands[0]
    assert "ORDER BY (`id`, `_ingested_at`)" in create_sql


def test_ensure_gold_table_renders_safe_partition_and_string_order_by(service):
    service.ensure_gold_table(
        "dataset_orders",
        [{"name": "id", "type": "BIGINT"}],
        {"partition_by": "toDate(_ingested_at)", "order_by": "toDate(_ingested_at)"},
    )

    create_sql = service.client.commands[0]
    assert "PARTITION BY toDate(`_ingested_at`)" in create_sql
    assert "ORDER BY toDate(`_ingested_at`)" in create_sql


def test_ensure_gold_table_rejects_unsafe_partition_by(service):
    with pytest.raises(ValueError, match="serving_config.partition_by"):
        service.ensure_gold_table(
            "dataset_orders",
            [{"name": "id", "type": "BIGINT"}],
            {"partition_by": "toYYYYMM(_ingested_at) SETTINGS index_granularity = 1"},
        )


def test_ensure_gold_table_rejects_unsafe_string_order_by(service):
    with pytest.raises(ValueError, match="serving_config.order_by"):
        service.ensure_gold_table(
            "dataset_orders",
            [{"name": "id", "type": "BIGINT"}],
            {"order_by": "_ingested_at DESC"},
        )


def test_ensure_gold_table_rejects_unsafe_list_order_by_identifier(service):
    with pytest.raises(ValueError, match="serving_config.order_by"):
        service.ensure_gold_table(
            "dataset_orders",
            [{"name": "id", "type": "BIGINT"}],
            {"order_by": ["id) SETTINGS allow_experimental_object_type = 1 --"]},
        )
