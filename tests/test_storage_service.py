from __future__ import annotations

import importlib.util
import types
from datetime import datetime, timezone
from pathlib import Path

import pytest


class DummySettings:
    def __init__(self) -> None:
        self.s3_region = "us-east-1"
        self.s3_access_key = "minioadmin"
        self.s3_secret_key = "minioadmin"
        self.s3_endpoint = "http://minio:9000"
        self.s3_public_endpoint = "http://public-minio:9000"
        self.s3_raw_bucket = "raw"
        self.s3_silver_bucket = "silver"
        self.s3_gold_bucket = "gold"


class DummyClient:
    def __init__(self) -> None:
        self.uploaded: list[tuple] = []
        self.put_objects: list[dict] = []
        self.downloaded: list[tuple] = []
        self.head_calls: list[tuple] = []
        self.head_bucket_calls: list[str] = []
        self.presigned_calls: list[dict] = []
        self.list_calls: list[dict] = []

    def upload_file(self, filename: str, bucket: str, key: str, ExtraArgs=None):
        self.uploaded.append((filename, bucket, key, ExtraArgs))

    def put_object(self, **kwargs):
        self.put_objects.append(kwargs)

    def download_file(self, bucket: str, key: str, filename: str):
        self.downloaded.append((bucket, key, filename))

    def head_object(self, **kwargs):
        self.head_calls.append((kwargs["Bucket"], kwargs["Key"]))
        return {"ContentLength": 1}

    def head_bucket(self, **kwargs):
        self.head_bucket_calls.append(kwargs["Bucket"])

    def generate_presigned_url(self, **kwargs):
        self.presigned_calls.append(kwargs)
        return "https://example.test/upload"

    def list_objects_v2(self, **kwargs):
        self.list_calls.append(kwargs)
        return {
            "Contents": [
                {"Key": "dataset/a.parquet", "LastModified": datetime(2024, 1, 1, tzinfo=timezone.utc)},
                {"Key": "dataset/b.parquet", "LastModified": datetime(2024, 1, 2, tzinfo=timezone.utc)},
            ]
        }


def _load_storage_service_module(module_name: str):
    module_path = Path(__file__).resolve().parents[1] / "src" / "data_platform" / "services" / "storage.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def storage_service_module():
    return _load_storage_service_module("_isolated_storage_service_test_module")


@pytest.fixture()
def service(storage_service_module):
    return storage_service_module.ObjectStorageService(
        settings=DummySettings(),
        client=DummyClient(),
        public_client=DummyClient(),
    )


def test_storage_service_module_imports_without_optional_dependencies(storage_service_module):
    assert storage_service_module.ObjectStorageService.__name__ == "ObjectStorageService"


def test_storage_service_raises_clear_error_when_settings_loader_is_unavailable(storage_service_module, monkeypatch):
    real_import_module = storage_service_module.importlib.import_module

    def fake_import_module(name: str):
        if name == "data_platform.settings":
            return types.SimpleNamespace()
        return real_import_module(name)

    monkeypatch.setattr(storage_service_module.importlib, "import_module", fake_import_module)

    service = storage_service_module.ObjectStorageService()
    with pytest.raises(RuntimeError, match="get_settings"):
        _ = service.settings


def test_storage_service_raises_clear_error_when_driver_is_unavailable(storage_service_module, monkeypatch):
    real_import_module = storage_service_module.importlib.import_module

    def fake_import_module(name: str):
        if name == "data_platform.settings":
            return types.SimpleNamespace(get_settings=lambda: DummySettings())
        if name in {"boto3", "botocore.config"}:
            raise ModuleNotFoundError(name)
        return real_import_module(name)

    monkeypatch.setattr(storage_service_module.importlib, "import_module", fake_import_module)

    service = storage_service_module.ObjectStorageService()
    with pytest.raises(RuntimeError, match="boto3 and botocore"):
        _ = service.client


def test_upload_bytes_returns_object_uri(service):
    uri = service.upload_bytes("raw", "orders/file 1.csv", b"hello", content_type="text/csv")

    assert uri == "s3://raw/orders/file%201.csv"
    assert service.client.put_objects == [
        {"Bucket": "raw", "Key": "orders/file 1.csv", "Body": b"hello", "ContentType": "text/csv"}
    ]


def test_latest_object_uri_returns_most_recent_key(service):
    latest = service.latest_object_uri("gold", "dataset/")

    assert latest == "s3://gold/dataset/b.parquet"
    assert service.client.list_calls == [{"Bucket": "gold", "Prefix": "dataset/", "MaxKeys": 1000}]


def test_generate_presigned_put_url_uses_public_client(service):
    url = service.generate_presigned_put_url("raw", "orders.csv", expires_in=60, content_type="text/csv")

    assert url == "https://example.test/upload"
    assert service.public_client.presigned_calls == [
        {
            "ClientMethod": "put_object",
            "Params": {"Bucket": "raw", "Key": "orders.csv", "ContentType": "text/csv"},
            "ExpiresIn": 60,
            "HttpMethod": "PUT",
        }
    ]
