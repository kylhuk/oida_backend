from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

from data_platform.utils.paths import object_uri


class ObjectStorageService:
    def __init__(
        self,
        *,
        settings: Any | None = None,
        client: Any | None = None,
        public_client: Any | None = None,
    ) -> None:
        self._settings = settings
        self._client = client
        self._public_client = public_client

    @property
    def settings(self) -> Any:
        if self._settings is None:
            self._settings = self._load_settings()
        return self._settings

    def _load_settings(self) -> Any:
        try:
            settings_module = importlib.import_module("data_platform.settings")
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "ObjectStorageService requires the application settings module to be available."
            ) from exc

        get_settings = getattr(settings_module, "get_settings", None)
        if not callable(get_settings):
            raise RuntimeError("ObjectStorageService requires data_platform.settings.get_settings().")
        return get_settings()

    def _build_client(self, endpoint_url: str) -> Any:
        try:
            boto3 = importlib.import_module("boto3")
            botocore_config = importlib.import_module("botocore.config")
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "ObjectStorageService requires boto3 and botocore to create S3 clients."
            ) from exc

        session = boto3.session.Session()
        return session.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=self.settings.s3_region,
            aws_access_key_id=self.settings.s3_access_key,
            aws_secret_access_key=self.settings.s3_secret_key,
            config=botocore_config.Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    @property
    def client(self) -> Any:
        if self._client is None:
            self._client = self._build_client(self.settings.s3_endpoint)
        return self._client

    @property
    def public_client(self) -> Any:
        if self._public_client is None:
            self._public_client = self._build_client(self.settings.s3_public_endpoint)
        return self._public_client

    def upload_file(self, bucket: str, key: str, local_path: str | Path, content_type: str | None = None) -> str:
        extra_args = {"ContentType": content_type} if content_type else {}
        self.client.upload_file(str(local_path), bucket, key, ExtraArgs=extra_args or None)
        return object_uri(bucket, key)

    def upload_bytes(self, bucket: str, key: str, payload: bytes, content_type: str | None = None) -> str:
        extra_args = {"ContentType": content_type} if content_type else {}
        self.client.put_object(Bucket=bucket, Key=key, Body=payload, **extra_args)
        return object_uri(bucket, key)

    def download_file(self, bucket: str, key: str, local_path: str | Path) -> None:
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(bucket, key, str(local_path))

    def head_object(self, bucket: str, key: str) -> dict:
        return self.client.head_object(Bucket=bucket, Key=key)

    def bucket_exists(self, bucket: str) -> bool:
        try:
            self.client.head_bucket(Bucket=bucket)
            return True
        except Exception:
            return False

    def ready(self) -> bool:
        required_buckets = [
            self.settings.s3_raw_bucket,
            self.settings.s3_silver_bucket,
            self.settings.s3_gold_bucket,
        ]
        return all(self.bucket_exists(bucket) for bucket in required_buckets)

    def generate_presigned_put_url(
        self,
        bucket: str,
        key: str,
        expires_in: int,
        content_type: str | None = None,
    ) -> str:
        params = {"Bucket": bucket, "Key": key}
        if content_type:
            params["ContentType"] = content_type
        return self.public_client.generate_presigned_url(
            ClientMethod="put_object",
            Params=params,
            ExpiresIn=expires_in,
            HttpMethod="PUT",
        )

    def list_objects(self, bucket: str, prefix: str, max_keys: int = 1000) -> list[dict]:
        response = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=max_keys)
        return response.get("Contents", [])

    def latest_object_uri(self, bucket: str, prefix: str) -> str | None:
        objects = self.list_objects(bucket=bucket, prefix=prefix)
        if not objects:
            return None
        latest = max(objects, key=lambda item: item["LastModified"])
        return object_uri(bucket, latest["Key"])
