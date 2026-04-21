from __future__ import annotations

import os
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from data_platform.secrets import DEFAULT_MANAGED_SECRET_DEFAULTS, collect_secret_settings_overrides


class Settings(BaseSettings):
    app_name: str = Field(default="Medallion Backend", alias="APP_NAME")
    app_env: str = Field(default="development", alias="APP_ENV")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    cors_allow_origins: str = Field(default="*", alias="CORS_ALLOW_ORIGINS")

    postgres_host: str = Field(default="postgres", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_user: str = Field(default="platform", alias="POSTGRES_USER")
    postgres_password: str = Field(default="platform", alias="POSTGRES_PASSWORD")
    postgres_db: str = Field(default="platform", alias="POSTGRES_DB")

    redis_url: str = Field(default="redis://redis:6379/0", alias="REDIS_URL")

    s3_endpoint: str = Field(default="http://minio:9000", alias="S3_ENDPOINT")
    s3_public_endpoint: str = Field(default="http://localhost:9000", alias="S3_PUBLIC_ENDPOINT")
    s3_region: str = Field(default="us-east-1", alias="S3_REGION")
    s3_access_key: str = Field(default="minioadmin", alias="S3_ACCESS_KEY")
    s3_secret_key: str = Field(default="minioadmin", alias="S3_SECRET_KEY")
    s3_raw_bucket: str = Field(default="raw", alias="S3_RAW_BUCKET")
    s3_silver_bucket: str = Field(default="silver", alias="S3_SILVER_BUCKET")
    s3_gold_bucket: str = Field(default="gold", alias="S3_GOLD_BUCKET")
    presign_expiration_seconds: int = Field(default=3600, alias="PRESIGN_EXPIRATION_SECONDS")

    clickhouse_host: str = Field(default="clickhouse", alias="CLICKHOUSE_HOST")
    clickhouse_port: int = Field(default=8123, alias="CLICKHOUSE_PORT")
    clickhouse_username: str = Field(default="default", alias="CLICKHOUSE_USERNAME")
    clickhouse_password: str = Field(default="clickhouse", alias="CLICKHOUSE_PASSWORD")
    clickhouse_database: str = Field(default="gold", alias="CLICKHOUSE_DATABASE")

    enable_api_key_auth: bool = Field(default=True, alias="ENABLE_API_KEY_AUTH")
    seed_dev_api_key: str = Field(default="dev-local-key", alias="SEED_DEV_API_KEY")
    enable_audit_trail: bool = Field(default=False, alias="ENABLE_AUDIT_TRAIL")
    audit_trail_exempt_paths: str = Field(
        default="/health,/docs,/openapi.json,/redoc",
        alias="AUDIT_TRAIL_EXEMPT_PATHS",
    )
    enable_rate_limit: bool = Field(default=False, alias="ENABLE_RATE_LIMIT")
    rate_limit_requests: int = Field(default=120, alias="RATE_LIMIT_REQUESTS")
    rate_limit_window_seconds: int = Field(default=60, alias="RATE_LIMIT_WINDOW_SECONDS")
    rate_limit_exempt_paths: str = Field(
        default="/health,/docs,/openapi.json,/redoc",
        alias="RATE_LIMIT_EXEMPT_PATHS",
    )
    enable_webhook_notifications: bool = Field(default=False, alias="ENABLE_WEBHOOK_NOTIFICATIONS")
    notification_webhook_urls: str = Field(default="", alias="NOTIFICATION_WEBHOOK_URLS")
    notification_events: str = Field(
        default="ingestion_job.failed,pipeline_run.failed",
        alias="NOTIFICATION_EVENTS",
    )
    notification_timeout_seconds: int = Field(default=5, alias="NOTIFICATION_TIMEOUT_SECONDS", ge=1)
    retention_pipeline_run_days: int = Field(default=90, alias="RETENTION_PIPELINE_RUN_DAYS")
    retention_preflight_attempt_days: int = Field(default=30, alias="RETENTION_PREFLIGHT_ATTEMPT_DAYS")
    retention_quality_result_days: int = Field(default=90, alias="RETENTION_QUALITY_RESULT_DAYS")
    retention_ingestion_job_days: int = Field(default=180, alias="RETENTION_INGESTION_JOB_DAYS")

    dagster_webserver_url: str = Field(default="http://dagster-webserver:3000", alias="DAGSTER_WEBSERVER_URL")
    duckdb_threads: int = Field(default=4, alias="DUCKDB_THREADS")

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False, extra="ignore")


    @property
    def audit_trail_exempt_path_prefixes(self) -> list[str]:
        value = self.audit_trail_exempt_paths.strip()
        if not value:
            return []
        return [item.strip() for item in value.split(",") if item.strip()]

    @property
    def rate_limit_exempt_path_prefixes(self) -> list[str]:
        value = self.rate_limit_exempt_paths.strip()
        if not value:
            return []
        return [item.strip() for item in value.split(",") if item.strip()]

    @property
    def notification_webhook_url_values(self) -> tuple[str, ...]:
        value = self.notification_webhook_urls.strip()
        if not value:
            return tuple()
        return tuple(dict.fromkeys(item.strip() for item in value.split(",") if item.strip()))

    @property
    def notification_event_values(self) -> tuple[str, ...]:
        value = self.notification_events.strip()
        if not value:
            return tuple()
        return tuple(dict.fromkeys(item.strip().lower() for item in value.split(",") if item.strip()))

    @property
    def sqlalchemy_database_uri(self) -> str:
        return (
            f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def cors_allowed_origins(self) -> list[str]:
        value = self.cors_allow_origins.strip()
        if not value:
            return ["*"]
        if value == "*":
            return ["*"]
        return [item.strip() for item in value.split(",") if item.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    secret_overrides = collect_secret_settings_overrides(
        os.environ,
        managed_secret_defaults=DEFAULT_MANAGED_SECRET_DEFAULTS,
    )
    return Settings(**secret_overrides)
