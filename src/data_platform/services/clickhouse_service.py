from __future__ import annotations

import importlib
import re
from typing import TYPE_CHECKING, Any

from data_platform.utils.paths import build_object_storage_url, parse_object_uri
from data_platform.utils.schemas import duckdb_to_clickhouse_type
from data_platform.utils.sql import quote_clickhouse_identifier, sql_string_literal
from data_platform.utils.validation import render_clickhouse_order_by, render_clickhouse_partition_by

if TYPE_CHECKING:  # pragma: no cover
    from data_platform.settings import Settings


_TYPE_WHITESPACE_RE = re.compile(r"\s+")


class ClickHouseService:
    METADATA_COLUMNS: list[dict[str, str]] = [
        {"name": "_ingestion_id", "type": "String"},
        {"name": "_dataset_slug", "type": "LowCardinality(String)"},
        {"name": "_source_filename", "type": "Nullable(String)"},
        {"name": "_ingested_at", "type": "DateTime64(3)"},
        {"name": "_raw_object_uri", "type": "Nullable(String)"},
        {"name": "_silver_object_uri", "type": "Nullable(String)"},
        {"name": "_gold_object_uri", "type": "Nullable(String)"},
    ]

    def __init__(self, settings: Settings | None = None, client: Any | None = None) -> None:
        self._settings = settings
        self._client = client

    @property
    def settings(self) -> Settings:
        if self._settings is None:
            self._settings = self._load_settings()
        return self._settings

    @property
    def client(self):
        if self._client is None:
            self._client = self._create_client()
        return self._client

    @staticmethod
    def _canonicalize_clickhouse_type(value: str) -> str:
        return _TYPE_WHITESPACE_RE.sub("", value).lower()

    @staticmethod
    def _load_settings() -> Settings:
        try:
            settings_module = importlib.import_module("data_platform.settings")
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "ClickHouseService settings are unavailable because data_platform.settings could not be imported. "
                "Install the project settings dependencies or inject settings explicitly."
            ) from exc

        get_settings = getattr(settings_module, "get_settings", None)
        if get_settings is None:
            raise RuntimeError(
                "ClickHouseService settings are unavailable because data_platform.settings.get_settings is missing."
            )
        return get_settings()

    def _create_client(self):
        try:
            clickhouse_module = importlib.import_module("clickhouse_connect")
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "clickhouse-connect is required to create a ClickHouse client. "
                "Install clickhouse-connect or inject a client explicitly."
            ) from exc

        get_client = getattr(clickhouse_module, "get_client", None)
        if get_client is None:
            raise RuntimeError("clickhouse-connect is installed but does not expose get_client().")

        return get_client(
            host=self.settings.clickhouse_host,
            port=self.settings.clickhouse_port,
            username=self.settings.clickhouse_username,
            password=self.settings.clickhouse_password,
            database=self.settings.clickhouse_database,
        )

    def ready(self) -> bool:
        return self.client.command("SELECT 1") is not None

    def table_exists(self, table_name: str) -> bool:
        query = (
            f"EXISTS TABLE {quote_clickhouse_identifier(self.settings.clickhouse_database)}."
            f"{quote_clickhouse_identifier(table_name)}"
        )
        result = self.client.query(query)
        return bool(result.result_rows[0][0])

    def get_columns(self, table_name: str) -> list[dict[str, str]]:
        query = (
            f"DESCRIBE TABLE {quote_clickhouse_identifier(self.settings.clickhouse_database)}."
            f"{quote_clickhouse_identifier(table_name)}"
        )
        result = self.client.query(query)
        return [{"name": row[0], "type": row[1]} for row in result.result_rows]

    def ensure_gold_table(self, table_name: str, canonical_schema: list[dict[str, str]], serving_config: dict | None = None) -> None:
        serving_config = serving_config or {}
        database = quote_clickhouse_identifier(self.settings.clickhouse_database)
        table = quote_clickhouse_identifier(table_name)

        desired_columns = [
            {
                "name": column["name"],
                "type": duckdb_to_clickhouse_type(column["type"]),
            }
            for column in canonical_schema
        ] + self.METADATA_COLUMNS

        user_columns = [
            f"{quote_clickhouse_identifier(column['name'])} {column['type']}"
            for column in desired_columns
        ]

        partition_by_clause = render_clickhouse_partition_by(serving_config.get("partition_by"))
        order_by_clause = render_clickhouse_order_by(serving_config.get("order_by"))

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {database}.{table} (
            {", ".join(user_columns)}
        )
        ENGINE = MergeTree()
        PARTITION BY {partition_by_clause}
        ORDER BY {order_by_clause}
        """
        self.client.command(create_sql)

        existing_columns = {column["name"]: column["type"] for column in self.get_columns(table_name)}
        for column in desired_columns:
            name = column["name"]
            target_type = column["type"]
            target_identifier = quote_clickhouse_identifier(name)
            existing_type = existing_columns.get(name)
            if existing_type is None:
                alter_sql = (
                    f"ALTER TABLE {database}.{table} "
                    f"ADD COLUMN IF NOT EXISTS {target_identifier} {target_type}"
                )
                self.client.command(alter_sql)
                continue
            if self._canonicalize_clickhouse_type(existing_type) == self._canonicalize_clickhouse_type(target_type):
                continue
            alter_sql = (
                f"ALTER TABLE {database}.{table} "
                f"MODIFY COLUMN {target_identifier} {target_type}"
            )
            self.client.command(alter_sql)

    def delete_rows_for_ingestion(self, table_name: str, ingestion_id: str) -> None:
        database = quote_clickhouse_identifier(self.settings.clickhouse_database)
        table = quote_clickhouse_identifier(table_name)
        sql = (
            f"ALTER TABLE {database}.{table} "
            f"DELETE WHERE _ingestion_id = {sql_string_literal(ingestion_id)} "
            "SETTINGS mutations_sync = 1"
        )
        self.client.command(sql)

    def insert_parquet_from_object_uri(
        self,
        table_name: str,
        canonical_schema: list[dict[str, str]],
        object_uri: str,
    ) -> None:
        bucket, key = parse_object_uri(object_uri)
        url = build_object_storage_url(self.settings.s3_endpoint, bucket, key)
        database = quote_clickhouse_identifier(self.settings.clickhouse_database)
        table = quote_clickhouse_identifier(table_name)

        ordered_columns = [column["name"] for column in canonical_schema] + [
            column["name"] for column in self.METADATA_COLUMNS
        ]
        column_list = ", ".join(quote_clickhouse_identifier(name) for name in ordered_columns)

        query = (
            f"INSERT INTO {database}.{table} ({column_list}) "
            f"SELECT * FROM s3('{url}', "
            f"'{self.settings.s3_access_key}', "
            f"'{self.settings.s3_secret_key}', "
            f"'Parquet')"
        )
        self.client.command(query)

    def _approximate_total_rows(self, table_name: str) -> int | None:
        database_name = sql_string_literal(self.settings.clickhouse_database)
        table_name_literal = sql_string_literal(table_name)
        query = (
            "SELECT total_rows FROM system.tables "
            f"WHERE database = {database_name} AND name = {table_name_literal}"
        )
        result = self.client.query(query)
        if not result.result_rows:
            return None
        total_rows = result.result_rows[0][0]
        return None if total_rows is None else int(total_rows)

    def preview_table(
        self,
        table_name: str,
        limit: int = 100,
        offset: int = 0,
        exact_total: bool = False,
    ) -> tuple[list[str], list[dict], int, bool]:
        limit = max(1, min(limit, 1000))
        offset = max(0, offset)

        database = quote_clickhouse_identifier(self.settings.clickhouse_database)
        table = quote_clickhouse_identifier(table_name)
        sql = (
            f"SELECT * FROM {database}.{table} "
            f"ORDER BY _ingested_at DESC "
            f"LIMIT {limit} OFFSET {offset}"
        )
        result = self.client.query(sql)
        columns = list(result.column_names)
        rows = [dict(zip(columns, row, strict=False)) for row in result.result_rows]

        total_rows_is_estimate = False
        if exact_total:
            total = int(self.client.query(f"SELECT count() FROM {database}.{table}").result_rows[0][0])
        else:
            approximate_total = self._approximate_total_rows(table_name)
            if approximate_total is None:
                total = int(self.client.query(f"SELECT count() FROM {database}.{table}").result_rows[0][0])
            else:
                total = approximate_total
                total_rows_is_estimate = True
        return columns, rows, total, total_rows_is_estimate

    def describe_table(self, table_name: str) -> list[dict[str, str]]:
        return self.get_columns(table_name)
