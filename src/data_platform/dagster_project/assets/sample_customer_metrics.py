from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import dagster as dg

from data_platform.services.duckdb_service import DuckDBService
from data_platform.services.storage import ObjectStorageService
from data_platform.settings import get_settings
from data_platform.utils.formats import FileFormat
from data_platform.utils.paths import build_layer_object_key, object_uri


@dg.asset(group_name="examples")
def latest_customers_silver_uri() -> str:
    settings = get_settings()
    storage = ObjectStorageService()
    uri = storage.latest_object_uri(settings.s3_silver_bucket, "customers/")
    if uri is None:
        raise dg.Failure("No silver objects found for dataset 'customers'. Ingest data first.")
    return uri


@dg.asset(group_name="examples")
def customer_country_metrics(latest_customers_silver_uri: str) -> str:
    settings = get_settings()
    storage = ObjectStorageService()
    duckdb_service = DuckDBService()

    with TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        local_silver = tmpdir_path / "customers_silver.parquet"

        bucket, key = latest_customers_silver_uri.replace("s3://", "").split("/", 1)
        storage.download_file(bucket, key, local_silver)

        source_sql = duckdb_service.source_sql_for_file(local_silver, FileFormat.PARQUET, tmpdir_path)
        query = """
        SELECT
            COALESCE(CAST(country AS VARCHAR), 'UNKNOWN') AS country,
            COUNT(*) AS customer_count
        FROM source
        GROUP BY 1
        ORDER BY customer_count DESC
        """

        output_path = tmpdir_path / "customer_country_metrics.parquet"
        duckdb_service.copy_query_to_parquet(query, output_path, views={"source": source_sql})

        ingestion_id = f"dagster-{datetime.now(timezone.utc):%Y%m%d%H%M%S}"
        key = build_layer_object_key("customers_country_metrics", ingestion_id, "part-00000.parquet")
        return storage.upload_file(settings.s3_gold_bucket, key, output_path, content_type="application/octet-stream")
