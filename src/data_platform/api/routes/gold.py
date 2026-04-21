from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from data_platform.api.deps import get_db, require_scopes
from data_platform.schemas.ingestion import GoldPreviewResponse, GoldSchemaResponse
from data_platform.services.clickhouse_service import ClickHouseService
from data_platform.services.dataset_service import DatasetService

router = APIRouter(prefix="/v1/gold", tags=["gold"])


@router.get("/datasets/{dataset_slug}/preview", response_model=GoldPreviewResponse)
def preview_dataset_gold(
    dataset_slug: str,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    exact_total: bool = Query(default=False),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("gold:read")),
) -> GoldPreviewResponse:
    dataset = DatasetService.get_dataset_by_slug(session, dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")

    clickhouse = ClickHouseService()
    if not clickhouse.table_exists(dataset.gold_table_name):
        raise HTTPException(status_code=404, detail="Gold table not found for dataset.")

    columns, rows, total, total_is_estimate = clickhouse.preview_table(
        dataset.gold_table_name,
        limit=limit,
        offset=offset,
        exact_total=exact_total,
    )
    return GoldPreviewResponse(
        dataset_slug=dataset.slug,
        columns=columns,
        rows=rows,
        total_rows=total,
        total_rows_is_estimate=total_is_estimate,
    )


@router.get("/datasets/{dataset_slug}/schema", response_model=GoldSchemaResponse)
def describe_dataset_gold(
    dataset_slug: str,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("gold:read")),
) -> GoldSchemaResponse:
    dataset = DatasetService.get_dataset_by_slug(session, dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")

    clickhouse = ClickHouseService()
    if not clickhouse.table_exists(dataset.gold_table_name):
        raise HTTPException(status_code=404, detail="Gold table not found for dataset.")

    columns = clickhouse.describe_table(dataset.gold_table_name)
    return GoldSchemaResponse(dataset_slug=dataset.slug, columns=columns)


@router.get("/data-products/{product_slug}/preview", response_model=GoldPreviewResponse)
def preview_data_product(
    product_slug: str,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    exact_total: bool = Query(default=False),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("gold:read")),
) -> GoldPreviewResponse:
    product = DatasetService.get_data_product_by_slug(session, product_slug)
    if not product:
        raise HTTPException(status_code=404, detail="Data product not found.")

    clickhouse = ClickHouseService()
    if not clickhouse.table_exists(product.table_name):
        raise HTTPException(status_code=404, detail="Gold table not found for data product.")

    columns, rows, total, total_is_estimate = clickhouse.preview_table(
        product.table_name,
        limit=limit,
        offset=offset,
        exact_total=exact_total,
    )
    dataset_slug = product.dataset.slug if getattr(product, "dataset", None) is not None else product.slug
    return GoldPreviewResponse(
        dataset_slug=dataset_slug,
        data_product_slug=product.slug,
        columns=columns,
        rows=rows,
        total_rows=total,
        total_rows_is_estimate=total_is_estimate,
    )


@router.get("/data-products/{product_slug}/schema", response_model=GoldSchemaResponse)
def describe_data_product_gold(
    product_slug: str,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("gold:read")),
) -> GoldSchemaResponse:
    product = DatasetService.get_data_product_by_slug(session, product_slug)
    if not product:
        raise HTTPException(status_code=404, detail="Data product not found.")

    clickhouse = ClickHouseService()
    if not clickhouse.table_exists(product.table_name):
        raise HTTPException(status_code=404, detail="Gold table not found for data product.")

    columns = clickhouse.describe_table(product.table_name)
    dataset_slug = product.dataset.slug if getattr(product, "dataset", None) is not None else product.slug
    return GoldSchemaResponse(dataset_slug=dataset_slug, data_product_slug=product.slug, columns=columns)
