from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from data_platform.api.deps import get_db, require_scopes
from data_platform.enums import DatasetLayer, PipelineEngine
from data_platform.schemas.catalog import (
    CatalogCountResponse,
    CatalogDatasetEntryResponse,
    CatalogPipelineEntryResponse,
    CatalogSearchResponse,
)
from data_platform.services.catalog_service import CatalogService

router = APIRouter(prefix="/v1/catalog", tags=["catalog"])


@router.get("/search", response_model=CatalogSearchResponse)
def search_catalog(
    q: str | None = Query(default=None),
    status: str | None = Query(default=None),
    tag: str | None = Query(default=None),
    dataset_limit: int = Query(default=20, ge=1, le=1000),
    dataset_offset: int = Query(default=0, ge=0),
    dataset_slug: str | None = Query(default=None),
    active: bool | None = Query(default=None),
    engine: PipelineEngine | None = Query(default=None),
    source_layer: DatasetLayer | None = Query(default=None),
    target_layer: DatasetLayer | None = Query(default=None),
    pipeline_limit: int = Query(default=20, ge=1, le=1000),
    pipeline_offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:read", "pipelines:read")),
) -> CatalogSearchResponse:
    results = CatalogService.search_catalog(
        session,
        query=q,
        status=status,
        tag=tag,
        dataset_limit=dataset_limit,
        dataset_offset=dataset_offset,
        dataset_slug=dataset_slug,
        active=active,
        engine=engine,
        source_layer=source_layer,
        target_layer=target_layer,
        pipeline_limit=pipeline_limit,
        pipeline_offset=pipeline_offset,
    )
    return CatalogSearchResponse(
        query=results["query"],
        dataset_count=results["dataset_count"],
        pipeline_count=results["pipeline_count"],
        datasets=[CatalogDatasetEntryResponse.model_validate(item) for item in results["datasets"]],
        pipelines=[CatalogPipelineEntryResponse.model_validate(item) for item in results["pipelines"]],
    )


@router.get("/datasets/count", response_model=CatalogCountResponse)
def count_catalog_datasets(
    q: str | None = Query(default=None),
    status: str | None = Query(default=None),
    tag: str | None = Query(default=None),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:read")),
) -> CatalogCountResponse:
    return CatalogCountResponse(
        count=CatalogService.count_catalog_datasets(session, query=q, status=status, tag=tag)
    )


@router.get("/datasets", response_model=list[CatalogDatasetEntryResponse])
def list_catalog_datasets(
    q: str | None = Query(default=None),
    status: str | None = Query(default=None),
    tag: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:read")),
) -> list[CatalogDatasetEntryResponse]:
    return [
        CatalogDatasetEntryResponse.model_validate(item)
        for item in CatalogService.list_catalog_datasets(
            session,
            query=q,
            status=status,
            tag=tag,
            limit=limit,
            offset=offset,
        )
    ]


@router.get("/pipelines/count", response_model=CatalogCountResponse)
def count_catalog_pipelines(
    q: str | None = Query(default=None),
    dataset_slug: str | None = Query(default=None),
    active: bool | None = Query(default=None),
    engine: PipelineEngine | None = Query(default=None),
    source_layer: DatasetLayer | None = Query(default=None),
    target_layer: DatasetLayer | None = Query(default=None),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:read")),
) -> CatalogCountResponse:
    return CatalogCountResponse(
        count=CatalogService.count_catalog_pipelines(
            session,
            query=q,
            dataset_slug=dataset_slug,
            active=active,
            engine=engine,
            source_layer=source_layer,
            target_layer=target_layer,
        )
    )


@router.get("/pipelines", response_model=list[CatalogPipelineEntryResponse])
def list_catalog_pipelines(
    q: str | None = Query(default=None),
    dataset_slug: str | None = Query(default=None),
    active: bool | None = Query(default=None),
    engine: PipelineEngine | None = Query(default=None),
    source_layer: DatasetLayer | None = Query(default=None),
    target_layer: DatasetLayer | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:read")),
) -> list[CatalogPipelineEntryResponse]:
    return [
        CatalogPipelineEntryResponse.model_validate(item)
        for item in CatalogService.list_catalog_pipelines(
            session,
            query=q,
            dataset_slug=dataset_slug,
            active=active,
            engine=engine,
            source_layer=source_layer,
            target_layer=target_layer,
            limit=limit,
            offset=offset,
        )
    ]
