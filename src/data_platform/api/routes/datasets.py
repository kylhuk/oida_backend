from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from data_platform.api.deps import get_db, require_scopes
from data_platform.schemas.dataset import (
    ApproveSchemaSnapshotRequest,
    CreateDataProductRequest,
    CreateDatasetRequest,
    DataProductResponse,
    DataProductVersionResponse,
    DataProductVersionedResponse,
    DatasetBackupResponse,
    DatasetExportResponse,
    DatasetImportRequest,
    DatasetRestoreRequest,
    DatasetRestoreResponse,
    DatasetResponse,
    DatasetStatsResponse,
    QualityResultResponse,
    QualityTrendResponse,
    QualityRuleCreate,
    QualityRuleResponse,
    SchemaApprovalResponse,
    SchemaCompatibilityRequest,
    SchemaCompatibilityResponse,
    SchemaDiffResponse,
    SchemaSnapshotResponse,
    UpdateDataProductRequest,
    UpdateDatasetRequest,
    UpdateQualityRuleRequest,
)
from data_platform.services.dataset_service import DatasetService
from data_platform.services.quality_service import QualityService

router = APIRouter(prefix="/v1/datasets", tags=["datasets"])


def _get_dataset_or_404(session: Session, dataset_slug: str):
    dataset = DatasetService.get_dataset_by_slug(session, dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    return dataset


def _get_data_product_or_404(session: Session, dataset_slug: str, product_slug: str):
    dataset = _get_dataset_or_404(session, dataset_slug)
    product = DatasetService.get_dataset_data_product(session, dataset.id, product_slug)
    if not product:
        raise HTTPException(status_code=404, detail="Data product not found.")
    return dataset, product


def _build_dataset_export_response(exported: dict) -> DatasetExportResponse:
    return DatasetExportResponse(
        dataset=DatasetResponse.model_validate(exported["dataset"]),
        quality_rules=[QualityRuleResponse.model_validate(item) for item in exported["quality_rules"]],
        data_products=[DataProductVersionedResponse.model_validate(item) for item in exported["data_products"]],
        pipelines=exported["pipelines"],
        schema_snapshots=[SchemaSnapshotResponse.model_validate(item) for item in exported["schema_snapshots"]],
    )


def _build_dataset_backup_response(exported: dict) -> DatasetBackupResponse:
    return DatasetBackupResponse(
        datasets=[_build_dataset_export_response(item) for item in exported["datasets"]],
    )



def _build_dataset_restore_response(restored: dict) -> DatasetRestoreResponse:
    return DatasetRestoreResponse(
        imported_datasets=[_build_dataset_export_response(item) for item in restored["imported_datasets"]],
        skipped_dataset_slugs=list(restored.get("skipped_dataset_slugs") or []),
    )



@router.post("", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
def create_dataset(
    payload: CreateDatasetRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:write")),
) -> DatasetResponse:
    try:
        dataset = DatasetService.create_dataset(session, payload)
        return DatasetResponse.model_validate(dataset)
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(status_code=409, detail=f"Dataset already exists or violates uniqueness: {exc}") from exc


@router.get("", response_model=list[DatasetResponse])
def list_datasets(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:read")),
) -> list[DatasetResponse]:
    datasets = DatasetService.list_datasets(session, limit=limit, offset=offset)
    return [DatasetResponse.model_validate(dataset) for dataset in datasets]


@router.get("/backup", response_model=DatasetBackupResponse)
def backup_datasets(
    include_schema_snapshots: bool = Query(default=True),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:read")),
) -> DatasetBackupResponse:
    exported = DatasetService.export_dataset_backup(
        session,
        include_schema_snapshots=include_schema_snapshots,
    )
    return _build_dataset_backup_response(exported)


@router.post("/restore", response_model=DatasetRestoreResponse, status_code=status.HTTP_201_CREATED)
def restore_datasets(
    payload: DatasetRestoreRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:write")),
) -> DatasetRestoreResponse:
    try:
        restored = DatasetService.restore_dataset_backup(session, payload)
    except ValueError as exc:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(status_code=409, detail=f"Dataset already exists or violates uniqueness: {exc}") from exc
    return _build_dataset_restore_response(restored)


@router.get("/{dataset_slug}", response_model=DatasetResponse)
def get_dataset(
    dataset_slug: str,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:read")),
) -> DatasetResponse:
    dataset = DatasetService.get_dataset_by_slug(session, dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    return DatasetResponse.model_validate(dataset)


@router.patch("/{dataset_slug}", response_model=DatasetResponse)
def update_dataset(
    dataset_slug: str,
    payload: UpdateDatasetRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:write")),
) -> DatasetResponse:
    dataset = DatasetService.get_dataset_by_slug(session, dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    dataset = DatasetService.update_dataset(session, dataset, payload)
    return DatasetResponse.model_validate(dataset)




@router.post("/import", response_model=DatasetExportResponse, status_code=status.HTTP_201_CREATED)
def import_dataset_definition(
    payload: DatasetImportRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:write")),
) -> DatasetExportResponse:
    try:
        imported = DatasetService.import_dataset(session, payload)
    except ValueError as exc:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(status_code=409, detail=f"Dataset already exists or violates uniqueness: {exc}") from exc
    return _build_dataset_export_response(imported)


@router.get("/{dataset_slug}/export", response_model=DatasetExportResponse)
def export_dataset_definition(
    dataset_slug: str,
    include_schema_snapshots: bool = Query(default=False),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:read")),
) -> DatasetExportResponse:
    dataset = DatasetService.get_dataset_by_slug(session, dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    exported = DatasetService.export_dataset(session, dataset, include_schema_snapshots=include_schema_snapshots)
    return _build_dataset_export_response(exported)


@router.get("/{dataset_slug}/schemas", response_model=list[SchemaSnapshotResponse])
def list_schema_snapshots(
    dataset_slug: str,
    layer: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:read")),
) -> list[SchemaSnapshotResponse]:
    dataset = DatasetService.get_dataset_by_slug(session, dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    snapshots = DatasetService.list_schema_snapshots(session, dataset.id, layer=layer, limit=limit, offset=offset)
    return [SchemaSnapshotResponse.model_validate(snapshot) for snapshot in snapshots]


@router.get("/{dataset_slug}/schemas/pending", response_model=list[SchemaSnapshotResponse])
def list_pending_schema_snapshots(
    dataset_slug: str,
    layer: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:read")),
) -> list[SchemaSnapshotResponse]:
    dataset = _get_dataset_or_404(session, dataset_slug)
    snapshots = DatasetService.list_pending_schema_snapshots(
        session,
        dataset.id,
        layer=layer,
        limit=limit,
        offset=offset,
    )
    return [SchemaSnapshotResponse.model_validate(snapshot) for snapshot in snapshots]


@router.get("/{dataset_slug}/schemas/approvals", response_model=list[SchemaApprovalResponse])
def list_schema_approvals(
    dataset_slug: str,
    layer: str | None = Query(default=None),
    approved_by: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:read")),
) -> list[SchemaApprovalResponse]:
    dataset = _get_dataset_or_404(session, dataset_slug)
    approvals = DatasetService.list_schema_approvals(
        session,
        dataset.id,
        layer=layer,
        approved_by=approved_by,
        limit=limit,
        offset=offset,
    )
    return [SchemaApprovalResponse.model_validate(item) for item in approvals]


@router.post("/{dataset_slug}/schemas/{layer}/{version}/approve", response_model=SchemaApprovalResponse, status_code=status.HTTP_201_CREATED)
def approve_schema_snapshot(
    dataset_slug: str,
    layer: str,
    version: int,
    payload: ApproveSchemaSnapshotRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:write")),
) -> SchemaApprovalResponse:
    dataset = _get_dataset_or_404(session, dataset_slug)
    try:
        approval = DatasetService.approve_schema_snapshot(session, dataset, layer, version, payload)
    except ValueError as exc:
        if "already approved" in str(exc):
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return SchemaApprovalResponse.model_validate(approval)


@router.get("/{dataset_slug}/schemas/diff", response_model=SchemaDiffResponse)
def get_schema_diff(
    dataset_slug: str,
    layer: str = Query(...),
    from_version: int | None = Query(default=None, ge=0),
    to_version: int | None = Query(default=None, ge=1),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:read")),
) -> SchemaDiffResponse:
    dataset = DatasetService.get_dataset_by_slug(session, dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    try:
        diff = DatasetService.build_schema_diff(
            session,
            dataset,
            layer=layer,
            from_version=from_version,
            to_version=to_version,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return SchemaDiffResponse(**diff)


@router.post("/{dataset_slug}/schemas/compatibility", response_model=SchemaCompatibilityResponse)
def check_schema_compatibility(
    dataset_slug: str,
    payload: SchemaCompatibilityRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:read")),
) -> SchemaCompatibilityResponse:
    dataset = DatasetService.get_dataset_by_slug(session, dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    try:
        report = DatasetService.build_schema_compatibility(
            session,
            dataset,
            layer=payload.layer.value,
            candidate_schema=payload.schema_items,
            against_version=payload.against_version,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return SchemaCompatibilityResponse(**report)


@router.get("/{dataset_slug}/stats", response_model=DatasetStatsResponse)
def get_dataset_stats(
    dataset_slug: str,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:read")),
) -> DatasetStatsResponse:
    dataset = DatasetService.get_dataset_by_slug(session, dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    return DatasetStatsResponse(**DatasetService.get_dataset_stats(session, dataset))


@router.get("/{dataset_slug}/quality-rules", response_model=list[QualityRuleResponse])
def list_quality_rules(
    dataset_slug: str,
    layer: str | None = Query(default=None),
    active: bool | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:read")),
) -> list[QualityRuleResponse]:
    dataset = DatasetService.get_dataset_by_slug(session, dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    rules = QualityService.list_rules(session, dataset.id, layer=layer, active=active, limit=limit, offset=offset)
    return [QualityRuleResponse.model_validate(rule) for rule in rules]


@router.post("/{dataset_slug}/quality-rules", response_model=QualityRuleResponse, status_code=status.HTTP_201_CREATED)
def create_quality_rule(
    dataset_slug: str,
    payload: QualityRuleCreate,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:write")),
) -> QualityRuleResponse:
    dataset = DatasetService.get_dataset_by_slug(session, dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    try:
        rule = QualityService(session).create_rule(dataset, payload)
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(status_code=409, detail=f"Quality rule already exists: {exc}") from exc
    return QualityRuleResponse.model_validate(rule)


@router.patch("/{dataset_slug}/quality-rules/{rule_id}", response_model=QualityRuleResponse)
def update_quality_rule(
    dataset_slug: str,
    rule_id: str,
    payload: UpdateQualityRuleRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:write")),
) -> QualityRuleResponse:
    dataset = DatasetService.get_dataset_by_slug(session, dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    rule = QualityService.get_rule(session, dataset.id, rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail="Quality rule not found.")
    try:
        updated = QualityService(session).update_rule(rule, payload)
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(status_code=409, detail=f"Quality rule already exists: {exc}") from exc
    return QualityRuleResponse.model_validate(updated)


@router.get("/{dataset_slug}/quality-results", response_model=list[QualityResultResponse])
def list_quality_results(
    dataset_slug: str,
    ingestion_job_id: str | None = Query(default=None),
    layer: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:read")),
) -> list[QualityResultResponse]:
    dataset = DatasetService.get_dataset_by_slug(session, dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    results = QualityService.list_results(
        session,
        dataset.id,
        ingestion_job_id=ingestion_job_id,
        layer=layer,
        status=status_filter,
        limit=limit,
        offset=offset,
    )
    return [QualityResultResponse(**result) for result in results]


@router.get("/{dataset_slug}/quality-results/trends", response_model=QualityTrendResponse)
def get_quality_result_trends(
    dataset_slug: str,
    ingestion_job_id: str | None = Query(default=None),
    layer: str | None = Query(default=None),
    quality_check_id: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    created_at_after: datetime | None = Query(default=None),
    created_at_before: datetime | None = Query(default=None),
    bucket: str = Query(default="day"),
    limit: int = Query(default=30, ge=1, le=1000),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:read")),
) -> QualityTrendResponse:
    dataset = DatasetService.get_dataset_by_slug(session, dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    if created_at_after is not None and created_at_before is not None and created_at_after > created_at_before:
        raise HTTPException(status_code=400, detail="created_at_after cannot be after created_at_before")
    try:
        trend_report = QualityService.get_result_trends(
            session,
            dataset.id,
            ingestion_job_id=ingestion_job_id,
            layer=layer,
            status=status_filter,
            quality_check_id=quality_check_id,
            created_at_after=created_at_after,
            created_at_before=created_at_before,
            bucket=bucket,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return QualityTrendResponse(
        dataset_id=dataset.id,
        dataset_slug=dataset.slug,
        bucket=bucket.strip().lower(),
        ingestion_job_id=ingestion_job_id,
        layer=layer,
        quality_check_id=quality_check_id,
        status=status_filter,
        created_at_after=created_at_after,
        created_at_before=created_at_before,
        series=trend_report["series"],
        quality_checks=trend_report["quality_checks"],
    )


@router.post("/{dataset_slug}/data-products", response_model=DataProductResponse, status_code=status.HTTP_201_CREATED)
def create_data_product(
    dataset_slug: str,
    payload: CreateDataProductRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:write")),
) -> DataProductResponse:
    dataset = _get_dataset_or_404(session, dataset_slug)
    try:
        product = DatasetService.create_data_product(session, dataset, payload)
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(status_code=409, detail=f"Data product already exists: {exc}") from exc
    return DataProductResponse.model_validate(DatasetService.build_data_product_response(product))


@router.get("/{dataset_slug}/data-products", response_model=list[DataProductResponse])
def list_data_products(
    dataset_slug: str,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:read")),
) -> list[DataProductResponse]:
    dataset = _get_dataset_or_404(session, dataset_slug)
    products = DatasetService.list_data_products(session, dataset, limit=limit, offset=offset)
    return [DataProductResponse.model_validate(DatasetService.build_data_product_response(item)) for item in products]


@router.get("/{dataset_slug}/data-products/{product_slug}", response_model=DataProductResponse)
def get_data_product(
    dataset_slug: str,
    product_slug: str,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:read")),
) -> DataProductResponse:
    _, product = _get_data_product_or_404(session, dataset_slug, product_slug)
    return DataProductResponse.model_validate(DatasetService.build_data_product_response(product))


@router.patch("/{dataset_slug}/data-products/{product_slug}", response_model=DataProductResponse)
def update_data_product(
    dataset_slug: str,
    product_slug: str,
    payload: UpdateDataProductRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:write")),
) -> DataProductResponse:
    dataset, product = _get_data_product_or_404(session, dataset_slug, product_slug)
    try:
        product = DatasetService.update_data_product(session, dataset, product, payload)
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(status_code=409, detail=f"Data product already exists: {exc}") from exc
    return DataProductResponse.model_validate(DatasetService.build_data_product_response(product))


@router.get("/{dataset_slug}/data-products/{product_slug}/versions", response_model=list[DataProductVersionResponse])
def list_data_product_versions(
    dataset_slug: str,
    product_slug: str,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:read")),
) -> list[DataProductVersionResponse]:
    _, product = _get_data_product_or_404(session, dataset_slug, product_slug)
    versions = DatasetService.list_data_product_versions(session, product, limit=limit, offset=offset)
    return [DataProductVersionResponse.model_validate(DatasetService.build_data_product_version_response(item)) for item in versions]


@router.get("/{dataset_slug}/data-products/{product_slug}/versions/{version}", response_model=DataProductVersionResponse)
def get_data_product_version(
    dataset_slug: str,
    product_slug: str,
    version: int,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("datasets:read")),
) -> DataProductVersionResponse:
    _, product = _get_data_product_or_404(session, dataset_slug, product_slug)
    product_version = DatasetService.get_data_product_version(session, product, version)
    if not product_version:
        raise HTTPException(status_code=404, detail="Data product version not found.")
    return DataProductVersionResponse.model_validate(DatasetService.build_data_product_version_response(product_version))
