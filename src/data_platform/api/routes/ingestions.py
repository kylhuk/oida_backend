from __future__ import annotations

import json

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from data_platform.api.deps import get_db, require_scopes
from data_platform.models.ingestion import IngestionJob
from data_platform.schemas.artifacts import ArtifactManifestResponse
from data_platform.schemas.ingestion import (
    CompleteUploadRequest,
    IngestionArtifactManifestResponse,
    IngestionJobResponse,
    InlineJsonIngestionRequest,
    ObjectUriIngestionRequest,
    PresignUploadRequest,
    PresignUploadResponse,
    ReprocessIngestionRequest,
    UrlIngestionRequest,
)
from data_platform.services.dataset_service import DatasetService
from data_platform.services.ingestion_service import IngestionService
from data_platform.services.worker_console_service import WorkerConsoleService
from data_platform.schemas.console import WorkerConsolePageResponse

router = APIRouter(prefix="/v1/ingestions", tags=["ingestions"])


@router.post("/presign", response_model=PresignUploadResponse)
def create_presigned_upload(
    payload: PresignUploadRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("ingestions:write")),
) -> PresignUploadResponse:
    dataset = DatasetService.get_dataset_by_slug(session, payload.dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    service = IngestionService(session)
    details = service.build_presigned_upload(dataset, payload.filename, payload.content_type)
    return PresignUploadResponse(**details)


@router.post("/complete", response_model=IngestionJobResponse, status_code=status.HTTP_202_ACCEPTED)
def complete_presigned_upload(
    payload: CompleteUploadRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("ingestions:write")),
) -> IngestionJobResponse:
    dataset = DatasetService.get_dataset_by_slug(session, payload.dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    service = IngestionService(session)
    job = service.complete_presigned_upload(dataset, payload)
    return IngestionJobResponse.model_validate(job)


@router.post("/object-uri", response_model=IngestionJobResponse, status_code=status.HTTP_202_ACCEPTED)
def create_object_uri_ingestion(
    payload: ObjectUriIngestionRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("ingestions:write")),
) -> IngestionJobResponse:
    dataset = DatasetService.get_dataset_by_slug(session, payload.dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    service = IngestionService(session)
    try:
        job = service.create_object_uri_ingestion(dataset, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return IngestionJobResponse.model_validate(job)


@router.post("/json", response_model=IngestionJobResponse, status_code=status.HTTP_202_ACCEPTED)
def create_inline_json_ingestion(
    payload: InlineJsonIngestionRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("ingestions:write")),
) -> IngestionJobResponse:
    dataset = DatasetService.get_dataset_by_slug(session, payload.dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    service = IngestionService(session)
    job = service.create_inline_json_ingestion(dataset, payload)
    return IngestionJobResponse.model_validate(job)


@router.post("/url", response_model=IngestionJobResponse, status_code=status.HTTP_202_ACCEPTED)
def create_url_ingestion(
    payload: UrlIngestionRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("ingestions:write")),
) -> IngestionJobResponse:
    dataset = DatasetService.get_dataset_by_slug(session, payload.dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    service = IngestionService(session)
    job = service.create_url_ingestion(dataset, payload)
    return IngestionJobResponse.model_validate(job)


@router.post("/upload", response_model=IngestionJobResponse, status_code=status.HTTP_202_ACCEPTED)
async def upload_file_ingestion(
    dataset_slug: str = Form(...),
    file: UploadFile = File(...),
    source_format: str | None = Form(default=None),
    idempotency_key: str | None = Form(default=None),
    metadata: str | None = Form(default=None),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("ingestions:write")),
) -> IngestionJobResponse:
    dataset = DatasetService.get_dataset_by_slug(session, dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")

    metadata_dict = {}
    if metadata:
        try:
            metadata_dict = json.loads(metadata)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail=f"metadata must be valid JSON: {exc}") from exc

    service = IngestionService(session)
    job = await service.create_direct_upload_ingestion(
        dataset=dataset,
        filename=file.filename or "upload.bin",
        file_bytes_stream=file,
        content_type=file.content_type,
        source_format=source_format,
        idempotency_key=idempotency_key,
        metadata=metadata_dict,
    )
    return IngestionJobResponse.model_validate(job)


@router.get("", response_model=list[IngestionJobResponse])
def list_ingestion_jobs(
    dataset_slug: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("ingestions:read")),
) -> list[IngestionJobResponse]:
    dataset_id = None
    if dataset_slug is not None:
        dataset = DatasetService.get_dataset_by_slug(session, dataset_slug)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found.")
        dataset_id = dataset.id

    jobs = IngestionService(session).list_jobs(
        dataset_id=dataset_id,
        status=status_filter,
        limit=limit,
        offset=offset,
    )
    return [IngestionJobResponse.model_validate(job) for job in jobs]




@router.get("/{job_id}/artifacts", response_model=ArtifactManifestResponse)
def get_ingestion_artifacts(
    job_id: str,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("ingestions:read")),
) -> ArtifactManifestResponse:
    job = session.get(IngestionJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Ingestion job not found.")
    manifest = IngestionService(session).build_artifacts(job)
    return ArtifactManifestResponse.model_validate(manifest)

@router.get("/{job_id}/artifact-manifest", response_model=IngestionArtifactManifestResponse)
def get_ingestion_artifact_manifest(
    job_id: str,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("ingestions:read")),
) -> IngestionArtifactManifestResponse:
    job = session.get(IngestionJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Ingestion job not found.")
    return IngestionArtifactManifestResponse.model_validate(
        IngestionService.build_detailed_artifact_manifest(session, job)
    )


@router.get("/{job_id}", response_model=IngestionJobResponse)
def get_ingestion_job(
    job_id: str,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("ingestions:read")),
) -> IngestionJobResponse:
    job = session.get(IngestionJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Ingestion job not found.")
    return IngestionJobResponse.model_validate(job)


@router.get("/{job_id}/console", response_model=WorkerConsolePageResponse)
def get_ingestion_job_console(
    job_id: str,
    cursor: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("ingestions:read")),
) -> WorkerConsolePageResponse:
    job = session.get(IngestionJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Ingestion job not found.")
    page = WorkerConsoleService(session).list_console_page(
        resource_path=f"/worker/ingestion-jobs/{job.id}",
        cursor=cursor,
        limit=limit,
    )
    return WorkerConsolePageResponse.model_validate(page)


@router.get("/{job_id}/console/tail")
def tail_ingestion_job_console(
    job_id: str,
    cursor: str | None = Query(default=None),
    poll_interval_seconds: int = Query(default=1, ge=1, le=10),
    heartbeat_seconds: int = Query(default=10, ge=1, le=60),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("ingestions:read")),
) -> StreamingResponse:
    job = session.get(IngestionJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Ingestion job not found.")
    stream = WorkerConsoleService(session).stream_console(
        resource_path=f"/worker/ingestion-jobs/{job.id}",
        cursor=cursor,
        poll_interval_seconds=poll_interval_seconds,
        heartbeat_seconds=heartbeat_seconds,
    )
    return StreamingResponse(stream, media_type="text/event-stream")


@router.post("/{job_id}/reprocess", response_model=IngestionJobResponse, status_code=status.HTTP_202_ACCEPTED)
def reprocess_ingestion_job(
    job_id: str,
    payload: ReprocessIngestionRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("ingestions:write")),
) -> IngestionJobResponse:
    source_job = session.get(IngestionJob, job_id)
    if not source_job:
        raise HTTPException(status_code=404, detail="Ingestion job not found.")
    try:
        job = IngestionService(session).reprocess_job(source_job, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return IngestionJobResponse.model_validate(job)
