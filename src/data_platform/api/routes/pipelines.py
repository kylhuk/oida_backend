from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from data_platform.api.deps import get_db, require_scopes
from data_platform.models.pipeline import PipelineRun
from data_platform.enums import PipelineStatus
from data_platform.schemas.artifacts import ArtifactManifestResponse
from data_platform.schemas.console import WorkerConsolePageResponse
from data_platform.schemas.pipeline import (
    CreatePipelineBackfillRunsPageRequest,
    CreatePipelineBackfillRunsRequest,
    CreatePipelineDefinitionRequest,
    CreatePipelineRunRequest,
    PipelineArtifactManifestResponse,
    PipelineBackfillRunsPageResponse,
    PipelineDefinitionResponse,
    PipelineExecutionPlanResponse,
    PipelinePreflightAttemptCountResponse,
    PipelinePreflightAttemptPageResponse,
    PipelinePreflightAttemptResponse,
    PipelineRunDetailResponse,
    PipelineRunCountResponse,
    PipelineRunPageResponse,
    PipelineRunResponse,
    PipelineSourceCandidateCountResponse,
    PipelineSourceCandidatePageResponse,
    PipelineSourceCandidateResponse,
    UpdatePipelineRunStatusRequest,
    UpdatePipelineDefinitionRequest,
)
from data_platform.services.dataset_service import DatasetService
from data_platform.services.pipeline_service import PipelineService
from data_platform.services.worker_console_service import WorkerConsoleService

router = APIRouter(prefix="/v1", tags=["pipelines"])


@router.post("/datasets/{dataset_slug}/pipelines", response_model=PipelineDefinitionResponse, status_code=status.HTTP_201_CREATED)
def create_pipeline_definition(
    dataset_slug: str,
    payload: CreatePipelineDefinitionRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:write")),
) -> PipelineDefinitionResponse:
    dataset = DatasetService.get_dataset_by_slug(session, dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    try:
        pipeline = PipelineService.create_pipeline(session, dataset, payload)
    except ValueError as exc:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(status_code=409, detail=f"Pipeline already exists or violates uniqueness: {exc}") from exc
    return PipelineDefinitionResponse.model_validate(pipeline)


@router.get("/datasets/{dataset_slug}/pipelines", response_model=list[PipelineDefinitionResponse])
def list_pipeline_definitions(
    dataset_slug: str,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:read")),
) -> list[PipelineDefinitionResponse]:
    dataset = DatasetService.get_dataset_by_slug(session, dataset_slug)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    pipelines = PipelineService.list_pipelines(session, dataset.id, limit=limit, offset=offset)
    return [PipelineDefinitionResponse.model_validate(pipeline) for pipeline in pipelines]


@router.get("/pipelines/{pipeline_id}", response_model=PipelineDefinitionResponse)
def get_pipeline_definition(
    pipeline_id: str,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:read")),
) -> PipelineDefinitionResponse:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    return PipelineDefinitionResponse.model_validate(pipeline)


@router.patch("/pipelines/{pipeline_id}", response_model=PipelineDefinitionResponse)
def update_pipeline_definition(
    pipeline_id: str,
    payload: UpdatePipelineDefinitionRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:write")),
) -> PipelineDefinitionResponse:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    try:
        updated = PipelineService.update_pipeline(session, pipeline, payload)
    except ValueError as exc:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(status_code=409, detail=f"Pipeline already exists or violates uniqueness: {exc}") from exc
    return PipelineDefinitionResponse.model_validate(updated)




@router.get("/pipelines/{pipeline_id}/execution-plan", response_model=PipelineExecutionPlanResponse)
def get_pipeline_execution_plan(
    pipeline_id: str,
    source_ingestion_job_id: str | None = Query(default=None),
    source_finished_at_gte: datetime | None = Query(default=None),
    source_finished_at_lte: datetime | None = Query(default=None),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:read")),
) -> PipelineExecutionPlanResponse:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    try:
        plan = PipelineService.build_execution_plan(
            session,
            pipeline,
            source_ingestion_job_id=source_ingestion_job_id,
            source_finished_at_gte=source_finished_at_gte,
            source_finished_at_lte=source_finished_at_lte,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return PipelineExecutionPlanResponse.model_validate(plan)

@router.get("/pipelines/{pipeline_id}/source-candidates/count", response_model=PipelineSourceCandidateCountResponse)
def count_pipeline_source_candidates(
    pipeline_id: str,
    source_finished_at_gte: datetime | None = Query(default=None),
    source_finished_at_lte: datetime | None = Query(default=None),
    exclude_existing_runs: bool = Query(default=False),
    has_existing_run: bool | None = Query(default=None),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:read")),
) -> PipelineSourceCandidateCountResponse:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    try:
        count = PipelineService.count_pipeline_source_candidates(
            session,
            pipeline,
            source_finished_at_gte=source_finished_at_gte,
            source_finished_at_lte=source_finished_at_lte,
            exclude_existing_runs=exclude_existing_runs,
            has_existing_run=has_existing_run,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return PipelineSourceCandidateCountResponse(count=count)


@router.get("/pipelines/{pipeline_id}/source-candidates/page", response_model=PipelineSourceCandidatePageResponse)
def list_pipeline_source_candidates_page(
    pipeline_id: str,
    source_finished_at_gte: datetime | None = Query(default=None),
    source_finished_at_lte: datetime | None = Query(default=None),
    run_ref_prefix: str | None = Query(default=None),
    exclude_existing_runs: bool = Query(default=False),
    has_existing_run: bool | None = Query(default=None),
    cursor: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:read")),
) -> PipelineSourceCandidatePageResponse:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    try:
        page = PipelineService.list_pipeline_source_candidates_page(
            session,
            pipeline,
            source_finished_at_gte=source_finished_at_gte,
            source_finished_at_lte=source_finished_at_lte,
            run_ref_prefix=run_ref_prefix,
            exclude_existing_runs=exclude_existing_runs,
            has_existing_run=has_existing_run,
            cursor=cursor,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return PipelineSourceCandidatePageResponse.model_validate(page)


@router.get("/pipelines/{pipeline_id}/source-candidates", response_model=list[PipelineSourceCandidateResponse])
def list_pipeline_source_candidates(
    pipeline_id: str,
    source_finished_at_gte: datetime | None = Query(default=None),
    source_finished_at_lte: datetime | None = Query(default=None),
    run_ref_prefix: str | None = Query(default=None),
    exclude_existing_runs: bool = Query(default=False),
    has_existing_run: bool | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:read")),
) -> list[PipelineSourceCandidateResponse]:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    try:
        candidates = PipelineService.list_pipeline_source_candidates(
            session,
            pipeline,
            source_finished_at_gte=source_finished_at_gte,
            source_finished_at_lte=source_finished_at_lte,
            run_ref_prefix=run_ref_prefix,
            exclude_existing_runs=exclude_existing_runs,
            has_existing_run=has_existing_run,
            limit=limit,
            offset=offset,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return [PipelineSourceCandidateResponse.model_validate(candidate) for candidate in candidates]


@router.get("/pipelines/{pipeline_id}/preflight-attempts/count", response_model=PipelinePreflightAttemptCountResponse)
def count_pipeline_preflight_attempts(
    pipeline_id: str,
    request_kind: str | None = Query(default=None),
    run_ref: str | None = Query(default=None),
    source_ingestion_job_id: str | None = Query(default=None),
    created_at_gte: datetime | None = Query(default=None),
    created_at_lte: datetime | None = Query(default=None),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:read")),
) -> PipelinePreflightAttemptCountResponse:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    try:
        count = PipelineService.count_pipeline_preflight_attempts(
            session,
            pipeline_id,
            request_kind=request_kind,
            run_ref=run_ref,
            source_ingestion_job_id=source_ingestion_job_id,
            created_at_gte=created_at_gte,
            created_at_lte=created_at_lte,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return PipelinePreflightAttemptCountResponse(count=count)


@router.get("/pipelines/{pipeline_id}/preflight-attempts", response_model=list[PipelinePreflightAttemptResponse])
def list_pipeline_preflight_attempts(
    pipeline_id: str,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    request_kind: str | None = Query(default=None),
    run_ref: str | None = Query(default=None),
    source_ingestion_job_id: str | None = Query(default=None),
    created_at_gte: datetime | None = Query(default=None),
    created_at_lte: datetime | None = Query(default=None),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:read")),
) -> list[PipelinePreflightAttemptResponse]:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    try:
        attempts = PipelineService.list_pipeline_preflight_attempts(
            session,
            pipeline_id,
            limit=limit,
            offset=offset,
            request_kind=request_kind,
            run_ref=run_ref,
            source_ingestion_job_id=source_ingestion_job_id,
            created_at_gte=created_at_gte,
            created_at_lte=created_at_lte,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return [
        PipelinePreflightAttemptResponse.model_validate(
            PipelineService.build_pipeline_preflight_attempt_response(attempt)
        )
        for attempt in attempts
    ]


@router.get("/pipelines/{pipeline_id}/preflight-attempts/page", response_model=PipelinePreflightAttemptPageResponse)
def list_pipeline_preflight_attempts_page(
    pipeline_id: str,
    request_kind: str | None = Query(default=None),
    run_ref: str | None = Query(default=None),
    source_ingestion_job_id: str | None = Query(default=None),
    created_at_gte: datetime | None = Query(default=None),
    created_at_lte: datetime | None = Query(default=None),
    cursor: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:read")),
) -> PipelinePreflightAttemptPageResponse:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    try:
        page = PipelineService.list_pipeline_preflight_attempts_page(
            session,
            pipeline_id,
            request_kind=request_kind,
            run_ref=run_ref,
            source_ingestion_job_id=source_ingestion_job_id,
            created_at_gte=created_at_gte,
            created_at_lte=created_at_lte,
            cursor=cursor,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return PipelinePreflightAttemptPageResponse.model_validate(
        {
            "items": [
                PipelineService.build_pipeline_preflight_attempt_response(attempt)
                for attempt in page["items"]
            ],
            "next_cursor": page.get("next_cursor"),
        }
    )


# Keep static preflight-attempt routes above this dynamic id route so "count" and "page" are not captured as ids.
@router.get("/pipelines/{pipeline_id}/preflight-attempts/{preflight_attempt_id}", response_model=PipelinePreflightAttemptResponse)
def get_pipeline_preflight_attempt(
    pipeline_id: str,
    preflight_attempt_id: str,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:read")),
) -> PipelinePreflightAttemptResponse:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    attempt = PipelineService.get_pipeline_preflight_attempt(session, pipeline_id, preflight_attempt_id)
    if attempt is None:
        raise HTTPException(status_code=404, detail="Pipeline preflight attempt not found.")
    return PipelinePreflightAttemptResponse.model_validate(
        PipelineService.build_pipeline_preflight_attempt_response(attempt)
    )


@router.post("/pipelines/{pipeline_id}/runs", response_model=PipelineRunResponse, status_code=status.HTTP_201_CREATED)
def create_pipeline_run(
    pipeline_id: str,
    payload: CreatePipelineRunRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:write")),
) -> PipelineRunResponse:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    try:
        run = PipelineService.create_pipeline_run(session, pipeline, payload)
    except ValueError as exc:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return PipelineRunResponse.model_validate(PipelineService.build_pipeline_run_response(run))


@router.post("/pipelines/{pipeline_id}/runs/backfill", response_model=list[PipelineRunResponse])
def create_pipeline_backfill_runs(
    pipeline_id: str,
    payload: CreatePipelineBackfillRunsRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:write")),
) -> list[PipelineRunResponse]:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    try:
        runs = PipelineService.create_pipeline_backfill_runs(session, pipeline, payload)
    except ValueError as exc:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return [PipelineRunResponse.model_validate(PipelineService.build_pipeline_run_response(run)) for run in runs]


@router.post("/pipelines/{pipeline_id}/runs/backfill/page", response_model=PipelineBackfillRunsPageResponse)
def create_pipeline_backfill_runs_page(
    pipeline_id: str,
    payload: CreatePipelineBackfillRunsPageRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:write")),
) -> PipelineBackfillRunsPageResponse:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    try:
        page = PipelineService.create_pipeline_backfill_runs_page(session, pipeline, payload)
    except ValueError as exc:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return PipelineBackfillRunsPageResponse.model_validate(
        {
            "items": [
                PipelineService.build_pipeline_run_response(run)
                for run in page["items"]
            ],
            "next_cursor": page.get("next_cursor"),
        }
    )


@router.post("/pipelines/{pipeline_id}/runs/claim", response_model=PipelineRunResponse)
def claim_pipeline_run(
    pipeline_id: str,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:write")),
) -> PipelineRunResponse:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    try:
        run = PipelineService.claim_next_pipeline_run(session, pipeline)
    except ValueError as exc:
        session.rollback()
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if run is None:
        raise HTTPException(status_code=404, detail="No planned sql pipeline run is available to claim.")
    return PipelineRunResponse.model_validate(PipelineService.build_pipeline_run_response(run))


@router.get("/pipelines/{pipeline_id}/runs/count", response_model=PipelineRunCountResponse)
def count_pipeline_runs(
    pipeline_id: str,
    run_status: PipelineStatus | None = Query(default=None, alias="status"),
    run_ref: str | None = Query(default=None),
    source_ingestion_job_id: str | None = Query(default=None),
    created_at_gte: datetime | None = Query(default=None),
    created_at_lte: datetime | None = Query(default=None),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:read")),
) -> PipelineRunCountResponse:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    try:
        count = PipelineService.count_pipeline_runs(
            session,
            pipeline_id,
            run_status=run_status,
            run_ref=run_ref,
            source_ingestion_job_id=source_ingestion_job_id,
            created_at_gte=created_at_gte,
            created_at_lte=created_at_lte,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return PipelineRunCountResponse(count=count)


@router.get("/pipelines/{pipeline_id}/runs/page", response_model=PipelineRunPageResponse)
def list_pipeline_runs_page(
    pipeline_id: str,
    limit: int = Query(default=100, ge=1, le=1000),
    run_status: PipelineStatus | None = Query(default=None, alias="status"),
    run_ref: str | None = Query(default=None),
    source_ingestion_job_id: str | None = Query(default=None),
    created_at_gte: datetime | None = Query(default=None),
    created_at_lte: datetime | None = Query(default=None),
    cursor: str | None = Query(default=None),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:read")),
) -> PipelineRunPageResponse:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    try:
        page = PipelineService.list_pipeline_runs_page(
            session,
            pipeline_id,
            run_status=run_status,
            run_ref=run_ref,
            source_ingestion_job_id=source_ingestion_job_id,
            created_at_gte=created_at_gte,
            created_at_lte=created_at_lte,
            cursor=cursor,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return PipelineRunPageResponse.model_validate(
        {
            "items": [
                PipelineService.build_pipeline_run_response(run)
                for run in page["items"]
            ],
            "next_cursor": page.get("next_cursor"),
        }
    )


# Keep static pipeline-run routes above this dynamic id route so "count" and "page" are not captured as ids.


@router.get("/pipelines/{pipeline_id}/runs/{run_id}/artifacts", response_model=ArtifactManifestResponse)
def get_pipeline_run_artifacts(
    pipeline_id: str,
    run_id: str,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:read")),
) -> ArtifactManifestResponse:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")

    run = session.get(PipelineRun, run_id)
    if not run or run.pipeline_id != pipeline.id:
        raise HTTPException(status_code=404, detail="Pipeline run not found.")

    manifest = PipelineService.build_run_artifact_manifest(run, pipeline)
    return ArtifactManifestResponse.model_validate(manifest)

@router.get("/pipelines/{pipeline_id}/runs/{run_id}", response_model=PipelineRunDetailResponse)
def get_pipeline_run(
    pipeline_id: str,
    run_id: str,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:read")),
) -> PipelineRunDetailResponse:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    run = PipelineService.get_pipeline_run(session, pipeline_id, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Pipeline run not found.")
    return PipelineRunDetailResponse.model_validate(PipelineService.build_pipeline_run_detail(run))


@router.get("/pipelines/{pipeline_id}/runs/{run_id}/console", response_model=WorkerConsolePageResponse)
def get_pipeline_run_console(
    pipeline_id: str,
    run_id: str,
    cursor: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:read")),
) -> WorkerConsolePageResponse:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    run = PipelineService.get_pipeline_run(session, pipeline_id, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Pipeline run not found.")
    page = WorkerConsoleService(session).list_console_page(
        resource_path=f"/worker/pipeline-runs/{run.id}",
        cursor=cursor,
        limit=limit,
    )
    return WorkerConsolePageResponse.model_validate(page)


@router.get("/pipelines/{pipeline_id}/runs/{run_id}/console/tail")
def tail_pipeline_run_console(
    pipeline_id: str,
    run_id: str,
    cursor: str | None = Query(default=None),
    poll_interval_seconds: int = Query(default=1, ge=1, le=10),
    heartbeat_seconds: int = Query(default=10, ge=1, le=60),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:read")),
) -> StreamingResponse:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    run = PipelineService.get_pipeline_run(session, pipeline_id, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Pipeline run not found.")
    stream = WorkerConsoleService(session).stream_console(
        resource_path=f"/worker/pipeline-runs/{run.id}",
        cursor=cursor,
        poll_interval_seconds=poll_interval_seconds,
        heartbeat_seconds=heartbeat_seconds,
    )
    return StreamingResponse(stream, media_type="text/event-stream")


@router.get("/pipelines/{pipeline_id}/runs/{run_id}/artifact-manifest", response_model=PipelineArtifactManifestResponse)
def get_pipeline_run_artifact_manifest(
    pipeline_id: str,
    run_id: str,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:read")),
) -> PipelineArtifactManifestResponse:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    run = PipelineService.get_pipeline_run(session, pipeline_id, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Pipeline run not found.")
    manifest = PipelineService.build_pipeline_run_artifact_manifest(run)
    if manifest is None:
        raise HTTPException(status_code=404, detail="Pipeline run artifact manifest not available.")
    return PipelineArtifactManifestResponse.model_validate(manifest)


@router.patch("/pipelines/{pipeline_id}/runs/{run_id}/status", response_model=PipelineRunResponse)
def update_pipeline_run_status(
    pipeline_id: str,
    run_id: str,
    payload: UpdatePipelineRunStatusRequest,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:write")),
) -> PipelineRunResponse:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    run = PipelineService.get_pipeline_run(session, pipeline_id, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Pipeline run not found.")
    try:
        updated = PipelineService.transition_pipeline_run(session, pipeline, run, payload)
    except ValueError as exc:
        session.rollback()
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return PipelineRunResponse.model_validate(PipelineService.build_pipeline_run_response(updated))


@router.get("/pipelines/{pipeline_id}/runs", response_model=list[PipelineRunResponse])
def list_pipeline_runs(
    pipeline_id: str,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    run_status: PipelineStatus | None = Query(default=None, alias="status"),
    run_ref: str | None = Query(default=None),
    source_ingestion_job_id: str | None = Query(default=None),
    created_at_gte: datetime | None = Query(default=None),
    created_at_lte: datetime | None = Query(default=None),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("pipelines:read")),
) -> list[PipelineRunResponse]:
    pipeline = PipelineService.get_pipeline(session, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found.")
    try:
        runs = PipelineService.list_pipeline_runs(
            session,
            pipeline_id,
            limit=limit,
            offset=offset,
            run_status=run_status,
            run_ref=run_ref,
            source_ingestion_job_id=source_ingestion_job_id,
            created_at_gte=created_at_gte,
            created_at_lte=created_at_lte,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return [PipelineRunResponse.model_validate(PipelineService.build_pipeline_run_response(run)) for run in runs]
