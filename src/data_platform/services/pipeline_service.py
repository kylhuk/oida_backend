from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import desc, func, select, update
from sqlalchemy.orm import Session

from data_platform.enums import DatasetLayer, IngestionStatus, PipelineEngine, PipelineStatus
from data_platform.models.dataset import Dataset
from data_platform.models.ingestion import IngestionJob
from data_platform.models.pipeline import PipelineDefinition, PipelinePreflightAttempt, PipelineRun
from data_platform.schemas.pipeline import (
    CreatePipelineBackfillRunsPageRequest,
    CreatePipelineBackfillRunsRequest,
    CreatePipelineDefinitionRequest,
    CreatePipelineRunRequest,
    UpdatePipelineDefinitionRequest,
    UpdatePipelineRunStatusRequest,
)
from data_platform.services.dataset_service import DatasetService
from data_platform.utils.artifacts import build_pipeline_run_artifact_manifest
from data_platform.utils.pipeline_definitions import (
    assert_pipeline_preflight_attempt_page_cursor_matches_scope,
    assert_pipeline_run_page_cursor_matches_scope,
    assert_pipeline_source_candidate_page_cursor_matches_scope,
    build_backfill_request_snapshot,
    build_backfill_run_ref,
    build_pipeline_execution_plan,
    decode_pipeline_preflight_attempt_page_cursor,
    decode_pipeline_run_page_cursor,
    decode_pipeline_source_candidate_page_cursor_position,
    encode_pipeline_preflight_attempt_page_cursor,
    encode_pipeline_run_page_cursor,
    encode_pipeline_source_candidate_page_cursor,
    build_pipeline_preflight_metrics,
    build_pipeline_run_payload,
    build_pipeline_schema_compatibility_preview,
    build_pipeline_schema_compatibility_preview_unavailable_reason,
    build_pipeline_schema_context,
    build_pipeline_schema_snapshot,
    build_pipeline_source_candidate,
    extract_pipeline_artifact_manifest,
    extract_pipeline_run_snapshot,
    normalize_optional_pipeline_status,
    normalize_optional_run_ref,
    normalize_pipeline_definition,
)


class PipelineService:

    @staticmethod
    def build_run_artifact_manifest(run: PipelineRun, pipeline: PipelineDefinition | None = None) -> dict:
        return build_pipeline_run_artifact_manifest(run, pipeline=pipeline or run.pipeline)

    @staticmethod
    def create_pipeline(session: Session, dataset: Dataset, payload: CreatePipelineDefinitionRequest) -> PipelineDefinition:
        pipeline = PipelineDefinition(
            dataset_id=dataset.id,
            name=payload.name.strip(),
            source_layer=payload.source_layer.value,
            target_layer=payload.target_layer.value,
            engine=payload.engine.value,
            definition_json=normalize_pipeline_definition(
                payload.engine,
                payload.target_layer,
                payload.definition_json,
            ),
            active=payload.active,
        )
        session.add(pipeline)
        session.commit()
        session.refresh(pipeline)
        return pipeline

    @staticmethod
    def get_pipeline(session: Session, pipeline_id: str) -> PipelineDefinition | None:
        return session.get(PipelineDefinition, pipeline_id)

    @staticmethod
    def update_pipeline(
        session: Session,
        pipeline: PipelineDefinition,
        payload: UpdatePipelineDefinitionRequest,
    ) -> PipelineDefinition:
        updates = payload.model_dump(exclude_unset=True)
        if "source_layer" in updates and updates["source_layer"] is not None:
            updates["source_layer"] = updates["source_layer"].value
        if "target_layer" in updates and updates["target_layer"] is not None:
            updates["target_layer"] = updates["target_layer"].value
        if "engine" in updates and updates["engine"] is not None:
            updates["engine"] = updates["engine"].value
        if "name" in updates and updates["name"] is not None:
            updates["name"] = updates["name"].strip()

        source_layer = updates.get("source_layer", pipeline.source_layer)
        target_layer = updates.get("target_layer", pipeline.target_layer)
        if source_layer == target_layer:
            raise ValueError("source_layer and target_layer must differ.")

        effective_engine = updates.get("engine", pipeline.engine)
        if {"definition_json", "engine", "target_layer"} & updates.keys():
            updates["definition_json"] = normalize_pipeline_definition(
                effective_engine,
                target_layer,
                updates.get("definition_json", pipeline.definition_json),
            )

        for field_name, value in updates.items():
            setattr(pipeline, field_name, value)

        session.add(pipeline)
        session.commit()
        session.refresh(pipeline)
        return pipeline

    @staticmethod
    def list_pipelines(session: Session, dataset_id: str, limit: int = 100, offset: int = 0) -> list[PipelineDefinition]:
        limit = max(1, min(limit, 1000))
        offset = max(0, offset)
        return list(
            session.scalars(
                select(PipelineDefinition)
                .where(PipelineDefinition.dataset_id == dataset_id)
                .order_by(PipelineDefinition.created_at.desc())
                .limit(limit)
                .offset(offset)
            ).all()
        )

    @staticmethod
    def _source_object_uri_for_layer(job: IngestionJob | None, layer: DatasetLayer | str) -> str | None:
        if job is None:
            return None

        layer_value = layer.value if isinstance(layer, DatasetLayer) else str(layer).strip().lower()
        if layer_value == DatasetLayer.RAW.value:
            return job.raw_object_uri
        if layer_value == DatasetLayer.SILVER.value:
            return job.silver_object_uri
        if layer_value == DatasetLayer.GOLD.value:
            return job.gold_object_uri
        raise ValueError(f"Unsupported dataset layer: {layer!r}")

    @staticmethod
    def _latest_source_job_for_layer(
        session: Session,
        pipeline: PipelineDefinition,
        finished_at_gte: datetime | None = None,
        finished_at_lte: datetime | None = None,
    ) -> IngestionJob | None:
        source_layer = DatasetLayer(pipeline.source_layer)
        source_uri_column = {
            DatasetLayer.RAW: IngestionJob.raw_object_uri,
            DatasetLayer.SILVER: IngestionJob.silver_object_uri,
            DatasetLayer.GOLD: IngestionJob.gold_object_uri,
        }[source_layer]
        effective_finished_at = func.coalesce(IngestionJob.finished_at, IngestionJob.created_at)
        query = (
            select(IngestionJob)
            .where(
                IngestionJob.dataset_id == pipeline.dataset_id,
                IngestionJob.status == IngestionStatus.SUCCEEDED.value,
                source_uri_column.is_not(None),
            )
            .order_by(desc(effective_finished_at), desc(IngestionJob.created_at), desc(IngestionJob.id))
        )
        if finished_at_gte is not None:
            query = query.where(effective_finished_at >= finished_at_gte)
        if finished_at_lte is not None:
            query = query.where(effective_finished_at <= finished_at_lte)

        return session.scalar(query)

    @staticmethod
    def _resolve_source_job(
        session: Session,
        pipeline: PipelineDefinition,
        source_ingestion_job_id: str | None = None,
        source_finished_at_gte: datetime | None = None,
        source_finished_at_lte: datetime | None = None,
    ) -> tuple[str, str | None, datetime | None, datetime | None, IngestionJob | None]:
        if source_ingestion_job_id is not None and (
            source_finished_at_gte is not None or source_finished_at_lte is not None
        ):
            raise ValueError(
                "source_ingestion_job_id cannot be combined with source_finished_at_gte or source_finished_at_lte."
            )
        if (
            source_finished_at_gte is not None
            and source_finished_at_lte is not None
            and source_finished_at_gte > source_finished_at_lte
        ):
            raise ValueError("source_finished_at_gte cannot be after source_finished_at_lte.")

        if source_ingestion_job_id is not None:
            requested_id = normalize_optional_run_ref(
                source_ingestion_job_id,
                field_name="source_ingestion_job_id",
            )
            source_job = session.get(IngestionJob, requested_id)
            if source_job is None:
                raise ValueError(f"Source ingestion job {requested_id!r} not found.")
            if source_job.dataset_id != pipeline.dataset_id:
                raise ValueError(
                    f"Source ingestion job {requested_id!r} does not belong to dataset {pipeline.dataset_id!r}."
                )

            return "explicit", requested_id, None, None, source_job

        if source_finished_at_gte is not None and source_finished_at_lte is not None:
            return (
                "latest_successful_between",
                None,
                source_finished_at_gte,
                source_finished_at_lte,
                PipelineService._latest_source_job_for_layer(
                    session,
                    pipeline,
                    finished_at_gte=source_finished_at_gte,
                    finished_at_lte=source_finished_at_lte,
                ),
            )

        if source_finished_at_gte is not None:
            return (
                "latest_successful_at_or_after",
                None,
                source_finished_at_gte,
                None,
                PipelineService._latest_source_job_for_layer(session, pipeline, finished_at_gte=source_finished_at_gte),
            )

        if source_finished_at_lte is not None:
            return (
                "latest_successful_at_or_before",
                None,
                None,
                source_finished_at_lte,
                PipelineService._latest_source_job_for_layer(session, pipeline, finished_at_lte=source_finished_at_lte),
            )

        return "latest_successful", None, None, None, PipelineService._latest_source_job_for_layer(session, pipeline)

    @staticmethod
    def count_pipeline_source_candidates(
        session: Session,
        pipeline: PipelineDefinition,
        *,
        source_finished_at_gte: datetime | None = None,
        source_finished_at_lte: datetime | None = None,
        exclude_existing_runs: bool = False,
        has_existing_run: bool | None = None,
    ) -> int:
        if (
            source_finished_at_gte is not None
            and source_finished_at_lte is not None
            and source_finished_at_gte > source_finished_at_lte
        ):
            raise ValueError("source_finished_at_gte cannot be after source_finished_at_lte.")
        if exclude_existing_runs and has_existing_run is True:
            raise ValueError("exclude_existing_runs cannot be combined with has_existing_run=true.")

        source_layer = DatasetLayer(pipeline.source_layer)
        source_uri_column = {
            DatasetLayer.RAW: IngestionJob.raw_object_uri,
            DatasetLayer.SILVER: IngestionJob.silver_object_uri,
            DatasetLayer.GOLD: IngestionJob.gold_object_uri,
        }[source_layer]
        effective_finished_at = func.coalesce(IngestionJob.finished_at, IngestionJob.created_at)
        query = select(func.count(IngestionJob.id)).where(
            IngestionJob.dataset_id == pipeline.dataset_id,
            IngestionJob.status == IngestionStatus.SUCCEEDED.value,
            source_uri_column.is_not(None),
        )
        existing_run_exists = (
            select(PipelineRun.id)
            .where(
                PipelineRun.pipeline_id == pipeline.id,
                PipelineRun.ingestion_job_id == IngestionJob.id,
            )
            .exists()
        )
        if exclude_existing_runs or has_existing_run is False:
            query = query.where(~existing_run_exists)
        elif has_existing_run is True:
            query = query.where(existing_run_exists)
        if source_finished_at_gte is not None:
            query = query.where(effective_finished_at >= source_finished_at_gte)
        if source_finished_at_lte is not None:
            query = query.where(effective_finished_at <= source_finished_at_lte)

        result = session.scalar(query)
        return int(result or 0)


    @staticmethod
    def build_execution_plan(
        session: Session,
        pipeline: PipelineDefinition,
        source_ingestion_job_id: str | None = None,
        source_finished_at_gte: datetime | None = None,
        source_finished_at_lte: datetime | None = None,
    ) -> dict:
        dataset = pipeline.dataset or session.get(Dataset, pipeline.dataset_id)
        if dataset is None:
            raise ValueError(f"Dataset {pipeline.dataset_id!r} not found for pipeline {pipeline.id!r}.")

        (
            source_selection,
            requested_source_ingestion_job_id,
            requested_source_finished_at_gte,
            requested_source_finished_at_lte,
            source_job,
        ) = PipelineService._resolve_source_job(
            session,
            pipeline,
            source_ingestion_job_id=source_ingestion_job_id,
            source_finished_at_gte=source_finished_at_gte,
            source_finished_at_lte=source_finished_at_lte,
        )

        return build_pipeline_execution_plan(
            pipeline_id=pipeline.id,
            dataset_id=pipeline.dataset_id,
            source_layer=pipeline.source_layer,
            target_layer=pipeline.target_layer,
            engine=pipeline.engine,
            definition_json=pipeline.definition_json,
            dataset_silver_sql=dataset.silver_sql,
            dataset_gold_sql=dataset.gold_sql,
            source_selection=source_selection,
            requested_source_ingestion_job_id=requested_source_ingestion_job_id,
            requested_source_finished_at_gte=requested_source_finished_at_gte,
            requested_source_finished_at_lte=requested_source_finished_at_lte,
            source_ingestion_job_id=source_job.id if source_job is not None else None,
            source_job_status=source_job.status if source_job is not None else None,
            source_finished_at=source_job.finished_at if source_job is not None else None,
            source_object_uri=PipelineService._source_object_uri_for_layer(source_job, pipeline.source_layer),
        )


    @staticmethod
    def _build_pipeline_source_candidates_from_rows(
        session: Session,
        pipeline: PipelineDefinition,
        rows: list[tuple[IngestionJob, datetime]],
        *,
        run_ref_prefix: str | None = None,
    ) -> list[dict]:
        source_layer = DatasetLayer(pipeline.source_layer)
        source_ingestion_job_ids = [job.id for job, _effective_finished_at in rows]
        existing_run_counts = PipelineService._existing_run_counts_by_source_id(
            session,
            pipeline.id,
            source_ingestion_job_ids,
        )
        latest_run_summaries = PipelineService._latest_run_summaries_by_source_id(
            session,
            pipeline.id,
            source_ingestion_job_ids,
        )
        candidates: list[dict] = []
        for job, effective_finished_at_value in rows:
            schema_context = PipelineService.build_pipeline_run_schema_context(
                session,
                pipeline,
                source_effective_finished_at=effective_finished_at_value,
            )
            schema_compatibility_preview = (
                schema_context.get("schema_compatibility_preview")
                if isinstance(schema_context, dict)
                else None
            )
            schema_compatibility_preview_unavailable_reason = (
                schema_context.get("schema_compatibility_preview_unavailable_reason")
                if isinstance(schema_context, dict)
                else None
            )
            candidates.append(
                build_pipeline_source_candidate(
                    ingestion_job_id=job.id,
                    dataset_id=job.dataset_id,
                    source_layer=source_layer,
                    status=job.status,
                    created_at=job.created_at,
                    finished_at=job.finished_at,
                    object_uri=PipelineService._source_object_uri_for_layer(job, source_layer) or "",
                    existing_run_count=existing_run_counts.get(job.id, 0),
                    run_ref_prefix=run_ref_prefix,
                    schema_compatibility_preview=schema_compatibility_preview,
                    schema_compatibility_preview_unavailable_reason=schema_compatibility_preview_unavailable_reason,
                    **latest_run_summaries.get(job.id, {}),
                )
            )
        return candidates

    @staticmethod
    def list_pipeline_source_candidates(
        session: Session,
        pipeline: PipelineDefinition,
        *,
        source_finished_at_gte: datetime | None = None,
        source_finished_at_lte: datetime | None = None,
        run_ref_prefix: str | None = None,
        exclude_existing_runs: bool = False,
        has_existing_run: bool | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        if (
            source_finished_at_gte is not None
            and source_finished_at_lte is not None
            and source_finished_at_gte > source_finished_at_lte
        ):
            raise ValueError("source_finished_at_gte cannot be after source_finished_at_lte.")
        if exclude_existing_runs and has_existing_run is True:
            raise ValueError("exclude_existing_runs cannot be combined with has_existing_run=true.")

        limit = max(1, min(limit, 1000))
        offset = max(0, offset)
        normalized_run_ref_prefix = normalize_optional_run_ref(run_ref_prefix, field_name="run_ref_prefix")
        source_layer = DatasetLayer(pipeline.source_layer)
        source_uri_column = {
            DatasetLayer.RAW: IngestionJob.raw_object_uri,
            DatasetLayer.SILVER: IngestionJob.silver_object_uri,
            DatasetLayer.GOLD: IngestionJob.gold_object_uri,
        }[source_layer]
        effective_finished_at = func.coalesce(IngestionJob.finished_at, IngestionJob.created_at)
        query = select(IngestionJob, effective_finished_at.label("effective_finished_at")).where(
            IngestionJob.dataset_id == pipeline.dataset_id,
            IngestionJob.status == IngestionStatus.SUCCEEDED.value,
            source_uri_column.is_not(None),
        )
        existing_run_exists = (
            select(PipelineRun.id)
            .where(
                PipelineRun.pipeline_id == pipeline.id,
                PipelineRun.ingestion_job_id == IngestionJob.id,
            )
            .exists()
        )
        if exclude_existing_runs or has_existing_run is False:
            query = query.where(~existing_run_exists)
        elif has_existing_run is True:
            query = query.where(existing_run_exists)
        if source_finished_at_gte is not None:
            query = query.where(effective_finished_at >= source_finished_at_gte)
        if source_finished_at_lte is not None:
            query = query.where(effective_finished_at <= source_finished_at_lte)
        query = query.order_by(desc(effective_finished_at), desc(IngestionJob.created_at), desc(IngestionJob.id)).limit(limit).offset(offset)

        rows = session.execute(query).all()
        source_ingestion_job_ids = [job.id for job, _effective_finished_at in rows]
        existing_run_counts = PipelineService._existing_run_counts_by_source_id(
            session,
            pipeline.id,
            source_ingestion_job_ids,
        )
        latest_run_summaries = PipelineService._latest_run_summaries_by_source_id(
            session,
            pipeline.id,
            source_ingestion_job_ids,
        )
        candidates: list[dict] = []
        for job, effective_finished_at_value in rows:
            schema_context = PipelineService.build_pipeline_run_schema_context(
                session,
                pipeline,
                source_effective_finished_at=effective_finished_at_value,
            )
            schema_compatibility_preview = (
                schema_context.get("schema_compatibility_preview")
                if isinstance(schema_context, dict)
                else None
            )
            schema_compatibility_preview_unavailable_reason = (
                schema_context.get("schema_compatibility_preview_unavailable_reason")
                if isinstance(schema_context, dict)
                else None
            )
            candidates.append(
                build_pipeline_source_candidate(
                    ingestion_job_id=job.id,
                    dataset_id=job.dataset_id,
                    source_layer=source_layer,
                    status=job.status,
                    created_at=job.created_at,
                    finished_at=job.finished_at,
                    object_uri=PipelineService._source_object_uri_for_layer(job, source_layer) or "",
                    existing_run_count=existing_run_counts.get(job.id, 0),
                    run_ref_prefix=normalized_run_ref_prefix,
                    schema_compatibility_preview=schema_compatibility_preview,
                    schema_compatibility_preview_unavailable_reason=schema_compatibility_preview_unavailable_reason,
                    **latest_run_summaries.get(job.id, {}),
                )
            )
        return candidates

    @staticmethod
    def list_pipeline_source_candidates_page(
        session: Session,
        pipeline: PipelineDefinition,
        *,
        source_finished_at_gte: datetime | None = None,
        source_finished_at_lte: datetime | None = None,
        run_ref_prefix: str | None = None,
        require_contract_compatible_schema: bool = False,
        exclude_existing_runs: bool = False,
        has_existing_run: bool | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> dict[str, object]:
        if (
            source_finished_at_gte is not None
            and source_finished_at_lte is not None
            and source_finished_at_gte > source_finished_at_lte
        ):
            raise ValueError("source_finished_at_gte cannot be after source_finished_at_lte.")
        if exclude_existing_runs and has_existing_run is True:
            raise ValueError("exclude_existing_runs cannot be combined with has_existing_run=true.")

        limit = max(1, min(limit, 1000))
        normalized_run_ref_prefix = normalize_optional_run_ref(run_ref_prefix, field_name="run_ref_prefix")
        source_layer = DatasetLayer(pipeline.source_layer)
        source_uri_column = {
            DatasetLayer.RAW: IngestionJob.raw_object_uri,
            DatasetLayer.SILVER: IngestionJob.silver_object_uri,
            DatasetLayer.GOLD: IngestionJob.gold_object_uri,
        }[source_layer]
        effective_finished_at = func.coalesce(IngestionJob.finished_at, IngestionJob.created_at)
        query = select(IngestionJob, effective_finished_at.label("effective_finished_at")).where(
            IngestionJob.dataset_id == pipeline.dataset_id,
            IngestionJob.status == IngestionStatus.SUCCEEDED.value,
            source_uri_column.is_not(None),
        )
        existing_run_exists = (
            select(PipelineRun.id)
            .where(
                PipelineRun.pipeline_id == pipeline.id,
                PipelineRun.ingestion_job_id == IngestionJob.id,
            )
            .exists()
        )
        if exclude_existing_runs or has_existing_run is False:
            query = query.where(~existing_run_exists)
        elif has_existing_run is True:
            query = query.where(existing_run_exists)
        if source_finished_at_gte is not None:
            query = query.where(effective_finished_at >= source_finished_at_gte)
        if source_finished_at_lte is not None:
            query = query.where(effective_finished_at <= source_finished_at_lte)
        if cursor is not None:
            assert_pipeline_source_candidate_page_cursor_matches_scope(
                cursor,
                pipeline_id=pipeline.id,
                source_finished_at_gte=source_finished_at_gte,
                source_finished_at_lte=source_finished_at_lte,
                run_ref_prefix=normalized_run_ref_prefix,
                require_contract_compatible_schema=require_contract_compatible_schema,
                exclude_existing_runs=exclude_existing_runs,
                has_existing_run=has_existing_run,
            )
            cursor_effective_finished_at, cursor_created_at, cursor_ingestion_job_id = decode_pipeline_source_candidate_page_cursor_position(cursor)
            if cursor_created_at is None:
                cursor_job = session.get(IngestionJob, cursor_ingestion_job_id)
                if cursor_job is None:
                    raise ValueError("cursor is invalid.")
                cursor_created_at = cursor_job.created_at
            query = query.where(
                (effective_finished_at < cursor_effective_finished_at)
                | (
                    (effective_finished_at == cursor_effective_finished_at)
                    & (IngestionJob.created_at < cursor_created_at)
                )
                | (
                    (effective_finished_at == cursor_effective_finished_at)
                    & (IngestionJob.created_at == cursor_created_at)
                    & (IngestionJob.id < cursor_ingestion_job_id)
                )
            )
        query = query.order_by(desc(effective_finished_at), desc(IngestionJob.created_at), desc(IngestionJob.id)).limit(limit + 1)

        rows = session.execute(query).all()
        page_rows = rows[:limit]
        next_cursor = None
        if len(rows) > limit and page_rows:
            last_job, last_effective_finished_at = page_rows[-1]
            next_cursor = encode_pipeline_source_candidate_page_cursor(
                effective_finished_at=last_effective_finished_at,
                ingestion_job_id=last_job.id,
                created_at=last_job.created_at,
                pipeline_id=pipeline.id,
                source_finished_at_gte=source_finished_at_gte,
                source_finished_at_lte=source_finished_at_lte,
                run_ref_prefix=normalized_run_ref_prefix,
                require_contract_compatible_schema=require_contract_compatible_schema,
                exclude_existing_runs=exclude_existing_runs,
                has_existing_run=has_existing_run,
            )

        return {
            "items": PipelineService._build_pipeline_source_candidates_from_rows(
                session,
                pipeline,
                page_rows,
                run_ref_prefix=normalized_run_ref_prefix,
            ),
            "next_cursor": next_cursor,
        }


    @staticmethod
    def build_pipeline_run_schema_context(
        session: Session,
        pipeline: PipelineDefinition,
        *,
        source_effective_finished_at: datetime | None = None,
        contract_compatibility_required: bool = False,
    ) -> dict | None:
        source_snapshot = DatasetService.latest_schema_snapshot_at_or_before(
            session,
            pipeline.dataset_id,
            pipeline.source_layer,
            source_effective_finished_at,
        )
        target_snapshot = DatasetService.latest_schema_snapshot(session, pipeline.dataset_id, pipeline.target_layer)
        source_schema_snapshot = (
            build_pipeline_schema_snapshot(
                layer=source_snapshot.layer,
                version=source_snapshot.version,
                fingerprint=source_snapshot.fingerprint,
                schema_json=source_snapshot.schema_json,
            )
            if source_snapshot is not None
            else None
        )
        target_schema_snapshot = (
            build_pipeline_schema_snapshot(
                layer=target_snapshot.layer,
                version=target_snapshot.version,
                fingerprint=target_snapshot.fingerprint,
                schema_json=target_snapshot.schema_json,
            )
            if target_snapshot is not None
            else None
        )
        return build_pipeline_schema_context(
            source_schema_snapshot=source_schema_snapshot,
            target_schema_snapshot=target_schema_snapshot,
            schema_compatibility_preview=build_pipeline_schema_compatibility_preview(
                engine=pipeline.engine,
                target_layer=pipeline.target_layer,
                definition_json=pipeline.definition_json,
                source_schema_snapshot=source_schema_snapshot,
                target_schema_snapshot=target_schema_snapshot,
            ),
            schema_compatibility_preview_unavailable_reason=build_pipeline_schema_compatibility_preview_unavailable_reason(
                engine=pipeline.engine,
                target_layer=pipeline.target_layer,
                definition_json=pipeline.definition_json,
                source_schema_snapshot=source_schema_snapshot,
            ),
            contract_compatibility_required=contract_compatibility_required,
        )


    @staticmethod
    def _ensure_contract_compatible_schema(schema_context: dict | None) -> None:
        if not isinstance(schema_context, dict):
            raise ValueError("Schema compatibility preview is unavailable.")

        preview = schema_context.get("schema_compatibility_preview")
        unavailable_reason = schema_context.get("schema_compatibility_preview_unavailable_reason")
        if not isinstance(preview, dict):
            if isinstance(unavailable_reason, str) and unavailable_reason.strip():
                raise ValueError(unavailable_reason.strip())
            raise ValueError("Schema compatibility preview is unavailable.")

        if not bool(preview.get("contract_compatible")):
            raise ValueError("Schema compatibility preview indicates contract-incompatible schema changes.")

    @staticmethod
    def persist_rejected_preflight_attempt(
        session: Session,
        pipeline: PipelineDefinition,
        *,
        execution_plan: dict,
        schema_context: dict | None,
        error_message: str,
        request_kind: str,
        run_ref: str | None = None,
        backfill_request: dict | None = None,
        preflighted_at: datetime | None = None,
    ) -> PipelinePreflightAttempt:
        normalized_error_message = str(error_message).strip() or "Pipeline preflight attempt was rejected."
        normalized_request_kind = str(request_kind).strip().lower()
        if normalized_request_kind not in {"run", "backfill"}:
            raise ValueError("request_kind must be one of: run, backfill.")

        attempt = PipelinePreflightAttempt(
            pipeline_id=pipeline.id,
            dataset_id=pipeline.dataset_id,
            ingestion_job_id=execution_plan.get("source_ingestion_job_id"),
            request_kind=normalized_request_kind,
            run_ref=normalize_optional_run_ref(run_ref),
            metrics_json=build_pipeline_preflight_metrics(
                execution_plan=execution_plan,
                preflighted_at=preflighted_at,
                backfill_request=backfill_request,
                schema_context=schema_context,
            ),
            error_message=normalized_error_message,
        )
        session.add(attempt)
        session.commit()
        session.refresh(attempt)
        return attempt

    @staticmethod
    def _apply_pipeline_preflight_attempt_filters(
        query,
        *,
        pipeline_id: str,
        request_kind: str | None = None,
        run_ref: str | None = None,
        source_ingestion_job_id: str | None = None,
        created_at_gte: datetime | None = None,
        created_at_lte: datetime | None = None,
    ):
        if created_at_gte is not None and created_at_lte is not None and created_at_gte > created_at_lte:
            raise ValueError("created_at_gte cannot be after created_at_lte.")

        query = query.where(PipelinePreflightAttempt.pipeline_id == pipeline_id)

        normalized_request_kind = str(request_kind).strip().lower() if request_kind is not None else None
        if normalized_request_kind is not None:
            if normalized_request_kind not in {"run", "backfill"}:
                raise ValueError("request_kind must be one of: run, backfill.")
            query = query.where(PipelinePreflightAttempt.request_kind == normalized_request_kind)

        normalized_run_ref = normalize_optional_run_ref(run_ref) if run_ref is not None else None
        if normalized_run_ref is not None:
            query = query.where(PipelinePreflightAttempt.run_ref == normalized_run_ref)

        normalized_source_ingestion_job_id = (
            normalize_optional_run_ref(source_ingestion_job_id, field_name="source_ingestion_job_id")
            if source_ingestion_job_id is not None
            else None
        )
        if normalized_source_ingestion_job_id is not None:
            query = query.where(PipelinePreflightAttempt.ingestion_job_id == normalized_source_ingestion_job_id)

        if created_at_gte is not None:
            query = query.where(PipelinePreflightAttempt.created_at >= created_at_gte)
        if created_at_lte is not None:
            query = query.where(PipelinePreflightAttempt.created_at <= created_at_lte)
        return query

    @staticmethod
    def count_pipeline_preflight_attempts(
        session: Session,
        pipeline_id: str,
        request_kind: str | None = None,
        run_ref: str | None = None,
        source_ingestion_job_id: str | None = None,
        created_at_gte: datetime | None = None,
        created_at_lte: datetime | None = None,
    ) -> int:
        query = PipelineService._apply_pipeline_preflight_attempt_filters(
            select(func.count(PipelinePreflightAttempt.id)),
            pipeline_id=pipeline_id,
            request_kind=request_kind,
            run_ref=run_ref,
            source_ingestion_job_id=source_ingestion_job_id,
            created_at_gte=created_at_gte,
            created_at_lte=created_at_lte,
        )
        result = session.scalar(query)
        return int(result or 0)

    @staticmethod
    def list_pipeline_preflight_attempts(
        session: Session,
        pipeline_id: str,
        limit: int = 100,
        offset: int = 0,
        request_kind: str | None = None,
        run_ref: str | None = None,
        source_ingestion_job_id: str | None = None,
        created_at_gte: datetime | None = None,
        created_at_lte: datetime | None = None,
    ) -> list[PipelinePreflightAttempt]:
        limit = max(1, min(limit, 1000))
        offset = max(0, offset)
        query = PipelineService._apply_pipeline_preflight_attempt_filters(
            select(PipelinePreflightAttempt),
            pipeline_id=pipeline_id,
            request_kind=request_kind,
            run_ref=run_ref,
            source_ingestion_job_id=source_ingestion_job_id,
            created_at_gte=created_at_gte,
            created_at_lte=created_at_lte,
        )
        return list(
            session.scalars(
                query.order_by(PipelinePreflightAttempt.created_at.desc(), PipelinePreflightAttempt.id.desc())
                .limit(limit)
                .offset(offset)
            ).all()
        )

    @staticmethod
    def list_pipeline_preflight_attempts_page(
        session: Session,
        pipeline_id: str,
        *,
        request_kind: str | None = None,
        run_ref: str | None = None,
        source_ingestion_job_id: str | None = None,
        created_at_gte: datetime | None = None,
        created_at_lte: datetime | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> dict[str, object]:
        limit = max(1, min(limit, 1000))
        query = PipelineService._apply_pipeline_preflight_attempt_filters(
            select(PipelinePreflightAttempt),
            pipeline_id=pipeline_id,
            request_kind=request_kind,
            run_ref=run_ref,
            source_ingestion_job_id=source_ingestion_job_id,
            created_at_gte=created_at_gte,
            created_at_lte=created_at_lte,
        )
        if cursor is not None:
            assert_pipeline_preflight_attempt_page_cursor_matches_scope(
                cursor,
                pipeline_id=pipeline_id,
                request_kind=request_kind,
                run_ref=run_ref,
                source_ingestion_job_id=source_ingestion_job_id,
                created_at_gte=created_at_gte,
                created_at_lte=created_at_lte,
            )
            cursor_created_at, cursor_preflight_attempt_id = decode_pipeline_preflight_attempt_page_cursor(cursor)
            query = query.where(
                (PipelinePreflightAttempt.created_at < cursor_created_at)
                | (
                    (PipelinePreflightAttempt.created_at == cursor_created_at)
                    & (PipelinePreflightAttempt.id < cursor_preflight_attempt_id)
                )
            )
        query = query.order_by(
            PipelinePreflightAttempt.created_at.desc(),
            PipelinePreflightAttempt.id.desc(),
        ).limit(limit + 1)

        attempts = list(session.scalars(query).all())
        page_items = attempts[:limit]
        next_cursor = None
        if len(attempts) > limit and page_items:
            last_attempt = page_items[-1]
            next_cursor = encode_pipeline_preflight_attempt_page_cursor(
                created_at=last_attempt.created_at,
                preflight_attempt_id=last_attempt.id,
                pipeline_id=pipeline_id,
                request_kind=request_kind,
                run_ref=run_ref,
                source_ingestion_job_id=source_ingestion_job_id,
                created_at_gte=created_at_gte,
                created_at_lte=created_at_lte,
            )
        return {
            "items": page_items,
            "next_cursor": next_cursor,
        }

    @staticmethod
    def _existing_run_counts_by_source_id(
        session: Session,
        pipeline_id: str,
        source_ingestion_job_ids: list[str],
    ) -> dict[str, int]:
        if not source_ingestion_job_ids:
            return {}
        rows = session.execute(
            select(PipelineRun.ingestion_job_id, func.count(PipelineRun.id))
            .where(
                PipelineRun.pipeline_id == pipeline_id,
                PipelineRun.ingestion_job_id.in_(source_ingestion_job_ids),
                PipelineRun.ingestion_job_id.is_not(None),
            )
            .group_by(PipelineRun.ingestion_job_id)
        ).all()
        return {
            source_id: int(run_count)
            for source_id, run_count in rows
            if source_id is not None
        }

    @staticmethod
    def _latest_run_summaries_by_source_id(
        session: Session,
        pipeline_id: str,
        source_ingestion_job_ids: list[str],
    ) -> dict[str, dict[str, object]]:
        if not source_ingestion_job_ids:
            return {}
        rows = session.execute(
            select(
                PipelineRun.ingestion_job_id,
                PipelineRun.id,
                PipelineRun.status,
                PipelineRun.run_ref,
                PipelineRun.created_at,
                PipelineRun.finished_at,
                PipelineRun.error_message,
            )
            .where(
                PipelineRun.pipeline_id == pipeline_id,
                PipelineRun.ingestion_job_id.in_(source_ingestion_job_ids),
                PipelineRun.ingestion_job_id.is_not(None),
            )
            .order_by(PipelineRun.ingestion_job_id.asc(), PipelineRun.created_at.desc(), PipelineRun.id.desc())
        ).all()
        summaries: dict[str, dict[str, object]] = {}
        for source_id, run_id, status, run_ref, created_at, finished_at, error_message in rows:
            if source_id is None or source_id in summaries:
                continue
            summaries[source_id] = {
                "latest_run_id": run_id,
                "latest_run_status": status,
                "latest_run_ref": run_ref,
                "latest_run_created_at": created_at,
                "latest_run_finished_at": finished_at,
                "latest_run_error_message": error_message,
            }
        return summaries

    @staticmethod
    def _existing_run_source_ids(
        session: Session,
        pipeline_id: str,
        source_ingestion_job_ids: list[str],
    ) -> set[str]:
        return set(
            PipelineService._existing_run_counts_by_source_id(
                session,
                pipeline_id,
                source_ingestion_job_ids,
            )
        )

    @staticmethod
    def create_pipeline_run(
        session: Session,
        pipeline: PipelineDefinition,
        payload: CreatePipelineRunRequest,
    ) -> PipelineRun:
        plan = PipelineService.build_execution_plan(
            session,
            pipeline,
            source_ingestion_job_id=payload.source_ingestion_job_id,
            source_finished_at_gte=payload.source_finished_at_gte,
            source_finished_at_lte=payload.source_finished_at_lte,
        )
        source_job = (
            session.get(IngestionJob, plan.get("source_ingestion_job_id"))
            if plan.get("source_ingestion_job_id") is not None
            else None
        )
        source_effective_finished_at = (
            source_job.finished_at or source_job.created_at
            if source_job is not None
            else None
        )
        schema_context = PipelineService.build_pipeline_run_schema_context(
            session,
            pipeline,
            source_effective_finished_at=source_effective_finished_at,
            contract_compatibility_required=payload.require_contract_compatible_schema,
        )
        if payload.require_contract_compatible_schema:
            try:
                PipelineService._ensure_contract_compatible_schema(schema_context)
            except ValueError as exc:
                PipelineService.persist_rejected_preflight_attempt(
                    session,
                    pipeline,
                    execution_plan=plan,
                    schema_context=schema_context,
                    error_message=str(exc),
                    request_kind="run",
                    run_ref=payload.run_ref,
                )
                raise
        run = PipelineRun(
            **build_pipeline_run_payload(
                pipeline_id=pipeline.id,
                dataset_id=pipeline.dataset_id,
                execution_plan=plan,
                run_ref=payload.run_ref,
                schema_context=schema_context,
            )
        )
        session.add(run)
        session.commit()
        session.refresh(run)
        return run

    @staticmethod
    def _materialize_pipeline_backfill_runs(
        session: Session,
        pipeline: PipelineDefinition,
        *,
        candidates: list[dict],
        run_ref_prefix: str | None,
        require_contract_compatible_schema: bool,
        backfill_request: dict,
    ) -> list[PipelineRun]:
        if not candidates:
            return []

        preflighted_at = datetime.now(timezone.utc)
        runs: list[PipelineRun] = []
        for candidate in candidates:
            source_ingestion_job_id = candidate["ingestion_job_id"]
            execution_plan = PipelineService.build_execution_plan(
                session,
                pipeline,
                source_ingestion_job_id=source_ingestion_job_id,
            )
            schema_context = PipelineService.build_pipeline_run_schema_context(
                session,
                pipeline,
                source_effective_finished_at=candidate.get("effective_finished_at"),
                contract_compatibility_required=require_contract_compatible_schema,
            )
            if require_contract_compatible_schema:
                try:
                    PipelineService._ensure_contract_compatible_schema(schema_context)
                except ValueError as exc:
                    PipelineService.persist_rejected_preflight_attempt(
                        session,
                        pipeline,
                        execution_plan=execution_plan,
                        schema_context=schema_context,
                        error_message=str(exc),
                        request_kind="backfill",
                        run_ref=build_backfill_run_ref(run_ref_prefix, source_ingestion_job_id),
                        backfill_request=backfill_request,
                        preflighted_at=preflighted_at,
                    )
                    raise
            run = PipelineRun(
                **build_pipeline_run_payload(
                    pipeline_id=pipeline.id,
                    dataset_id=pipeline.dataset_id,
                    execution_plan=execution_plan,
                    run_ref=build_backfill_run_ref(run_ref_prefix, source_ingestion_job_id),
                    preflighted_at=preflighted_at,
                    backfill_request=backfill_request,
                    schema_context=schema_context,
                )
            )
            runs.append(run)

        session.add_all(runs)
        session.commit()
        for run in runs:
            session.refresh(run)
        return runs

    @staticmethod
    def create_pipeline_backfill_runs(
        session: Session,
        pipeline: PipelineDefinition,
        payload: CreatePipelineBackfillRunsRequest,
    ) -> list[PipelineRun]:
        candidates = PipelineService.list_pipeline_source_candidates(
            session,
            pipeline,
            source_finished_at_gte=payload.source_finished_at_gte,
            source_finished_at_lte=payload.source_finished_at_lte,
            exclude_existing_runs=payload.skip_existing_runs,
            has_existing_run=payload.has_existing_run,
            limit=payload.limit,
            offset=payload.offset,
        )
        if not candidates:
            return []

        backfill_request = build_backfill_request_snapshot(
            source_finished_at_gte=payload.source_finished_at_gte,
            source_finished_at_lte=payload.source_finished_at_lte,
            limit=payload.limit,
            offset=payload.offset,
            run_ref_prefix=payload.run_ref_prefix,
            skip_existing_runs=payload.skip_existing_runs,
            has_existing_run=payload.has_existing_run,
            require_contract_compatible_schema=payload.require_contract_compatible_schema,
        )
        return PipelineService._materialize_pipeline_backfill_runs(
            session,
            pipeline,
            candidates=candidates,
            run_ref_prefix=payload.run_ref_prefix,
            require_contract_compatible_schema=payload.require_contract_compatible_schema,
            backfill_request=backfill_request,
        )

    @staticmethod
    def create_pipeline_backfill_runs_page(
        session: Session,
        pipeline: PipelineDefinition,
        payload: CreatePipelineBackfillRunsPageRequest,
    ) -> dict[str, object]:
        page = PipelineService.list_pipeline_source_candidates_page(
            session,
            pipeline,
            source_finished_at_gte=payload.source_finished_at_gte,
            source_finished_at_lte=payload.source_finished_at_lte,
            run_ref_prefix=payload.run_ref_prefix,
            require_contract_compatible_schema=payload.require_contract_compatible_schema,
            exclude_existing_runs=payload.skip_existing_runs,
            has_existing_run=payload.has_existing_run,
            cursor=payload.cursor,
            limit=payload.limit,
        )
        candidates = page["items"]
        if not candidates:
            return {"items": [], "next_cursor": None}

        backfill_request = build_backfill_request_snapshot(
            source_finished_at_gte=payload.source_finished_at_gte,
            source_finished_at_lte=payload.source_finished_at_lte,
            limit=payload.limit,
            offset=0,
            cursor=payload.cursor,
            run_ref_prefix=payload.run_ref_prefix,
            skip_existing_runs=payload.skip_existing_runs,
            has_existing_run=payload.has_existing_run,
            require_contract_compatible_schema=payload.require_contract_compatible_schema,
        )
        runs = PipelineService._materialize_pipeline_backfill_runs(
            session,
            pipeline,
            candidates=candidates,
            run_ref_prefix=payload.run_ref_prefix,
            require_contract_compatible_schema=payload.require_contract_compatible_schema,
            backfill_request=backfill_request,
        )
        return {
            "items": runs,
            "next_cursor": page.get("next_cursor"),
        }

    @staticmethod
    def claim_next_pipeline_run(session: Session, pipeline: PipelineDefinition) -> PipelineRun | None:
        if pipeline.engine != PipelineEngine.SQL.value:
            raise ValueError("Only sql pipelines can be claimed by the built-in executor.")

        claim_started_at = datetime.now(timezone.utc)
        while True:
            candidate_run_id = session.scalar(
                select(PipelineRun.id)
                .where(
                    PipelineRun.pipeline_id == pipeline.id,
                    PipelineRun.status == PipelineStatus.PLANNED.value,
                )
                .order_by(PipelineRun.created_at.asc(), PipelineRun.id.asc())
                .limit(1)
            )
            if candidate_run_id is None:
                return None

            claimed = session.execute(
                update(PipelineRun)
                .where(
                    PipelineRun.id == candidate_run_id,
                    PipelineRun.status == PipelineStatus.PLANNED.value,
                )
                .values(
                    status=PipelineStatus.PENDING.value,
                    started_at=claim_started_at,
                    finished_at=None,
                    error_message=None,
                    updated_at=claim_started_at,
                )
            )
            if claimed.rowcount:
                session.commit()
                run = session.get(PipelineRun, candidate_run_id)
                if run is None:
                    raise RuntimeError(f"Claimed pipeline run {candidate_run_id!r} disappeared before refresh.")
                return run

            session.rollback()

    @staticmethod
    def transition_pipeline_run(
        session: Session,
        pipeline: PipelineDefinition,
        run: PipelineRun,
        payload: UpdatePipelineRunStatusRequest,
    ) -> PipelineRun:
        if pipeline.engine != PipelineEngine.SQL.value:
            raise ValueError("Only sql pipelines can transition through the built-in executor lifecycle.")

        if run.pipeline_id != pipeline.id:
            raise ValueError(f"Pipeline run {run.id!r} does not belong to pipeline {pipeline.id!r}.")

        next_status = payload.status.value
        allowed_previous_statuses = {
            PipelineStatus.RUNNING.value: {PipelineStatus.PENDING.value},
            PipelineStatus.SUCCEEDED.value: {PipelineStatus.PENDING.value, PipelineStatus.RUNNING.value},
            PipelineStatus.FAILED.value: {PipelineStatus.PENDING.value, PipelineStatus.RUNNING.value},
        }
        if run.status not in allowed_previous_statuses[next_status]:
            raise ValueError(
                f"Pipeline run {run.id!r} in status {run.status!r} cannot transition to {next_status!r}."
            )

        transitioned_at = datetime.now(timezone.utc)
        run.status = next_status
        run.started_at = run.started_at or transitioned_at
        run.updated_at = transitioned_at
        if next_status == PipelineStatus.RUNNING.value:
            run.finished_at = None
            run.error_message = None
        elif next_status == PipelineStatus.SUCCEEDED.value:
            run.finished_at = transitioned_at
            run.error_message = None
        else:
            run.finished_at = transitioned_at
            run.error_message = payload.error_message

        session.add(run)
        session.commit()
        session.refresh(run)
        return run


    @staticmethod
    def get_pipeline_run(session: Session, pipeline_id: str, run_id: str) -> PipelineRun | None:
        run = session.get(PipelineRun, run_id)
        if run is None or run.pipeline_id != pipeline_id:
            return None
        return run

    @staticmethod
    def get_pipeline_preflight_attempt(
        session: Session,
        pipeline_id: str,
        preflight_attempt_id: str,
    ) -> PipelinePreflightAttempt | None:
        attempt = session.get(PipelinePreflightAttempt, preflight_attempt_id)
        if attempt is None or attempt.pipeline_id != pipeline_id:
            return None
        return attempt

    @staticmethod
    def build_pipeline_preflight_attempt_response(attempt: PipelinePreflightAttempt) -> dict:
        response = {
            "id": attempt.id,
            "pipeline_id": attempt.pipeline_id,
            "dataset_id": attempt.dataset_id,
            "ingestion_job_id": attempt.ingestion_job_id,
            "request_kind": attempt.request_kind,
            "run_ref": attempt.run_ref,
            "metrics_json": attempt.metrics_json,
            "error_message": attempt.error_message,
            "created_at": attempt.created_at,
            "updated_at": attempt.updated_at,
        }
        response.update(extract_pipeline_run_snapshot(attempt.metrics_json))
        return response

    @staticmethod
    def build_pipeline_run_response(run: PipelineRun) -> dict:
        response = {
            "id": run.id,
            "pipeline_id": run.pipeline_id,
            "dataset_id": run.dataset_id,
            "ingestion_job_id": run.ingestion_job_id,
            "status": run.status,
            "run_ref": run.run_ref,
            "metrics_json": run.metrics_json,
            "error_message": run.error_message,
            "started_at": run.started_at,
            "finished_at": run.finished_at,
            "created_at": run.created_at,
            "updated_at": run.updated_at,
        }
        response.update(extract_pipeline_run_snapshot(run.metrics_json))
        return response

    @staticmethod
    def build_pipeline_run_detail(run: PipelineRun) -> dict:
        return PipelineService.build_pipeline_run_response(run)

    @staticmethod
    def build_pipeline_run_artifact_manifest(run: PipelineRun) -> dict | None:
        return extract_pipeline_artifact_manifest(
            run.metrics_json,
            run_id=run.id,
            pipeline_id=run.pipeline_id,
            dataset_id=run.dataset_id,
            source_ingestion_job_id=run.ingestion_job_id,
            run_status=run.status,
        )

    @staticmethod
    def _apply_pipeline_run_filters(
        query,
        *,
        pipeline_id: str,
        run_status: PipelineStatus | str | None = None,
        run_ref: str | None = None,
        source_ingestion_job_id: str | None = None,
        created_at_gte: datetime | None = None,
        created_at_lte: datetime | None = None,
    ):
        if created_at_gte is not None and created_at_lte is not None and created_at_gte > created_at_lte:
            raise ValueError("created_at_gte cannot be after created_at_lte.")

        query = query.where(PipelineRun.pipeline_id == pipeline_id)
        if run_status is not None:
            normalized_status = normalize_optional_pipeline_status(run_status)
            query = query.where(PipelineRun.status == normalized_status)

        normalized_run_ref = normalize_optional_run_ref(run_ref) if run_ref is not None else None
        if normalized_run_ref is not None:
            query = query.where(PipelineRun.run_ref == normalized_run_ref)

        normalized_source_ingestion_job_id = (
            normalize_optional_run_ref(source_ingestion_job_id, field_name="source_ingestion_job_id")
            if source_ingestion_job_id is not None
            else None
        )
        if normalized_source_ingestion_job_id is not None:
            query = query.where(PipelineRun.ingestion_job_id == normalized_source_ingestion_job_id)

        if created_at_gte is not None:
            query = query.where(PipelineRun.created_at >= created_at_gte)
        if created_at_lte is not None:
            query = query.where(PipelineRun.created_at <= created_at_lte)
        return query

    @staticmethod
    def count_pipeline_runs(
        session: Session,
        pipeline_id: str,
        run_status: PipelineStatus | str | None = None,
        run_ref: str | None = None,
        source_ingestion_job_id: str | None = None,
        created_at_gte: datetime | None = None,
        created_at_lte: datetime | None = None,
    ) -> int:
        query = PipelineService._apply_pipeline_run_filters(
            select(func.count(PipelineRun.id)),
            pipeline_id=pipeline_id,
            run_status=run_status,
            run_ref=run_ref,
            source_ingestion_job_id=source_ingestion_job_id,
            created_at_gte=created_at_gte,
            created_at_lte=created_at_lte,
        )
        result = session.scalar(query)
        return int(result or 0)

    @staticmethod
    def list_pipeline_runs(
        session: Session,
        pipeline_id: str,
        limit: int = 100,
        offset: int = 0,
        run_status: PipelineStatus | str | None = None,
        run_ref: str | None = None,
        source_ingestion_job_id: str | None = None,
        created_at_gte: datetime | None = None,
        created_at_lte: datetime | None = None,
    ) -> list[PipelineRun]:
        limit = max(1, min(limit, 1000))
        offset = max(0, offset)
        query = PipelineService._apply_pipeline_run_filters(
            select(PipelineRun),
            pipeline_id=pipeline_id,
            run_status=run_status,
            run_ref=run_ref,
            source_ingestion_job_id=source_ingestion_job_id,
            created_at_gte=created_at_gte,
            created_at_lte=created_at_lte,
        )
        return list(
            session.scalars(
                query.order_by(PipelineRun.created_at.desc(), PipelineRun.id.desc())
                .limit(limit)
                .offset(offset)
            ).all()
        )

    @staticmethod
    def list_pipeline_runs_page(
        session: Session,
        pipeline_id: str,
        *,
        run_status: PipelineStatus | str | None = None,
        run_ref: str | None = None,
        source_ingestion_job_id: str | None = None,
        created_at_gte: datetime | None = None,
        created_at_lte: datetime | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> dict[str, object]:
        limit = max(1, min(limit, 1000))
        query = PipelineService._apply_pipeline_run_filters(
            select(PipelineRun),
            pipeline_id=pipeline_id,
            run_status=run_status,
            run_ref=run_ref,
            source_ingestion_job_id=source_ingestion_job_id,
            created_at_gte=created_at_gte,
            created_at_lte=created_at_lte,
        )
        if cursor is not None:
            assert_pipeline_run_page_cursor_matches_scope(
                cursor,
                pipeline_id=pipeline_id,
                run_status=run_status,
                run_ref=run_ref,
                source_ingestion_job_id=source_ingestion_job_id,
                created_at_gte=created_at_gte,
                created_at_lte=created_at_lte,
            )
            cursor_created_at, cursor_run_id = decode_pipeline_run_page_cursor(cursor)
            query = query.where(
                (PipelineRun.created_at < cursor_created_at)
                | ((PipelineRun.created_at == cursor_created_at) & (PipelineRun.id < cursor_run_id))
            )
        query = query.order_by(PipelineRun.created_at.desc(), PipelineRun.id.desc()).limit(limit + 1)

        runs = list(session.scalars(query).all())
        page_items = runs[:limit]
        next_cursor = None
        if len(runs) > limit and page_items:
            last_run = page_items[-1]
            next_cursor = encode_pipeline_run_page_cursor(
                created_at=last_run.created_at,
                run_id=last_run.id,
                pipeline_id=pipeline_id,
                run_status=run_status,
                run_ref=run_ref,
                source_ingestion_job_id=source_ingestion_job_id,
                created_at_gte=created_at_gte,
                created_at_lte=created_at_lte,
            )
        return {
            "items": page_items,
            "next_cursor": next_cursor,
        }
