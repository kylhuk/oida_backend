from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from data_platform.enums import DatasetLayer, PipelineEngine
from data_platform.models.dataset import DataProduct, Dataset
from data_platform.models.ingestion import IngestionJob
from data_platform.models.pipeline import PipelineDefinition, PipelineRun
from data_platform.utils.catalog import build_catalog_search_pattern, normalize_catalog_search_query


class CatalogService:
    @staticmethod
    def _normalize_exact_filter(value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @staticmethod
    def _normalize_enum_filter(value: Any) -> str | None:
        if value is None:
            return None
        normalized = getattr(value, "value", value)
        if normalized is None:
            return None
        normalized = str(normalized).strip()
        return normalized or None

    @staticmethod
    def _apply_dataset_catalog_filters(stmt: Any, query: str | None, status: str | None, tag: str | None) -> Any:
        pattern = build_catalog_search_pattern(query)
        if pattern is not None:
            stmt = stmt.where(
                or_(
                    Dataset.slug.ilike(pattern, escape="\\"),
                    Dataset.name.ilike(pattern, escape="\\"),
                    Dataset.description.ilike(pattern, escape="\\"),
                    Dataset.gold_table_name.ilike(pattern, escape="\\"),
                    sa.cast(Dataset.tags, sa.Text).ilike(pattern),
                )
            )
        normalized_status = CatalogService._normalize_exact_filter(status)
        if normalized_status is not None:
            stmt = stmt.where(Dataset.status == normalized_status)
        normalized_tag = CatalogService._normalize_exact_filter(tag)
        if normalized_tag is not None:
            stmt = stmt.where(sa.cast(Dataset.tags, sa.Text).ilike(f'%"{normalized_tag}"%'))
        return stmt

    @staticmethod
    def list_catalog_datasets(
        session: Session,
        query: str | None = None,
        *,
        status: str | None = None,
        tag: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(limit, 1000))
        offset = max(0, offset)
        data_product_counts = (
            select(DataProduct.dataset_id.label("dataset_id"), func.count(DataProduct.id).label("data_product_count"))
            .group_by(DataProduct.dataset_id)
            .subquery()
        )
        pipeline_counts = (
            select(PipelineDefinition.dataset_id.label("dataset_id"), func.count(PipelineDefinition.id).label("pipeline_count"))
            .group_by(PipelineDefinition.dataset_id)
            .subquery()
        )
        ingestion_latest = (
            select(
                IngestionJob.dataset_id.label("dataset_id"),
                func.max(IngestionJob.created_at).label("latest_ingestion_created_at"),
            )
            .group_by(IngestionJob.dataset_id)
            .subquery()
        )
        stmt = (
            select(
                Dataset.id,
                Dataset.slug,
                Dataset.name,
                Dataset.description,
                Dataset.status,
                Dataset.schema_mode,
                Dataset.gold_table_name,
                Dataset.tags,
                Dataset.latest_raw_schema_fingerprint,
                Dataset.latest_silver_schema_fingerprint,
                Dataset.latest_gold_schema_fingerprint,
                ingestion_latest.c.latest_ingestion_created_at,
                func.coalesce(pipeline_counts.c.pipeline_count, 0).label("pipeline_count"),
                func.coalesce(data_product_counts.c.data_product_count, 0).label("data_product_count"),
                Dataset.created_at,
                Dataset.updated_at,
            )
            .outerjoin(data_product_counts, data_product_counts.c.dataset_id == Dataset.id)
            .outerjoin(pipeline_counts, pipeline_counts.c.dataset_id == Dataset.id)
            .outerjoin(ingestion_latest, ingestion_latest.c.dataset_id == Dataset.id)
        )
        stmt = CatalogService._apply_dataset_catalog_filters(stmt, query, status, tag)
        stmt = stmt.order_by(Dataset.updated_at.desc(), Dataset.id.desc()).limit(limit).offset(offset)
        return [dict(row) for row in session.execute(stmt).mappings().all()]

    @staticmethod
    def count_catalog_datasets(
        session: Session,
        query: str | None = None,
        *,
        status: str | None = None,
        tag: str | None = None,
    ) -> int:
        stmt = select(func.count(Dataset.id))
        stmt = CatalogService._apply_dataset_catalog_filters(stmt, query, status, tag)
        return int(session.scalar(stmt) or 0)

    @staticmethod
    def _apply_pipeline_catalog_filters(
        stmt: Any,
        query: str | None,
        dataset_slug: str | None,
        active: bool | None,
        engine: PipelineEngine | None,
        source_layer: DatasetLayer | None,
        target_layer: DatasetLayer | None,
    ) -> Any:
        pattern = build_catalog_search_pattern(query)
        if pattern is not None:
            stmt = stmt.where(
                or_(
                    PipelineDefinition.name.ilike(pattern, escape="\\"),
                    Dataset.slug.ilike(pattern, escape="\\"),
                    Dataset.name.ilike(pattern, escape="\\"),
                )
            )
        normalized_dataset_slug = CatalogService._normalize_exact_filter(dataset_slug)
        if normalized_dataset_slug is not None:
            stmt = stmt.where(Dataset.slug == normalized_dataset_slug)
        if active is not None:
            stmt = stmt.where(PipelineDefinition.active == active)
        normalized_engine = CatalogService._normalize_enum_filter(engine)
        if normalized_engine is not None:
            stmt = stmt.where(PipelineDefinition.engine == normalized_engine)
        normalized_source_layer = CatalogService._normalize_enum_filter(source_layer)
        if normalized_source_layer is not None:
            stmt = stmt.where(PipelineDefinition.source_layer == normalized_source_layer)
        normalized_target_layer = CatalogService._normalize_enum_filter(target_layer)
        if normalized_target_layer is not None:
            stmt = stmt.where(PipelineDefinition.target_layer == normalized_target_layer)
        return stmt

    @staticmethod
    def list_catalog_pipelines(
        session: Session,
        query: str | None = None,
        *,
        dataset_slug: str | None = None,
        active: bool | None = None,
        engine: PipelineEngine | None = None,
        source_layer: DatasetLayer | None = None,
        target_layer: DatasetLayer | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(limit, 1000))
        offset = max(0, offset)
        run_counts = (
            select(PipelineRun.pipeline_id.label("pipeline_id"), func.count(PipelineRun.id).label("run_count"))
            .group_by(PipelineRun.pipeline_id)
            .subquery()
        )
        latest_run_id = (
            select(PipelineRun.id)
            .where(PipelineRun.pipeline_id == PipelineDefinition.id)
            .order_by(PipelineRun.created_at.desc(), PipelineRun.id.desc())
            .limit(1)
            .scalar_subquery()
        )
        latest_run_status = (
            select(PipelineRun.status)
            .where(PipelineRun.pipeline_id == PipelineDefinition.id)
            .order_by(PipelineRun.created_at.desc(), PipelineRun.id.desc())
            .limit(1)
            .scalar_subquery()
        )
        latest_run_ref = (
            select(PipelineRun.run_ref)
            .where(PipelineRun.pipeline_id == PipelineDefinition.id)
            .order_by(PipelineRun.created_at.desc(), PipelineRun.id.desc())
            .limit(1)
            .scalar_subquery()
        )
        latest_run_created_at = (
            select(PipelineRun.created_at)
            .where(PipelineRun.pipeline_id == PipelineDefinition.id)
            .order_by(PipelineRun.created_at.desc(), PipelineRun.id.desc())
            .limit(1)
            .scalar_subquery()
        )
        latest_run_finished_at = (
            select(PipelineRun.finished_at)
            .where(PipelineRun.pipeline_id == PipelineDefinition.id)
            .order_by(PipelineRun.created_at.desc(), PipelineRun.id.desc())
            .limit(1)
            .scalar_subquery()
        )
        latest_run_error_message = (
            select(PipelineRun.error_message)
            .where(PipelineRun.pipeline_id == PipelineDefinition.id)
            .order_by(PipelineRun.created_at.desc(), PipelineRun.id.desc())
            .limit(1)
            .scalar_subquery()
        )
        stmt = (
            select(
                PipelineDefinition.id,
                PipelineDefinition.dataset_id,
                Dataset.slug.label("dataset_slug"),
                Dataset.name.label("dataset_name"),
                PipelineDefinition.name,
                PipelineDefinition.source_layer,
                PipelineDefinition.target_layer,
                PipelineDefinition.engine,
                PipelineDefinition.active,
                func.coalesce(run_counts.c.run_count, 0).label("run_count"),
                latest_run_id.label("latest_run_id"),
                latest_run_status.label("latest_run_status"),
                latest_run_ref.label("latest_run_ref"),
                latest_run_created_at.label("latest_run_created_at"),
                latest_run_finished_at.label("latest_run_finished_at"),
                latest_run_error_message.label("latest_run_error_message"),
                PipelineDefinition.created_at,
                PipelineDefinition.updated_at,
            )
            .join(Dataset, PipelineDefinition.dataset_id == Dataset.id)
            .outerjoin(run_counts, run_counts.c.pipeline_id == PipelineDefinition.id)
        )
        stmt = CatalogService._apply_pipeline_catalog_filters(
            stmt,
            query,
            dataset_slug,
            active,
            engine,
            source_layer,
            target_layer,
        )
        stmt = stmt.order_by(PipelineDefinition.updated_at.desc(), PipelineDefinition.id.desc()).limit(limit).offset(offset)
        return [dict(row) for row in session.execute(stmt).mappings().all()]

    @staticmethod
    def count_catalog_pipelines(
        session: Session,
        query: str | None = None,
        *,
        dataset_slug: str | None = None,
        active: bool | None = None,
        engine: PipelineEngine | None = None,
        source_layer: DatasetLayer | None = None,
        target_layer: DatasetLayer | None = None,
    ) -> int:
        stmt = select(func.count(PipelineDefinition.id)).select_from(PipelineDefinition).join(
            Dataset, PipelineDefinition.dataset_id == Dataset.id
        )
        stmt = CatalogService._apply_pipeline_catalog_filters(
            stmt,
            query,
            dataset_slug,
            active,
            engine,
            source_layer,
            target_layer,
        )
        return int(session.scalar(stmt) or 0)

    @staticmethod
    def search_catalog(
        session: Session,
        query: str | None = None,
        *,
        status: str | None = None,
        tag: str | None = None,
        dataset_limit: int = 20,
        dataset_offset: int = 0,
        dataset_slug: str | None = None,
        active: bool | None = None,
        engine: PipelineEngine | None = None,
        source_layer: DatasetLayer | None = None,
        target_layer: DatasetLayer | None = None,
        pipeline_limit: int = 20,
        pipeline_offset: int = 0,
    ) -> dict[str, Any]:
        normalized_query = normalize_catalog_search_query(query)
        datasets = CatalogService.list_catalog_datasets(
            session,
            normalized_query,
            status=status,
            tag=tag,
            limit=dataset_limit,
            offset=dataset_offset,
        )
        pipelines = CatalogService.list_catalog_pipelines(
            session,
            normalized_query,
            dataset_slug=dataset_slug,
            active=active,
            engine=engine,
            source_layer=source_layer,
            target_layer=target_layer,
            limit=pipeline_limit,
            offset=pipeline_offset,
        )
        return {
            "query": normalized_query,
            "dataset_count": CatalogService.count_catalog_datasets(
                session,
                normalized_query,
                status=status,
                tag=tag,
            ),
            "pipeline_count": CatalogService.count_catalog_pipelines(
                session,
                normalized_query,
                dataset_slug=dataset_slug,
                active=active,
                engine=engine,
                source_layer=source_layer,
                target_layer=target_layer,
            ),
            "datasets": datasets,
            "pipelines": pipelines,
        }
