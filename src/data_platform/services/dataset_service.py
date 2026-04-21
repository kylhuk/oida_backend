from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from data_platform.models.dataset import DataProduct, DataProductVersion, Dataset, SchemaApproval, SchemaSnapshot
from data_platform.models.ingestion import IngestionJob
from data_platform.models.pipeline import PipelineDefinition, QualityCheck, QualityResult
from data_platform.schemas.dataset import (
    ApproveSchemaSnapshotRequest,
    CreateDataProductRequest,
    CreateDatasetRequest,
    DatasetImportRequest,
    DatasetRestoreRequest,
    UpdateDataProductRequest,
    UpdateDatasetRequest,
)
from data_platform.utils.data_product_versions import build_data_product_version_snapshot
from data_platform.utils.pipeline_definitions import normalize_pipeline_definition
from data_platform.utils.schemas import assess_schema_compatibility, diff_schemas, schema_fingerprint
from data_platform.utils.sql import default_gold_table_name


class DatasetService:
    @staticmethod
    def create_dataset(session: Session, payload: CreateDatasetRequest) -> Dataset:
        gold_table_name = payload.gold_table_name or default_gold_table_name(payload.slug)

        dataset = Dataset(
            slug=payload.slug,
            name=payload.name,
            description=payload.description,
            status=payload.status,
            schema_mode=payload.schema_mode.value,
            silver_sql=payload.silver_sql,
            gold_sql=payload.gold_sql,
            partitioning=payload.partitioning,
            serving_config=payload.serving_config,
            tags=payload.tags,
            gold_table_name=gold_table_name,
        )
        session.add(dataset)
        session.flush()

        for rule in payload.quality_rules:
            session.add(
                QualityCheck(
                    dataset_id=dataset.id,
                    name=rule.name,
                    layer=rule.layer.value,
                    severity=rule.severity.value,
                    sql_expression=rule.sql_expression,
                    active=rule.active,
                )
            )

        if payload.auto_create_default_data_product:
            data_product = DataProduct(
                dataset_id=dataset.id,
                slug=payload.slug,
                name=f"{payload.name} Default Product",
                description="Default frontend-facing product for the dataset.",
                table_name=gold_table_name,
                config={},
                is_default=True,
                current_version=1,
            )
            session.add(data_product)
            session.flush()
            DatasetService._record_data_product_version(session, data_product)

        session.commit()
        session.refresh(dataset)
        return dataset

    @staticmethod
    def update_dataset(session: Session, dataset: Dataset, payload: UpdateDatasetRequest) -> Dataset:
        updates = payload.model_dump(exclude_unset=True)
        if "schema_mode" in updates and updates["schema_mode"] is not None:
            updates["schema_mode"] = updates["schema_mode"].value

        for field_name, value in updates.items():
            setattr(dataset, field_name, value)

        session.add(dataset)
        session.commit()
        session.refresh(dataset)
        return dataset

    @staticmethod
    def list_datasets(session: Session, limit: int = 100, offset: int = 0) -> list[Dataset]:
        limit = max(1, min(limit, 1000))
        offset = max(0, offset)
        return list(
            session.scalars(
                select(Dataset).order_by(Dataset.created_at.desc()).limit(limit).offset(offset)
            ).all()
        )

    @staticmethod
    def get_dataset_by_slug(session: Session, slug: str) -> Dataset | None:
        return session.scalar(select(Dataset).where(Dataset.slug == slug))

    @staticmethod
    def _unset_existing_default_data_products(
        session: Session,
        dataset_id: str,
        *,
        exclude_product_id: str | None = None,
    ) -> list[DataProduct]:
        stmt = select(DataProduct).where(DataProduct.dataset_id == dataset_id, DataProduct.is_default.is_(True))
        if exclude_product_id is not None:
            stmt = stmt.where(DataProduct.id != exclude_product_id)
        changed = list(session.scalars(stmt).all())
        for product in changed:
            product.is_default = False
            session.add(product)
        return changed

    @staticmethod
    def _next_data_product_version(session: Session, data_product_id: str) -> int:
        return int(
            session.scalar(
                select(func.max(DataProductVersion.version)).where(DataProductVersion.data_product_id == data_product_id)
            )
            or 0
        ) + 1

    @staticmethod
    def _record_data_product_version(
        session: Session,
        product: DataProduct,
        *,
        snapshot_source: Mapping | None = None,
        version: int | None = None,
        changed_at: datetime | None = None,
    ) -> DataProductVersion:
        version_number = version if version is not None else DatasetService._next_data_product_version(session, product.id)
        snapshot = build_data_product_version_snapshot(snapshot_source or product, version=version_number)
        normalized_changed_at = changed_at or product.updated_at or product.created_at
        version_record = DataProductVersion(
            dataset_id=product.dataset_id,
            data_product_id=product.id,
            version=version_number,
            slug=snapshot["slug"],
            name=snapshot["name"],
            description=snapshot["description"],
            table_name=snapshot["table_name"],
            config=snapshot["config"],
            is_default=bool(snapshot["is_default"]),
            created_at=normalized_changed_at,
            updated_at=normalized_changed_at,
        )
        product.current_version = version_number
        session.add(product)
        session.add(version_record)
        session.flush()
        return version_record

    @staticmethod
    def build_data_product_response(product: DataProduct) -> dict:
        return {
            "id": product.id,
            "dataset_id": product.dataset_id,
            "slug": product.slug,
            "name": product.name,
            "description": product.description,
            "table_name": product.table_name,
            "config": product.config,
            "is_default": product.is_default,
            "current_version": int(product.current_version or 1),
            "created_at": product.created_at,
            "updated_at": product.updated_at,
        }

    @staticmethod
    def build_data_product_version_response(version: DataProductVersion) -> dict:
        return {
            "id": version.id,
            "dataset_id": version.dataset_id,
            "data_product_id": version.data_product_id,
            "version": version.version,
            "slug": version.slug,
            "name": version.name,
            "description": version.description,
            "table_name": version.table_name,
            "config": version.config,
            "is_default": version.is_default,
            "created_at": version.created_at,
            "updated_at": version.updated_at,
        }

    @staticmethod
    def build_data_product_export_response(session: Session, product: DataProduct) -> dict:
        payload = DatasetService.build_data_product_response(product)
        payload["versions"] = [
            DatasetService.build_data_product_version_response(item)
            for item in session.scalars(
                select(DataProductVersion)
                .where(DataProductVersion.data_product_id == product.id)
                .order_by(DataProductVersion.version.asc())
            ).all()
        ]
        return payload

    @staticmethod
    def _create_data_product_record(
        session: Session,
        dataset: Dataset,
        *,
        slug: str,
        name: str,
        description: str | None,
        table_name: str | None,
        config: dict,
        is_default: bool,
        versions: list[Mapping] | None = None,
        current_version: int = 1,
    ) -> DataProduct:
        product = DataProduct(
            dataset_id=dataset.id,
            slug=slug,
            name=name,
            description=description,
            table_name=table_name or dataset.gold_table_name,
            config=config,
            is_default=is_default,
            current_version=1,
        )
        session.add(product)
        session.flush()

        if versions:
            for version_payload in sorted(versions, key=lambda item: int(item["version"])):
                snapshot_source = {
                    "slug": version_payload.get("slug") or slug,
                    "name": version_payload["name"],
                    "description": version_payload.get("description"),
                    "table_name": version_payload.get("table_name") or dataset.gold_table_name,
                    "config": version_payload.get("config", {}),
                    "is_default": version_payload.get("is_default", False),
                }
                DatasetService._record_data_product_version(
                    session,
                    product,
                    snapshot_source=snapshot_source,
                    version=int(version_payload["version"]),
                )
            product.current_version = max(int(current_version), int(product.current_version or 1))
            session.add(product)
            session.flush()
            return product

        DatasetService._record_data_product_version(session, product)
        return product

    @staticmethod
    def create_data_product(session: Session, dataset: Dataset, payload: CreateDataProductRequest) -> DataProduct:
        changed_defaults: list[DataProduct] = []
        if payload.is_default:
            changed_defaults = DatasetService._unset_existing_default_data_products(session, dataset.id)

        product = DatasetService._create_data_product_record(
            session,
            dataset,
            slug=payload.slug,
            name=payload.name,
            description=payload.description,
            table_name=payload.table_name,
            config=payload.config,
            is_default=payload.is_default,
            current_version=1,
        )

        for existing in changed_defaults:
            DatasetService._record_data_product_version(session, existing)

        session.commit()
        session.refresh(product)
        return product

    @staticmethod
    def update_data_product(
        session: Session,
        dataset: Dataset,
        product: DataProduct,
        payload: UpdateDataProductRequest,
    ) -> DataProduct:
        updates = payload.model_dump(exclude_unset=True)
        changed_defaults: list[DataProduct] = []
        current_changed = False

        if updates.get("is_default") is True and not product.is_default:
            changed_defaults = DatasetService._unset_existing_default_data_products(
                session,
                dataset.id,
                exclude_product_id=product.id,
            )

        for field_name, value in updates.items():
            if field_name == "table_name" and value is None:
                value = dataset.gold_table_name
            if getattr(product, field_name) != value:
                setattr(product, field_name, value)
                current_changed = True

        if not current_changed and not changed_defaults:
            return product

        session.add(product)
        session.flush()

        for existing in changed_defaults:
            DatasetService._record_data_product_version(session, existing)
        if current_changed:
            DatasetService._record_data_product_version(session, product)

        session.commit()
        session.refresh(product)
        return product

    @staticmethod
    def list_data_products(session: Session, dataset: Dataset, limit: int = 100, offset: int = 0) -> list[DataProduct]:
        limit = max(1, min(limit, 1000))
        offset = max(0, offset)
        return list(
            session.scalars(
                select(DataProduct)
                .where(DataProduct.dataset_id == dataset.id)
                .order_by(DataProduct.created_at.desc())
                .limit(limit)
                .offset(offset)
            ).all()
        )

    @staticmethod
    def get_data_product_by_slug(session: Session, slug: str) -> DataProduct | None:
        return session.scalar(select(DataProduct).where(DataProduct.slug == slug))

    @staticmethod
    def get_dataset_data_product(session: Session, dataset_id: str, slug: str) -> DataProduct | None:
        return session.scalar(
            select(DataProduct).where(
                DataProduct.dataset_id == dataset_id,
                DataProduct.slug == slug,
            )
        )

    @staticmethod
    def list_data_product_versions(
        session: Session,
        product: DataProduct,
        limit: int = 100,
        offset: int = 0,
    ) -> list[DataProductVersion]:
        limit = max(1, min(limit, 1000))
        offset = max(0, offset)
        return list(
            session.scalars(
                select(DataProductVersion)
                .where(DataProductVersion.data_product_id == product.id)
                .order_by(DataProductVersion.version.desc())
                .limit(limit)
                .offset(offset)
            ).all()
        )

    @staticmethod
    def get_data_product_version(
        session: Session,
        product: DataProduct,
        version: int,
    ) -> DataProductVersion | None:
        return session.scalar(
            select(DataProductVersion).where(
                DataProductVersion.data_product_id == product.id,
                DataProductVersion.version == version,
            )
        )

    @staticmethod
    def latest_schema_snapshot(session: Session, dataset_id: str, layer: str) -> SchemaSnapshot | None:
        return session.scalar(
            select(SchemaSnapshot)
            .options(selectinload(SchemaSnapshot.approval))
            .where(SchemaSnapshot.dataset_id == dataset_id, SchemaSnapshot.layer == layer)
            .order_by(SchemaSnapshot.version.desc())
        )

    @staticmethod
    def latest_schema_snapshot_at_or_before(
        session: Session,
        dataset_id: str,
        layer: str,
        effective_at: datetime | None,
    ) -> SchemaSnapshot | None:
        if effective_at is None:
            return DatasetService.latest_schema_snapshot(session, dataset_id, layer)
        return session.scalar(
            select(SchemaSnapshot)
            .options(selectinload(SchemaSnapshot.approval))
            .where(
                SchemaSnapshot.dataset_id == dataset_id,
                SchemaSnapshot.layer == layer,
                SchemaSnapshot.created_at <= effective_at,
            )
            .order_by(SchemaSnapshot.created_at.desc(), SchemaSnapshot.version.desc())
        )

    @staticmethod
    def get_schema_snapshot_version(
        session: Session,
        dataset_id: str,
        layer: str,
        version: int,
    ) -> SchemaSnapshot | None:
        return session.scalar(
            select(SchemaSnapshot)
            .options(selectinload(SchemaSnapshot.approval))
            .where(
                SchemaSnapshot.dataset_id == dataset_id,
                SchemaSnapshot.layer == layer,
                SchemaSnapshot.version == version,
            )
        )


    @staticmethod
    def list_schema_approvals(
        session: Session,
        dataset_id: str,
        layer: str | None = None,
        approved_by: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[SchemaApproval]:
        limit = max(1, min(limit, 1000))
        offset = max(0, offset)
        stmt = select(SchemaApproval).where(SchemaApproval.dataset_id == dataset_id)
        if layer:
            stmt = stmt.where(SchemaApproval.layer == layer)
        if approved_by:
            stmt = stmt.where(SchemaApproval.approved_by == approved_by.strip())
        stmt = stmt.order_by(SchemaApproval.approved_at.desc(), SchemaApproval.created_at.desc()).limit(limit).offset(offset)
        return list(session.scalars(stmt).all())

    @staticmethod
    def list_pending_schema_snapshots(
        session: Session,
        dataset_id: str,
        layer: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[SchemaSnapshot]:
        limit = max(1, min(limit, 1000))
        offset = max(0, offset)
        stmt = (
            select(SchemaSnapshot)
            .options(selectinload(SchemaSnapshot.approval))
            .outerjoin(SchemaApproval, SchemaApproval.schema_snapshot_id == SchemaSnapshot.id)
            .where(SchemaSnapshot.dataset_id == dataset_id, SchemaApproval.id.is_(None))
        )
        if layer:
            stmt = stmt.where(SchemaSnapshot.layer == layer)
        stmt = stmt.order_by(SchemaSnapshot.created_at.desc(), SchemaSnapshot.version.desc()).limit(limit).offset(offset)
        return list(session.scalars(stmt).all())

    @staticmethod
    def approve_schema_snapshot(
        session: Session,
        dataset: Dataset,
        layer: str,
        version: int,
        payload: ApproveSchemaSnapshotRequest,
    ) -> SchemaApproval:
        normalized_layer = layer.strip().lower()
        if normalized_layer not in {"raw", "silver", "gold"}:
            raise ValueError("layer must be one of: raw, silver, gold.")

        snapshot = DatasetService.get_schema_snapshot_version(session, dataset.id, normalized_layer, version)
        if snapshot is None:
            raise ValueError(f"Schema snapshot version {version} not found for layer '{normalized_layer}'.")
        if snapshot.approval is not None:
            raise ValueError("Schema snapshot is already approved.")

        approval = SchemaApproval(
            dataset_id=dataset.id,
            schema_snapshot_id=snapshot.id,
            layer=snapshot.layer,
            version=snapshot.version,
            approved_by=payload.approved_by,
            note=payload.note,
            approved_at=datetime.now(timezone.utc),
        )
        session.add(approval)
        session.commit()
        session.refresh(approval)
        return approval

    @staticmethod
    def build_schema_diff(
        session: Session,
        dataset: Dataset,
        layer: str,
        from_version: int | None = None,
        to_version: int | None = None,
    ) -> dict:
        normalized_layer = layer.strip().lower()
        if normalized_layer not in {"raw", "silver", "gold"}:
            raise ValueError("layer must be one of: raw, silver, gold.")

        snapshots = list(
            session.scalars(
                select(SchemaSnapshot)
                .where(SchemaSnapshot.dataset_id == dataset.id, SchemaSnapshot.layer == normalized_layer)
                .order_by(SchemaSnapshot.version.asc())
            ).all()
        )
        if not snapshots:
            raise ValueError(f"No schema snapshots found for layer '{normalized_layer}'.")

        to_snapshot = snapshots[-1] if to_version is None else DatasetService.get_schema_snapshot_version(
            session, dataset.id, normalized_layer, to_version
        )
        if to_snapshot is None:
            raise ValueError(
                f"Schema snapshot version {to_version} not found for layer '{normalized_layer}'."
            )

        if from_version is None:
            candidates = [item for item in snapshots if item.version < to_snapshot.version]
            from_snapshot = candidates[-1] if candidates else None
        else:
            if from_version == 0:
                from_snapshot = None
            else:
                from_snapshot = DatasetService.get_schema_snapshot_version(session, dataset.id, normalized_layer, from_version)
                if from_snapshot is None:
                    raise ValueError(
                        f"Schema snapshot version {from_version} not found for layer '{normalized_layer}'."
                    )

        if from_snapshot is not None and from_snapshot.version >= to_snapshot.version:
            raise ValueError("from_version must be lower than to_version.")

        diff = diff_schemas(
            from_snapshot.schema_json if from_snapshot is not None else [],
            to_snapshot.schema_json,
        )
        return {
            "dataset_id": dataset.id,
            "dataset_slug": dataset.slug,
            "layer": normalized_layer,
            "from_version": from_snapshot.version if from_snapshot is not None else 0,
            "to_version": to_snapshot.version,
            "from_fingerprint": from_snapshot.fingerprint if from_snapshot is not None else None,
            "to_fingerprint": to_snapshot.fingerprint,
            "added_columns": diff["added_columns"],
            "removed_columns": diff["removed_columns"],
            "changed_columns": diff["changed_columns"],
            "breaking_changes": bool(diff["breaking_changes"]),
            "has_changes": bool(diff["has_changes"]),
        }

    @staticmethod
    def build_schema_compatibility(
        session: Session,
        dataset: Dataset,
        layer: str,
        candidate_schema: list[dict[str, str]],
        against_version: int | None = None,
    ) -> dict:
        normalized_layer = layer.strip().lower()
        if normalized_layer not in {"raw", "silver", "gold"}:
            raise ValueError("layer must be one of: raw, silver, gold.")

        if against_version == 0:
            baseline_snapshot = None
        elif against_version is None:
            baseline_snapshot = DatasetService.latest_schema_snapshot(session, dataset.id, normalized_layer)
        else:
            baseline_snapshot = DatasetService.get_schema_snapshot_version(
                session,
                dataset.id,
                normalized_layer,
                against_version,
            )
            if baseline_snapshot is None:
                raise ValueError(
                    f"Schema snapshot version {against_version} not found for layer '{normalized_layer}'."
                )

        report = assess_schema_compatibility(
            baseline_snapshot.schema_json if baseline_snapshot is not None else [],
            candidate_schema,
        )
        return {
            "dataset_id": dataset.id,
            "dataset_slug": dataset.slug,
            "layer": normalized_layer,
            "against_version": baseline_snapshot.version if baseline_snapshot is not None else 0,
            "against_fingerprint": baseline_snapshot.fingerprint if baseline_snapshot is not None else None,
            "current_schema": report["current_schema"],
            "candidate_schema": report["candidate_schema"],
            "merged_schema": report["merged_schema"],
            "added_columns": report["added_columns"],
            "removed_columns": report["removed_columns"],
            "changed_columns": report["changed_columns"],
            "breaking_changes": bool(report["breaking_changes"]),
            "has_changes": bool(report["has_changes"]),
            "contract_compatible": bool(report["contract_compatible"]),
            "strict_mode_compatible": bool(report["strict_mode_compatible"]),
        }

    @staticmethod
    def list_schema_snapshots(
        session: Session,
        dataset_id: str,
        layer: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[SchemaSnapshot]:
        limit = max(1, min(limit, 1000))
        offset = max(0, offset)
        stmt = select(SchemaSnapshot).options(selectinload(SchemaSnapshot.approval)).where(SchemaSnapshot.dataset_id == dataset_id)
        if layer:
            stmt = stmt.where(SchemaSnapshot.layer == layer)
        stmt = stmt.order_by(SchemaSnapshot.created_at.desc()).limit(limit).offset(offset)
        return list(session.scalars(stmt).all())

    @staticmethod
    def save_schema_snapshot(
        session: Session,
        dataset: Dataset,
        layer: str,
        schema_json: list[dict[str, str]],
    ) -> SchemaSnapshot:
        fingerprint = schema_fingerprint(schema_json)
        latest_snapshot = DatasetService.latest_schema_snapshot(session, dataset.id, layer)
        if latest_snapshot and latest_snapshot.fingerprint == fingerprint:
            if layer == "raw":
                dataset.latest_raw_schema_fingerprint = fingerprint
            elif layer == "silver":
                dataset.latest_silver_schema_fingerprint = fingerprint
            elif layer == "gold":
                dataset.latest_gold_schema_fingerprint = fingerprint
            session.add(dataset)
            session.commit()
            session.refresh(latest_snapshot)
            return latest_snapshot

        latest_version = session.scalar(
            select(func.max(SchemaSnapshot.version)).where(
                SchemaSnapshot.dataset_id == dataset.id, SchemaSnapshot.layer == layer
            )
        )
        snapshot = SchemaSnapshot(
            dataset_id=dataset.id,
            layer=layer,
            version=(latest_version or 0) + 1,
            fingerprint=fingerprint,
            schema_json=schema_json,
        )
        session.add(snapshot)

        if layer == "raw":
            dataset.latest_raw_schema_fingerprint = snapshot.fingerprint
        elif layer == "silver":
            dataset.latest_silver_schema_fingerprint = snapshot.fingerprint
        elif layer == "gold":
            dataset.latest_gold_schema_fingerprint = snapshot.fingerprint

        session.commit()
        session.refresh(snapshot)
        return snapshot

    @staticmethod
    def get_dataset_stats(session: Session, dataset: Dataset) -> dict:
        ingestion_counts = dict(
            session.execute(
                select(IngestionJob.status, func.count(IngestionJob.id))
                .where(IngestionJob.dataset_id == dataset.id)
                .group_by(IngestionJob.status)
            ).all()
        )
        quality_counts = dict(
            session.execute(
                select(QualityResult.status, func.count(QualityResult.id))
                .where(QualityResult.dataset_id == dataset.id)
                .group_by(QualityResult.status)
            ).all()
        )

        schema_versions = {
            layer: int(
                session.scalar(
                    select(func.max(SchemaSnapshot.version)).where(
                        SchemaSnapshot.dataset_id == dataset.id,
                        SchemaSnapshot.layer == layer,
                    )
                )
                or 0
            )
            for layer in ("raw", "silver", "gold")
        }

        last_ingestion_at = session.scalar(
            select(func.max(IngestionJob.created_at)).where(IngestionJob.dataset_id == dataset.id)
        )
        latest_success_at = session.scalar(
            select(func.max(IngestionJob.finished_at)).where(
                IngestionJob.dataset_id == dataset.id,
                IngestionJob.status == "succeeded",
            )
        )
        data_product_count = int(
            session.scalar(select(func.count(DataProduct.id)).where(DataProduct.dataset_id == dataset.id)) or 0
        )

        return {
            "dataset_slug": dataset.slug,
            "ingestion_status_counts": {key: int(value) for key, value in ingestion_counts.items()},
            "quality_status_counts": {key: int(value) for key, value in quality_counts.items()},
            "schema_versions": schema_versions,
            "data_product_count": data_product_count,
            "last_ingestion_at": last_ingestion_at,
            "latest_success_at": latest_success_at,
        }

    @staticmethod
    def _import_dataset_definition(session: Session, payload: DatasetImportRequest) -> Dataset:
        dataset_payload = payload.dataset
        dataset = Dataset(
            slug=dataset_payload.slug,
            name=dataset_payload.name,
            description=dataset_payload.description,
            status=dataset_payload.status,
            schema_mode=dataset_payload.schema_mode.value,
            silver_sql=dataset_payload.silver_sql,
            gold_sql=dataset_payload.gold_sql,
            partitioning=dataset_payload.partitioning,
            serving_config=dataset_payload.serving_config,
            tags=dataset_payload.tags,
            gold_table_name=dataset_payload.gold_table_name or default_gold_table_name(dataset_payload.slug),
        )
        session.add(dataset)
        session.flush()

        for rule in payload.quality_rules:
            session.add(
                QualityCheck(
                    dataset_id=dataset.id,
                    name=rule.name,
                    layer=rule.layer.value,
                    severity=rule.severity.value,
                    sql_expression=rule.sql_expression,
                    active=rule.active,
                )
            )

        for product in payload.data_products:
            DatasetService._create_data_product_record(
                session,
                dataset,
                slug=product.slug,
                name=product.name,
                description=product.description,
                table_name=product.table_name,
                config=product.config,
                is_default=product.is_default,
                versions=[
                    {
                        "version": item.version,
                        "slug": item.slug,
                        "name": item.name,
                        "description": item.description,
                        "table_name": item.table_name,
                        "config": item.config,
                        "is_default": item.is_default,
                    }
                    for item in product.versions
                ],
                current_version=product.current_version,
            )

        for pipeline in payload.pipelines:
            session.add(
                PipelineDefinition(
                    dataset_id=dataset.id,
                    name=pipeline.name.strip(),
                    source_layer=pipeline.source_layer.value,
                    target_layer=pipeline.target_layer.value,
                    engine=pipeline.engine.value,
                    definition_json=normalize_pipeline_definition(
                        pipeline.engine,
                        pipeline.target_layer,
                        pipeline.definition_json,
                    ),
                    active=pipeline.active,
                )
            )

        latest_fingerprint_by_layer: dict[str, tuple[int, str]] = {}
        for snapshot in sorted(payload.schema_snapshots, key=lambda item: (item.layer.value, item.version)):
            fingerprint = snapshot.fingerprint or schema_fingerprint(snapshot.schema_items)
            snapshot_record = SchemaSnapshot(
                dataset_id=dataset.id,
                layer=snapshot.layer.value,
                version=snapshot.version,
                fingerprint=fingerprint,
                schema_json=snapshot.schema_items,
            )
            session.add(snapshot_record)
            session.flush()
            if snapshot.approval is not None:
                session.add(
                    SchemaApproval(
                        dataset_id=dataset.id,
                        schema_snapshot_id=snapshot_record.id,
                        layer=snapshot_record.layer,
                        version=snapshot_record.version,
                        approved_by=snapshot.approval.approved_by,
                        note=snapshot.approval.note,
                        approved_at=datetime.now(timezone.utc),
                    )
                )
            current_latest = latest_fingerprint_by_layer.get(snapshot.layer.value)
            if current_latest is None or snapshot.version >= current_latest[0]:
                latest_fingerprint_by_layer[snapshot.layer.value] = (snapshot.version, fingerprint)

        dataset.latest_raw_schema_fingerprint = latest_fingerprint_by_layer.get("raw", (0, None))[1]
        dataset.latest_silver_schema_fingerprint = latest_fingerprint_by_layer.get("silver", (0, None))[1]
        dataset.latest_gold_schema_fingerprint = latest_fingerprint_by_layer.get("gold", (0, None))[1]

        session.add(dataset)
        session.flush()
        return dataset

    @staticmethod
    def import_dataset(session: Session, payload: DatasetImportRequest) -> dict:
        dataset = DatasetService._import_dataset_definition(session, payload)
        session.commit()
        session.refresh(dataset)
        return DatasetService.export_dataset(session, dataset, include_schema_snapshots=True)

    @staticmethod
    def export_dataset_backup(session: Session, include_schema_snapshots: bool = True) -> dict:
        datasets = list(
            session.scalars(
                select(Dataset)
                .order_by(Dataset.slug.asc())
            ).all()
        )
        return {
            "datasets": [
                DatasetService.export_dataset(
                    session,
                    dataset,
                    include_schema_snapshots=include_schema_snapshots,
                )
                for dataset in datasets
            ]
        }

    @staticmethod
    def restore_dataset_backup(session: Session, payload: DatasetRestoreRequest) -> dict:
        imported: list[Dataset] = []
        skipped_dataset_slugs: list[str] = []

        for dataset_payload in payload.datasets:
            existing = DatasetService.get_dataset_by_slug(session, dataset_payload.dataset.slug)
            if existing is not None:
                if payload.skip_existing:
                    skipped_dataset_slugs.append(existing.slug)
                    continue
                raise ValueError(f"Dataset '{dataset_payload.dataset.slug}' already exists.")

            imported.append(DatasetService._import_dataset_definition(session, dataset_payload))

        session.commit()
        for dataset in imported:
            session.refresh(dataset)

        return {
            "imported_datasets": [
                DatasetService.export_dataset(session, dataset, include_schema_snapshots=True)
                for dataset in imported
            ],
            "skipped_dataset_slugs": skipped_dataset_slugs,
        }

    @staticmethod
    def export_dataset(session: Session, dataset: Dataset, include_schema_snapshots: bool = False) -> dict:
        quality_rules = list(
            session.scalars(
                select(QualityCheck)
                .where(QualityCheck.dataset_id == dataset.id)
                .order_by(QualityCheck.layer.asc(), QualityCheck.name.asc())
            ).all()
        )
        data_products = [
            DatasetService.build_data_product_export_response(session, product)
            for product in session.scalars(
                select(DataProduct)
                .where(DataProduct.dataset_id == dataset.id)
                .order_by(DataProduct.created_at.asc())
            ).all()
        ]
        pipelines = list(
            session.scalars(
                select(PipelineDefinition)
                .where(PipelineDefinition.dataset_id == dataset.id)
                .order_by(PipelineDefinition.created_at.asc())
            ).all()
        )
        schema_snapshots = (
            list(
                session.scalars(
                    select(SchemaSnapshot)
                    .options(selectinload(SchemaSnapshot.approval))
                    .where(SchemaSnapshot.dataset_id == dataset.id)
                    .order_by(SchemaSnapshot.layer.asc(), SchemaSnapshot.version.asc())
                ).all()
            )
            if include_schema_snapshots
            else []
        )

        return {
            "dataset": dataset,
            "quality_rules": quality_rules,
            "data_products": data_products,
            "pipelines": [
                {
                    "id": pipeline.id,
                    "dataset_id": pipeline.dataset_id,
                    "name": pipeline.name,
                    "source_layer": pipeline.source_layer,
                    "target_layer": pipeline.target_layer,
                    "engine": pipeline.engine,
                    "definition_json": pipeline.definition_json,
                    "active": pipeline.active,
                    "created_at": pipeline.created_at,
                    "updated_at": pipeline.updated_at,
                }
                for pipeline in pipelines
            ],
            "schema_snapshots": schema_snapshots,
        }
