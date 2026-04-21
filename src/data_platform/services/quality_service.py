from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from data_platform.enums import QualitySeverity
from data_platform.models.dataset import Dataset
from data_platform.models.pipeline import QualityCheck, QualityResult
from data_platform.schemas.dataset import QualityRuleCreate, UpdateQualityRuleRequest
from data_platform.services.duckdb_service import DuckDBService
from data_platform.utils.quality_trends import build_quality_result_trend_report, normalize_quality_trend_bucket


class QualityService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.duckdb = DuckDBService()

    @staticmethod
    def _coerce_passed_value(value: object, check: QualityCheck, layer: str) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and value in {0, 1}:
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "t", "1", "yes", "y"}:
                return True
            if normalized in {"false", "f", "0", "no", "n"}:
                return False

        raise ValueError(
            f"Quality check 'passed' column must be boolean-like on layer={layer!r}: {check.name}."
        )

    @classmethod
    def _coerce_check_result(cls, check: QualityCheck, layer: str, rows: list[dict]) -> tuple[bool, object | None, dict]:
        if not rows:
            raise ValueError(
                f"Quality check must return at least one row with a 'passed' column on layer={layer!r}: {check.name}."
            )

        first = rows[0]
        if "passed" not in first:
            raise ValueError(
                f"Quality check must return a 'passed' column on layer={layer!r}: {check.name}."
            )

        passed = cls._coerce_passed_value(first["passed"], check, layer)
        observed_value = first.get("observed_value")
        details = {key: value for key, value in first.items() if key not in {"passed", "observed_value"}}
        return passed, observed_value, details

    def create_rule(self, dataset: Dataset, payload: QualityRuleCreate) -> QualityCheck:
        rule = QualityCheck(
            dataset_id=dataset.id,
            name=payload.name,
            layer=payload.layer.value,
            severity=payload.severity.value,
            sql_expression=payload.sql_expression,
            active=payload.active,
        )
        self.session.add(rule)
        self.session.commit()
        self.session.refresh(rule)
        return rule

    def update_rule(self, rule: QualityCheck, payload: UpdateQualityRuleRequest) -> QualityCheck:
        updates = payload.model_dump(exclude_unset=True)
        if "layer" in updates and updates["layer"] is not None:
            updates["layer"] = updates["layer"].value
        if "severity" in updates and updates["severity"] is not None:
            updates["severity"] = updates["severity"].value

        for field_name, value in updates.items():
            setattr(rule, field_name, value)

        self.session.add(rule)
        self.session.commit()
        self.session.refresh(rule)
        return rule

    @staticmethod
    def list_rules(
        session: Session,
        dataset_id: str,
        layer: str | None = None,
        active: bool | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[QualityCheck]:
        limit = max(1, min(limit, 1000))
        offset = max(0, offset)

        stmt = select(QualityCheck).where(QualityCheck.dataset_id == dataset_id)
        if layer:
            stmt = stmt.where(QualityCheck.layer == layer)
        if active is not None:
            stmt = stmt.where(QualityCheck.active.is_(active))

        stmt = stmt.order_by(QualityCheck.created_at.desc()).limit(limit).offset(offset)
        return list(session.scalars(stmt).all())

    @staticmethod
    def get_rule(session: Session, dataset_id: str, rule_id: str) -> QualityCheck | None:
        return session.scalar(
            select(QualityCheck).where(
                QualityCheck.dataset_id == dataset_id,
                QualityCheck.id == rule_id,
            )
        )

    def run_checks(
        self,
        dataset_id: str,
        ingestion_job_id: str,
        layer: str,
        source_query: str,
    ) -> list[QualityResult]:
        checks = list(
            self.session.scalars(
                select(QualityCheck).where(
                    QualityCheck.dataset_id == dataset_id,
                    QualityCheck.layer == layer,
                    QualityCheck.active.is_(True),
                )
            ).all()
        )

        if not checks:
            return []

        created_results: list[QualityResult] = []

        for check in checks:
            rows = self.duckdb.execute_records(check.sql_expression, views={"source": source_query})
            try:
                passed, observed_value, details = self._coerce_check_result(check, layer, rows)
            except ValueError:
                # Persist any earlier completed results before surfacing the malformed rule.
                self.session.commit()
                raise

            result = QualityResult(
                dataset_id=dataset_id,
                ingestion_job_id=ingestion_job_id,
                quality_check_id=check.id,
                layer=layer,
                status="passed" if passed else "failed",
                observed_value=None if observed_value is None else str(observed_value),
                details_json=details,
            )
            self.session.add(result)
            self.session.flush()
            created_results.append(result)

            if not passed and check.severity == QualitySeverity.ERROR.value:
                self.session.commit()
                raise ValueError(
                    f"Quality check failed on layer={layer!r}: {check.name}. "
                    f"Observed={observed_value!r}"
                )

        self.session.commit()
        return created_results

    @staticmethod
    def list_results(
        session: Session,
        dataset_id: str,
        ingestion_job_id: str | None = None,
        layer: str | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        limit = max(1, min(limit, 1000))
        offset = max(0, offset)

        stmt = (
            select(QualityResult, QualityCheck.name, QualityCheck.severity)
            .join(QualityCheck, QualityResult.quality_check_id == QualityCheck.id)
            .where(QualityResult.dataset_id == dataset_id)
        )

        if ingestion_job_id:
            stmt = stmt.where(QualityResult.ingestion_job_id == ingestion_job_id)
        if layer:
            stmt = stmt.where(QualityResult.layer == layer)
        if status:
            stmt = stmt.where(QualityResult.status == status)

        stmt = stmt.order_by(QualityResult.created_at.desc()).limit(limit).offset(offset)
        rows = session.execute(stmt).all()

        results: list[dict] = []
        for quality_result, quality_check_name, severity in rows:
            results.append(
                {
                    "id": quality_result.id,
                    "dataset_id": quality_result.dataset_id,
                    "ingestion_job_id": quality_result.ingestion_job_id,
                    "quality_check_id": quality_result.quality_check_id,
                    "quality_check_name": quality_check_name,
                    "severity": severity,
                    "layer": quality_result.layer,
                    "status": quality_result.status,
                    "observed_value": quality_result.observed_value,
                    "details_json": quality_result.details_json,
                    "created_at": quality_result.created_at,
                    "updated_at": quality_result.updated_at,
                }
            )
        return results

    @staticmethod
    def get_result_trends(
        session: Session,
        dataset_id: str,
        *,
        ingestion_job_id: str | None = None,
        layer: str | None = None,
        status: str | None = None,
        quality_check_id: str | None = None,
        created_at_after: datetime | None = None,
        created_at_before: datetime | None = None,
        bucket: str = "day",
        limit: int = 30,
    ) -> dict[str, list[dict]]:
        bounded_limit = max(1, min(limit, 1000))
        normalized_bucket = normalize_quality_trend_bucket(bucket)

        stmt = (
            select(
                QualityResult.created_at,
                QualityResult.status,
                QualityResult.quality_check_id,
                QualityCheck.name,
                QualityCheck.severity,
            )
            .join(QualityCheck, QualityResult.quality_check_id == QualityCheck.id)
            .where(QualityResult.dataset_id == dataset_id)
        )

        if ingestion_job_id:
            stmt = stmt.where(QualityResult.ingestion_job_id == ingestion_job_id)
        if layer:
            stmt = stmt.where(QualityResult.layer == layer)
        if status:
            stmt = stmt.where(QualityResult.status == status)
        if quality_check_id:
            stmt = stmt.where(QualityResult.quality_check_id == quality_check_id)
        if created_at_after is not None:
            stmt = stmt.where(QualityResult.created_at >= created_at_after)
        if created_at_before is not None:
            stmt = stmt.where(QualityResult.created_at <= created_at_before)

        stmt = stmt.order_by(QualityResult.created_at.desc(), QualityResult.id.desc())
        rows = session.execute(stmt).all()
        return build_quality_result_trend_report(
            [
                {
                    "created_at": created_at,
                    "status": result_status,
                    "quality_check_id": result_quality_check_id,
                    "quality_check_name": quality_check_name,
                    "severity": severity,
                }
                for created_at, result_status, result_quality_check_id, quality_check_name, severity in rows
            ],
            bucket=normalized_bucket,
            limit=bounded_limit,
        )
