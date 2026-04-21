from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from data_platform.enums import DatasetLayer, PipelineEngine, QualitySeverity, SchemaMode
from data_platform.utils.data_product_versions import (
    data_product_version_matches_current,
    validate_contiguous_data_product_versions,
)
from data_platform.utils.schemas import schema_fingerprint
from data_platform.utils.validation import (
    validate_optional_identifier,
    validate_read_only_sql,
    validate_serving_config,
    validate_slug,
)


class QualityRuleCreate(BaseModel):
    name: str
    layer: DatasetLayer
    severity: QualitySeverity = QualitySeverity.ERROR
    sql_expression: str
    active: bool = True

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("Quality rule name cannot be empty.")
        return stripped

    @field_validator("sql_expression")
    @classmethod
    def validate_sql_expression(cls, value: str) -> str:
        validated = validate_read_only_sql(value, "sql_expression")
        if validated is None:
            raise ValueError("sql_expression cannot be empty.")
        return validated


class UpdateQualityRuleRequest(BaseModel):
    name: str | None = None
    layer: DatasetLayer | None = None
    severity: QualitySeverity | None = None
    sql_expression: str | None = None
    active: bool | None = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            raise ValueError("Quality rule name cannot be empty.")
        return stripped

    @field_validator("sql_expression")
    @classmethod
    def validate_sql_expression(cls, value: str | None) -> str | None:
        return validate_read_only_sql(value, "sql_expression")


class CreateDatasetRequest(BaseModel):
    slug: str
    name: str
    description: str | None = None
    schema_mode: SchemaMode = SchemaMode.EVOLVE
    silver_sql: str | None = None
    gold_sql: str | None = None
    partitioning: dict[str, Any] = Field(default_factory=dict)
    serving_config: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    gold_table_name: str | None = None
    status: str = "active"
    auto_create_default_data_product: bool = True
    quality_rules: list[QualityRuleCreate] = Field(default_factory=list)

    @field_validator("slug")
    @classmethod
    def validate_dataset_slug(cls, value: str) -> str:
        return validate_slug(value, "slug")

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("Dataset name cannot be empty.")
        return stripped

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: str) -> str:
        stripped = value.strip().lower()
        if not stripped:
            raise ValueError("Dataset status cannot be empty.")
        return stripped

    @field_validator("tags")
    @classmethod
    def normalize_tags(cls, value: list[str]) -> list[str]:
        normalized = []
        seen: set[str] = set()
        for item in value:
            tag = item.strip().lower()
            if not tag or tag in seen:
                continue
            seen.add(tag)
            normalized.append(tag)
        return normalized

    @field_validator("silver_sql", "gold_sql")
    @classmethod
    def validate_transform_sql(cls, value: str | None, info) -> str | None:
        return validate_read_only_sql(value, info.field_name)

    @field_validator("gold_table_name")
    @classmethod
    def validate_gold_table_name(cls, value: str | None) -> str | None:
        return validate_optional_identifier(value, "gold_table_name")

    @field_validator("serving_config")
    @classmethod
    def validate_serving_config_field(cls, value: dict[str, Any]) -> dict[str, Any]:
        return validate_serving_config(value)


class UpdateDatasetRequest(BaseModel):
    name: str | None = None
    description: str | None = None
    status: str | None = None
    schema_mode: SchemaMode | None = None
    silver_sql: str | None = None
    gold_sql: str | None = None
    partitioning: dict[str, Any] | None = None
    serving_config: dict[str, Any] | None = None
    tags: list[str] | None = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            raise ValueError("Dataset name cannot be empty.")
        return stripped

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip().lower()
        if not stripped:
            raise ValueError("Dataset status cannot be empty.")
        return stripped

    @field_validator("tags")
    @classmethod
    def normalize_tags(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        normalized = []
        seen: set[str] = set()
        for item in value:
            tag = item.strip().lower()
            if not tag or tag in seen:
                continue
            seen.add(tag)
            normalized.append(tag)
        return normalized

    @field_validator("silver_sql", "gold_sql")
    @classmethod
    def validate_transform_sql(cls, value: str | None, info) -> str | None:
        return validate_read_only_sql(value, info.field_name)

    @field_validator("serving_config")
    @classmethod
    def validate_serving_config_field(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        if value is None:
            return None
        return validate_serving_config(value)


class DatasetResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    slug: str
    name: str
    description: str | None = None
    status: str
    schema_mode: str
    silver_sql: str | None = None
    gold_sql: str | None = None
    partitioning: dict[str, Any]
    serving_config: dict[str, Any]
    tags: list[str]
    gold_table_name: str
    created_at: datetime
    updated_at: datetime


class SchemaDiffColumnChangeResponse(BaseModel):
    name: str
    from_type: str
    to_type: str
    compatible: bool


class SchemaDiffResponse(BaseModel):
    dataset_id: str
    dataset_slug: str
    layer: str
    from_version: int
    to_version: int
    from_fingerprint: str | None = None
    to_fingerprint: str | None = None
    added_columns: list[dict[str, str]] = Field(default_factory=list)
    removed_columns: list[dict[str, str]] = Field(default_factory=list)
    changed_columns: list[SchemaDiffColumnChangeResponse] = Field(default_factory=list)
    breaking_changes: bool
    has_changes: bool


class SchemaCompatibilityRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    layer: DatasetLayer
    schema_items: list[dict[str, str]] = Field(
        alias="schema_json",
        serialization_alias="schema_json",
        default_factory=list,
    )
    against_version: int | None = Field(default=None, ge=0)


class SchemaCompatibilityResponse(BaseModel):
    dataset_id: str
    dataset_slug: str
    layer: str
    against_version: int
    against_fingerprint: str | None = None
    current_schema: list[dict[str, str]] = Field(default_factory=list)
    candidate_schema: list[dict[str, str]] = Field(default_factory=list)
    merged_schema: list[dict[str, str]] = Field(default_factory=list)
    added_columns: list[dict[str, str]] = Field(default_factory=list)
    removed_columns: list[dict[str, str]] = Field(default_factory=list)
    changed_columns: list[SchemaDiffColumnChangeResponse] = Field(default_factory=list)
    breaking_changes: bool
    has_changes: bool
    contract_compatible: bool
    strict_mode_compatible: bool


class CreateDataProductRequest(BaseModel):
    slug: str
    name: str
    description: str | None = None
    table_name: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    is_default: bool = False

    @field_validator("slug")
    @classmethod
    def validate_product_slug(cls, value: str) -> str:
        return validate_slug(value, "slug")

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("Data product name cannot be empty.")
        return stripped

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, value: str | None) -> str | None:
        return validate_optional_identifier(value, "table_name")


class UpdateDataProductRequest(BaseModel):
    name: str | None = None
    description: str | None = None
    table_name: str | None = None
    config: dict[str, Any] | None = None
    is_default: bool | None = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            raise ValueError("Data product name cannot be empty.")
        return stripped

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, value: str | None) -> str | None:
        return validate_optional_identifier(value, "table_name")


class DataProductResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    dataset_id: str
    slug: str
    name: str
    description: str | None = None
    table_name: str
    config: dict[str, Any]
    is_default: bool
    current_version: int = 1
    created_at: datetime
    updated_at: datetime


class DataProductVersionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    dataset_id: str
    data_product_id: str
    version: int
    slug: str
    name: str
    description: str | None = None
    table_name: str
    config: dict[str, Any]
    is_default: bool
    created_at: datetime
    updated_at: datetime


class DataProductVersionedResponse(DataProductResponse):
    versions: list[DataProductVersionResponse] = Field(default_factory=list)


class SchemaApprovalResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    dataset_id: str
    schema_snapshot_id: str
    layer: str
    version: int
    approved_by: str
    note: str | None = None
    approved_at: datetime
    created_at: datetime
    updated_at: datetime


class ApproveSchemaSnapshotRequest(BaseModel):
    approved_by: str
    note: str | None = None

    @field_validator("approved_by")
    @classmethod
    def validate_approved_by(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("approved_by cannot be empty.")
        return stripped

    @field_validator("note")
    @classmethod
    def normalize_note(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        return stripped or None


class SchemaSnapshotResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: str
    dataset_id: str
    layer: str
    version: int
    fingerprint: str
    schema_items: list[dict[str, str]] = Field(alias="schema_json", serialization_alias="schema_json")
    approval: SchemaApprovalResponse | None = None
    created_at: datetime
    updated_at: datetime


class DatasetStatsResponse(BaseModel):
    dataset_slug: str
    ingestion_status_counts: dict[str, int]
    quality_status_counts: dict[str, int]
    schema_versions: dict[str, int]
    data_product_count: int
    last_ingestion_at: datetime | None = None
    latest_success_at: datetime | None = None


class QualityRuleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    dataset_id: str
    name: str
    layer: str
    severity: str
    sql_expression: str
    active: bool
    created_at: datetime
    updated_at: datetime


class QualityResultResponse(BaseModel):
    id: str
    dataset_id: str
    ingestion_job_id: str | None = None
    quality_check_id: str
    quality_check_name: str
    severity: str
    layer: str
    status: str
    observed_value: str | None = None
    details_json: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class QualityTrendBucketResponse(BaseModel):
    bucket_start: datetime
    bucket_end: datetime
    total_results: int
    passed_results: int
    failed_results: int
    status_counts: dict[str, int] = Field(default_factory=dict)
    latest_created_at: datetime | None = None
    pass_rate: float | None = None


class QualityCheckTrendSummaryResponse(BaseModel):
    quality_check_id: str
    quality_check_name: str
    severity: str
    total_results: int
    passed_results: int
    failed_results: int
    status_counts: dict[str, int] = Field(default_factory=dict)
    latest_status: str | None = None
    latest_created_at: datetime | None = None
    pass_rate: float | None = None


class QualityTrendResponse(BaseModel):
    dataset_id: str
    dataset_slug: str
    bucket: str
    ingestion_job_id: str | None = None
    layer: str | None = None
    quality_check_id: str | None = None
    status_filter: str | None = Field(default=None, alias="status")
    created_at_after: datetime | None = None
    created_at_before: datetime | None = None
    series: list[QualityTrendBucketResponse] = Field(default_factory=list)
    quality_checks: list[QualityCheckTrendSummaryResponse] = Field(default_factory=list)


class DatasetExportResponse(BaseModel):
    dataset: DatasetResponse
    quality_rules: list[QualityRuleResponse] = Field(default_factory=list)
    data_products: list[DataProductVersionedResponse] = Field(default_factory=list)
    pipelines: list[dict[str, Any]] = Field(default_factory=list)
    schema_snapshots: list[SchemaSnapshotResponse] = Field(default_factory=list)

class DatasetBackupResponse(BaseModel):
    datasets: list[DatasetExportResponse] = Field(default_factory=list)



class DatasetImportDatasetRequest(BaseModel):
    slug: str
    name: str
    description: str | None = None
    schema_mode: SchemaMode = SchemaMode.EVOLVE
    silver_sql: str | None = None
    gold_sql: str | None = None
    partitioning: dict[str, Any] = Field(default_factory=dict)
    serving_config: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    gold_table_name: str | None = None
    status: str = "active"

    @field_validator("slug")
    @classmethod
    def validate_dataset_slug(cls, value: str) -> str:
        return validate_slug(value, "slug")

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("Dataset name cannot be empty.")
        return stripped

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: str) -> str:
        stripped = value.strip().lower()
        if not stripped:
            raise ValueError("Dataset status cannot be empty.")
        return stripped

    @field_validator("tags")
    @classmethod
    def normalize_tags(cls, value: list[str]) -> list[str]:
        normalized = []
        seen: set[str] = set()
        for item in value:
            tag = item.strip().lower()
            if not tag or tag in seen:
                continue
            seen.add(tag)
            normalized.append(tag)
        return normalized

    @field_validator("silver_sql", "gold_sql")
    @classmethod
    def validate_transform_sql(cls, value: str | None, info) -> str | None:
        return validate_read_only_sql(value, info.field_name)

    @field_validator("gold_table_name")
    @classmethod
    def validate_gold_table_name(cls, value: str | None) -> str | None:
        return validate_optional_identifier(value, "gold_table_name")

    @field_validator("serving_config")
    @classmethod
    def validate_serving_config_field(cls, value: dict[str, Any]) -> dict[str, Any]:
        return validate_serving_config(value)


class DataProductVersionImportRequest(BaseModel):
    version: int = Field(ge=1)
    slug: str | None = None
    name: str
    description: str | None = None
    table_name: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    is_default: bool = False

    @field_validator("slug")
    @classmethod
    def validate_product_slug(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return validate_slug(value, "slug")

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("Data product name cannot be empty.")
        return stripped

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, value: str | None) -> str | None:
        return validate_optional_identifier(value, "table_name")


class DataProductImportRequest(CreateDataProductRequest):
    current_version: int = Field(default=1, ge=1)
    versions: list[DataProductVersionImportRequest] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_versions(self) -> "DataProductImportRequest":
        if not self.versions:
            if self.current_version != 1:
                raise ValueError("current_version greater than 1 requires imported data product versions.")
            return self

        validate_contiguous_data_product_versions(item.version for item in self.versions)
        for version in self.versions:
            if version.slug is not None and version.slug != self.slug:
                raise ValueError("data product version slug must match the imported data product slug.")

        latest = max(self.versions, key=lambda item: item.version)
        if latest.version != self.current_version:
            raise ValueError("current_version must equal the latest imported data product version.")

        current_payload = {
            "slug": self.slug,
            "name": self.name,
            "description": self.description,
            "table_name": self.table_name,
            "config": self.config,
            "is_default": self.is_default,
            "current_version": self.current_version,
        }
        latest_payload = {
            "slug": latest.slug or self.slug,
            "name": latest.name,
            "description": latest.description,
            "table_name": latest.table_name,
            "config": latest.config,
            "is_default": latest.is_default,
            "current_version": latest.version,
        }
        if not data_product_version_matches_current(current_payload, latest_payload):
            raise ValueError("The latest imported data product version must match the imported current data product state.")

        return self


class PipelineDefinitionImportRequest(BaseModel):
    name: str
    source_layer: DatasetLayer
    target_layer: DatasetLayer
    engine: PipelineEngine
    definition_json: dict[str, Any] = Field(default_factory=dict)
    active: bool = True

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("name cannot be empty.")
        return stripped

    @model_validator(mode="after")
    def validate_layers(self) -> "PipelineDefinitionImportRequest":
        if self.source_layer == self.target_layer:
            raise ValueError("source_layer and target_layer must differ.")
        return self


class SchemaApprovalImportRequest(BaseModel):
    approved_by: str
    note: str | None = None

    @field_validator("approved_by")
    @classmethod
    def validate_approved_by(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("approved_by cannot be empty.")
        return stripped

    @field_validator("note")
    @classmethod
    def normalize_note(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        return stripped or None


class SchemaSnapshotImportRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    layer: DatasetLayer
    version: int = Field(ge=1)
    fingerprint: str | None = None
    schema_items: list[dict[str, str]] = Field(alias="schema_json", serialization_alias="schema_json")
    approval: SchemaApprovalImportRequest | None = None

    @model_validator(mode="after")
    def validate_fingerprint(self) -> "SchemaSnapshotImportRequest":
        if self.fingerprint is None:
            return self
        computed = schema_fingerprint(self.schema_items)
        if self.fingerprint != computed:
            raise ValueError("schema_json fingerprint does not match the supplied fingerprint.")
        return self


class DatasetImportRequest(BaseModel):
    dataset: DatasetImportDatasetRequest
    quality_rules: list[QualityRuleCreate] = Field(default_factory=list)
    data_products: list[DataProductImportRequest] = Field(default_factory=list)
    pipelines: list[PipelineDefinitionImportRequest] = Field(default_factory=list)
    schema_snapshots: list[SchemaSnapshotImportRequest] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_configuration(self) -> "DatasetImportRequest":
        default_products = [item for item in self.data_products if item.is_default]
        if len(default_products) > 1:
            raise ValueError("At most one imported data product can be marked as default.")

        default_version_products = [item for item in self.data_products if item.versions and item.versions[-1].is_default]
        if len(default_version_products) > 1:
            raise ValueError("At most one imported data product version history can end in a default product.")

        versions_by_layer: dict[DatasetLayer, list[int]] = {}
        for snapshot in self.schema_snapshots:
            versions_by_layer.setdefault(snapshot.layer, []).append(snapshot.version)
        for layer, versions in versions_by_layer.items():
            ordered = sorted(versions)
            expected = list(range(1, len(ordered) + 1))
            if ordered != expected:
                raise ValueError(
                    f"schema_snapshots for layer '{layer.value}' must use consecutive versions starting at 1."
                )

        return self


class DatasetRestoreRequest(BaseModel):
    datasets: list[DatasetImportRequest] = Field(default_factory=list)
    skip_existing: bool = False

    @model_validator(mode="after")
    def validate_unique_dataset_slugs(self) -> "DatasetRestoreRequest":
        seen: set[str] = set()
        for item in self.datasets:
            slug = item.dataset.slug
            if slug in seen:
                raise ValueError(f"datasets restore bundle contains duplicate dataset slug '{slug}'.")
            seen.add(slug)
        return self


class DatasetRestoreResponse(BaseModel):
    imported_datasets: list[DatasetExportResponse] = Field(default_factory=list)
    skipped_dataset_slugs: list[str] = Field(default_factory=list)
