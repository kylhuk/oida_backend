from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from data_platform.enums import DatasetLayer, PipelineEngine, PipelineStatus
from data_platform.schemas.dataset import SchemaDiffColumnChangeResponse
from data_platform.utils.pipeline_definitions import normalize_optional_run_ref


class CreatePipelineDefinitionRequest(BaseModel):
    name: str
    source_layer: DatasetLayer
    target_layer: DatasetLayer
    engine: PipelineEngine
    definition_json: dict[str, Any] = Field(default_factory=dict)
    active: bool = True

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("name cannot be empty.")
        return value

    @model_validator(mode="after")
    def validate_layers(self) -> "CreatePipelineDefinitionRequest":
        if self.source_layer == self.target_layer:
            raise ValueError("source_layer and target_layer must differ.")
        return self


class UpdatePipelineDefinitionRequest(BaseModel):
    name: str | None = None
    source_layer: DatasetLayer | None = None
    target_layer: DatasetLayer | None = None
    engine: PipelineEngine | None = None
    definition_json: dict[str, Any] | None = None
    active: bool | None = None

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str | None) -> str | None:
        if value is None:
            return value
        value = value.strip()
        if not value:
            raise ValueError("name cannot be empty.")
        return value

    @model_validator(mode="after")
    def validate_layers(self) -> "UpdatePipelineDefinitionRequest":
        if self.source_layer is not None and self.target_layer is not None and self.source_layer == self.target_layer:
            raise ValueError("source_layer and target_layer must differ.")
        return self


class CreatePipelineRunRequest(BaseModel):
    run_ref: str | None = None
    source_ingestion_job_id: str | None = None
    source_finished_at_gte: datetime | None = None
    source_finished_at_lte: datetime | None = None
    require_contract_compatible_schema: bool = False

    @field_validator("run_ref")
    @classmethod
    def normalize_run_ref(cls, value: str | None) -> str | None:
        return normalize_optional_run_ref(value)

    @field_validator("source_ingestion_job_id")
    @classmethod
    def normalize_source_ingestion_job_id(cls, value: str | None) -> str | None:
        return normalize_optional_run_ref(value, field_name="source_ingestion_job_id")

    @model_validator(mode="after")
    def validate_source_selection(self) -> "CreatePipelineRunRequest":
        if self.source_ingestion_job_id is not None and (
            self.source_finished_at_gte is not None or self.source_finished_at_lte is not None
        ):
            raise ValueError(
                "source_ingestion_job_id cannot be combined with source_finished_at_gte or source_finished_at_lte."
            )
        if (
            self.source_finished_at_gte is not None
            and self.source_finished_at_lte is not None
            and self.source_finished_at_gte > self.source_finished_at_lte
        ):
            raise ValueError("source_finished_at_gte cannot be after source_finished_at_lte.")
        return self




class CreatePipelineBackfillRunsRequest(BaseModel):
    run_ref_prefix: str | None = None
    source_finished_at_gte: datetime | None = None
    source_finished_at_lte: datetime | None = None
    skip_existing_runs: bool = False
    has_existing_run: bool | None = None
    require_contract_compatible_schema: bool = False
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)

    @field_validator("run_ref_prefix")
    @classmethod
    def normalize_run_ref_prefix(cls, value: str | None) -> str | None:
        return normalize_optional_run_ref(value, field_name="run_ref_prefix")

    @model_validator(mode="after")
    def validate_time_window(self) -> "CreatePipelineBackfillRunsRequest":
        if self.source_finished_at_gte is None and self.source_finished_at_lte is None:
            raise ValueError("At least one of source_finished_at_gte or source_finished_at_lte is required.")
        if (
            self.source_finished_at_gte is not None
            and self.source_finished_at_lte is not None
            and self.source_finished_at_gte > self.source_finished_at_lte
        ):
            raise ValueError("source_finished_at_gte cannot be after source_finished_at_lte.")
        if self.skip_existing_runs and self.has_existing_run is True:
            raise ValueError("skip_existing_runs cannot be combined with has_existing_run=true.")
        return self


class CreatePipelineBackfillRunsPageRequest(BaseModel):
    run_ref_prefix: str | None = None
    source_finished_at_gte: datetime | None = None
    source_finished_at_lte: datetime | None = None
    skip_existing_runs: bool = False
    has_existing_run: bool | None = None
    require_contract_compatible_schema: bool = False
    cursor: str | None = None
    limit: int = Field(default=100, ge=1, le=1000)

    @field_validator("run_ref_prefix")
    @classmethod
    def normalize_run_ref_prefix(cls, value: str | None) -> str | None:
        return normalize_optional_run_ref(value, field_name="run_ref_prefix")

    @field_validator("cursor")
    @classmethod
    def normalize_cursor(cls, value: str | None) -> str | None:
        return normalize_optional_run_ref(value, field_name="cursor")

    @model_validator(mode="after")
    def validate_time_window(self) -> "CreatePipelineBackfillRunsPageRequest":
        if self.source_finished_at_gte is None and self.source_finished_at_lte is None:
            raise ValueError("At least one of source_finished_at_gte or source_finished_at_lte is required.")
        if (
            self.source_finished_at_gte is not None
            and self.source_finished_at_lte is not None
            and self.source_finished_at_gte > self.source_finished_at_lte
        ):
            raise ValueError("source_finished_at_gte cannot be after source_finished_at_lte.")
        if self.skip_existing_runs and self.has_existing_run is True:
            raise ValueError("skip_existing_runs cannot be combined with has_existing_run=true.")
        return self


class UpdatePipelineRunStatusRequest(BaseModel):
    status: PipelineStatus
    error_message: str | None = None

    @field_validator("error_message")
    @classmethod
    def normalize_error_message(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("error_message cannot be empty.")
        return normalized

    @model_validator(mode="after")
    def validate_transition_payload(self) -> "UpdatePipelineRunStatusRequest":
        if self.status not in {
            PipelineStatus.RUNNING,
            PipelineStatus.SUCCEEDED,
            PipelineStatus.FAILED,
        }:
            raise ValueError("status must be one of: running, succeeded, failed.")
        if self.status == PipelineStatus.FAILED and self.error_message is None:
            raise ValueError("error_message is required when status='failed'.")
        if self.status != PipelineStatus.FAILED and self.error_message is not None:
            raise ValueError("error_message is only allowed when status='failed'.")
        return self


class PipelineDefinitionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    dataset_id: str
    name: str
    source_layer: str
    target_layer: str
    engine: str
    definition_json: dict[str, Any]
    active: bool
    created_at: datetime
    updated_at: datetime


class PipelineBackfillRequestResponse(BaseModel):
    run_ref_prefix: str | None = None
    source_finished_at_gte: datetime | None = None
    source_finished_at_lte: datetime | None = None
    skip_existing_runs: bool = False
    has_existing_run: bool | None = None
    require_contract_compatible_schema: bool = False
    limit: int
    offset: int
    cursor: str | None = None


class PipelineSchemaSnapshotResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    layer: str
    version: int
    fingerprint: str
    schema_items: list[dict[str, str]] = Field(alias="schema_json", serialization_alias="schema_json", default_factory=list)


class PipelineSchemaCompatibilityPreviewResponse(BaseModel):
    layer: str
    against_version: int
    against_fingerprint: str | None = None
    candidate_fingerprint: str
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


class PipelineExecutionDetailsResponse(BaseModel):
    executor: str | None = None
    status: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    task_id: str | None = None
    error_message: str | None = None
    source_object_uri: str | None = None
    target_object_uri: str | None = None
    output_row_count: int | None = None
    output_schema: list[dict[str, str]] = Field(default_factory=list)
    target_schema_version: int | None = None
    target_schema_fingerprint: str | None = None


class PipelineArtifactManifestResponse(BaseModel):
    run_id: str
    pipeline_id: str | None = None
    dataset_id: str | None = None
    source_ingestion_job_id: str | None = None
    run_status: str | None = None
    execution_status: str | None = None
    engine: str | None = None
    source_layer: str | None = None
    target_layer: str | None = None
    executor: str | None = None
    task_id: str | None = None
    source_object_uri: str | None = None
    target_object_uri: str | None = None
    output_row_count: int | None = None
    output_schema: list[dict[str, str]] = Field(default_factory=list)
    target_schema_version: int | None = None
    target_schema_fingerprint: str | None = None
    source_schema_snapshot: PipelineSchemaSnapshotResponse | None = None
    target_schema_snapshot: PipelineSchemaSnapshotResponse | None = None


class PipelineRunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    pipeline_id: str | None = None
    dataset_id: str | None = None
    ingestion_job_id: str | None = None
    status: str
    run_ref: str | None = None
    metrics_json: dict[str, Any]
    error_message: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    created_at: datetime
    updated_at: datetime
    preflighted_at: datetime | None = None
    execution_plan: dict[str, Any] | None = None
    backfill_request: PipelineBackfillRequestResponse | None = None
    execution_details: PipelineExecutionDetailsResponse | None = None
    source_schema_snapshot: PipelineSchemaSnapshotResponse | None = None
    target_schema_snapshot: PipelineSchemaSnapshotResponse | None = None
    schema_compatibility_preview: PipelineSchemaCompatibilityPreviewResponse | None = None
    schema_compatibility_preview_unavailable_reason: str | None = None
    contract_compatibility_required: bool = False
    contract_compatibility_outcome: str | None = None


class PipelinePreflightAttemptResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    pipeline_id: str | None = None
    dataset_id: str | None = None
    ingestion_job_id: str | None = None
    request_kind: str
    run_ref: str | None = None
    metrics_json: dict[str, Any]
    error_message: str
    created_at: datetime
    updated_at: datetime
    preflighted_at: datetime | None = None
    execution_plan: dict[str, Any] | None = None
    backfill_request: PipelineBackfillRequestResponse | None = None
    source_schema_snapshot: PipelineSchemaSnapshotResponse | None = None
    target_schema_snapshot: PipelineSchemaSnapshotResponse | None = None
    schema_compatibility_preview: PipelineSchemaCompatibilityPreviewResponse | None = None
    schema_compatibility_preview_unavailable_reason: str | None = None
    contract_compatibility_required: bool = False
    contract_compatibility_outcome: str | None = None


class PipelinePreflightAttemptPageResponse(BaseModel):
    items: list[PipelinePreflightAttemptResponse] = Field(default_factory=list)
    next_cursor: str | None = None


class PipelineRunDetailResponse(PipelineRunResponse):
    pass


class PipelineRunPageResponse(BaseModel):
    items: list[PipelineRunResponse] = Field(default_factory=list)
    next_cursor: str | None = None


class PipelineRunCountResponse(BaseModel):
    count: int


class PipelinePreflightAttemptCountResponse(BaseModel):
    count: int


class PipelineSourceCandidateCountResponse(BaseModel):
    count: int


class PipelineSourceCandidateResponse(BaseModel):
    ingestion_job_id: str
    dataset_id: str
    source_layer: str
    status: str
    created_at: datetime
    finished_at: datetime | None = None
    effective_finished_at: datetime
    object_uri: str
    existing_run_count: int = 0
    has_existing_run: bool = False
    latest_run_id: str | None = None
    latest_run_status: str | None = None
    latest_run_ref: str | None = None
    latest_run_created_at: datetime | None = None
    latest_run_finished_at: datetime | None = None
    latest_run_error_message: str | None = None
    suggested_run_ref: str | None = None
    would_skip_with_skip_existing_runs: bool = False
    schema_compatibility_preview: PipelineSchemaCompatibilityPreviewResponse | None = None
    schema_compatibility_preview_unavailable_reason: str | None = None
    would_fail_require_contract_compatible_schema: bool = False


class PipelineSourceCandidatePageResponse(BaseModel):
    items: list[PipelineSourceCandidateResponse] = Field(default_factory=list)
    next_cursor: str | None = None


class PipelineBackfillRunsPageResponse(BaseModel):
    items: list[PipelineRunResponse] = Field(default_factory=list)
    next_cursor: str | None = None


class PipelineExecutionPlanResponse(BaseModel):
    pipeline_id: str
    dataset_id: str
    engine: str
    source_layer: str
    target_layer: str
    definition_json: dict[str, Any]
    resolved_query: str | None = None
    source_selection: str
    requested_source_ingestion_job_id: str | None = None
    requested_source_finished_at_gte: datetime | None = None
    requested_source_finished_at_lte: datetime | None = None
    source_ingestion_job_id: str | None = None
    source_job_status: str | None = None
    source_finished_at: datetime | None = None
    source_object_uri: str | None = None
    executable: bool
    issues: list[str] = Field(default_factory=list)
