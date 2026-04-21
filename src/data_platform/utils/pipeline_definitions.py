from __future__ import annotations

import base64
import binascii
import json
from datetime import datetime, timezone
from typing import Any

from data_platform.enums import DatasetLayer, PipelineEngine, PipelineStatus
from data_platform.utils.schemas import assess_schema_compatibility, schema_fingerprint
from data_platform.utils.validation import validate_read_only_sql


_SQL_PIPELINE_MODE_DATASET_TRANSFORM = "dataset_transform"
_SQL_PIPELINE_MODE_CUSTOM_SQL = "custom_sql"
_ALLOWED_SQL_PIPELINE_KEYS = frozenset({"mode", "sql"})


def _coerce_utc_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _coerce_pipeline_engine(engine: PipelineEngine | str) -> PipelineEngine:
    if isinstance(engine, PipelineEngine):
        return engine
    return PipelineEngine(str(engine).strip().lower())


def _coerce_dataset_layer(layer: DatasetLayer | str) -> DatasetLayer:
    if isinstance(layer, DatasetLayer):
        return layer
    return DatasetLayer(str(layer).strip().lower())


def normalize_optional_run_ref(value: str | None, *, field_name: str = "run_ref") -> str | None:
    if value is None:
        return None

    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} cannot be empty.")
    return normalized


def build_backfill_run_ref(run_ref_prefix: str | None, source_ingestion_job_id: str) -> str | None:
    normalized_prefix = normalize_optional_run_ref(run_ref_prefix, field_name="run_ref_prefix") if run_ref_prefix is not None else None
    normalized_source_id = normalize_optional_run_ref(source_ingestion_job_id, field_name="source_ingestion_job_id")
    if normalized_prefix is None:
        return None
    return f"{normalized_prefix}:{normalized_source_id}"


def _encode_optional_cursor_datetime(value: datetime | None) -> str | None:
    normalized_value = _coerce_utc_datetime(value)
    if normalized_value is None:
        return None
    return normalized_value.isoformat()



def _build_pipeline_source_candidate_page_cursor_scope(
    *,
    pipeline_id: str,
    source_finished_at_gte: datetime | None = None,
    source_finished_at_lte: datetime | None = None,
    run_ref_prefix: str | None = None,
    require_contract_compatible_schema: bool = False,
    exclude_existing_runs: bool = False,
    has_existing_run: bool | None = None,
) -> dict[str, Any]:
    if has_existing_run not in {None, True, False}:
        raise ValueError("has_existing_run must be true, false, or null.")

    return {
        "pipeline_id": normalize_optional_run_ref(pipeline_id, field_name="pipeline_id"),
        "source_finished_at_gte": _encode_optional_cursor_datetime(source_finished_at_gte),
        "source_finished_at_lte": _encode_optional_cursor_datetime(source_finished_at_lte),
        "run_ref_prefix": (
            normalize_optional_run_ref(run_ref_prefix, field_name="run_ref_prefix")
            if run_ref_prefix is not None
            else None
        ),
        "require_contract_compatible_schema": bool(require_contract_compatible_schema),
        "exclude_existing_runs": bool(exclude_existing_runs),
        "has_existing_run": has_existing_run,
    }


def normalize_optional_pipeline_status(value: PipelineStatus | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, PipelineStatus):
        return value.value
    normalized = str(value).strip().lower()
    if not normalized:
        raise ValueError("status cannot be empty.")
    try:
        return PipelineStatus(normalized).value
    except ValueError as exc:
        raise ValueError("status must be one of: planned, blocked, pending, running, succeeded, failed.") from exc



def _normalize_optional_pipeline_status(value: PipelineStatus | str | None) -> str | None:
    return normalize_optional_pipeline_status(value)



def _normalize_optional_request_kind(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if not normalized:
        raise ValueError("request_kind cannot be empty.")
    if normalized not in {"run", "backfill"}:
        raise ValueError("request_kind must be one of: run, backfill.")
    return normalized



def _build_pipeline_preflight_attempt_page_cursor_scope(
    *,
    pipeline_id: str,
    request_kind: str | None = None,
    run_ref: str | None = None,
    source_ingestion_job_id: str | None = None,
    created_at_gte: datetime | None = None,
    created_at_lte: datetime | None = None,
) -> dict[str, Any]:
    return {
        "pipeline_id": normalize_optional_run_ref(pipeline_id, field_name="pipeline_id"),
        "request_kind": _normalize_optional_request_kind(request_kind),
        "run_ref": normalize_optional_run_ref(run_ref) if run_ref is not None else None,
        "source_ingestion_job_id": (
            normalize_optional_run_ref(source_ingestion_job_id, field_name="source_ingestion_job_id")
            if source_ingestion_job_id is not None
            else None
        ),
        "created_at_gte": _encode_optional_cursor_datetime(created_at_gte),
        "created_at_lte": _encode_optional_cursor_datetime(created_at_lte),
    }



def _build_pipeline_run_page_cursor_scope(
    *,
    pipeline_id: str,
    run_status: PipelineStatus | str | None = None,
    run_ref: str | None = None,
    source_ingestion_job_id: str | None = None,
    created_at_gte: datetime | None = None,
    created_at_lte: datetime | None = None,
) -> dict[str, Any]:
    return {
        "pipeline_id": normalize_optional_run_ref(pipeline_id, field_name="pipeline_id"),
        "run_status": normalize_optional_pipeline_status(run_status),
        "run_ref": normalize_optional_run_ref(run_ref) if run_ref is not None else None,
        "source_ingestion_job_id": (
            normalize_optional_run_ref(source_ingestion_job_id, field_name="source_ingestion_job_id")
            if source_ingestion_job_id is not None
            else None
        ),
        "created_at_gte": _encode_optional_cursor_datetime(created_at_gte),
        "created_at_lte": _encode_optional_cursor_datetime(created_at_lte),
    }



def encode_pipeline_source_candidate_page_cursor(
    *,
    effective_finished_at: datetime,
    ingestion_job_id: str,
    created_at: datetime | None = None,
    pipeline_id: str | None = None,
    source_finished_at_gte: datetime | None = None,
    source_finished_at_lte: datetime | None = None,
    run_ref_prefix: str | None = None,
    require_contract_compatible_schema: bool = False,
    exclude_existing_runs: bool = False,
    has_existing_run: bool | None = None,
) -> str:
    normalized_effective_finished_at = _coerce_utc_datetime(effective_finished_at)
    if normalized_effective_finished_at is None:
        raise ValueError("effective_finished_at is required.")

    normalized_ingestion_job_id = normalize_optional_run_ref(
        ingestion_job_id,
        field_name="ingestion_job_id",
    )
    normalized_created_at = _coerce_utc_datetime(created_at)
    payload = {
        "v": 1,
        "effective_finished_at": normalized_effective_finished_at.isoformat(),
        "ingestion_job_id": normalized_ingestion_job_id,
    }
    if normalized_created_at is not None:
        payload["created_at"] = normalized_created_at.isoformat()
    if pipeline_id is not None:
        payload["v"] = 5
        payload["scope"] = _build_pipeline_source_candidate_page_cursor_scope(
            pipeline_id=pipeline_id,
            source_finished_at_gte=source_finished_at_gte,
            source_finished_at_lte=source_finished_at_lte,
            run_ref_prefix=run_ref_prefix,
            require_contract_compatible_schema=require_contract_compatible_schema,
            exclude_existing_runs=exclude_existing_runs,
            has_existing_run=has_existing_run,
        )
    encoded = base64.urlsafe_b64encode(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    return encoded.rstrip("=")


def _decode_page_cursor_payload(cursor: str, *, expected_versions: tuple[int, ...] = (1,)) -> dict[str, Any]:
    normalized_cursor = normalize_optional_run_ref(cursor, field_name="cursor")
    padded_cursor = normalized_cursor + ("=" * (-len(normalized_cursor) % 4))
    try:
        payload = json.loads(base64.urlsafe_b64decode(padded_cursor.encode("ascii")).decode("utf-8"))
    except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError, binascii.Error) as exc:
        raise ValueError("cursor is invalid.") from exc

    if not isinstance(payload, dict) or payload.get("v") not in expected_versions:
        raise ValueError("cursor is invalid.")
    return payload



def assert_pipeline_source_candidate_page_cursor_matches_scope(
    cursor: str,
    *,
    pipeline_id: str,
    source_finished_at_gte: datetime | None = None,
    source_finished_at_lte: datetime | None = None,
    run_ref_prefix: str | None = None,
    require_contract_compatible_schema: bool = False,
    exclude_existing_runs: bool = False,
    has_existing_run: bool | None = None,
) -> None:
    payload = _decode_page_cursor_payload(cursor, expected_versions=(1, 2, 3, 4, 5))
    if payload.get("v") == 1:
        return

    expected_scope = _build_pipeline_source_candidate_page_cursor_scope(
        pipeline_id=pipeline_id,
        source_finished_at_gte=source_finished_at_gte,
        source_finished_at_lte=source_finished_at_lte,
        run_ref_prefix=run_ref_prefix,
        require_contract_compatible_schema=require_contract_compatible_schema,
        exclude_existing_runs=exclude_existing_runs,
        has_existing_run=has_existing_run,
    )
    actual_scope = payload.get("scope")
    if not isinstance(actual_scope, dict):
        raise ValueError("cursor is invalid.")
    if payload.get("v") == 2:
        expected_scope = {
            key: value
            for key, value in expected_scope.items()
            if key not in {"run_ref_prefix", "require_contract_compatible_schema"}
        }
    elif payload.get("v") in {3, 4}:
        expected_scope = {
            key: value
            for key, value in expected_scope.items()
            if key != "require_contract_compatible_schema"
        }
    if actual_scope != expected_scope:
        raise ValueError("cursor does not match the current pipeline source-candidate selection.")



def decode_pipeline_source_candidate_page_cursor_position(cursor: str) -> tuple[datetime, datetime | None, str]:
    payload = _decode_page_cursor_payload(cursor, expected_versions=(1, 2, 3, 4, 5))

    effective_finished_at = _coerce_optional_datetime(payload.get("effective_finished_at"))
    created_at = _coerce_optional_datetime(payload.get("created_at"))
    ingestion_job_id = payload.get("ingestion_job_id")
    if effective_finished_at is None or not isinstance(ingestion_job_id, str):
        raise ValueError("cursor is invalid.")

    normalized_ingestion_job_id = normalize_optional_run_ref(
        ingestion_job_id,
        field_name="cursor.ingestion_job_id",
    )
    return (
        _coerce_utc_datetime(effective_finished_at),
        _coerce_utc_datetime(created_at),
        normalized_ingestion_job_id,
    )



def decode_pipeline_source_candidate_page_cursor(cursor: str) -> tuple[datetime, str]:
    effective_finished_at, _created_at, ingestion_job_id = decode_pipeline_source_candidate_page_cursor_position(cursor)
    return effective_finished_at, ingestion_job_id


def encode_pipeline_preflight_attempt_page_cursor(
    *,
    created_at: datetime,
    preflight_attempt_id: str,
    pipeline_id: str | None = None,
    request_kind: str | None = None,
    run_ref: str | None = None,
    source_ingestion_job_id: str | None = None,
    created_at_gte: datetime | None = None,
    created_at_lte: datetime | None = None,
) -> str:
    normalized_created_at = _coerce_utc_datetime(created_at)
    if normalized_created_at is None:
        raise ValueError("created_at is required.")

    normalized_preflight_attempt_id = normalize_optional_run_ref(
        preflight_attempt_id,
        field_name="preflight_attempt_id",
    )
    payload = {
        "v": 1,
        "created_at": normalized_created_at.isoformat(),
        "preflight_attempt_id": normalized_preflight_attempt_id,
    }
    if pipeline_id is not None:
        payload["v"] = 2
        payload["scope"] = _build_pipeline_preflight_attempt_page_cursor_scope(
            pipeline_id=pipeline_id,
            request_kind=request_kind,
            run_ref=run_ref,
            source_ingestion_job_id=source_ingestion_job_id,
            created_at_gte=created_at_gte,
            created_at_lte=created_at_lte,
        )
    encoded = base64.urlsafe_b64encode(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    return encoded.rstrip("=")



def assert_pipeline_preflight_attempt_page_cursor_matches_scope(
    cursor: str,
    *,
    pipeline_id: str,
    request_kind: str | None = None,
    run_ref: str | None = None,
    source_ingestion_job_id: str | None = None,
    created_at_gte: datetime | None = None,
    created_at_lte: datetime | None = None,
) -> None:
    payload = _decode_page_cursor_payload(cursor, expected_versions=(1, 2))
    if payload.get("v") == 1:
        return

    expected_scope = _build_pipeline_preflight_attempt_page_cursor_scope(
        pipeline_id=pipeline_id,
        request_kind=request_kind,
        run_ref=run_ref,
        source_ingestion_job_id=source_ingestion_job_id,
        created_at_gte=created_at_gte,
        created_at_lte=created_at_lte,
    )
    if payload.get("scope") != expected_scope:
        raise ValueError("cursor does not match the current pipeline preflight-attempt selection.")



def decode_pipeline_preflight_attempt_page_cursor(cursor: str) -> tuple[datetime, str]:
    payload = _decode_page_cursor_payload(cursor, expected_versions=(1, 2))

    created_at = _coerce_optional_datetime(payload.get("created_at"))
    preflight_attempt_id = payload.get("preflight_attempt_id")
    if created_at is None or not isinstance(preflight_attempt_id, str):
        raise ValueError("cursor is invalid.")

    normalized_preflight_attempt_id = normalize_optional_run_ref(
        preflight_attempt_id,
        field_name="cursor.preflight_attempt_id",
    )
    return _coerce_utc_datetime(created_at), normalized_preflight_attempt_id



def encode_pipeline_run_page_cursor(
    *,
    created_at: datetime,
    run_id: str,
    pipeline_id: str | None = None,
    run_status: PipelineStatus | str | None = None,
    run_ref: str | None = None,
    source_ingestion_job_id: str | None = None,
    created_at_gte: datetime | None = None,
    created_at_lte: datetime | None = None,
) -> str:
    normalized_created_at = _coerce_utc_datetime(created_at)
    if normalized_created_at is None:
        raise ValueError("created_at is required.")

    normalized_run_id = normalize_optional_run_ref(
        run_id,
        field_name="run_id",
    )
    payload = {
        "v": 1,
        "created_at": normalized_created_at.isoformat(),
        "run_id": normalized_run_id,
    }
    if pipeline_id is not None:
        payload["v"] = 2
        payload["scope"] = _build_pipeline_run_page_cursor_scope(
            pipeline_id=pipeline_id,
            run_status=run_status,
            run_ref=run_ref,
            source_ingestion_job_id=source_ingestion_job_id,
            created_at_gte=created_at_gte,
            created_at_lte=created_at_lte,
        )
    encoded = base64.urlsafe_b64encode(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    return encoded.rstrip("=")



def assert_pipeline_run_page_cursor_matches_scope(
    cursor: str,
    *,
    pipeline_id: str,
    run_status: PipelineStatus | str | None = None,
    run_ref: str | None = None,
    source_ingestion_job_id: str | None = None,
    created_at_gte: datetime | None = None,
    created_at_lte: datetime | None = None,
) -> None:
    payload = _decode_page_cursor_payload(cursor, expected_versions=(1, 2))
    if payload.get("v") == 1:
        return

    expected_scope = _build_pipeline_run_page_cursor_scope(
        pipeline_id=pipeline_id,
        run_status=run_status,
        run_ref=run_ref,
        source_ingestion_job_id=source_ingestion_job_id,
        created_at_gte=created_at_gte,
        created_at_lte=created_at_lte,
    )
    if payload.get("scope") != expected_scope:
        raise ValueError("cursor does not match the current pipeline run selection.")



def decode_pipeline_run_page_cursor(cursor: str) -> tuple[datetime, str]:
    payload = _decode_page_cursor_payload(cursor, expected_versions=(1, 2))

    created_at = _coerce_optional_datetime(payload.get("created_at"))
    run_id = payload.get("run_id")
    if created_at is None or not isinstance(run_id, str):
        raise ValueError("cursor is invalid.")

    normalized_run_id = normalize_optional_run_ref(
        run_id,
        field_name="cursor.run_id",
    )
    return _coerce_utc_datetime(created_at), normalized_run_id


def build_backfill_request_snapshot(
    *,
    source_finished_at_gte: datetime | None = None,
    source_finished_at_lte: datetime | None = None,
    limit: int,
    offset: int,
    cursor: str | None = None,
    run_ref_prefix: str | None = None,
    skip_existing_runs: bool = False,
    has_existing_run: bool | None = None,
    require_contract_compatible_schema: bool = False,
) -> dict[str, Any]:
    snapshot = {
        "source_finished_at_gte": source_finished_at_gte,
        "source_finished_at_lte": source_finished_at_lte,
        "limit": int(limit),
        "offset": int(offset),
        "run_ref_prefix": normalize_optional_run_ref(run_ref_prefix, field_name="run_ref_prefix") if run_ref_prefix is not None else None,
        "skip_existing_runs": bool(skip_existing_runs),
    }
    if cursor is not None:
        snapshot["cursor"] = normalize_optional_run_ref(cursor, field_name="cursor")
    if has_existing_run is not None:
        snapshot["has_existing_run"] = has_existing_run
    if require_contract_compatible_schema:
        snapshot["require_contract_compatible_schema"] = True
    return snapshot


def build_pipeline_schema_snapshot(
    *,
    layer: DatasetLayer | str,
    version: int,
    fingerprint: str,
    schema_json: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    layer_value = _coerce_dataset_layer(layer)
    normalized_fingerprint = str(fingerprint).strip()
    if version < 1:
        raise ValueError("schema snapshot version must be at least 1.")
    if not normalized_fingerprint:
        raise ValueError("schema snapshot fingerprint cannot be empty.")

    normalized_schema: list[dict[str, str]] = []
    for item in schema_json or []:
        if not isinstance(item, dict):
            raise ValueError("schema snapshot items must be objects.")
        name = str(item.get("name", "")).strip()
        column_type = str(item.get("type", "")).strip()
        if not name or not column_type:
            raise ValueError("schema snapshot items must include non-empty name and type.")
        normalized_schema.append({"name": name, "type": column_type})

    return {
        "layer": layer_value.value,
        "version": int(version),
        "fingerprint": normalized_fingerprint,
        "schema_json": normalized_schema,
    }


def build_pipeline_schema_compatibility_preview_unavailable_reason(
    *,
    engine: PipelineEngine | str,
    target_layer: DatasetLayer | str,
    definition_json: dict[str, Any] | None,
    source_schema_snapshot: dict[str, Any] | None,
) -> str | None:
    engine_value = _coerce_pipeline_engine(engine)
    target_layer_value = _coerce_dataset_layer(target_layer)
    if engine_value != PipelineEngine.SQL:
        return "Compatibility preview is only available for sql pipelines."

    normalized_definition = normalize_pipeline_definition(engine_value, target_layer_value, definition_json)
    if normalized_definition.get("mode") != _SQL_PIPELINE_MODE_DATASET_TRANSFORM:
        return "Compatibility preview is only available for sql dataset-transform pipelines."
    if source_schema_snapshot is None:
        return "Compatibility preview requires a source schema snapshot for the pipeline source layer."
    return None



def build_pipeline_schema_compatibility_preview(
    *,
    engine: PipelineEngine | str,
    target_layer: DatasetLayer | str,
    definition_json: dict[str, Any] | None,
    source_schema_snapshot: dict[str, Any] | None,
    target_schema_snapshot: dict[str, Any] | None,
) -> dict[str, Any] | None:
    target_layer_value = _coerce_dataset_layer(target_layer)
    unavailable_reason = build_pipeline_schema_compatibility_preview_unavailable_reason(
        engine=engine,
        target_layer=target_layer_value,
        definition_json=definition_json,
        source_schema_snapshot=source_schema_snapshot,
    )
    if unavailable_reason is not None:
        return None

    candidate_schema = [dict(item) for item in source_schema_snapshot.get("schema_json", [])]
    current_schema = [dict(item) for item in (target_schema_snapshot or {}).get("schema_json", [])]
    report = assess_schema_compatibility(current_schema, candidate_schema)

    return {
        "layer": target_layer_value.value,
        "against_version": int((target_schema_snapshot or {}).get("version", 0)),
        "against_fingerprint": (target_schema_snapshot or {}).get("fingerprint"),
        "candidate_fingerprint": schema_fingerprint(candidate_schema),
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



def build_pipeline_schema_compatibility_outcome(
    *,
    schema_compatibility_preview: dict[str, Any] | None = None,
    schema_compatibility_preview_unavailable_reason: str | None = None,
    contract_compatibility_required: bool = False,
) -> str | None:
    if isinstance(schema_compatibility_preview, dict):
        contract_compatible = bool(schema_compatibility_preview.get("contract_compatible"))
        if contract_compatibility_required:
            return "required_compatible" if contract_compatible else "required_incompatible"
        return "compatible" if contract_compatible else "incompatible"

    normalized_reason = str(schema_compatibility_preview_unavailable_reason or "").strip()
    if normalized_reason:
        return "required_preview_unavailable" if contract_compatibility_required else "preview_unavailable"

    return "required_unavailable" if contract_compatibility_required else None


def build_pipeline_schema_context(
    *,
    source_schema_snapshot: dict[str, Any] | None = None,
    target_schema_snapshot: dict[str, Any] | None = None,
    schema_compatibility_preview: dict[str, Any] | None = None,
    schema_compatibility_preview_unavailable_reason: str | None = None,
    contract_compatibility_required: bool = False,
) -> dict[str, Any] | None:
    context: dict[str, Any] = {}
    if source_schema_snapshot is not None:
        context["source_schema_snapshot"] = source_schema_snapshot
    if target_schema_snapshot is not None:
        context["target_schema_snapshot"] = target_schema_snapshot
    if schema_compatibility_preview is not None:
        context["schema_compatibility_preview"] = schema_compatibility_preview
    if schema_compatibility_preview_unavailable_reason is not None:
        normalized_reason = str(schema_compatibility_preview_unavailable_reason).strip()
        if normalized_reason:
            context["schema_compatibility_preview_unavailable_reason"] = normalized_reason
    compatibility_outcome = build_pipeline_schema_compatibility_outcome(
        schema_compatibility_preview=schema_compatibility_preview,
        schema_compatibility_preview_unavailable_reason=schema_compatibility_preview_unavailable_reason,
        contract_compatibility_required=contract_compatibility_required,
    )
    if compatibility_outcome is not None:
        context["contract_compatibility_outcome"] = compatibility_outcome
    if contract_compatibility_required:
        context["contract_compatibility_required"] = True
    return context or None


def normalize_pipeline_definition(
    engine: PipelineEngine | str,
    target_layer: DatasetLayer | str,
    definition_json: dict[str, Any] | None,
) -> dict[str, Any]:
    engine_value = _coerce_pipeline_engine(engine)
    target_layer_value = _coerce_dataset_layer(target_layer)

    if definition_json is None:
        normalized: dict[str, Any] = {}
    elif isinstance(definition_json, dict):
        normalized = dict(definition_json)
    else:
        raise ValueError("definition_json must be an object.")

    if engine_value != PipelineEngine.SQL:
        return normalized

    unknown_keys = sorted(set(normalized) - _ALLOWED_SQL_PIPELINE_KEYS)
    if unknown_keys:
        joined = ", ".join(unknown_keys)
        raise ValueError(f"definition_json contains unsupported keys for sql pipelines: {joined}.")

    raw_sql = normalized.get("sql")
    explicit_mode = normalized.get("mode")

    if explicit_mode is None:
        mode = _SQL_PIPELINE_MODE_CUSTOM_SQL if raw_sql is not None else _SQL_PIPELINE_MODE_DATASET_TRANSFORM
    else:
        if not isinstance(explicit_mode, str):
            raise ValueError("definition_json.mode must be a string.")
        mode = explicit_mode.strip().lower()

    if mode not in {_SQL_PIPELINE_MODE_DATASET_TRANSFORM, _SQL_PIPELINE_MODE_CUSTOM_SQL}:
        raise ValueError(
            "definition_json.mode must be one of: "
            f"{_SQL_PIPELINE_MODE_DATASET_TRANSFORM}, {_SQL_PIPELINE_MODE_CUSTOM_SQL}."
        )

    if mode == _SQL_PIPELINE_MODE_DATASET_TRANSFORM:
        if target_layer_value == DatasetLayer.RAW:
            raise ValueError("SQL pipelines targeting the raw layer must use definition_json.mode='custom_sql'.")
        if raw_sql is not None:
            raise ValueError("definition_json.sql is only allowed when definition_json.mode='custom_sql'.")
        return {"mode": _SQL_PIPELINE_MODE_DATASET_TRANSFORM}

    if not isinstance(raw_sql, str):
        raise ValueError("definition_json.sql must be a string when definition_json.mode='custom_sql'.")

    validated_sql = validate_read_only_sql(raw_sql, "definition_json.sql")
    if validated_sql is None:
        raise ValueError("definition_json.sql cannot be empty when definition_json.mode='custom_sql'.")

    return {
        "mode": _SQL_PIPELINE_MODE_CUSTOM_SQL,
        "sql": validated_sql,
    }


def resolve_sql_pipeline_query(
    *,
    target_layer: DatasetLayer | str,
    definition_json: dict[str, Any] | None,
    dataset_silver_sql: str | None,
    dataset_gold_sql: str | None,
) -> str:
    target_layer_value = _coerce_dataset_layer(target_layer)
    normalized = normalize_pipeline_definition(PipelineEngine.SQL, target_layer_value, definition_json)

    if normalized["mode"] == _SQL_PIPELINE_MODE_CUSTOM_SQL:
        return normalized["sql"]

    if target_layer_value == DatasetLayer.SILVER:
        return dataset_silver_sql or "SELECT * FROM source"
    if target_layer_value == DatasetLayer.GOLD:
        return dataset_gold_sql or "SELECT * FROM source"

    raise ValueError("SQL dataset-transform pipelines can only target the silver or gold layer.")


def build_pipeline_source_candidate(
    *,
    ingestion_job_id: str,
    dataset_id: str,
    source_layer: DatasetLayer | str,
    status: str,
    created_at: datetime,
    finished_at: datetime | None,
    object_uri: str,
    existing_run_count: int = 0,
    latest_run_id: str | None = None,
    latest_run_status: str | None = None,
    latest_run_ref: str | None = None,
    latest_run_created_at: datetime | None = None,
    latest_run_finished_at: datetime | None = None,
    latest_run_error_message: str | None = None,
    run_ref_prefix: str | None = None,
    schema_compatibility_preview: dict[str, Any] | None = None,
    schema_compatibility_preview_unavailable_reason: str | None = None,
) -> dict[str, Any]:
    source_layer_value = _coerce_dataset_layer(source_layer)
    normalized_created_at = _coerce_utc_datetime(created_at)
    normalized_finished_at = _coerce_utc_datetime(finished_at)
    normalized_latest_run_created_at = _coerce_utc_datetime(latest_run_created_at)
    normalized_latest_run_finished_at = _coerce_utc_datetime(latest_run_finished_at)
    effective_finished_at = normalized_finished_at or normalized_created_at
    normalized_existing_run_count = int(existing_run_count)
    if normalized_existing_run_count < 0:
        raise ValueError("existing_run_count cannot be negative.")
    suggested_run_ref = build_backfill_run_ref(run_ref_prefix, ingestion_job_id) if run_ref_prefix is not None else None
    if schema_compatibility_preview is not None and not isinstance(schema_compatibility_preview, dict):
        raise ValueError("schema_compatibility_preview must be an object when provided.")
    normalized_unavailable_reason = None
    if schema_compatibility_preview_unavailable_reason is not None:
        normalized_unavailable_reason = str(schema_compatibility_preview_unavailable_reason).strip() or None
    would_fail_require_contract_compatible_schema = (
        schema_compatibility_preview is None
        or not bool(schema_compatibility_preview.get("contract_compatible"))
    )

    return {
        "ingestion_job_id": ingestion_job_id,
        "dataset_id": dataset_id,
        "source_layer": source_layer_value.value,
        "status": str(status),
        "created_at": normalized_created_at,
        "finished_at": normalized_finished_at,
        "effective_finished_at": effective_finished_at,
        "object_uri": object_uri,
        "existing_run_count": normalized_existing_run_count,
        "has_existing_run": normalized_existing_run_count > 0,
        "latest_run_id": latest_run_id,
        "latest_run_status": str(latest_run_status) if latest_run_status is not None else None,
        "latest_run_ref": latest_run_ref,
        "latest_run_created_at": normalized_latest_run_created_at,
        "latest_run_finished_at": normalized_latest_run_finished_at,
        "latest_run_error_message": latest_run_error_message,
        "suggested_run_ref": suggested_run_ref,
        "would_skip_with_skip_existing_runs": normalized_existing_run_count > 0,
        "schema_compatibility_preview": schema_compatibility_preview,
        "schema_compatibility_preview_unavailable_reason": normalized_unavailable_reason,
        "would_fail_require_contract_compatible_schema": would_fail_require_contract_compatible_schema,
    }


def build_pipeline_execution_plan(
    *,
    pipeline_id: str,
    dataset_id: str,
    source_layer: DatasetLayer | str,
    target_layer: DatasetLayer | str,
    engine: PipelineEngine | str,
    definition_json: dict[str, Any] | None,
    dataset_silver_sql: str | None,
    dataset_gold_sql: str | None,
    source_selection: str = "latest_successful",
    requested_source_ingestion_job_id: str | None = None,
    requested_source_finished_at_gte: datetime | None = None,
    requested_source_finished_at_lte: datetime | None = None,
    source_ingestion_job_id: str | None = None,
    source_job_status: str | None = None,
    source_finished_at: datetime | None = None,
    source_object_uri: str | None = None,
) -> dict[str, Any]:
    source_layer_value = _coerce_dataset_layer(source_layer)
    target_layer_value = _coerce_dataset_layer(target_layer)
    engine_value = _coerce_pipeline_engine(engine)

    plan: dict[str, Any] = {
        "pipeline_id": pipeline_id,
        "dataset_id": dataset_id,
        "engine": engine_value.value,
        "source_layer": source_layer_value.value,
        "target_layer": target_layer_value.value,
        "definition_json": normalize_pipeline_definition(engine_value, target_layer_value, definition_json),
        "resolved_query": None,
        "source_selection": str(source_selection).strip().lower() or "latest_successful",
        "requested_source_ingestion_job_id": requested_source_ingestion_job_id,
        "requested_source_finished_at_gte": requested_source_finished_at_gte,
        "requested_source_finished_at_lte": requested_source_finished_at_lte,
        "source_ingestion_job_id": source_ingestion_job_id,
        "source_job_status": source_job_status,
        "source_finished_at": source_finished_at,
        "source_object_uri": source_object_uri,
        "executable": False,
        "issues": [],
    }

    issues: list[str] = plan["issues"]
    if engine_value != PipelineEngine.SQL:
        issues.append(f"Pipeline engine '{engine_value.value}' is not yet executable.")
        return plan

    plan["resolved_query"] = resolve_sql_pipeline_query(
        target_layer=target_layer_value,
        definition_json=plan["definition_json"],
        dataset_silver_sql=dataset_silver_sql,
        dataset_gold_sql=dataset_gold_sql,
    )

    if source_ingestion_job_id is None:
        if plan["source_selection"] == "explicit" and requested_source_ingestion_job_id is not None:
            issues.append(
                f"Requested source ingestion job '{requested_source_ingestion_job_id}' is not available for source layer '{source_layer_value.value}'."
            )
        elif (
            plan["source_selection"] == "latest_successful_between"
            and requested_source_finished_at_gte is not None
            and requested_source_finished_at_lte is not None
        ):
            issues.append(
                f"No successful ingestion between '{requested_source_finished_at_gte.isoformat()}' and '{requested_source_finished_at_lte.isoformat()}' is available for source layer '{source_layer_value.value}'."
            )
        elif plan["source_selection"] == "latest_successful_at_or_after" and requested_source_finished_at_gte is not None:
            issues.append(
                f"No successful ingestion at or after '{requested_source_finished_at_gte.isoformat()}' is available for source layer '{source_layer_value.value}'."
            )
        elif plan["source_selection"] == "latest_successful_at_or_before" and requested_source_finished_at_lte is not None:
            issues.append(
                f"No successful ingestion at or before '{requested_source_finished_at_lte.isoformat()}' is available for source layer '{source_layer_value.value}'."
            )
        else:
            issues.append(
                f"No successful ingestion is available for source layer '{source_layer_value.value}'."
            )
    elif source_job_status is not None and source_job_status != "succeeded":
        issues.append(
            f"Source ingestion job '{source_ingestion_job_id}' is in status '{source_job_status}', expected 'succeeded'."
        )

    if not source_object_uri:
        if source_ingestion_job_id is not None:
            issues.append(
                f"Source ingestion job '{source_ingestion_job_id}' does not expose a '{source_layer_value.value}' object URI."
            )
        elif (
            plan["source_selection"] == "latest_successful_between"
            and requested_source_finished_at_gte is not None
            and requested_source_finished_at_lte is not None
        ):
            issues.append(
                f"No source object URI is available for source layer '{source_layer_value.value}' between '{requested_source_finished_at_gte.isoformat()}' and '{requested_source_finished_at_lte.isoformat()}'."
            )
        elif plan["source_selection"] == "latest_successful_at_or_after" and requested_source_finished_at_gte is not None:
            issues.append(
                f"No source object URI is available for source layer '{source_layer_value.value}' at or after '{requested_source_finished_at_gte.isoformat()}'."
            )
        elif plan["source_selection"] == "latest_successful_at_or_before" and requested_source_finished_at_lte is not None:
            issues.append(
                f"No source object URI is available for source layer '{source_layer_value.value}' at or before '{requested_source_finished_at_lte.isoformat()}'."
            )
        else:
            issues.append(
                f"No source object URI is available for source layer '{source_layer_value.value}'."
            )

    plan["executable"] = not issues
    return plan


def _json_compatible_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_compatible_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_compatible_value(item) for item in value]
    return value


def _coerce_optional_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        if candidate.endswith("Z"):
            candidate = f"{candidate[:-1]}+00:00"
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            return None
    return None


def _extract_schema_items(value: Any) -> list[dict[str, str]] | None:
    if value is None:
        return []
    if not isinstance(value, list):
        return None
    normalized_schema: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            return None
        name = item.get("name")
        column_type = item.get("type")
        if not isinstance(name, str) or not name.strip():
            return None
        if not isinstance(column_type, str) or not column_type.strip():
            return None
        normalized_schema.append({"name": name.strip(), "type": column_type.strip()})
    return normalized_schema



def _extract_schema_snapshot(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None

    layer = value.get("layer")
    if not isinstance(layer, str):
        return None
    normalized_layer = layer.strip().lower()
    if normalized_layer not in {item.value for item in DatasetLayer}:
        return None

    version = value.get("version")
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        return None

    fingerprint = value.get("fingerprint")
    if not isinstance(fingerprint, str) or not fingerprint.strip():
        return None

    normalized_schema = _extract_schema_items(value.get("schema_json"))
    if normalized_schema is None:
        return None

    return {
        "layer": normalized_layer,
        "version": version,
        "fingerprint": fingerprint.strip(),
        "schema_json": normalized_schema,
    }


def _extract_schema_compatibility_preview_unavailable_reason(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _extract_contract_compatibility_required(value: Any) -> bool:
    return bool(value) if isinstance(value, bool) else False


def _extract_contract_compatibility_outcome(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized_value = value.strip()
    if not normalized_value:
        return None
    allowed = {
        "compatible",
        "incompatible",
        "preview_unavailable",
        "required_compatible",
        "required_incompatible",
        "required_preview_unavailable",
        "required_unavailable",
    }
    return normalized_value if normalized_value in allowed else None


def _extract_schema_compatibility_preview(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None

    layer = value.get("layer")
    if not isinstance(layer, str):
        return None
    normalized_layer = layer.strip().lower()
    if normalized_layer not in {item.value for item in DatasetLayer}:
        return None

    against_version = value.get("against_version")
    if not isinstance(against_version, int) or isinstance(against_version, bool) or against_version < 0:
        return None

    against_fingerprint = value.get("against_fingerprint")
    if against_fingerprint is not None:
        if not isinstance(against_fingerprint, str) or not against_fingerprint.strip():
            return None
        normalized_against_fingerprint = against_fingerprint.strip()
    else:
        normalized_against_fingerprint = None

    candidate_fingerprint = value.get("candidate_fingerprint")
    if not isinstance(candidate_fingerprint, str) or not candidate_fingerprint.strip():
        return None

    current_schema = _extract_schema_items(value.get("current_schema"))
    candidate_schema = _extract_schema_items(value.get("candidate_schema"))
    merged_schema = _extract_schema_items(value.get("merged_schema"))
    added_columns = _extract_schema_items(value.get("added_columns"))
    removed_columns = _extract_schema_items(value.get("removed_columns"))
    if (
        current_schema is None
        or candidate_schema is None
        or merged_schema is None
        or added_columns is None
        or removed_columns is None
    ):
        return None

    changed_columns_value = value.get("changed_columns")
    if not isinstance(changed_columns_value, list):
        return None
    changed_columns: list[dict[str, Any]] = []
    for item in changed_columns_value:
        if not isinstance(item, dict):
            return None
        name = item.get("name")
        from_type = item.get("from_type")
        to_type = item.get("to_type")
        compatible = item.get("compatible")
        if not isinstance(name, str) or not name.strip():
            return None
        if not isinstance(from_type, str) or not from_type.strip():
            return None
        if not isinstance(to_type, str) or not to_type.strip():
            return None
        if not isinstance(compatible, bool):
            return None
        changed_columns.append({
            "name": name.strip(),
            "from_type": from_type.strip(),
            "to_type": to_type.strip(),
            "compatible": compatible,
        })

    for key in ("breaking_changes", "has_changes", "contract_compatible", "strict_mode_compatible"):
        if not isinstance(value.get(key), bool):
            return None

    return {
        "layer": normalized_layer,
        "against_version": against_version,
        "against_fingerprint": normalized_against_fingerprint,
        "candidate_fingerprint": candidate_fingerprint.strip(),
        "current_schema": current_schema,
        "candidate_schema": candidate_schema,
        "merged_schema": merged_schema,
        "added_columns": added_columns,
        "removed_columns": removed_columns,
        "changed_columns": changed_columns,
        "breaking_changes": value["breaking_changes"],
        "has_changes": value["has_changes"],
        "contract_compatible": value["contract_compatible"],
        "strict_mode_compatible": value["strict_mode_compatible"],
    }



def _extract_backfill_request_snapshot(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None

    limit = value.get("limit")
    offset = value.get("offset")
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        return None
    if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
        return None

    source_finished_at_gte = _coerce_optional_datetime(value.get("source_finished_at_gte"))
    if value.get("source_finished_at_gte") is not None and source_finished_at_gte is None:
        return None

    source_finished_at_lte = _coerce_optional_datetime(value.get("source_finished_at_lte"))
    if value.get("source_finished_at_lte") is not None and source_finished_at_lte is None:
        return None

    if source_finished_at_gte is not None and source_finished_at_lte is not None and source_finished_at_gte > source_finished_at_lte:
        return None

    run_ref_prefix = value.get("run_ref_prefix")
    if run_ref_prefix is None:
        normalized_run_ref_prefix = None
    elif isinstance(run_ref_prefix, str):
        normalized_run_ref_prefix = run_ref_prefix.strip() or None
    else:
        return None

    skip_existing_runs = value.get("skip_existing_runs", False)
    if not isinstance(skip_existing_runs, bool):
        return None

    has_existing_run = value.get("has_existing_run")
    if has_existing_run is not None and not isinstance(has_existing_run, bool):
        return None

    require_contract_compatible_schema = value.get("require_contract_compatible_schema", False)
    if not isinstance(require_contract_compatible_schema, bool):
        return None

    cursor = value.get("cursor")
    if cursor is None:
        normalized_cursor = None
    elif isinstance(cursor, str):
        normalized_cursor = cursor.strip() or None
    else:
        return None

    snapshot = {
        "run_ref_prefix": normalized_run_ref_prefix,
        "source_finished_at_gte": source_finished_at_gte,
        "source_finished_at_lte": source_finished_at_lte,
        "limit": limit,
        "offset": offset,
    }
    if normalized_cursor is not None:
        snapshot["cursor"] = normalized_cursor
    if skip_existing_runs:
        snapshot["skip_existing_runs"] = True
    if has_existing_run is not None:
        snapshot["has_existing_run"] = has_existing_run
    if require_contract_compatible_schema:
        snapshot["require_contract_compatible_schema"] = True
    return snapshot


def _extract_pipeline_execution_details(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None

    details: dict[str, Any] = {}

    executor = value.get("executor")
    if isinstance(executor, str) and executor.strip():
        details["executor"] = executor.strip()

    try:
        normalized_status = normalize_optional_pipeline_status(value.get("status"))
    except ValueError:
        normalized_status = None
    if normalized_status is not None:
        details["status"] = normalized_status

    for key in ("started_at", "finished_at"):
        normalized_datetime = _coerce_optional_datetime(value.get(key))
        if normalized_datetime is not None:
            details[key] = _coerce_utc_datetime(normalized_datetime)

    for key in ("task_id", "error_message", "source_object_uri", "target_object_uri", "target_schema_fingerprint"):
        normalized_value = value.get(key)
        if isinstance(normalized_value, str) and normalized_value.strip():
            details[key] = normalized_value.strip()

    output_row_count = value.get("output_row_count")
    if isinstance(output_row_count, int) and not isinstance(output_row_count, bool) and output_row_count >= 0:
        details["output_row_count"] = output_row_count

    output_schema = _extract_schema_items(value.get("output_schema"))
    if output_schema is not None:
        details["output_schema"] = output_schema

    target_schema_version = value.get("target_schema_version")
    if isinstance(target_schema_version, int) and not isinstance(target_schema_version, bool) and target_schema_version >= 1:
        details["target_schema_version"] = target_schema_version

    return details or None


def _extract_pipeline_artifact_manifest(
    snapshot: dict[str, Any],
    *,
    run_id: str | None = None,
    pipeline_id: str | None = None,
    dataset_id: str | None = None,
    source_ingestion_job_id: str | None = None,
    run_status: PipelineStatus | str | None = None,
) -> dict[str, Any] | None:
    execution_plan = snapshot.get("execution_plan") if isinstance(snapshot.get("execution_plan"), dict) else {}
    execution_details = snapshot.get("execution_details") if isinstance(snapshot.get("execution_details"), dict) else {}

    source_object_uri = execution_details.get("source_object_uri") or execution_plan.get("source_object_uri")
    target_object_uri = execution_details.get("target_object_uri")
    output_schema = execution_details.get("output_schema") if isinstance(execution_details.get("output_schema"), list) else []
    source_schema_snapshot = (
        snapshot.get("source_schema_snapshot") if isinstance(snapshot.get("source_schema_snapshot"), dict) else None
    )
    target_schema_snapshot = (
        snapshot.get("target_schema_snapshot") if isinstance(snapshot.get("target_schema_snapshot"), dict) else None
    )

    has_manifest_data = any(
        item is not None and item != []
        for item in (
            source_object_uri,
            target_object_uri,
            execution_details.get("output_row_count"),
            output_schema,
            execution_details.get("target_schema_version"),
            execution_details.get("target_schema_fingerprint"),
            source_schema_snapshot,
            target_schema_snapshot,
        )
    )
    if not has_manifest_data:
        return None

    manifest: dict[str, Any] = {}
    for key, raw_value in (
        ("run_id", run_id),
        ("pipeline_id", pipeline_id),
        ("dataset_id", dataset_id),
        ("source_ingestion_job_id", source_ingestion_job_id),
    ):
        if isinstance(raw_value, str) and raw_value.strip():
            manifest[key] = raw_value.strip()

    try:
        normalized_run_status = normalize_optional_pipeline_status(run_status)
    except ValueError:
        normalized_run_status = None
    if normalized_run_status is not None:
        manifest["run_status"] = normalized_run_status

    for key, value in (
        ("execution_status", execution_details.get("status")),
        ("executor", execution_details.get("executor")),
        ("task_id", execution_details.get("task_id")),
        ("engine", execution_plan.get("engine")),
        ("source_layer", execution_plan.get("source_layer")),
        ("target_layer", execution_plan.get("target_layer")),
        ("source_object_uri", source_object_uri),
        ("target_object_uri", target_object_uri),
        ("target_schema_fingerprint", execution_details.get("target_schema_fingerprint")),
    ):
        if isinstance(value, str) and value.strip():
            manifest[key] = value.strip()

    output_row_count = execution_details.get("output_row_count")
    if isinstance(output_row_count, int) and not isinstance(output_row_count, bool) and output_row_count >= 0:
        manifest["output_row_count"] = output_row_count

    if output_schema:
        manifest["output_schema"] = output_schema

    target_schema_version = execution_details.get("target_schema_version")
    if isinstance(target_schema_version, int) and not isinstance(target_schema_version, bool) and target_schema_version >= 1:
        manifest["target_schema_version"] = target_schema_version

    if source_schema_snapshot is not None:
        manifest["source_schema_snapshot"] = source_schema_snapshot
    if target_schema_snapshot is not None:
        manifest["target_schema_snapshot"] = target_schema_snapshot

    return manifest or None


def extract_pipeline_artifact_manifest(
    metrics_json: Any,
    *,
    run_id: str | None = None,
    pipeline_id: str | None = None,
    dataset_id: str | None = None,
    source_ingestion_job_id: str | None = None,
    run_status: PipelineStatus | str | None = None,
) -> dict[str, Any] | None:
    return _extract_pipeline_artifact_manifest(
        extract_pipeline_run_snapshot(metrics_json),
        run_id=run_id,
        pipeline_id=pipeline_id,
        dataset_id=dataset_id,
        source_ingestion_job_id=source_ingestion_job_id,
        run_status=run_status,
    )


def extract_pipeline_run_snapshot(metrics_json: Any) -> dict[str, Any]:
    normalized_metrics = metrics_json if isinstance(metrics_json, dict) else {}
    execution_plan = normalized_metrics.get("execution_plan")
    if not isinstance(execution_plan, dict):
        execution_plan = None

    schema_context = normalized_metrics.get("schema_context")
    if not isinstance(schema_context, dict):
        schema_context = {}

    snapshot = {
        "preflighted_at": _coerce_optional_datetime(normalized_metrics.get("preflighted_at")),
        "execution_plan": execution_plan,
        "backfill_request": _extract_backfill_request_snapshot(normalized_metrics.get("backfill_request")),
        "source_schema_snapshot": _extract_schema_snapshot(schema_context.get("source_schema_snapshot")),
        "target_schema_snapshot": _extract_schema_snapshot(schema_context.get("target_schema_snapshot")),
        "contract_compatibility_outcome": _extract_contract_compatibility_outcome(
            schema_context.get("contract_compatibility_outcome")
        ),
    }

    execution_details = _extract_pipeline_execution_details(normalized_metrics.get("execution"))
    if execution_details is not None:
        snapshot["execution_details"] = execution_details

    schema_compatibility_preview = _extract_schema_compatibility_preview(schema_context.get("schema_compatibility_preview"))
    schema_compatibility_preview_unavailable_reason = _extract_schema_compatibility_preview_unavailable_reason(
        schema_context.get("schema_compatibility_preview_unavailable_reason")
    )
    if schema_compatibility_preview is not None or schema_compatibility_preview_unavailable_reason is not None:
        snapshot["schema_compatibility_preview"] = schema_compatibility_preview
    if schema_compatibility_preview_unavailable_reason is not None:
        snapshot["schema_compatibility_preview_unavailable_reason"] = schema_compatibility_preview_unavailable_reason

    contract_compatibility_required = _extract_contract_compatibility_required(
        schema_context.get("contract_compatibility_required")
    )
    if contract_compatibility_required:
        snapshot["contract_compatibility_required"] = True

    if snapshot["contract_compatibility_outcome"] is None:
        snapshot.pop("contract_compatibility_outcome")

    return snapshot


def build_pipeline_preflight_metrics(
    *,
    execution_plan: dict[str, Any],
    preflighted_at: datetime | None = None,
    backfill_request: dict[str, Any] | None = None,
    schema_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    preflighted_at_value = preflighted_at or datetime.now(timezone.utc)
    return {
        "preflighted_at": preflighted_at_value.isoformat(),
        "execution_plan": _json_compatible_value(execution_plan),
        **({"backfill_request": _json_compatible_value(backfill_request)} if backfill_request is not None else {}),
        **({"schema_context": _json_compatible_value(schema_context)} if schema_context is not None else {}),
    }



def build_pipeline_run_payload(
    *,
    pipeline_id: str,
    dataset_id: str,
    execution_plan: dict[str, Any],
    run_ref: str | None = None,
    preflighted_at: datetime | None = None,
    backfill_request: dict[str, Any] | None = None,
    schema_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_run_ref = normalize_optional_run_ref(run_ref)

    preflighted_at_value = preflighted_at or datetime.now(timezone.utc)
    executable = bool(execution_plan.get("executable"))
    status = "planned" if executable else "blocked"
    issues = [str(issue) for issue in execution_plan.get("issues") or []]

    return {
        "pipeline_id": pipeline_id,
        "dataset_id": dataset_id,
        "ingestion_job_id": execution_plan.get("source_ingestion_job_id"),
        "status": status,
        "run_ref": normalized_run_ref,
        "metrics_json": build_pipeline_preflight_metrics(
            execution_plan=execution_plan,
            preflighted_at=preflighted_at_value,
            backfill_request=backfill_request,
            schema_context=schema_context,
        ),
        "error_message": None if executable else "; ".join(issues),
        "started_at": None,
        "finished_at": None if executable else preflighted_at_value,
    }
