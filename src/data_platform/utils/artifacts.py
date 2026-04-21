from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any

from data_platform.utils.paths import parse_object_uri
from data_platform.utils.pipeline_definitions import extract_pipeline_artifact_manifest


def _normalize_object_uri_parts(object_uri: str | None) -> tuple[str | None, str | None, str | None]:
    normalized = (object_uri or "").strip()
    if not normalized:
        return None, None, None
    try:
        bucket, key = parse_object_uri(normalized)
    except ValueError:
        return normalized, None, None
    return normalized, bucket, key


def _infer_object_format(object_uri: str | None) -> str | None:
    normalized = (object_uri or "").strip()
    if not normalized:
        return None
    name = PurePosixPath(normalized).name.lower()
    if name.endswith('.parquet'):
        return 'parquet'
    if name.endswith('.csv'):
        return 'csv'
    if name.endswith('.tsv'):
        return 'tsv'
    if name.endswith('.json'):
        return 'json'
    if name.endswith('.jsonl') or name.endswith('.ndjson'):
        return 'ndjson'
    if name.endswith('.xlsx') or name.endswith('.xlsm'):
        return 'xlsx'
    return None


def _build_artifact_manifest_entry(
    *,
    name: str,
    role: str,
    object_uri: str | None,
    layer: str | None = None,
    format: str | None = None,
    content_type: str | None = None,
    row_count: int | None = None,
    schema_version: int | None = None,
    schema_fingerprint: str | None = None,
) -> dict[str, Any] | None:
    normalized_uri, bucket, object_key = _normalize_object_uri_parts(object_uri)
    if normalized_uri is None:
        return None

    entry: dict[str, Any] = {
        'name': name,
        'role': role,
        'object_uri': normalized_uri,
        'bucket': bucket,
        'object_key': object_key,
        'layer': layer,
        'format': format or _infer_object_format(normalized_uri),
        'content_type': content_type,
        'row_count': row_count,
        'schema_version': schema_version,
        'schema_fingerprint': schema_fingerprint,
    }
    return {key: value for key, value in entry.items() if value is not None}


def build_ingestion_artifact_manifest(job: Any) -> dict[str, Any]:
    items = [
        _build_artifact_manifest_entry(
            name='raw',
            role='source',
            object_uri=getattr(job, 'raw_object_uri', None),
            layer='raw',
            format=getattr(job, 'source_format', None),
            content_type=getattr(job, 'source_content_type', None),
        ),
        _build_artifact_manifest_entry(
            name='silver',
            role='derived',
            object_uri=getattr(job, 'silver_object_uri', None),
            layer='silver',
            format='parquet',
        ),
        _build_artifact_manifest_entry(
            name='gold',
            role='derived',
            object_uri=getattr(job, 'gold_object_uri', None),
            layer='gold',
            format='parquet',
            row_count=getattr(job, 'row_count', None),
        ),
    ]
    return {
        'resource_type': 'ingestion',
        'resource_id': getattr(job, 'id', None),
        'dataset_id': getattr(job, 'dataset_id', None),
        'ingestion_job_id': getattr(job, 'id', None),
        'status': getattr(job, 'status', None),
        'items': [item for item in items if item is not None],
    }


def _normalize_schema_snapshot(snapshot: Any) -> dict[str, Any] | None:
    if snapshot is None:
        return None
    if isinstance(snapshot, Mapping):
        layer = snapshot.get('layer')
        version = snapshot.get('version')
        fingerprint = snapshot.get('fingerprint')
        schema_json = snapshot.get('schema_json')
    else:
        layer = getattr(snapshot, 'layer', None)
        version = getattr(snapshot, 'version', None)
        fingerprint = getattr(snapshot, 'fingerprint', None)
        schema_json = getattr(snapshot, 'schema_json', None)
    if not isinstance(layer, str) or not layer.strip():
        return None
    if not isinstance(fingerprint, str) or not fingerprint.strip():
        return None
    try:
        normalized_version = int(version)
    except (TypeError, ValueError):
        return None
    normalized_items: list[dict[str, Any]] = []
    if isinstance(schema_json, list):
        for item in schema_json:
            if isinstance(item, Mapping):
                normalized_items.append(dict(item))
    return {
        'layer': layer.strip().lower(),
        'version': normalized_version,
        'fingerprint': fingerprint.strip(),
        'schema_json': normalized_items,
    }


def build_ingestion_artifact_manifest_item(
    *,
    layer: str,
    object_uri: str | None,
    schema_snapshot: Any = None,
) -> dict[str, Any] | None:
    normalized_layer = str(layer).strip().lower()
    normalized_object_uri = (object_uri or '').strip()
    if not normalized_layer or not normalized_object_uri:
        return None
    payload: dict[str, Any] = {
        'layer': normalized_layer,
        'object_uri': normalized_object_uri,
    }
    normalized_snapshot = _normalize_schema_snapshot(schema_snapshot)
    if normalized_snapshot is not None:
        payload['schema_snapshot'] = normalized_snapshot
    return payload


def build_ingestion_artifact_manifest_payload(
    *,
    ingestion_job_id: str,
    dataset_id: str,
    dataset_slug: str | None,
    status: str,
    source_type: str,
    filename: str | None,
    source_format: str | None,
    source_content_type: str | None,
    content_hash: str | None,
    size_bytes: int | None,
    row_count: int | None,
    created_at: datetime,
    started_at: datetime | None,
    finished_at: datetime | None,
    effective_at: datetime,
    artifacts: list[dict[str, Any] | None],
) -> dict[str, Any]:
    return {
        'ingestion_job_id': ingestion_job_id,
        'dataset_id': dataset_id,
        'dataset_slug': dataset_slug,
        'status': status,
        'source_type': source_type,
        'filename': filename,
        'source_format': source_format,
        'source_content_type': source_content_type,
        'content_hash': content_hash,
        'size_bytes': size_bytes,
        'row_count': row_count,
        'created_at': created_at,
        'started_at': started_at,
        'finished_at': finished_at,
        'effective_at': effective_at,
        'artifacts': [item for item in artifacts if item is not None],
    }


def build_pipeline_run_artifact_manifest(run: Any, *, pipeline: Any | None = None) -> dict[str, Any]:
    manifest = extract_pipeline_artifact_manifest(
        getattr(run, 'metrics_json', None),
        run_id=getattr(run, 'id', None),
        pipeline_id=getattr(run, 'pipeline_id', None),
        dataset_id=getattr(run, 'dataset_id', None),
        source_ingestion_job_id=getattr(run, 'ingestion_job_id', None),
        run_status=getattr(run, 'status', None),
    ) or {}

    source_snapshot = manifest.get('source_schema_snapshot') if isinstance(manifest, Mapping) else None
    target_snapshot = manifest.get('target_schema_snapshot') if isinstance(manifest, Mapping) else None
    source_layer = manifest.get('source_layer') if isinstance(manifest, Mapping) else None
    target_layer = manifest.get('target_layer') if isinstance(manifest, Mapping) else None

    items = [
        _build_artifact_manifest_entry(
            name='source',
            role='source',
            object_uri=manifest.get('source_object_uri') if isinstance(manifest, Mapping) else None,
            layer=source_layer,
            format=_infer_object_format(manifest.get('source_object_uri') if isinstance(manifest, Mapping) else None),
            schema_version=source_snapshot.get('version') if isinstance(source_snapshot, Mapping) else None,
            schema_fingerprint=source_snapshot.get('fingerprint') if isinstance(source_snapshot, Mapping) else None,
        ),
        _build_artifact_manifest_entry(
            name='target',
            role='derived',
            object_uri=manifest.get('target_object_uri') if isinstance(manifest, Mapping) else None,
            layer=target_layer,
            format=_infer_object_format(manifest.get('target_object_uri') if isinstance(manifest, Mapping) else None),
            row_count=manifest.get('output_row_count') if isinstance(manifest, Mapping) else None,
            schema_version=manifest.get('target_schema_version') if isinstance(manifest, Mapping) else None,
            schema_fingerprint=manifest.get('target_schema_fingerprint') if isinstance(manifest, Mapping) else None,
        ),
    ]

    return {
        'resource_type': 'pipeline_run',
        'resource_id': getattr(run, 'id', None),
        'dataset_id': getattr(run, 'dataset_id', None),
        'ingestion_job_id': getattr(run, 'ingestion_job_id', None),
        'pipeline_id': getattr(run, 'pipeline_id', None),
        'run_id': getattr(run, 'id', None),
        'status': getattr(run, 'status', None),
        'items': [item for item in items if item is not None],
    }
