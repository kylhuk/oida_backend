from __future__ import annotations

from enum import Enum


class SchemaMode(str, Enum):
    EVOLVE = "evolve"
    STRICT = "strict"


class DatasetLayer(str, Enum):
    RAW = "raw"
    SILVER = "silver"
    GOLD = "gold"


class IngestionStatus(str, Enum):
    RECEIVED = "received"
    DOWNLOADING = "downloading"
    RAW_REGISTERED = "raw_registered"
    SILVERIZING = "silverizing"
    GOLDIZING = "goldizing"
    LOADING = "loading"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class SourceType(str, Enum):
    UPLOAD = "upload"
    PRESIGNED_UPLOAD = "presigned_upload"
    INLINE_JSON = "inline_json"
    URL = "url"
    OBJECT_URI = "object_uri"
    REPROCESS = "reprocess"


class QualitySeverity(str, Enum):
    WARN = "warn"
    ERROR = "error"


class PipelineEngine(str, Enum):
    SQL = "sql"
    PYTHON = "python"
    DAGSTER = "dagster"


class PipelineStatus(str, Enum):
    PLANNED = "planned"
    BLOCKED = "blocked"
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
