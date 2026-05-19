-- Artifact registry: index of objects stored in MinIO/S3.
-- Workers INSERT a row alongside each PutObject call; the API reads it for GET /v1/artifacts/{ref}.
-- The artifact_ref convention is: art:<bucket>:<base64url(object_key)>

CREATE TABLE IF NOT EXISTS meta.artifact
(
    artifact_ref      String,
    bucket            String,
    object_key        String,
    content_type      LowCardinality(String) DEFAULT '',
    content_length    UInt64 DEFAULT 0,
    marking           LowCardinality(String) DEFAULT 'UNCLASSIFIED',
    source_id         String DEFAULT '',
    snapshot_id       String DEFAULT 'live',
    sha256            String DEFAULT '',
    created_at        DateTime64(3, 'UTC') DEFAULT now64(3),
    schema_version    UInt16  DEFAULT 1,
    record_version    UInt64  DEFAULT 1,
    api_contract_version UInt16 DEFAULT 1,
    updated_at        DateTime64(3, 'UTC') DEFAULT now64(3),
    attrs             String DEFAULT '{}',
    evidence          String DEFAULT '[]'
) ENGINE = ReplacingMergeTree(record_version)
ORDER BY (artifact_ref);

CREATE OR REPLACE VIEW gold.api_v1_artifacts AS
SELECT
    artifact_ref,
    bucket,
    object_key,
    content_type,
    content_length,
    marking AS artifact_marking,
    source_id,
    snapshot_id,
    sha256,
    created_at
FROM meta.artifact
FINAL;
