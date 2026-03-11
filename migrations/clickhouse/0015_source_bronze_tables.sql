CREATE TABLE IF NOT EXISTS bronze.src_seed_gdelt_v1
(
    raw_id String,
    fetch_id String,
    source_id LowCardinality(String),
    parser_id LowCardinality(String),
    parser_version String,
    source_record_key String,
    source_record_index UInt32,
    record_kind LowCardinality(String),
    native_id Nullable(String),
    source_url String,
    canonical_url Nullable(String),
    fetched_at DateTime64(3, 'UTC'),
    parsed_at DateTime64(3, 'UTC'),
    occurred_at Nullable(DateTime64(3, 'UTC')),
    published_at Nullable(DateTime64(3, 'UTC')),
    title Nullable(String),
    summary Nullable(String),
    status Nullable(String),
    place_hint Nullable(String),
    lat Nullable(Float64),
    lon Nullable(Float64),
    severity Nullable(String),
    content_hash String,
    schema_version UInt32,
    record_version UInt64,
    attrs String,
    evidence String,
    payload_json String
)
ENGINE = ReplacingMergeTree(record_version)
PARTITION BY toYYYYMM(parsed_at)
ORDER BY (source_record_key, parsed_at, raw_id, source_record_index);

CREATE TABLE IF NOT EXISTS bronze.src_fixture_reliefweb_v1 AS bronze.src_seed_gdelt_v1;

CREATE TABLE IF NOT EXISTS bronze.src_fixture_acled_v1 AS bronze.src_seed_gdelt_v1;

CREATE TABLE IF NOT EXISTS bronze.src_fixture_opensanctions_v1 AS bronze.src_seed_gdelt_v1;

CREATE TABLE IF NOT EXISTS bronze.src_fixture_nasa_firms_v1 AS bronze.src_seed_gdelt_v1;

CREATE TABLE IF NOT EXISTS bronze.src_fixture_noaa_hazards_v1 AS bronze.src_seed_gdelt_v1;

CREATE TABLE IF NOT EXISTS bronze.src_fixture_kev_v1 AS bronze.src_seed_gdelt_v1;
