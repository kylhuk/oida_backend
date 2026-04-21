import pytest

from data_platform.utils.paths import (
    build_layer_object_key,
    build_object_storage_url,
    object_uri,
    parse_object_uri,
    sanitize_filename,
)


def test_build_layer_object_key_contains_dataset_and_ingestion():
    key = build_layer_object_key("orders", "abc123", "orders.csv")
    assert "orders/" in key
    assert "/abc123/" in key
    assert key.endswith("/orders.csv")


def test_parse_object_uri():
    bucket, key = parse_object_uri("s3://gold/orders/part.parquet")
    assert bucket == "gold"
    assert key == "orders/part.parquet"


def test_object_uri_percent_encodes_reserved_key_characters_and_round_trips():
    uri = object_uri("gold", "orders archive/part#01.parquet")
    assert uri == "s3://gold/orders%20archive/part%2301.parquet"
    assert parse_object_uri(uri) == ("gold", "orders archive/part#01.parquet")


def test_object_uri_preserves_common_safe_path_characters():
    uri = object_uri("gold", "orders/year=2026/month=04/part_01.parquet")
    assert uri == "s3://gold/orders/year=2026/month=04/part_01.parquet"


def test_object_uri_trims_surrounding_bucket_whitespace_and_normalizes_leading_key_slash():
    assert object_uri("  gold  ", "/orders/part.parquet") == "s3://gold/orders/part.parquet"


def test_sanitize_filename_replaces_dot_segment_with_fallback_name():
    assert sanitize_filename(".") == "payload.bin"
    assert sanitize_filename("..") == "payload.bin"
    assert sanitize_filename(" ../.. ") == "payload.bin"


def test_build_layer_object_key_never_emits_dot_segment_filename():
    key = build_layer_object_key("orders", "abc123", "..")
    assert key.endswith("/payload.bin")
    assert "/../" not in key


def test_sanitize_filename_preserves_normal_leaf_name_after_path_stripping():
    assert sanitize_filename("../nested/orders.csv") == "orders.csv"


def test_sanitize_filename_truncates_long_name_and_preserves_multi_suffix():
    filename = ("a" * 300) + ".csv.gz"
    sanitized = sanitize_filename(filename)
    assert len(sanitized) == 255
    assert sanitized.endswith(".csv.gz")


def test_build_layer_object_key_uses_truncated_filename_for_overlong_names():
    filename = ("b" * 320) + ".parquet"
    key = build_layer_object_key("orders", "abc123", filename)
    leaf_name = key.rsplit("/", 1)[-1]
    assert len(leaf_name) == 255
    assert leaf_name.endswith(".parquet")


def test_parse_object_uri_accepts_s3a_and_trims_whitespace():
    bucket, key = parse_object_uri("  s3a://gold/orders/part.parquet  ")
    assert bucket == "gold"
    assert key == "orders/part.parquet"


def test_parse_object_uri_accepts_s3n_and_decodes_key():
    bucket, key = parse_object_uri("s3n://gold/orders%20archive/part%2001.parquet")
    assert bucket == "gold"
    assert key == "orders archive/part 01.parquet"


def test_parse_object_uri_rejects_empty_or_dot_segment_keys():
    for uri in ["s3://gold/", "s3://gold/.", "s3://gold/.."]:
        with pytest.raises(ValueError):
            parse_object_uri(uri)


def test_parse_object_uri_rejects_query_strings_and_fragments():
    for uri in [
        "s3://gold/orders/part.parquet?versionId=123",
        "s3://gold/orders/part.parquet#latest",
    ]:
        with pytest.raises(ValueError):
            parse_object_uri(uri)


def test_parse_object_uri_accepts_percent_encoded_reserved_characters_in_key():
    bucket, key = parse_object_uri("s3://gold/orders%3Farchive/part%2301.parquet")
    assert bucket == "gold"
    assert key == "orders?archive/part#01.parquet"


def test_build_object_storage_url_encodes_reserved_key_characters():
    url = build_object_storage_url(
        "http://minio:9000",
        "gold",
        "orders archive/part #01?.parquet",
    )
    assert url == "http://minio:9000/gold/orders%20archive/part%20%2301%3F.parquet"


def test_build_object_storage_url_preserves_endpoint_path_prefix():
    url = build_object_storage_url(
        "https://storage.example/internal/minio/",
        "gold",
        "exports/part+01.parquet",
    )
    assert url == "https://storage.example/internal/minio/gold/exports/part%2B01.parquet"


def test_build_object_storage_url_trims_surrounding_bucket_whitespace():
    url = build_object_storage_url(
        "http://minio:9000",
        "  gold  ",
        "orders/part.parquet",
    )
    assert url == "http://minio:9000/gold/orders/part.parquet"




def test_build_object_storage_url_rejects_endpoint_query_or_fragment():
    for endpoint in [
        "http://minio:9000?x=1",
        "http://minio:9000#frag",
        "https://storage.example/internal?x=1",
    ]:
        with pytest.raises(ValueError, match="endpoint URL"):
            build_object_storage_url(endpoint, "gold", "orders/part.parquet")


def test_build_object_storage_url_rejects_endpoint_without_http_scheme_or_host():
    for endpoint in ["minio:9000", "//minio:9000", "http:///missing-host", "   "]:
        with pytest.raises(ValueError, match="endpoint URL"):
            build_object_storage_url(endpoint, "gold", "orders/part.parquet")


def test_parse_object_uri_allows_hidden_segments_that_are_not_dot_segments():
    bucket, key = parse_object_uri("s3://gold/orders/.hidden/part.parquet")
    assert bucket == "gold"
    assert key == "orders/.hidden/part.parquet"


@pytest.mark.parametrize(
    "key",
    [
        "orders/./part.parquet",
        "orders/../part.parquet",
        "./orders/part.parquet",
        "../orders/part.parquet",
    ],
)
def test_object_uri_rejects_dot_segment_path_components(key: str):
    with pytest.raises(ValueError, match="dot-segment"):
        object_uri("gold", key)


@pytest.mark.parametrize(
    "key",
    [
        "orders/./part.parquet",
        "orders/../part.parquet",
        "./orders/part.parquet",
        "../orders/part.parquet",
    ],
)
def test_build_object_storage_url_rejects_dot_segment_path_components(key: str):
    with pytest.raises(ValueError, match="dot-segment"):
        build_object_storage_url("http://minio:9000", "gold", key)


@pytest.mark.parametrize(
    "uri",
    [
        "s3://gold/orders/./part.parquet",
        "s3://gold/orders/../part.parquet",
        "s3://gold/orders/%2E%2E/part.parquet",
        "s3://gold/%2E/orders/part.parquet",
    ],
)
def test_parse_object_uri_rejects_dot_segment_path_components(uri: str):
    with pytest.raises(ValueError, match=r"Invalid S3-compatible URI.*dot-segment"):
        parse_object_uri(uri)


def test_parse_object_uri_preserves_normalized_object_key_error_detail():
    with pytest.raises(
        ValueError,
        match=r"Invalid S3-compatible URI 's3://gold/orders/%2E%2E/part.parquet': Object key cannot contain dot-segment path components",
    ):
        parse_object_uri("s3://gold/orders/%2E%2E/part.parquet")


@pytest.mark.parametrize("bucket", ["", " ", "gold raw", "gold/raw", "gold?raw", "gold#raw", "gold%20raw"])
def test_object_uri_rejects_invalid_bucket_names(bucket: str):
    with pytest.raises(ValueError, match="Bucket name"):
        object_uri(bucket, "orders/part.parquet")


@pytest.mark.parametrize("uri", ["s3://gold raw/orders/part.parquet", "s3://gold%20raw/orders/part.parquet"])
def test_parse_object_uri_rejects_invalid_bucket_names(uri: str):
    with pytest.raises(ValueError, match="Bucket name"):
        parse_object_uri(uri)


@pytest.mark.parametrize("key", ["", "/", ".", "..", "/.."])
def test_object_uri_rejects_invalid_object_keys(key: str):
    with pytest.raises(ValueError, match="Object key"):
        object_uri("gold", key)


@pytest.mark.parametrize("key", ["", "/", ".", "..", "/.."])
def test_build_object_storage_url_rejects_invalid_object_keys(key: str):
    with pytest.raises(ValueError, match="Object key"):
        build_object_storage_url("http://minio:9000", "gold", key)


def test_build_object_storage_url_rejects_endpoint_with_embedded_credentials():
    for endpoint in [
        "http://access:secret@minio:9000",
        "https://token@storage.example/internal",
    ]:
        with pytest.raises(ValueError, match="embedded credentials"):
            build_object_storage_url(endpoint, "gold", "orders/part.parquet")
