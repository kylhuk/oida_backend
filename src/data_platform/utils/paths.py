from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit, urlunsplit


_S3_URI_RE = re.compile(r"^(s3|s3a|s3n)://([^/]+)/(.+)$", re.IGNORECASE)


_DOT_ONLY_FILENAME_RE = re.compile(r"^\.+$")
_INVALID_BUCKET_CHARACTER_RE = re.compile(r"[\s/\\?#%]")
_FALLBACK_FILENAME = "payload.bin"
_MAX_FILENAME_LENGTH = 255


def _truncate_filename(filename: str, max_length: int = _MAX_FILENAME_LENGTH) -> str:
    if len(filename) <= max_length:
        return filename

    suffix = "".join(Path(filename).suffixes)
    if suffix and len(suffix) < max_length:
        base = filename[: -len(suffix)]
        base_length = max_length - len(suffix)
        return f"{base[:base_length]}{suffix}"

    return filename[:max_length]


def sanitize_filename(filename: str) -> str:
    filename = filename.strip().replace("\\", "/").split("/")[-1]
    filename = re.sub(r"[^A-Za-z0-9._-]+", "_", filename)
    if not filename or _DOT_ONLY_FILENAME_RE.match(filename):
        return _FALLBACK_FILENAME
    return _truncate_filename(filename)


def build_layer_object_key(
    dataset_slug: str,
    ingestion_id: str,
    filename: str,
    event_time: datetime | None = None,
) -> str:
    event_time = event_time or datetime.now(timezone.utc)
    safe_filename = sanitize_filename(filename)
    return (
        f"{dataset_slug}/year={event_time:%Y}/month={event_time:%m}/day={event_time:%d}/"
        f"{ingestion_id}/{safe_filename}"
    )


def normalize_bucket_name(bucket: str) -> str:
    normalized = bucket.strip()
    if not normalized:
        raise ValueError("Bucket name cannot be empty.")
    if _INVALID_BUCKET_CHARACTER_RE.search(normalized):
        raise ValueError(
            "Bucket name must not contain whitespace, path separators, or URI query/fragment delimiters."
        )
    return normalized


def normalize_object_key(key: str) -> str:
    normalized = key.lstrip("/")
    if not normalized:
        raise ValueError("Object key cannot be empty.")

    if any(segment in {".", ".."} for segment in normalized.split("/")):
        raise ValueError("Object key cannot contain dot-segment path components.")

    return normalized


_OBJECT_URI_SAFE_KEY_CHARS = "/-_.~="


def object_uri(bucket: str, key: str) -> str:
    normalized_bucket = normalize_bucket_name(bucket)
    encoded_key = quote(normalize_object_key(key), safe=_OBJECT_URI_SAFE_KEY_CHARS)
    return f"s3://{normalized_bucket}/{encoded_key}"


def build_object_storage_url(endpoint_url: str, bucket: str, key: str) -> str:
    normalized_endpoint = endpoint_url.strip()
    parts = urlsplit(normalized_endpoint)
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        raise ValueError("Object storage endpoint URL must include an http or https scheme and network location.")
    if parts.username is not None or parts.password is not None:
        raise ValueError("Object storage endpoint URL must not include embedded credentials.")
    if parts.query or parts.fragment:
        raise ValueError("Object storage endpoint URL must not include query strings or fragments.")

    base_path = parts.path.rstrip("/")
    encoded_bucket = quote(normalize_bucket_name(bucket), safe="-._~")
    encoded_key = quote(normalize_object_key(key), safe="/-._~=")

    path_segments = [segment for segment in [base_path, encoded_bucket, encoded_key] if segment]
    resolved_path = "/".join(path_segments)
    if not resolved_path.startswith("/"):
        resolved_path = f"/{resolved_path}"

    return urlunsplit(parts._replace(path=resolved_path, query="", fragment=""))


def parse_object_uri(uri: str) -> tuple[str, str]:
    normalized = uri.strip()
    match = _S3_URI_RE.match(normalized)
    if not match:
        raise ValueError(f"Expected S3-compatible URI, got {uri!r}.")

    parsed = urlsplit(normalized)
    if parsed.query or parsed.fragment:
        raise ValueError(f"S3-compatible object URIs must not include query strings or fragments: {uri!r}.")

    bucket = normalize_bucket_name(match.group(2))
    raw_path = parsed.path[1:] if parsed.path.startswith("/") else parsed.path
    try:
        key = normalize_object_key(unquote(raw_path))
    except ValueError as exc:
        raise ValueError(f"Invalid S3-compatible URI {uri!r}: {exc}") from exc
    return bucket, key
