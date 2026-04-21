from __future__ import annotations

import gzip
import io
import zipfile
from enum import Enum
from pathlib import Path


class FileFormat(str, Enum):
    CSV = "csv"
    TSV = "tsv"
    JSON = "json"
    NDJSON = "ndjson"
    PARQUET = "parquet"
    XLSX = "xlsx"
    XLSM = "xlsm"


_UTF8_BOM = b"\xef\xbb\xbf"
_UTF16_BOMS = (b"\xff\xfe", b"\xfe\xff")
_OBVIOUS_BINARY_SIGNATURES = (
    b"%PDF-",
    b"\x89PNG\r\n\x1a\n",
    b"\xff\xd8\xff",
    b"GIF87a",
    b"GIF89a",
)

_MIME_MAP = {
    "text/csv": FileFormat.CSV,
    "text/tab-separated-values": FileFormat.TSV,
    "application/json": FileFormat.JSON,
    "application/x-ndjson": FileFormat.NDJSON,
    "application/ndjson": FileFormat.NDJSON,
    "application/jsonl": FileFormat.NDJSON,
    "application/jsonlines": FileFormat.NDJSON,
    "application/x-jsonlines": FileFormat.NDJSON,
    "application/parquet": FileFormat.PARQUET,
    "application/vnd.apache.parquet": FileFormat.PARQUET,
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": FileFormat.XLSX,
    "application/vnd.ms-excel.sheet.macroenabled.12": FileFormat.XLSM,
}

_EXPLICIT_FORMAT_ALIASES = {
    "csv": FileFormat.CSV,
    "csv.gz": FileFormat.CSV,
    "tsv": FileFormat.TSV,
    "tsv.gz": FileFormat.TSV,
    "json": FileFormat.JSON,
    "json.gz": FileFormat.JSON,
    "jsonl": FileFormat.NDJSON,
    "jsonl.gz": FileFormat.NDJSON,
    "ndjson": FileFormat.NDJSON,
    "ndjson.gz": FileFormat.NDJSON,
    "parquet": FileFormat.PARQUET,
    "xlsx": FileFormat.XLSX,
    "xlsm": FileFormat.XLSM,
}


def normalize_content_type(content_type: str | None) -> str | None:
    if not content_type:
        return None
    return content_type.split(";", 1)[0].strip().lower() or None


def normalize_explicit_format(explicit_format: str | None) -> FileFormat | None:
    if explicit_format is None:
        return None
    key = explicit_format.strip().lower().lstrip(".")
    if not key:
        return None
    try:
        return _EXPLICIT_FORMAT_ALIASES[key]
    except KeyError as exc:
        raise ValueError(f"Unsupported explicit file format {explicit_format!r}.") from exc


def _detect_by_suffix(filename: str) -> FileFormat | None:
    suffixes = [suffix.lower().lstrip(".") for suffix in Path(filename).suffixes]
    if not suffixes:
        return None

    if suffixes[-2:] == ["csv", "gz"]:
        return FileFormat.CSV
    if suffixes[-2:] == ["tsv", "gz"]:
        return FileFormat.TSV
    if suffixes[-2:] == ["json", "gz"]:
        return FileFormat.JSON
    if suffixes[-2:] == ["jsonl", "gz"]:
        return FileFormat.NDJSON
    if suffixes[-2:] == ["ndjson", "gz"]:
        return FileFormat.NDJSON

    suffix = suffixes[-1]
    if suffix == "csv":
        return FileFormat.CSV
    if suffix == "tsv":
        return FileFormat.TSV
    if suffix == "json":
        return FileFormat.JSON
    if suffix in {"jsonl", "ndjson"}:
        return FileFormat.NDJSON
    if suffix == "parquet":
        return FileFormat.PARQUET
    if suffix == "xlsx":
        return FileFormat.XLSX
    if suffix == "xlsm":
        return FileFormat.XLSM
    return None


def _read_sniff_bytes(local_path: str | Path, max_bytes: int = 65536) -> bytes | None:
    path = Path(local_path)
    with path.open("rb") as source:
        prefix = source.read(4)
        source.seek(0)
        if prefix.startswith(b"\x1f\x8b"):
            try:
                with gzip.open(path, "rb") as gzip_source:
                    return gzip_source.read(max_bytes)
            except (OSError, EOFError):
                # Some uploads may be truncated or otherwise corrupted while still
                # carrying the gzip magic bytes. Treat them as unsupported instead
                # of falling back to the raw bytes, which can misclassify broken
                # gzip-looking payloads as text formats such as CSV or JSON.
                return None
        return source.read(max_bytes)


def _detect_excel_from_zip(source: bytes | str | Path) -> FileFormat | None:
    archive_source: io.BytesIO | str | Path
    if isinstance(source, bytes):
        archive_source = io.BytesIO(source)
    else:
        archive_source = source

    try:
        with zipfile.ZipFile(archive_source) as archive:
            names = set(archive.namelist())
    except (zipfile.BadZipFile, OSError):
        return None

    if "xl/workbook.xml" not in names:
        return None
    if "xl/vbaProject.bin" in names:
        return FileFormat.XLSM
    return FileFormat.XLSX


def _decode_text_sniff(payload: bytes) -> str:
    if payload.startswith(_UTF8_BOM):
        return payload.decode("utf-8-sig", errors="ignore")
    if payload.startswith(_UTF16_BOMS):
        try:
            return payload.decode("utf-16")
        except UnicodeDecodeError:
            return payload.decode("utf-8", errors="ignore")
    return payload.decode("utf-8", errors="ignore")


def _looks_like_ndjson(lines: list[str]) -> bool:
    if len(lines) < 2:
        return False
    non_empty = [line.strip() for line in lines if line.strip()]
    if len(non_empty) < 2:
        return False
    return all(line.startswith("{") or line.startswith("[") for line in non_empty[:20])


def _looks_like_markup_document(text: str) -> bool:
    normalized = text.lstrip().lower()
    return normalized.startswith(("<!doctype html", "<html", "<?xml", "<error>"))


def _sniff_text_payload(local_path: str | Path) -> str | None:
    sniff = _read_sniff_bytes(local_path)
    if not sniff:
        return None
    return _decode_text_sniff(sniff).strip() or None


def _looks_like_unsupported_markup_payload(local_path: str | Path) -> bool:
    text = _sniff_text_payload(local_path)
    if not text:
        return False
    return _looks_like_markup_document(text)


def _claimed_binary_format_matches_content(local_path: str | Path, claimed_format: FileFormat) -> bool:
    sniff = _read_sniff_bytes(local_path)
    if not sniff:
        return False

    if claimed_format == FileFormat.PARQUET:
        return sniff.startswith(b"PAR1")

    if claimed_format == FileFormat.XLSX:
        return _detect_excel_from_zip(local_path) == FileFormat.XLSX

    if claimed_format == FileFormat.XLSM:
        return _detect_excel_from_zip(local_path) == FileFormat.XLSM

    return True


def _looks_like_json_text(text: str) -> bool:
    return text.startswith("{") or text.startswith("[")


def _looks_like_claimed_ndjson_text(text: str) -> bool:
    non_empty = [line.strip() for line in text.splitlines() if line.strip()]
    if not non_empty:
        return False
    return all(line.startswith("{") or line.startswith("[") for line in non_empty[:20])


def _looks_like_obvious_binary_payload(payload: bytes) -> bool:
    if payload.startswith(_UTF16_BOMS):
        return False

    if payload.startswith(_OBVIOUS_BINARY_SIGNATURES):
        return True

    return b"\x00" in payload


def _validate_claimed_text_format_matches_content(
    filename: str,
    local_path: str | Path,
    claimed_format: FileFormat,
) -> None:
    sniff = _read_sniff_bytes(local_path)
    if not sniff:
        raise ValueError(f"Unsupported file format for filename={filename!r}.")

    if sniff.startswith((b"PAR1", b"PK\x03\x04")) or _looks_like_obvious_binary_payload(sniff):
        raise ValueError(f"Unsupported file format for filename={filename!r}.")

    text = _decode_text_sniff(sniff).strip()
    if not text or _looks_like_markup_document(text):
        raise ValueError(f"Unsupported file format for filename={filename!r}.")

    if claimed_format == FileFormat.JSON and not _looks_like_json_text(text):
        raise ValueError(f"Unsupported file format for filename={filename!r}.")

    if claimed_format == FileFormat.NDJSON and not _looks_like_claimed_ndjson_text(text):
        raise ValueError(f"Unsupported file format for filename={filename!r}.")


def _validate_claimed_format_against_content(
    filename: str,
    local_path: str | Path | None,
    claimed_format: FileFormat,
) -> None:
    if local_path is None:
        return

    if claimed_format in {FileFormat.CSV, FileFormat.TSV, FileFormat.JSON, FileFormat.NDJSON}:
        _validate_claimed_text_format_matches_content(filename, local_path, claimed_format)
        return

    if claimed_format in {FileFormat.PARQUET, FileFormat.XLSX, FileFormat.XLSM}:
        if not _claimed_binary_format_matches_content(local_path, claimed_format):
            raise ValueError(f"Unsupported file format for filename={filename!r}.")


def _detect_from_content(local_path: str | Path) -> FileFormat | None:
    sniff = _read_sniff_bytes(local_path)
    if not sniff:
        return None

    if sniff.startswith(b"PAR1"):
        return FileFormat.PARQUET

    if sniff.startswith(b"PK\x03\x04"):
        # Supported ZIP-based uploads are limited to Excel workbooks. Do not
        # fall through to text heuristics for generic ZIP archives because
        # member names can contain commas or tabs and trigger false CSV/TSV
        # detection for otherwise unsupported binary payloads.
        return _detect_excel_from_zip(local_path)

    if _looks_like_obvious_binary_payload(sniff):
        return None

    text = _decode_text_sniff(sniff).strip()
    if not text:
        return None

    lines = text.splitlines()
    if _looks_like_ndjson(lines):
        return FileFormat.NDJSON

    if text.startswith("{") or text.startswith("["):
        return FileFormat.JSON

    if _looks_like_markup_document(text):
        return None

    sample_lines = [line for line in lines[:20] if line.strip()]
    if sample_lines:
        tab_score = sum(line.count("\t") for line in sample_lines)
        comma_score = sum(line.count(",") for line in sample_lines)
        if tab_score > 0 and tab_score >= comma_score:
            return FileFormat.TSV
        if comma_score > 0:
            return FileFormat.CSV

    return None


def detect_file_format(
    filename: str,
    content_type: str | None = None,
    explicit_format: str | None = None,
    local_path: str | Path | None = None,
) -> FileFormat:
    normalized_explicit_format = normalize_explicit_format(explicit_format)
    if normalized_explicit_format is not None:
        _validate_claimed_format_against_content(filename, local_path, normalized_explicit_format)
        return normalized_explicit_format

    normalized_content_type = normalize_content_type(content_type)
    if normalized_content_type and normalized_content_type in _MIME_MAP:
        detected_from_content_type = _MIME_MAP[normalized_content_type]
        _validate_claimed_format_against_content(filename, local_path, detected_from_content_type)
        return detected_from_content_type

    detected_by_suffix = _detect_by_suffix(filename)
    if detected_by_suffix is not None:
        _validate_claimed_format_against_content(filename, local_path, detected_by_suffix)
        return detected_by_suffix

    if local_path is not None:
        detected_by_content = _detect_from_content(local_path)
        if detected_by_content is not None:
            return detected_by_content

    raise ValueError(f"Unsupported file format for filename={filename!r}.")
