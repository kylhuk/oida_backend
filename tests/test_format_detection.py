from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from data_platform.utils.formats import FileFormat, detect_file_format, normalize_content_type


def _write_extensionless_excel_archive(payload: Path, *, macro_enabled: bool = False, filler_bytes: int = 0) -> None:
    with zipfile.ZipFile(payload, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("xl/workbook.xml", "<workbook />")
        archive.writestr("[Content_Types].xml", "<types />")
        if macro_enabled:
            archive.writestr("xl/vbaProject.bin", b"macro")
        if filler_bytes:
            archive.writestr("xl/filler.bin", b"x" * filler_bytes)


def _write_generic_zip_archive(payload: Path, member_name: str) -> None:
    with zipfile.ZipFile(payload, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr(member_name, "hello")


def test_detect_csv():
    assert detect_file_format("orders.csv") == FileFormat.CSV


def test_detect_ndjson_gz():
    assert detect_file_format("events.ndjson.gz") == FileFormat.NDJSON


def test_detect_from_explicit():
    assert detect_file_format("unknown.bin", explicit_format="parquet") == FileFormat.PARQUET


def test_detect_from_explicit_rejects_binary_claim_when_payload_is_html(tmp_path: Path):
    payload = tmp_path / "payload"
    payload.write_text(
        "<!DOCTYPE html><html><body>Proxy error, please retry later.</body></html>",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unsupported file format"):
        detect_file_format("payload.bin", explicit_format="parquet", local_path=payload)


def test_detect_from_explicit_rejects_text_claim_when_payload_is_markup(tmp_path: Path):
    payload = tmp_path / "payload"
    payload.write_text(
        "<?xml version='1.0'?><Error><Message>missing,key</Message></Error>",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unsupported file format"):
        detect_file_format("payload.bin", explicit_format="csv", local_path=payload)


def test_detect_from_explicit_accepts_matching_excel_payload(tmp_path: Path):
    payload = tmp_path / "spreadsheet"
    _write_extensionless_excel_archive(payload)

    assert detect_file_format("payload.bin", explicit_format="xlsx", local_path=payload) == FileFormat.XLSX


@pytest.mark.parametrize(
    ("filename", "kwargs"),
    [
        ("payload.json", {}),
        ("payload", {"content_type": "application/json"}),
        ("payload.bin", {"explicit_format": "json"}),
    ],
)
def test_detect_from_content_rejects_json_claims_when_payload_is_plain_text(
    tmp_path: Path,
    filename: str,
    kwargs: dict[str, str],
):
    payload = tmp_path / "payload"
    payload.write_text("hello world, still not structured json", encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported file format"):
        detect_file_format(filename, local_path=payload, **kwargs)


@pytest.mark.parametrize(
    ("filename", "kwargs"),
    [
        ("events.ndjson", {}),
        ("payload", {"content_type": "application/x-ndjson"}),
        ("payload.bin", {"explicit_format": "ndjson"}),
    ],
)
def test_detect_from_content_rejects_ndjson_claims_when_payload_is_plain_text(
    tmp_path: Path,
    filename: str,
    kwargs: dict[str, str],
):
    payload = tmp_path / "payload"
    payload.write_text("hello\nworld\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported file format"):
        detect_file_format(filename, local_path=payload, **kwargs)


def test_detect_from_explicit_accepts_single_line_ndjson_payload(tmp_path: Path):
    payload = tmp_path / "payload"
    payload.write_text('{"id": 1}', encoding="utf-8")

    assert detect_file_format("payload.bin", explicit_format="ndjson", local_path=payload) == FileFormat.NDJSON


@pytest.mark.parametrize(
    ("explicit_format", "expected"),
    [
        ("jsonl", FileFormat.NDJSON),
        (" .jsonl ", FileFormat.NDJSON),
        (".csv.gz", FileFormat.CSV),
        ("  .jsonl.gz  ", FileFormat.NDJSON),
        ("ndjson.gz", FileFormat.NDJSON),
        ("json.gz", FileFormat.JSON),
    ],
)
def test_detect_from_explicit_common_aliases(explicit_format: str, expected: FileFormat):
    assert detect_file_format("unknown.bin", explicit_format=explicit_format) == expected


@pytest.mark.parametrize(
    "content_type",
    [
        "text/csv; charset=utf-8",
        "application/jsonl; charset=utf-8",
        "application/jsonlines; charset=utf-8",
        "application/x-jsonlines; charset=utf-8",
    ],
)
def test_detect_from_content_type_aliases_with_charset(content_type: str):
    expected = FileFormat.CSV if content_type.startswith("text/csv") else FileFormat.NDJSON
    assert detect_file_format("orders.unknown", content_type=content_type) == expected


def test_normalize_content_type_strips_parameters():
    assert normalize_content_type("application/json; charset=UTF-8") == "application/json"


def test_detect_by_content_for_extensionless_csv(tmp_path: Path):
    payload = tmp_path / "payload"
    payload.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
    assert detect_file_format("payload", local_path=payload) == FileFormat.CSV


def test_detect_by_content_for_extensionless_ndjson(tmp_path: Path):
    payload = tmp_path / "payload"
    payload.write_text('{"id": 1}\n{"id": 2}\n', encoding="utf-8")
    assert detect_file_format("payload", local_path=payload) == FileFormat.NDJSON


def test_detect_by_content_for_extensionless_json_with_utf8_bom(tmp_path: Path):
    payload = tmp_path / "payload"
    payload.write_bytes('\ufeff{"id": 1}'.encode("utf-8"))
    assert detect_file_format("payload", local_path=payload) == FileFormat.JSON


def test_detect_by_content_for_extensionless_ndjson_with_utf8_bom(tmp_path: Path):
    payload = tmp_path / "payload"
    payload.write_bytes('\ufeff{"id": 1}\n{"id": 2}\n'.encode("utf-8"))
    assert detect_file_format("payload", local_path=payload) == FileFormat.NDJSON


def test_detect_by_content_for_extensionless_utf16_csv(tmp_path: Path):
    payload = tmp_path / "payload"
    payload.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-16")
    assert detect_file_format("payload", local_path=payload) == FileFormat.CSV


def test_detect_extensionless_xlsx_from_zip_signature(tmp_path: Path):
    payload = tmp_path / "spreadsheet"
    _write_extensionless_excel_archive(payload)
    assert detect_file_format("spreadsheet", local_path=payload) == FileFormat.XLSX


def test_detect_extensionless_large_xlsx_from_full_zip_archive(tmp_path: Path):
    payload = tmp_path / "spreadsheet"
    _write_extensionless_excel_archive(payload, filler_bytes=70_000)
    assert payload.stat().st_size > 65_536
    assert detect_file_format("spreadsheet", local_path=payload) == FileFormat.XLSX


def test_detect_extensionless_large_xlsm_from_full_zip_archive(tmp_path: Path):
    payload = tmp_path / "spreadsheet"
    _write_extensionless_excel_archive(payload, macro_enabled=True, filler_bytes=70_000)
    assert payload.stat().st_size > 65_536
    assert detect_file_format("spreadsheet", local_path=payload) == FileFormat.XLSM


@pytest.mark.parametrize("member_name", ["a,b.txt", "a	b.txt"])
def test_detect_from_content_does_not_misclassify_generic_zip_archives(tmp_path: Path, member_name: str):
    payload = tmp_path / "payload"
    _write_generic_zip_archive(payload, member_name)

    with pytest.raises(ValueError, match="Unsupported file format"):
        detect_file_format("payload", local_path=payload)


def test_detect_from_content_fails_cleanly_for_truncated_gzip_magic(tmp_path: Path):
    payload = tmp_path / "payload"
    payload.write_bytes(b"\x1f\x8bnot-a-valid-gzip-stream")

    with pytest.raises(ValueError, match="Unsupported file format"):
        detect_file_format("payload", local_path=payload)


def test_detect_from_content_rejects_corrupt_gzip_that_looks_like_csv(tmp_path: Path):
    payload = tmp_path / "payload"
    payload.write_bytes(b"\x1f\x8bid,name\n1,Alice\n2,Bob\n")

    with pytest.raises(ValueError, match="Unsupported file format"):
        detect_file_format("payload", local_path=payload)


@pytest.mark.parametrize(
    ("filename", "kwargs"),
    [
        ("orders.csv", {}),
        ("payload", {"content_type": "text/csv"}),
        ("payload.bin", {"explicit_format": "csv"}),
    ],
)
def test_detect_from_content_rejects_text_claims_when_payload_is_corrupt_gzip(
    tmp_path: Path,
    filename: str,
    kwargs: dict[str, str],
):
    payload = tmp_path / "payload"
    payload.write_bytes(b"\x1f\x8bnot-a-valid-gzip-stream")

    with pytest.raises(ValueError, match="Unsupported file format"):
        detect_file_format(filename, local_path=payload, **kwargs)


@pytest.mark.parametrize(
    ("filename", "kwargs"),
    [
        ("orders.csv", {}),
        ("payload", {"content_type": "text/csv"}),
        ("payload.bin", {"explicit_format": "csv"}),
    ],
)
def test_detect_from_content_rejects_text_claims_when_payload_is_zip_archive(
    tmp_path: Path,
    filename: str,
    kwargs: dict[str, str],
):
    payload = tmp_path / "payload"
    _write_generic_zip_archive(payload, "a,b.txt")

    with pytest.raises(ValueError, match="Unsupported file format"):
        detect_file_format(filename, local_path=payload, **kwargs)


@pytest.mark.parametrize(
    ("filename", "kwargs"),
    [
        ("orders.csv", {}),
        ("payload", {"content_type": "text/csv"}),
        ("payload.bin", {"explicit_format": "csv"}),
    ],
)
def test_detect_from_content_rejects_text_claims_when_payload_is_pdf(
    tmp_path: Path,
    filename: str,
    kwargs: dict[str, str],
):
    payload = tmp_path / "payload"
    payload.write_bytes(b"%PDF-1.7\n1 0 obj\n<< /Type /Catalog >>\n")

    with pytest.raises(ValueError, match="Unsupported file format"):
        detect_file_format(filename, local_path=payload, **kwargs)


def test_detect_from_content_rejects_text_claims_when_payload_has_binary_nuls(tmp_path: Path):
    payload = tmp_path / "payload"
    payload.write_bytes(b"binary\x00payload\x00with,commas")

    with pytest.raises(ValueError, match="Unsupported file format"):
        detect_file_format("payload.bin", explicit_format="csv", local_path=payload)


def test_detect_from_content_accepts_utf16_csv_claimed_by_text_hint(tmp_path: Path):
    payload = tmp_path / "payload"
    payload.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-16")

    assert detect_file_format("payload", content_type="text/csv", local_path=payload) == FileFormat.CSV


def test_detect_from_content_does_not_misclassify_extensionless_null_byte_payload_as_csv(tmp_path: Path):
    payload = tmp_path / "payload"
    payload.write_bytes(b"\x00\x01id,name\n\x02\x03")

    with pytest.raises(ValueError, match="Unsupported file format"):
        detect_file_format("payload", local_path=payload)


def test_detect_from_content_does_not_misclassify_extensionless_png_payload_as_csv(tmp_path: Path):
    payload = tmp_path / "payload"
    payload.write_bytes(b"\x89PNG\r\n\x1a\n....,....")

    with pytest.raises(ValueError, match="Unsupported file format"):
        detect_file_format("payload", local_path=payload)


def test_detect_from_content_does_not_misclassify_extensionless_html_error_pages(tmp_path: Path):
    payload = tmp_path / "payload"
    payload.write_text(
        "<!DOCTYPE html><html><body>Error, please sign in again.</body></html>",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unsupported file format"):
        detect_file_format("payload", local_path=payload)


def test_detect_from_content_does_not_misclassify_extensionless_xml_error_documents(tmp_path: Path):
    payload = tmp_path / "payload"
    payload.write_text(
        "<?xml version='1.0'?><Error><Code>NoSuchKey</Code><Message>missing,key</Message></Error>",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unsupported file format"):
        detect_file_format("payload", local_path=payload)


def test_detect_from_content_rejects_csv_suffix_when_payload_is_html(tmp_path: Path):
    payload = tmp_path / "orders.csv"
    payload.write_text(
        "<!DOCTYPE html><html><body>Login expired, please sign in again.</body></html>",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unsupported file format"):
        detect_file_format("orders.csv", local_path=payload)


def test_detect_from_content_rejects_csv_content_type_when_payload_is_xml(tmp_path: Path):
    payload = tmp_path / "payload"
    payload.write_text(
        "<?xml version='1.0'?><Error><Message>missing,key</Message></Error>",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unsupported file format"):
        detect_file_format("payload", content_type="text/csv", local_path=payload)


def test_detect_from_content_rejects_parquet_suffix_when_payload_is_html(tmp_path: Path):
    payload = tmp_path / "orders.parquet"
    payload.write_text(
        "<!DOCTYPE html><html><body>Proxy error, please retry later.</body></html>",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unsupported file format"):
        detect_file_format("orders.parquet", local_path=payload)


def test_detect_from_content_rejects_xlsx_content_type_when_payload_is_html(tmp_path: Path):
    payload = tmp_path / "payload"
    payload.write_text(
        "<!DOCTYPE html><html><body>Session expired.</body></html>",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unsupported file format"):
        detect_file_format(
            "payload",
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            local_path=payload,
        )
