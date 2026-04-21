from __future__ import annotations

import pytest

from data_platform.utils.validation import validate_optional_identifier


def test_validate_optional_identifier_normalizes_custom_table_name():
    assert validate_optional_identifier(" Orders Table ", "table_name") == "orders_table"


def test_validate_optional_identifier_allows_missing_value():
    assert validate_optional_identifier(None, "table_name") is None


def test_validate_optional_identifier_rejects_overlong_identifier_after_normalization():
    with pytest.raises(ValueError, match=r"table_name must be at most 255 characters"):
        validate_optional_identifier("x" * 256, "table_name")
