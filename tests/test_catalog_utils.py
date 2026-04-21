from __future__ import annotations

from data_platform.utils.catalog import (
    build_catalog_search_pattern,
    escape_catalog_like_fragment,
    normalize_catalog_search_query,
)


def test_normalize_catalog_search_query_collapses_whitespace() -> None:
    assert normalize_catalog_search_query("  orders   pipeline  ") == "orders pipeline"
    assert normalize_catalog_search_query("\n\t  ") is None


def test_escape_catalog_like_fragment_escapes_like_metacharacters() -> None:
    assert escape_catalog_like_fragment(r"100%_done\\check") == "100\\%\\_done\\\\\\\\check"


def test_build_catalog_search_pattern_wraps_normalized_query() -> None:
    assert build_catalog_search_pattern("  orders % ") == "%orders \\%%"
    assert build_catalog_search_pattern(None) is None
