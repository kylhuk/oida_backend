from __future__ import annotations


def normalize_catalog_search_query(query: str | None) -> str | None:
    if query is None:
        return None
    normalized = " ".join(query.strip().split())
    return normalized or None


def escape_catalog_like_fragment(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def build_catalog_search_pattern(query: str | None) -> str | None:
    normalized = normalize_catalog_search_query(query)
    if normalized is None:
        return None
    return f"%{escape_catalog_like_fragment(normalized)}%"
