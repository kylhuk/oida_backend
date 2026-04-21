from __future__ import annotations

import pytest

from data_platform.utils.validation import validate_read_only_sql


def test_validate_read_only_sql_allows_semicolon_inside_string_literal():
    query = "SELECT ';' AS delimiter"
    assert validate_read_only_sql(query, "gold_sql") == query


def test_validate_read_only_sql_allows_dangerous_keyword_inside_string_literal():
    query = "SELECT 'DROP TABLE users' AS message"
    assert validate_read_only_sql(query, "gold_sql") == query


def test_validate_read_only_sql_allows_leading_comments_and_trims_terminal_semicolon():
    query = "-- drop is mentioned here only as a comment\nSELECT 1;"
    validated = validate_read_only_sql(query, "gold_sql")
    assert validated.splitlines() == [
        "-- drop is mentioned here only as a comment",
        "SELECT 1",
    ]


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 1; DELETE FROM source",
        "WITH staged AS (DELETE FROM source RETURNING *) SELECT * FROM staged",
    ],
)
def test_validate_read_only_sql_rejects_actual_non_read_only_queries(query: str):
    with pytest.raises(ValueError):
        validate_read_only_sql(query, "gold_sql")


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 'unterminated",
        'SELECT "unterminated',
        "SELECT 1 /* unterminated",
    ],
)
def test_validate_read_only_sql_rejects_unterminated_literals_or_block_comments(query: str):
    with pytest.raises(ValueError, match="unterminated string literal or block comment"):
        validate_read_only_sql(query, "gold_sql")
