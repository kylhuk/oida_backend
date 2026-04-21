from __future__ import annotations

import pytest

from data_platform.schemas.dataset import CreateDatasetRequest, QualityRuleCreate, UpdateDatasetRequest
from data_platform.utils.validation import validate_read_only_sql, validate_slug


def test_validate_slug_normalizes_and_accepts_valid_value():
    assert validate_slug("Orders_2025") == "orders_2025"


@pytest.mark.parametrize(
    "value",
    ["", "Orders!", "UPPER CASE", "*invalid*"],
)
def test_validate_slug_rejects_invalid_values(value: str):
    with pytest.raises(ValueError):
        validate_slug(value)


def test_validate_read_only_sql_allows_select_and_trims_semicolon():
    assert validate_read_only_sql("SELECT * FROM source;", "gold_sql") == "SELECT * FROM source"


@pytest.mark.parametrize(
    "query",
    [
        "DELETE FROM source",
        "SELECT * FROM source; DROP TABLE x",
        "COPY source TO '/tmp/file'",
    ],
)
def test_validate_read_only_sql_rejects_mutating_queries(query: str):
    with pytest.raises(ValueError):
        validate_read_only_sql(query, "gold_sql")


def test_dataset_request_validates_transforms_and_quality_rule_sql():
    payload = CreateDatasetRequest(
        slug="orders",
        name="Orders",
        gold_sql="SELECT * FROM source;",
        quality_rules=[
            QualityRuleCreate(
                name="has_rows",
                layer="gold",
                severity="error",
                sql_expression="SELECT COUNT(*) > 0 AS passed FROM source;",
            )
        ],
    )
    assert payload.gold_sql == "SELECT * FROM source"
    assert payload.quality_rules[0].sql_expression == "SELECT COUNT(*) > 0 AS passed FROM source"


def test_dataset_request_validates_and_normalizes_serving_config():
    payload = CreateDatasetRequest(
        slug="orders",
        name="Orders",
        serving_config={
            "partition_by": "toYYYYMM(_ingested_at)",
            "order_by": ["toDate(_ingested_at)", "_ingestion_id"],
        },
    )

    assert payload.serving_config == {
        "partition_by": "toYYYYMM(`_ingested_at`)",
        "order_by": "(toDate(`_ingested_at`), `_ingestion_id`)",
    }


def test_dataset_request_rejects_invalid_serving_config():
    with pytest.raises(ValueError, match="serving_config.order_by"):
        CreateDatasetRequest(
            slug="orders",
            name="Orders",
            serving_config={"order_by": "_ingested_at DESC"},
        )


def test_update_dataset_request_validates_serving_config_without_adding_defaults():
    payload = UpdateDatasetRequest(serving_config={"partition_by": "toYYYYMM(_ingested_at)"})

    assert payload.serving_config == {"partition_by": "toYYYYMM(`_ingested_at`)"}
