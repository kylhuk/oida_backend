from __future__ import annotations

import pytest

from data_platform.utils.validation import render_clickhouse_order_by, render_clickhouse_partition_by, validate_serving_config


def test_render_clickhouse_partition_by_quotes_identifier_arguments():
    assert render_clickhouse_partition_by("toYYYYMM(_ingested_at)") == "toYYYYMM(`_ingested_at`)"


def test_render_clickhouse_order_by_allows_string_expression():
    assert render_clickhouse_order_by("toDate(_ingested_at)") == "toDate(`_ingested_at`)"


def test_render_clickhouse_order_by_allows_expression_lists():
    assert render_clickhouse_order_by(["toDate(_ingested_at)", "_ingestion_id"]) == (
        "(toDate(`_ingested_at`), `_ingestion_id`)"
    )


def test_validate_serving_config_normalizes_only_present_fields():
    assert validate_serving_config({"partition_by": "toYYYYMM(_ingested_at)"}) == {
        "partition_by": "toYYYYMM(`_ingested_at`)"
    }
    assert validate_serving_config({"order_by": "_ingestion_id"}) == {
        "order_by": "`_ingestion_id`"
    }
    assert validate_serving_config({}) == {}


@pytest.mark.parametrize(
    "renderer,value,field_name",
    [
        (render_clickhouse_partition_by, "toYYYYMM(_ingested_at) SETTINGS allow_experimental_object_type = 1", "serving_config.partition_by"),
        (render_clickhouse_order_by, "_ingested_at DESC", "serving_config.order_by"),
        (render_clickhouse_order_by, "_ingested_at; DROP TABLE gold.dataset_orders", "serving_config.order_by"),
        (render_clickhouse_order_by, ["_ingestion_id -- comment"], "serving_config.order_by"),
    ],
)
def test_clickhouse_serving_renderers_reject_unsupported_syntax(renderer, value, field_name):
    with pytest.raises(ValueError, match=field_name):
        renderer(value)


def test_render_clickhouse_partition_by_rejects_empty_string():
    with pytest.raises(ValueError, match="serving_config.partition_by"):
        render_clickhouse_partition_by("")


def test_render_clickhouse_partition_by_rejects_non_string_value():
    with pytest.raises(ValueError, match="serving_config.partition_by"):
        render_clickhouse_partition_by(True)


@pytest.mark.parametrize("value", [[None], [True], ["_ingestion_id", None]])
def test_render_clickhouse_order_by_rejects_non_string_list_items(value):
    with pytest.raises(ValueError, match="serving_config.order_by"):
        render_clickhouse_order_by(value)
