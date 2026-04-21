from data_platform.utils.schemas import (
    assess_schema_compatibility,
    duckdb_to_clickhouse_type,
    diff_schemas,
    merge_schemas,
    normalize_duckdb_type,
    schema_fingerprint,
)


def test_merge_schema_appends_new_columns():
    existing = [{"name": "id", "type": "BIGINT"}]
    new = [{"name": "id", "type": "BIGINT"}, {"name": "name", "type": "VARCHAR"}]
    merged = merge_schemas(existing, new)
    assert merged == [{"name": "id", "type": "BIGINT"}, {"name": "name", "type": "VARCHAR"}]


def test_merge_schema_widens_conflicts():
    existing = [{"name": "amount", "type": "BIGINT"}]
    new = [{"name": "amount", "type": "DOUBLE"}]
    merged = merge_schemas(existing, new)
    assert merged == [{"name": "amount", "type": "DOUBLE"}]


def test_normalize_duckdb_type_treats_list_like_types_as_varchar():
    assert normalize_duckdb_type("INTEGER[]") == "VARCHAR"
    assert normalize_duckdb_type("TIMESTAMP[]") == "VARCHAR"
    assert normalize_duckdb_type("DECIMAL(10,2)[]") == "VARCHAR"


def test_normalize_duckdb_type_canonicalizes_decimal_spacing_and_case():
    assert normalize_duckdb_type(" decimal(10, 2) ") == "DECIMAL(10,2)"
    assert normalize_duckdb_type("DECIMAL(18)") == "DECIMAL(18,0)"


def test_normalize_duckdb_type_treats_numeric_as_decimal():
    assert normalize_duckdb_type("numeric(10, 2)") == "DECIMAL(10,2)"
    assert normalize_duckdb_type("NUMERIC(18)") == "DECIMAL(18,0)"


def test_merge_schema_coerces_list_like_types_to_varchar():
    existing = [{"name": "tags", "type": "VARCHAR"}]
    new = [{"name": "tags", "type": "INTEGER[]"}, {"name": "events", "type": "TIMESTAMP[]"}]
    merged = merge_schemas(existing, new)
    assert merged == [{"name": "tags", "type": "VARCHAR"}, {"name": "events", "type": "VARCHAR"}]


def test_nested_duckdb_types_map_to_clickhouse_string():
    assert duckdb_to_clickhouse_type("INTEGER[]") == "Nullable(String)"


def test_merge_schema_preserves_decimal_scale_when_new_data_is_integer():
    existing = [{"name": "amount", "type": "DECIMAL(10,2)"}]
    new = [{"name": "amount", "type": "BIGINT"}]
    merged = merge_schemas(existing, new)
    assert merged == [{"name": "amount", "type": "DECIMAL(38,2)"}]


def test_merge_schema_preserves_decimal_scale_when_existing_data_is_integer():
    existing = [{"name": "amount", "type": "BIGINT"}]
    new = [{"name": "amount", "type": "DECIMAL(10,2)"}]
    merged = merge_schemas(existing, new)
    assert merged == [{"name": "amount", "type": "DECIMAL(38,2)"}]


def test_merge_schema_uses_highest_decimal_scale_for_decimal_inputs():
    existing = [{"name": "amount", "type": "DECIMAL(10,2)"}]
    new = [{"name": "amount", "type": "DECIMAL(18,6)"}]
    merged = merge_schemas(existing, new)
    assert merged == [{"name": "amount", "type": "DECIMAL(38,6)"}]


def test_merge_schema_does_not_force_incompatible_decimal_to_numeric():
    existing = [{"name": "amount", "type": "DECIMAL(10,2)"}]
    new = [{"name": "amount", "type": "TIMESTAMP"}]
    merged = merge_schemas(existing, new)
    assert merged == [{"name": "amount", "type": "VARCHAR"}]


def test_duckdb_to_clickhouse_type_preserves_decimal_scale():
    assert duckdb_to_clickhouse_type("DECIMAL(10,4)") == "Nullable(Decimal(38,4))"


def test_duckdb_to_clickhouse_type_maps_numeric_to_decimal():
    assert duckdb_to_clickhouse_type("NUMERIC(10,4)") == "Nullable(Decimal(38,4))"


def test_merge_schema_preserves_numeric_scale_when_new_data_is_integer():
    existing = [{"name": "amount", "type": "NUMERIC(10,4)"}]
    new = [{"name": "amount", "type": "BIGINT"}]
    merged = merge_schemas(existing, new)
    assert merged == [{"name": "amount", "type": "DECIMAL(38,4)"}]


def test_schema_fingerprint_treats_equivalent_decimal_spellings_as_equal():
    left = schema_fingerprint([{"name": "amount", "type": "decimal(10, 2)"}])
    right = schema_fingerprint([{"name": "amount", "type": "DECIMAL(10,2)"}])
    assert left == right



def test_diff_schemas_reports_added_removed_and_changed_columns():
    diff = diff_schemas(
        [
            {"name": "id", "type": "BIGINT"},
            {"name": "amount", "type": "BIGINT"},
            {"name": "legacy", "type": "VARCHAR"},
        ],
        [
            {"name": "id", "type": "BIGINT"},
            {"name": "amount", "type": "DOUBLE"},
            {"name": "status", "type": "VARCHAR"},
        ],
    )

    assert diff["added_columns"] == [{"name": "status", "type": "VARCHAR"}]
    assert diff["removed_columns"] == [{"name": "legacy", "type": "VARCHAR"}]
    assert diff["changed_columns"] == [
        {"name": "amount", "from_type": "BIGINT", "to_type": "DOUBLE", "compatible": True}
    ]
    assert diff["breaking_changes"] is True
    assert diff["has_changes"] is True


def test_diff_schemas_uses_empty_baseline_when_from_schema_missing():
    diff = diff_schemas(None, [{"name": "id", "type": "INTEGER"}])

    assert diff["added_columns"] == [{"name": "id", "type": "BIGINT"}]
    assert diff["removed_columns"] == []
    assert diff["changed_columns"] == []
    assert diff["breaking_changes"] is False


def test_assess_schema_compatibility_marks_compatible_widening_and_strict_rejection():
    report = assess_schema_compatibility(
        [{"name": "amount", "type": "BIGINT"}],
        [{"name": "amount", "type": "DOUBLE"}],
    )

    assert report["contract_compatible"] is True
    assert report["strict_mode_compatible"] is False
    assert report["merged_schema"] == [{"name": "amount", "type": "DOUBLE"}]



def test_assess_schema_compatibility_marks_column_removal_as_contract_breaking():
    report = assess_schema_compatibility(
        [{"name": "id", "type": "BIGINT"}, {"name": "legacy", "type": "VARCHAR"}],
        [{"name": "id", "type": "BIGINT"}],
    )

    assert report["contract_compatible"] is False
    assert report["removed_columns"] == [{"name": "legacy", "type": "VARCHAR"}]
    assert report["merged_schema"] == [{"name": "id", "type": "BIGINT"}, {"name": "legacy", "type": "VARCHAR"}]
