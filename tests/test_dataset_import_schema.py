from pathlib import Path

SCHEMA_PATH = Path("src/data_platform/schemas/dataset.py")


def test_dataset_import_schema_declares_request_models_and_validators():
    text = SCHEMA_PATH.read_text()

    assert "class DatasetImportDatasetRequest(BaseModel):" in text
    assert "class DataProductImportRequest(CreateDataProductRequest):" in text
    assert "class PipelineDefinitionImportRequest(BaseModel):" in text
    assert "class SchemaSnapshotImportRequest(BaseModel):" in text
    assert "class DatasetImportRequest(BaseModel):" in text
    assert 'fingerprint: str | None = None' in text
    assert 'schema_items: list[dict[str, str]] = Field(alias="schema_json", serialization_alias="schema_json")' in text
    assert 'At most one imported data product can be marked as default.' in text
    assert "schema_snapshots for layer '" in text
    assert 'schema_json fingerprint does not match the supplied fingerprint.' in text


def test_dataset_import_schema_supports_export_round_trip_shape():
    text = SCHEMA_PATH.read_text()

    assert 'quality_rules: list[QualityRuleCreate] = Field(default_factory=list)' in text
    assert 'data_products: list[DataProductImportRequest] = Field(default_factory=list)' in text
    assert 'pipelines: list[PipelineDefinitionImportRequest] = Field(default_factory=list)' in text
    assert 'schema_snapshots: list[SchemaSnapshotImportRequest] = Field(default_factory=list)' in text
    assert 'slug: str' in text
    assert 'gold_table_name: str | None = None' in text
    assert 'serving_config: dict[str, Any] = Field(default_factory=dict)' in text
