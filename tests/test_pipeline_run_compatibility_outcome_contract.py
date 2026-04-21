from pathlib import Path


def test_pipeline_run_response_includes_contract_compatibility_outcome_field() -> None:
    text = Path("src/data_platform/schemas/pipeline.py").read_text()
    assert "contract_compatibility_outcome: str | None = None" in text


def test_extract_pipeline_run_snapshot_reads_contract_compatibility_outcome() -> None:
    text = Path("src/data_platform/utils/pipeline_definitions.py").read_text()
    assert '"contract_compatibility_outcome": _extract_contract_compatibility_outcome(' in text
    assert 'context["contract_compatibility_outcome"] = compatibility_outcome' in text
