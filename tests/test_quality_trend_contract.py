from __future__ import annotations

from pathlib import Path


ROUTES_PATH = Path("src/data_platform/api/routes/datasets.py")
SERVICE_PATH = Path("src/data_platform/services/quality_service.py")
SCHEMA_PATH = Path("src/data_platform/schemas/dataset.py")
README_PATH = Path("README.md")
UTILS_PATH = Path("src/data_platform/utils/quality_trends.py")


def test_quality_trend_feature_is_wired_and_documented() -> None:
    routes_text = ROUTES_PATH.read_text()
    service_text = SERVICE_PATH.read_text()
    schema_text = SCHEMA_PATH.read_text()
    readme_text = README_PATH.read_text()
    utils_text = UTILS_PATH.read_text()

    assert 'class QualityTrendBucketResponse' in schema_text
    assert 'class QualityCheckTrendSummaryResponse' in schema_text
    assert 'class QualityTrendResponse' in schema_text

    assert 'def get_result_trends(' in service_text
    assert 'build_quality_result_trend_report' in service_text
    assert 'def build_quality_result_trend_report(' in utils_text

    assert '@router.get("/{dataset_slug}/quality-results/trends", response_model=QualityTrendResponse)' in routes_text
    assert 'created_at_after cannot be after created_at_before' in routes_text

    assert 'quality trend history endpoints' in readme_text
    assert 'GET /v1/datasets/{dataset_slug}/quality-results/trends' in readme_text
    assert 'day` or `hour` buckets' in readme_text
