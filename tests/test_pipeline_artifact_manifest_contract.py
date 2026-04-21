from pathlib import Path

SCHEMA_PATH = Path("src/data_platform/schemas/pipeline.py")
ROUTES_PATH = Path("src/data_platform/api/routes/pipelines.py")
SERVICE_PATH = Path("src/data_platform/services/pipeline_service.py")
UTILS_PATH = Path("src/data_platform/utils/pipeline_definitions.py")
README_PATH = Path("README.md")


def test_pipeline_artifact_manifest_schema_and_route_contract_exist() -> None:
    schema_text = SCHEMA_PATH.read_text()
    route_text = ROUTES_PATH.read_text()
    service_text = SERVICE_PATH.read_text()
    utils_text = UTILS_PATH.read_text()

    assert "class PipelineArtifactManifestResponse(BaseModel):" in schema_text
    assert '@router.get("/pipelines/{pipeline_id}/runs/{run_id}/artifact-manifest", response_model=PipelineArtifactManifestResponse)' in route_text
    assert "def get_pipeline_run_artifact_manifest(" in route_text
    assert "PipelineService.build_pipeline_run_artifact_manifest(run)" in route_text
    assert "def build_pipeline_run_artifact_manifest(run: PipelineRun) -> dict | None:" in service_text
    assert "def extract_pipeline_artifact_manifest(" in utils_text
    assert "def _extract_pipeline_artifact_manifest(" in utils_text


def test_readme_documents_pipeline_artifact_manifest_retrieval() -> None:
    text = README_PATH.read_text()

    assert "run artifact-manifest retrieval" in text
    assert "`GET /v1/pipelines/{pipeline_id}/runs/{run_id}/artifact-manifest`" in text
    assert "first-class manifest projection" in text
