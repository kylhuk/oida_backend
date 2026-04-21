from pathlib import Path

INGESTION_SCHEMA_PATH = Path("src/data_platform/schemas/ingestion.py")
PIPELINE_SCHEMA_PATH = Path("src/data_platform/schemas/pipeline.py")
INGESTION_ROUTE_PATH = Path("src/data_platform/api/routes/ingestions.py")
PIPELINE_ROUTE_PATH = Path("src/data_platform/api/routes/pipelines.py")
INGESTION_SERVICE_PATH = Path("src/data_platform/services/ingestion_service.py")
UTILS_PATH = Path("src/data_platform/utils/artifacts.py")
README_PATH = Path("README.md")


def test_ingestion_artifact_manifest_schema_route_and_service_exist() -> None:
    ingestion_schema_text = INGESTION_SCHEMA_PATH.read_text()
    ingestion_route_text = INGESTION_ROUTE_PATH.read_text()
    ingestion_service_text = INGESTION_SERVICE_PATH.read_text()
    utils_text = UTILS_PATH.read_text()

    assert "class IngestionArtifactManifestResponse(BaseModel):" in ingestion_schema_text
    assert '@router.get("/{job_id}/artifact-manifest", response_model=IngestionArtifactManifestResponse)' in ingestion_route_text
    assert "IngestionService.build_detailed_artifact_manifest(session, job)" in ingestion_route_text
    assert "def build_detailed_artifact_manifest(session: Session, job: IngestionJob) -> dict:" in ingestion_service_text
    assert "def build_ingestion_artifact_manifest(job: Any) -> dict[str, Any]:" in utils_text


def test_pipeline_first_class_artifact_manifest_route_remains_present() -> None:
    pipeline_schema_text = PIPELINE_SCHEMA_PATH.read_text()
    pipeline_route_text = PIPELINE_ROUTE_PATH.read_text()

    assert "class PipelineArtifactManifestResponse(BaseModel):" in pipeline_schema_text
    assert '@router.get("/pipelines/{pipeline_id}/runs/{run_id}/artifact-manifest", response_model=PipelineArtifactManifestResponse)' in pipeline_route_text


def test_readme_documents_artifact_manifest_retrieval_endpoints() -> None:
    text = README_PATH.read_text()

    assert "artifact manifest retrieval endpoints" in text
    assert "GET /v1/ingestions/{job_id}/artifact-manifest" in text
    assert "GET /v1/pipelines/{pipeline_id}/runs/{run_id}/artifact-manifest" in text
