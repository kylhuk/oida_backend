from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]



def _read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text()



def test_audit_trail_model_migration_and_settings_are_present() -> None:
    model_source = _read("src/data_platform/models/audit.py")
    migration_source = _read("src/data_platform/migrations/manager.py")
    settings_source = _read("src/data_platform/settings.py")
    security_source = _read("src/data_platform/security.py")
    env_source = _read(".env.example")

    assert 'class AuditEvent' in model_source
    assert '__tablename__ = "audit_events"' in model_source
    assert 'name="0011_audit_events"' in migration_source
    assert 'table_name="audit_events"' in migration_source
    assert 'ENABLE_AUDIT_TRAIL' in settings_source
    assert 'AUDIT_TRAIL_EXEMPT_PATHS' in settings_source
    assert '"audit:read"' in security_source
    assert 'ENABLE_AUDIT_TRAIL=true' in env_source
    assert 'AUDIT_TRAIL_EXEMPT_PATHS=/health,/docs,/openapi.json,/redoc' in env_source



def test_audit_trail_route_and_app_wiring_are_present() -> None:
    route_source = _read("src/data_platform/api/routes/audit.py")
    main_source = _read("src/data_platform/api/main.py")
    deps_source = _read("src/data_platform/api/deps.py")
    service_source = _read("src/data_platform/services/audit_service.py")

    assert 'prefix="/v1"' in route_source
    assert '@router.get("/audit/events"' in route_source
    assert '@router.get("/audit/events/count"' in route_source
    assert '@router.get("/audit/events/page"' in route_source
    assert '@router.get("/audit/events/{event_id}"' in route_source
    assert 'event_type: str | None = Query(default=None)' in route_source
    assert 'require_scopes("audit:read")' in route_source
    assert 'AuditTrailMiddleware' in main_source
    assert 'data_platform.api.routes.audit' in main_source
    assert 'app.include_router(audit_router)' in main_source
    assert 'state["api_client_id"] = client.id' in deps_source
    assert 'def list_events_page(' in service_source
    assert 'event_type: str | None = None' in service_source



def test_readme_documents_audit_trail_feature_and_endpoints() -> None:
    readme_source = _read("README.md")

    assert 'first-class control-plane and operational audit trail' in readme_source
    assert 'ENABLE_AUDIT_TRAIL' in readme_source
    assert 'GET /v1/audit/events' in readme_source
    assert 'GET /v1/audit/events/count' in readme_source
    assert 'GET /v1/audit/events/page' in readme_source
    assert 'GET /v1/audit/events/{event_id}' in readme_source
    assert 'event_type' in readme_source
    assert 'WORKER' in readme_source
    assert 'CLI' in readme_source
