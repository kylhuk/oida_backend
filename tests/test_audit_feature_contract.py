from pathlib import Path

SETTINGS_PATH = Path("src/data_platform/settings.py")
MAIN_PATH = Path("src/data_platform/api/main.py")
ROUTES_PATH = Path("src/data_platform/api/routes/audit.py")
SERVICE_PATH = Path("src/data_platform/services/audit_service.py")
SCHEMA_PATH = Path("src/data_platform/schemas/audit.py")
MODEL_PATH = Path("src/data_platform/models/audit.py")
MIGRATIONS_PATH = Path("src/data_platform/migrations/manager.py")
ENV_PATH = Path(".env.example")
README_PATH = Path("README.md")
SECURITY_PATH = Path("src/data_platform/security.py")


def test_audit_feature_is_wired_in_source() -> None:
    settings_text = SETTINGS_PATH.read_text()
    main_text = MAIN_PATH.read_text()
    routes_text = ROUTES_PATH.read_text()
    service_text = SERVICE_PATH.read_text()
    schema_text = SCHEMA_PATH.read_text()
    model_text = MODEL_PATH.read_text()
    migrations_text = MIGRATIONS_PATH.read_text()
    env_text = ENV_PATH.read_text()
    security_text = SECURITY_PATH.read_text()

    assert 'enable_audit_trail: bool = Field(default=False, alias="ENABLE_AUDIT_TRAIL")' in settings_text
    assert 'def audit_trail_exempt_path_prefixes(self) -> list[str]:' in settings_text
    assert 'AuditTrailMiddleware' in main_text
    assert 'enabled=runtime_settings.enable_audit_trail' in main_text
    assert 'exempt_path_prefixes=runtime_settings.audit_trail_exempt_path_prefixes' in main_text
    assert 'audit_router = _load_router("data_platform.api.routes.audit")' in main_text
    assert '@router.get("/audit/events", response_model=list[AuditEventResponse])' in routes_text
    assert '@router.get("/audit/events/count", response_model=AuditEventCountResponse)' in routes_text
    assert '@router.get("/audit/events/page", response_model=AuditEventPageResponse)' in routes_text
    assert '@router.get("/audit/events/{event_id}", response_model=AuditEventResponse)' in routes_text
    assert 'event_type: str | None = Query(default=None)' in routes_text
    assert 'def list_events_page(' in service_text
    assert 'def count_events(' in service_text
    assert 'event_type: str | None = None' in service_text
    assert 'class AuditEvent(TimestampMixin, Base):' in model_text
    assert '__tablename__ = "audit_events"' in model_text
    assert 'name="0011_audit_events"' in migrations_text
    assert 'ENABLE_AUDIT_TRAIL=true' in env_text
    assert 'AUDIT_TRAIL_EXEMPT_PATHS=/health,/docs,/openapi.json,/redoc' in env_text
    assert '"audit:read"' in security_text


def test_readme_documents_audit_trail_endpoints() -> None:
    text = README_PATH.read_text()

    assert 'first-class control-plane and operational audit trail' in text
    assert '`ENABLE_AUDIT_TRAIL` and `AUDIT_TRAIL_EXEMPT_PATHS`' in text
    assert '`GET /v1/audit/events`' in text
    assert '`GET /v1/audit/events/count`' in text
    assert '`GET /v1/audit/events/page`' in text
    assert '`GET /v1/audit/events/{event_id}`' in text
    assert '`event_type`' in text
    assert '`WORKER`' in text
    assert '`CLI`' in text
