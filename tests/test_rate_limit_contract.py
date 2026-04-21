from __future__ import annotations

from pathlib import Path


def test_rate_limit_is_wired_into_app_settings_and_docs() -> None:
    settings_source = Path("src/data_platform/settings.py").read_text()
    api_main_source = Path("src/data_platform/api/main.py").read_text()
    env_source = Path(".env.example").read_text()
    readme_source = Path("README.md").read_text()

    assert 'enable_rate_limit: bool = Field(default=False, alias="ENABLE_RATE_LIMIT")' in settings_source
    assert 'rate_limit_requests: int = Field(default=120, alias="RATE_LIMIT_REQUESTS")' in settings_source
    assert 'rate_limit_window_seconds: int = Field(default=60, alias="RATE_LIMIT_WINDOW_SECONDS")' in settings_source
    assert 'rate_limit_exempt_paths: str = Field(' in settings_source
    assert 'app.add_middleware(\n        RateLimitMiddleware,' in api_main_source
    assert 'enabled=runtime_settings.enable_rate_limit' in api_main_source
    assert 'limit=runtime_settings.rate_limit_requests' in api_main_source
    assert 'window_seconds=runtime_settings.rate_limit_window_seconds' in api_main_source
    assert 'exempt_path_prefixes=runtime_settings.rate_limit_exempt_path_prefixes' in api_main_source
    assert 'ENABLE_RATE_LIMIT=false' in env_source
    assert 'RATE_LIMIT_REQUESTS=120' in env_source
    assert 'RATE_LIMIT_WINDOW_SECONDS=60' in env_source
    assert 'RATE_LIMIT_EXEMPT_PATHS=/health,/docs,/openapi.json,/redoc' in env_source
    assert 'opt-in per-client API rate limiting with `429` responses' in readme_source
    assert 'Optional request throttling is available through `ENABLE_RATE_LIMIT`' in readme_source
