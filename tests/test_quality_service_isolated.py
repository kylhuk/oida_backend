from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest


class DummyColumn:
    def __eq__(self, other):
        return ("eq", other)

    def is_(self, value):
        return ("is", value)

    def desc(self):
        return self


class FakeSelect:
    def where(self, *args, **kwargs):
        return self

    def join(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self

    def offset(self, *args, **kwargs):
        return self


class FakeQualityCheck:
    dataset_id = DummyColumn()
    layer = DummyColumn()
    active = DummyColumn()
    id = DummyColumn()
    name = DummyColumn()
    severity = DummyColumn()
    created_at = DummyColumn()

    def __init__(self, *, id: str, name: str, severity: str, sql_expression: str, layer: str = "gold") -> None:
        self.id = id
        self.name = name
        self.severity = severity
        self.sql_expression = sql_expression
        self.layer = layer
        self.active = True


class FakeQualityResult:
    dataset_id = DummyColumn()
    ingestion_job_id = DummyColumn()
    quality_check_id = DummyColumn()
    layer = DummyColumn()
    status = DummyColumn()
    created_at = DummyColumn()

    def __init__(self, **kwargs) -> None:
        self.__dict__.update(kwargs)


class DummySession:
    def __init__(self, checks: list[FakeQualityCheck]) -> None:
        self._checks = checks
        self.added: list[object] = []
        self.flush_count = 0
        self.commit_count = 0

    def scalars(self, stmt):
        return types.SimpleNamespace(all=lambda: list(self._checks))

    def add(self, value) -> None:
        self.added.append(value)

    def flush(self) -> None:
        self.flush_count += 1

    def commit(self) -> None:
        self.commit_count += 1


class DummyDuckDB:
    def __init__(self, rows_by_sql: dict[str, list[dict]]) -> None:
        self.rows_by_sql = rows_by_sql

    def execute_records(self, sql_expression: str, views: dict[str, str]):
        return list(self.rows_by_sql[sql_expression])


@pytest.fixture()
def quality_service_module(monkeypatch):
    sqlalchemy_module = types.ModuleType("sqlalchemy")
    sqlalchemy_module.select = lambda *args, **kwargs: FakeSelect()
    sqlalchemy_orm_module = types.ModuleType("sqlalchemy.orm")
    sqlalchemy_orm_module.Session = object

    models_package = types.ModuleType("data_platform.models")
    schemas_package = types.ModuleType("data_platform.schemas")
    services_package = types.ModuleType("data_platform.services")

    dataset_module = types.ModuleType("data_platform.models.dataset")
    dataset_module.Dataset = object

    pipeline_module = types.ModuleType("data_platform.models.pipeline")
    pipeline_module.QualityCheck = FakeQualityCheck
    pipeline_module.QualityResult = FakeQualityResult

    enums_module = types.ModuleType("data_platform.enums")
    enums_module.QualitySeverity = types.SimpleNamespace(ERROR=types.SimpleNamespace(value="error"))

    schemas_dataset_module = types.ModuleType("data_platform.schemas.dataset")
    schemas_dataset_module.QualityRuleCreate = object
    schemas_dataset_module.UpdateQualityRuleRequest = object

    duckdb_module = types.ModuleType("data_platform.services.duckdb_service")
    duckdb_module.DuckDBService = object

    monkeypatch.setitem(sys.modules, "sqlalchemy", sqlalchemy_module)
    monkeypatch.setitem(sys.modules, "sqlalchemy.orm", sqlalchemy_orm_module)
    monkeypatch.setitem(sys.modules, "data_platform.models", models_package)
    monkeypatch.setitem(sys.modules, "data_platform.schemas", schemas_package)
    monkeypatch.setitem(sys.modules, "data_platform.services", services_package)
    monkeypatch.setitem(sys.modules, "data_platform.models.dataset", dataset_module)
    monkeypatch.setitem(sys.modules, "data_platform.models.pipeline", pipeline_module)
    monkeypatch.setitem(sys.modules, "data_platform.enums", enums_module)
    monkeypatch.setitem(sys.modules, "data_platform.schemas.dataset", schemas_dataset_module)
    monkeypatch.setitem(sys.modules, "data_platform.services.duckdb_service", duckdb_module)

    module_name = "_isolated_quality_service_test_module"
    module_path = Path(__file__).resolve().parents[1] / "src" / "data_platform" / "services" / "quality_service.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
        yield module
    finally:
        sys.modules.pop(module_name, None)


def test_run_checks_persists_passed_result_shape(quality_service_module):
    session = DummySession(
        [
            FakeQualityCheck(
                id="qc-1",
                name="has_rows",
                severity="warn",
                sql_expression="SELECT TRUE AS passed, 10 AS observed_value, 'ok' AS note",
            )
        ]
    )
    service = quality_service_module.QualityService(session)
    service.duckdb = DummyDuckDB(
        {
            "SELECT TRUE AS passed, 10 AS observed_value, 'ok' AS note": [
                {"passed": True, "observed_value": 10, "note": "ok"}
            ]
        }
    )

    results = service.run_checks("dataset-1", "job-1", "gold", "SELECT * FROM source")

    assert len(results) == 1
    assert results[0].status == "passed"
    assert results[0].observed_value == "10"
    assert results[0].details_json == {"note": "ok"}
    assert session.flush_count == 1
    assert session.commit_count == 1


def test_run_checks_rejects_empty_result_sets(quality_service_module):
    session = DummySession(
        [
            FakeQualityCheck(
                id="qc-1",
                name="has_rows",
                severity="error",
                sql_expression="SELECT COUNT(*) > 0 AS passed FROM source",
            )
        ]
    )
    service = quality_service_module.QualityService(session)
    service.duckdb = DummyDuckDB({"SELECT COUNT(*) > 0 AS passed FROM source": []})

    with pytest.raises(ValueError, match="at least one row with a 'passed' column"):
        service.run_checks("dataset-1", "job-1", "gold", "SELECT * FROM source")

    assert session.added == []
    assert session.commit_count == 1


def test_run_checks_rejects_missing_passed_column_after_persisting_prior_results(quality_service_module):
    first_sql = "SELECT TRUE AS passed, 1 AS observed_value"
    malformed_sql = "SELECT 0 AS observed_value"
    session = DummySession(
        [
            FakeQualityCheck(id="qc-1", name="first_rule", severity="warn", sql_expression=first_sql),
            FakeQualityCheck(id="qc-2", name="second_rule", severity="warn", sql_expression=malformed_sql),
        ]
    )
    service = quality_service_module.QualityService(session)
    service.duckdb = DummyDuckDB(
        {
            first_sql: [{"passed": True, "observed_value": 1}],
            malformed_sql: [{"observed_value": 0}],
        }
    )

    with pytest.raises(ValueError, match="must return a 'passed' column"):
        service.run_checks("dataset-1", "job-1", "gold", "SELECT * FROM source")

    assert len(session.added) == 1
    assert session.added[0].status == "passed"
    assert session.commit_count == 1


def test_run_checks_coerces_boolean_like_string_values(quality_service_module):
    session = DummySession(
        [
            FakeQualityCheck(
                id="qc-1",
                name="string_false",
                severity="warn",
                sql_expression="SELECT 'false' AS passed, '0' AS observed_value",
            )
        ]
    )
    service = quality_service_module.QualityService(session)
    service.duckdb = DummyDuckDB(
        {
            "SELECT 'false' AS passed, '0' AS observed_value": [
                {"passed": "false", "observed_value": "0"}
            ]
        }
    )

    results = service.run_checks("dataset-1", "job-1", "gold", "SELECT * FROM source")

    assert len(results) == 1
    assert results[0].status == "failed"
    assert results[0].observed_value == "0"


def test_run_checks_rejects_non_boolean_like_passed_values(quality_service_module):
    session = DummySession(
        [
            FakeQualityCheck(
                id="qc-1",
                name="ambiguous_passed",
                severity="warn",
                sql_expression="SELECT 'maybe' AS passed",
            )
        ]
    )
    service = quality_service_module.QualityService(session)
    service.duckdb = DummyDuckDB({"SELECT 'maybe' AS passed": [{"passed": "maybe"}]})

    with pytest.raises(ValueError, match="must be boolean-like"):
        service.run_checks("dataset-1", "job-1", "gold", "SELECT * FROM source")

    assert session.added == []
    assert session.commit_count == 1
