from __future__ import annotations

import importlib.util
import types
from pathlib import Path

import pytest


class DummySettings:
    def __init__(self) -> None:
        self.duckdb_threads = 4


class DummyCursor:
    def __init__(self, description=None, rows=None):
        self.description = description or []
        self._rows = rows or []

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0]


class DummyConnection:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.closed = False

    def execute(self, sql: str):
        self.executed.append(sql)
        if sql.startswith("PRAGMA threads="):
            return DummyCursor()
        if sql.startswith("SELECT * FROM ("):
            return DummyCursor(description=[("id", "INTEGER")])
        if sql == "SELECT id, name FROM source":
            return DummyCursor(description=[("id", "INTEGER"), ("name", "VARCHAR")], rows=[(1, "alice")])
        if sql.startswith("SELECT COUNT(*) FROM"):
            return DummyCursor(rows=[(7,)])
        if sql.startswith("CREATE OR REPLACE VIEW"):
            return DummyCursor()
        if sql.startswith("COPY ("):
            return DummyCursor()
        raise AssertionError(f"Unexpected SQL: {sql}")

    def close(self) -> None:
        self.closed = True


class DummyDuckDBModule:
    def __init__(self) -> None:
        self.connections: list[DummyConnection] = []

    def connect(self, database: str):
        assert database == ":memory:"
        connection = DummyConnection()
        self.connections.append(connection)
        return connection


class DummyDataFrame:
    def __init__(self) -> None:
        self.parquet_writes: list[tuple[Path, bool]] = []

    def to_parquet(self, path: str | Path, index: bool = False) -> None:
        self.parquet_writes.append((Path(path), index))


class DummyPandasModule:
    def __init__(self) -> None:
        self.read_excel_calls: list[Path] = []
        self.frame = DummyDataFrame()

    def read_excel(self, path: str | Path):
        self.read_excel_calls.append(Path(path))
        return self.frame


def _load_duckdb_service_module(module_name: str):
    module_path = Path(__file__).resolve().parents[1] / "src" / "data_platform" / "services" / "duckdb_service.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def duckdb_service_module():
    return _load_duckdb_service_module("_isolated_duckdb_service_test_module")


@pytest.fixture()
def service(duckdb_service_module):
    return duckdb_service_module.DuckDBService(
        settings=DummySettings(),
        duckdb_module=DummyDuckDBModule(),
        pandas_module=DummyPandasModule(),
    )


def test_duckdb_service_module_imports_without_optional_dependencies(duckdb_service_module):
    assert duckdb_service_module.DuckDBService.__name__ == "DuckDBService"


def test_duckdb_service_raises_clear_error_when_settings_loader_is_unavailable(duckdb_service_module, monkeypatch):
    real_import_module = duckdb_service_module.importlib.import_module

    def fake_import_module(name: str):
        if name == "data_platform.settings":
            return types.SimpleNamespace()
        return real_import_module(name)

    monkeypatch.setattr(duckdb_service_module.importlib, "import_module", fake_import_module)

    service = duckdb_service_module.DuckDBService()
    with pytest.raises(RuntimeError, match="get_settings"):
        _ = service.settings


def test_duckdb_service_raises_clear_error_when_duckdb_is_unavailable(duckdb_service_module, monkeypatch):
    real_import_module = duckdb_service_module.importlib.import_module

    def fake_import_module(name: str):
        if name == "data_platform.settings":
            return types.SimpleNamespace(get_settings=lambda: DummySettings())
        if name == "duckdb":
            raise ModuleNotFoundError("No module named 'duckdb'")
        return real_import_module(name)

    monkeypatch.setattr(duckdb_service_module.importlib, "import_module", fake_import_module)

    service = duckdb_service_module.DuckDBService()
    with pytest.raises(RuntimeError, match="requires duckdb"):
        with service.connection():
            pass


def test_duckdb_service_raises_clear_error_when_pandas_is_unavailable(duckdb_service_module, monkeypatch, tmp_path):
    real_import_module = duckdb_service_module.importlib.import_module

    def fake_import_module(name: str):
        if name == "pandas":
            raise ModuleNotFoundError("No module named 'pandas'")
        return real_import_module(name)

    monkeypatch.setattr(duckdb_service_module.importlib, "import_module", fake_import_module)

    service = duckdb_service_module.DuckDBService(settings=DummySettings(), duckdb_module=DummyDuckDBModule())
    with pytest.raises(RuntimeError, match="requires pandas"):
        service.source_sql_for_file(tmp_path / "orders.xlsx", duckdb_service_module.FileFormat.XLSX, tmp_path)


def test_duckdb_service_uses_injected_duckdb_module_for_queries(service):
    description = service.describe_query("SELECT id FROM source")
    records = service.execute_records("SELECT id, name FROM source")
    row_count = service.count_query_rows("SELECT id FROM source")

    assert description == [{"name": "id", "type": "BIGINT"}]
    assert records == [{"id": 1, "name": "alice"}]
    assert row_count == 7
    assert service.duckdb_module.connections[0].executed[0] == "PRAGMA threads=4"


def test_duckdb_service_uses_injected_pandas_module_for_excel_conversion(service, duckdb_service_module, tmp_path):
    workbook_path = tmp_path / "orders.xlsx"
    sql = service.source_sql_for_file(workbook_path, duckdb_service_module.FileFormat.XLSX, tmp_path)

    expected_parquet_path = (tmp_path / "orders.converted.parquet").resolve()
    assert sql == f"SELECT * FROM parquet_scan('{expected_parquet_path}')"
    assert service.pandas_module.read_excel_calls == [workbook_path]
    assert service.pandas_module.frame.parquet_writes == [(tmp_path / "orders.converted.parquet", False)]
