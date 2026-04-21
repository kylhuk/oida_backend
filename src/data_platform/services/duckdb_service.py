from __future__ import annotations

import importlib
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from data_platform.utils.formats import FileFormat
from data_platform.utils.schemas import normalize_duckdb_type
from data_platform.utils.sql import quote_duckdb_identifier, sql_string_literal


class DuckDBService:
    def __init__(
        self,
        *,
        settings: Any | None = None,
        duckdb_module: Any | None = None,
        pandas_module: Any | None = None,
    ) -> None:
        self._settings = settings
        self._duckdb_module = duckdb_module
        self._pandas_module = pandas_module

    @property
    def settings(self) -> Any:
        if self._settings is None:
            self._settings = self._load_settings()
        return self._settings

    @property
    def duckdb_module(self) -> Any:
        if self._duckdb_module is None:
            self._duckdb_module = self._load_duckdb_module()
        return self._duckdb_module

    @property
    def pandas_module(self) -> Any:
        if self._pandas_module is None:
            self._pandas_module = self._load_pandas_module()
        return self._pandas_module

    @staticmethod
    def _load_settings() -> Any:
        try:
            settings_module = importlib.import_module("data_platform.settings")
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "DuckDBService requires the application settings module to be available."
            ) from exc

        get_settings = getattr(settings_module, "get_settings", None)
        if not callable(get_settings):
            raise RuntimeError("DuckDBService requires data_platform.settings.get_settings().")
        return get_settings()

    @staticmethod
    def _load_duckdb_module() -> Any:
        try:
            return importlib.import_module("duckdb")
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "DuckDBService requires duckdb to execute transforms. Install duckdb or inject a duckdb module explicitly."
            ) from exc

    @staticmethod
    def _load_pandas_module() -> Any:
        try:
            return importlib.import_module("pandas")
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "DuckDBService requires pandas for Excel ingestion. Install pandas or inject a pandas module explicitly."
            ) from exc

    @contextmanager
    def connection(self):
        con = self.duckdb_module.connect(database=":memory:")
        con.execute(f"PRAGMA threads={int(self.settings.duckdb_threads)}")
        try:
            yield con
        finally:
            con.close()

    @staticmethod
    def _quoted_path(path: str | Path) -> str:
        return sql_string_literal(str(Path(path).resolve()))

    def source_sql_for_file(self, local_path: str | Path, file_format: FileFormat, working_dir: str | Path) -> str:
        path = Path(local_path)
        working_dir = Path(working_dir)

        if file_format == FileFormat.CSV:
            return f"SELECT * FROM read_csv_auto({self._quoted_path(path)}, SAMPLE_SIZE=-1)"
        if file_format == FileFormat.TSV:
            return (
                "SELECT * FROM read_csv_auto("
                f"{self._quoted_path(path)}, SAMPLE_SIZE=-1, delim='\t'"
                ")"
            )
        if file_format in {FileFormat.JSON, FileFormat.NDJSON}:
            return f"SELECT * FROM read_json_auto({self._quoted_path(path)})"
        if file_format == FileFormat.PARQUET:
            return f"SELECT * FROM parquet_scan({self._quoted_path(path)})"
        if file_format in {FileFormat.XLSX, FileFormat.XLSM}:
            parquet_path = working_dir / f"{path.stem}.converted.parquet"
            df = self.pandas_module.read_excel(path)
            df.to_parquet(parquet_path, index=False)
            return f"SELECT * FROM parquet_scan({self._quoted_path(parquet_path)})"

        raise ValueError(f"Unsupported file format {file_format!r}.")

    def describe_query(self, query: str, views: dict[str, str] | None = None) -> list[dict[str, str]]:
        with self.connection() as con:
            self._register_views(con, views or {})
            cursor = con.execute(f"SELECT * FROM ({query}) AS q LIMIT 0")
            description = cursor.description or []
            return [
                {"name": column[0], "type": normalize_duckdb_type(str(column[1]))}
                for column in description
            ]

    def execute_records(self, query: str, views: dict[str, str] | None = None) -> list[dict]:
        with self.connection() as con:
            self._register_views(con, views or {})
            cursor = con.execute(query)
            columns = [column[0] for column in (cursor.description or [])]
            rows = cursor.fetchall()
            return [dict(zip(columns, row, strict=False)) for row in rows]

    def copy_query_to_parquet(self, query: str, output_path: str | Path, views: dict[str, str] | None = None) -> None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as con:
            self._register_views(con, views or {})
            con.execute(
                f"COPY ({query}) TO {self._quoted_path(output_path)} "
                "(FORMAT PARQUET, COMPRESSION ZSTD)"
            )

    def count_query_rows(self, query: str, views: dict[str, str] | None = None) -> int:
        with self.connection() as con:
            self._register_views(con, views or {})
            return int(con.execute(f"SELECT COUNT(*) FROM ({query}) AS q").fetchone()[0])

    def count_parquet_rows(self, parquet_path: str | Path) -> int:
        query = f"SELECT COUNT(*) AS row_count FROM parquet_scan({self._quoted_path(parquet_path)})"
        return int(self.execute_records(query)[0]["row_count"])

    def build_aligned_query(
        self,
        base_query: str,
        current_schema: list[dict[str, str]],
        canonical_schema: list[dict[str, str]],
        metadata_columns: list[dict[str, str]],
    ) -> str:
        current_names = {column["name"] for column in current_schema}
        expressions: list[str] = []

        for column in canonical_schema:
            name = column["name"]
            dtype = normalize_duckdb_type(column["type"])
            identifier = quote_duckdb_identifier(name)
            if name in current_names:
                expressions.append(f"TRY_CAST({identifier} AS {dtype}) AS {identifier}")
            else:
                expressions.append(f"CAST(NULL AS {dtype}) AS {identifier}")

        for metadata in metadata_columns:
            name = quote_duckdb_identifier(metadata["name"])
            dtype = metadata["type"]
            value = metadata["value"]
            if value is None:
                expressions.append(f"CAST(NULL AS {dtype}) AS {name}")
            else:
                expressions.append(f"CAST({sql_string_literal(str(value))} AS {dtype}) AS {name}")

        projection = ", ".join(expressions)
        return f"WITH result AS ({base_query}) SELECT {projection} FROM result"

    @staticmethod
    def _register_views(con: Any, views: dict[str, str]) -> None:
        for name, query in views.items():
            identifier = quote_duckdb_identifier(name)
            con.execute(f"CREATE OR REPLACE VIEW {identifier} AS {query}")
