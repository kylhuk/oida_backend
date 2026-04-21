from __future__ import annotations

import ast
from pathlib import Path


def test_latest_schema_snapshot_at_or_before_exists_and_filters_by_created_at() -> None:
    source = Path('src/data_platform/services/dataset_service.py').read_text()
    tree = ast.parse(source)

    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == 'DatasetService':
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == 'latest_schema_snapshot_at_or_before':
                    source_segment = ast.get_source_segment(source, item) or ''
                    assert 'SchemaSnapshot.created_at <= effective_at' in source_segment
                    assert 'SchemaSnapshot.created_at.desc()' in source_segment
                    assert 'SchemaSnapshot.version.desc()' in source_segment
                    return

    raise AssertionError('DatasetService.latest_schema_snapshot_at_or_before not found')
