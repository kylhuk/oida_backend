from __future__ import annotations

import ast
from pathlib import Path


def test_list_pipeline_source_candidates_normalizes_run_ref_prefix_before_use() -> None:
    source = Path('src/data_platform/services/pipeline_service.py').read_text()
    module = ast.parse(source)

    function = None
    for node in module.body:
        if isinstance(node, ast.ClassDef) and node.name == 'PipelineService':
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == 'list_pipeline_source_candidates':
                    function = item
                    break
    assert function is not None

    assigned_normalizer = False
    used_normalized_name = False
    for node in ast.walk(function):
        if isinstance(node, ast.Assign):
            targets = [target.id for target in node.targets if isinstance(target, ast.Name)]
            if 'normalized_run_ref_prefix' in targets:
                call = node.value
                assert isinstance(call, ast.Call)
                assert isinstance(call.func, ast.Name)
                assert call.func.id == 'normalize_optional_run_ref'
                assigned_normalizer = True
        elif isinstance(node, ast.Name) and node.id == 'normalized_run_ref_prefix' and isinstance(node.ctx, ast.Load):
            used_normalized_name = True

    assert assigned_normalizer is True
    assert used_normalized_name is True


def test_list_pipeline_source_candidates_supports_excluding_existing_runs_before_pagination() -> None:
    source = Path('src/data_platform/services/pipeline_service.py').read_text()
    tree = ast.parse(source)

    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == 'PipelineService':
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == 'list_pipeline_source_candidates':
                    arg_names = [arg.arg for arg in item.args.args]
                    kwonly_names = [arg.arg for arg in item.args.kwonlyargs]
                    all_names = arg_names + kwonly_names
                    assert 'exclude_existing_runs' in all_names

                    source_segment = ast.get_source_segment(source, item) or ''
                    assert 'exclude_existing_runs=payload.skip_existing_runs' not in source_segment
                    assert 'PipelineRun.pipeline_id == pipeline.id' in source_segment
                    assert 'PipelineRun.ingestion_job_id == IngestionJob.id' in source_segment
                    assert '.exists()' in source_segment
                    return

    raise AssertionError('PipelineService.list_pipeline_source_candidates not found')


def test_list_pipeline_source_candidates_attaches_source_time_aware_schema_compatibility_preview_context() -> None:
    source = Path('src/data_platform/services/pipeline_service.py').read_text()
    tree = ast.parse(source)

    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == 'PipelineService':
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == 'list_pipeline_source_candidates':
                    source_segment = ast.get_source_segment(source, item) or ''
                    assert 'build_pipeline_run_schema_context(' in source_segment
                    assert 'source_effective_finished_at=effective_finished_at_value' in source_segment
                    assert 'schema_compatibility_preview=schema_compatibility_preview' in source_segment
                    assert 'schema_compatibility_preview_unavailable_reason=schema_compatibility_preview_unavailable_reason' in source_segment
                    return

    raise AssertionError('PipelineService.list_pipeline_source_candidates not found')


def test_build_pipeline_run_schema_context_uses_latest_schema_snapshot_at_or_before_for_source_layer() -> None:
    source = Path('src/data_platform/services/pipeline_service.py').read_text()
    tree = ast.parse(source)

    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == 'PipelineService':
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == 'build_pipeline_run_schema_context':
                    source_segment = ast.get_source_segment(source, item) or ''
                    assert 'DatasetService.latest_schema_snapshot_at_or_before(' in source_segment
                    assert 'source_effective_finished_at' in source_segment
                    return

    raise AssertionError('PipelineService.build_pipeline_run_schema_context not found')
