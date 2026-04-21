from __future__ import annotations

import ast
from pathlib import Path


def test_pipeline_service_exposes_count_pipeline_source_candidates_with_exclude_existing_runs() -> None:
    source = Path('src/data_platform/services/pipeline_service.py').read_text()
    tree = ast.parse(source)

    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == 'PipelineService':
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == 'count_pipeline_source_candidates':
                    arg_names = [arg.arg for arg in item.args.args]
                    kwonly_names = [arg.arg for arg in item.args.kwonlyargs]
                    all_names = arg_names + kwonly_names
                    assert 'exclude_existing_runs' in all_names

                    source_segment = ast.get_source_segment(source, item) or ''
                    assert 'select(func.count(IngestionJob.id))' in source_segment
                    assert 'PipelineRun.pipeline_id == pipeline.id' in source_segment
                    assert 'PipelineRun.ingestion_job_id == IngestionJob.id' in source_segment
                    assert '.exists()' in source_segment
                    return

    raise AssertionError('PipelineService.count_pipeline_source_candidates not found')


def test_pipeline_routes_expose_source_candidate_count_endpoint() -> None:
    source = Path('src/data_platform/api/routes/pipelines.py').read_text()
    tree = ast.parse(source)

    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == 'count_pipeline_source_candidates':
            source_segment = ast.get_source_segment(source, node) or ''
            decorators = [ast.get_source_segment(source, dec) or '' for dec in node.decorator_list]
            assert any('/pipelines/{pipeline_id}/source-candidates/count' in dec for dec in decorators)
            assert 'PipelineService.count_pipeline_source_candidates(' in source_segment
            assert 'exclude_existing_runs=exclude_existing_runs' in source_segment
            return

    raise AssertionError('count_pipeline_source_candidates route not found')
