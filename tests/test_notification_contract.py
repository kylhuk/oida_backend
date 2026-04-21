from __future__ import annotations

from pathlib import Path


def test_webhook_notifications_feature_is_wired_and_documented() -> None:
    readme_text = Path("README.md").read_text()
    env_text = Path(".env.example").read_text()
    settings_text = Path("src/data_platform/settings.py").read_text()
    notifications_text = Path("src/data_platform/services/notifications.py").read_text()
    processing_text = Path("src/data_platform/services/processing_service.py").read_text()
    pipeline_execution_text = Path("src/data_platform/services/pipeline_execution_service.py").read_text()
    worker_tasks_text = Path("src/data_platform/workers/tasks.py").read_text()

    assert "webhook notifications for terminal ingestion-job and pipeline-run outcomes" in readme_text
    assert "ENABLE_WEBHOOK_NOTIFICATIONS=true" in readme_text
    assert "ENABLE_WEBHOOK_NOTIFICATIONS=false" in env_text
    assert "NOTIFICATION_WEBHOOK_URLS=" in env_text
    assert "NOTIFICATION_EVENTS=ingestion_job.failed,pipeline_run.failed" in env_text
    assert 'enable_webhook_notifications: bool = Field(default=False, alias="ENABLE_WEBHOOK_NOTIFICATIONS")' in settings_text
    assert "notification_webhook_url_values" in settings_text
    assert "notification_event_values" in settings_text
    assert "class WebhookNotificationService" in notifications_text
    assert "build_ingestion_job_notification_payload" in notifications_text
    assert "build_pipeline_run_notification_payload" in notifications_text
    assert 'self._notify_ingestion_job("ingestion_job.succeeded"' in processing_text
    assert '"pipeline_run.succeeded"' in pipeline_execution_text
    assert '"pipeline_run.failed"' in pipeline_execution_text
    assert '"ingestion_job.failed"' in worker_tasks_text
    assert "WebhookNotificationService().notify(" in worker_tasks_text
