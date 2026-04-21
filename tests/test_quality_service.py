from __future__ import annotations

from data_platform.schemas.dataset import CreateDatasetRequest, QualityRuleCreate, UpdateQualityRuleRequest
from data_platform.services.dataset_service import DatasetService
from data_platform.services.quality_service import QualityService



def test_create_and_list_quality_rules(db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    service = QualityService(db_session)

    created = service.create_rule(
        dataset,
        QualityRuleCreate(
            name="has_rows",
            layer="gold",
            severity="error",
            sql_expression="SELECT COUNT(*) > 0 AS passed FROM source",
        ),
    )

    rules = QualityService.list_rules(db_session, dataset.id)
    assert len(rules) == 1
    assert rules[0].id == created.id
    assert rules[0].layer == "gold"



def test_update_quality_rule_can_disable_and_change_severity(db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    service = QualityService(db_session)
    rule = service.create_rule(
        dataset,
        QualityRuleCreate(
            name="non_negative_amount",
            layer="silver",
            severity="error",
            sql_expression="SELECT TRUE AS passed",
        ),
    )

    updated = service.update_rule(
        rule,
        UpdateQualityRuleRequest(active=False, severity="warn", sql_expression="SELECT FALSE AS passed"),
    )

    assert updated.active is False
    assert updated.severity == "warn"
    assert updated.sql_expression == "SELECT FALSE AS passed"



def test_list_quality_rules_can_filter_by_active(db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    service = QualityService(db_session)
    service.create_rule(
        dataset,
        QualityRuleCreate(
            name="active_rule",
            layer="gold",
            severity="error",
            sql_expression="SELECT TRUE AS passed",
        ),
    )
    disabled = service.create_rule(
        dataset,
        QualityRuleCreate(
            name="inactive_rule",
            layer="gold",
            severity="warn",
            sql_expression="SELECT TRUE AS passed",
        ),
    )
    service.update_rule(disabled, UpdateQualityRuleRequest(active=False))

    active_rules = QualityService.list_rules(db_session, dataset.id, active=True)
    inactive_rules = QualityService.list_rules(db_session, dataset.id, active=False)

    assert [rule.name for rule in active_rules] == ["active_rule"]
    assert [rule.name for rule in inactive_rules] == ["inactive_rule"]
