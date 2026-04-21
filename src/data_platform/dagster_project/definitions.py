from __future__ import annotations

from dagster import Definitions, load_assets_from_modules

from data_platform.dagster_project.assets import sample_customer_metrics

defs = Definitions(
    assets=load_assets_from_modules([sample_customer_metrics]),
)
