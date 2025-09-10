import os
import aws_cdk as cdk
from aws_cdk.assertions import Template, Match

from infrastructure.config.settings import PipelineSettings
from infrastructure.stacks.storage_stack import StorageStack
from infrastructure.stacks.catalog_stack import CatalogStack
from infrastructure.stacks.compute_stack import ComputeStack
from infrastructure.stacks.analytics_stack import AnalyticsStack


def _base_env(monkeypatch, enable_lf):
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("REGION", "us-east-1")
    monkeypatch.setenv("ACCOUNT_ID", "123456789012")
    monkeypatch.setenv("PROJECT_NAME", "data-pipeline")
    monkeypatch.setenv("OWNER_TAG", "data-engineering")
    monkeypatch.setenv("GLUE_DATABASE_NAME", "data_pipeline_db")
    monkeypatch.setenv("GLUE_CRAWLER_NAME", "data_pipeline_crawler")
    monkeypatch.setenv("CRAWLER_SCHEDULE", "cron(0 2 * * ? *)")
    monkeypatch.setenv("API_ENDPOINT", "https://example.com")
    monkeypatch.setenv("LAMBDA_TIMEOUT", "60")
    monkeypatch.setenv("LAMBDA_MEMORY", "256")
    monkeypatch.setenv("OUTPUT_FORMAT", "parquet")
    monkeypatch.setenv("ENABLE_LAKE_FORMATION", "true" if enable_lf else "false")
    monkeypatch.setenv("DATA_LAKE_ADMIN_ARN", f"arn:aws:iam::{os.getenv('ACCOUNT_ID','123456789012')}:role/admin-dev-cw")


def _mk_stacks(monkeypatch, enable_lf=False):
    _base_env(monkeypatch, enable_lf)
    settings = PipelineSettings()
    app = cdk.App()
    storage = StorageStack(app, "S", settings=settings)
    compute = ComputeStack(app, "C", settings=settings, storage_stack=storage)
    catalog = CatalogStack(app, "G", settings=settings, storage_stack=storage)
    analytics = AnalyticsStack(app, "A", settings=settings,
                               storage_stack=storage,
                               catalog_stack=catalog,
                               compute_stack=compute)
    return analytics, settings, storage


def test_athena_workgroup(monkeypatch):
    analytics, settings, storage = _mk_stacks(monkeypatch, enable_lf=False)
    t = Template.from_stack(analytics)

    t.has_resource_properties("AWS::Athena::WorkGroup", {
        "Name": f"{settings.project_name}-workgroup",
        "WorkGroupConfiguration": {
            "EngineVersion": {"SelectedEngineVersion": "Athena engine version 3"},
            "EnforceWorkGroupConfiguration": True,
            "ResultConfiguration": {
                "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
                "OutputLocation": Match.any_value()
            }
        }
    })


def test_lake_formation_resources_present_when_enabled(monkeypatch):
    analytics, _, _ = _mk_stacks(monkeypatch, enable_lf=True)
    t = Template.from_stack(analytics)

    t.has_resource_properties("AWS::LakeFormation::Tag", {
        "TagKey": Match.string_like_regexp("^(Environment|DataClassification)$")
    })
    t.has_resource_properties("AWS::LakeFormation::Permissions", {
        "Permissions": Match.any_value()
    })
