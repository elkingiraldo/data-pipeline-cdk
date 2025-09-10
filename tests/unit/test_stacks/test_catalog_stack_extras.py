import os
import aws_cdk as cdk
from aws_cdk.assertions import Template, Match

from infrastructure.config.settings import PipelineSettings
from infrastructure.stacks.storage_stack import StorageStack
from infrastructure.stacks.catalog_stack import CatalogStack


def _settings(monkeypatch):
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
    monkeypatch.setenv("ENABLE_LAKE_FORMATION", "false")
    monkeypatch.setenv("DATA_LAKE_ADMIN_ARN", f"arn:aws:iam::{os.getenv('ACCOUNT_ID','123456789012')}:role/admin-dev-cw")
    return PipelineSettings()


def test_catalog_stack_glue_objects(monkeypatch):
    settings = _settings(monkeypatch)
    app = cdk.App()
    storage = StorageStack(app, "Test-Storage", settings=settings)
    catalog = CatalogStack(app, "Test-Catalog", settings=settings, storage_stack=storage)
    t = Template.from_stack(catalog)

    t.has_resource_properties("AWS::Glue::Database", {
        "DatabaseInput": {"Name": "data_pipeline_db"}
    })

    # Crawler: name, database name, targets (S3 path)
    t.has_resource_properties("AWS::Glue::Crawler", {
        "Name": "data_pipeline_crawler",
        "DatabaseName": "data_pipeline_db",
        "Targets": Match.any_value(),
    })

    t.has_output("GlueDatabaseName", {"Value": "data_pipeline_db"})
    t.has_output("GlueCrawlerName", {"Value": "data_pipeline_crawler"})
