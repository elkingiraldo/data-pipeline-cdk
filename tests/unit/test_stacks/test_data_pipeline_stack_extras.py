import aws_cdk as cdk
from aws_cdk.assertions import Template, Match
from infrastructure.config.settings import PipelineSettings
from infrastructure.stacks.data_pipeline_stack import DataPipelineStack


def test_main_stack_outputs(monkeypatch):
    # Add environment variables needed for PipelineSettings
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("REGION", "us-east-1")
    monkeypatch.setenv("ACCOUNT_ID", "123456789012")
    monkeypatch.setenv("PROJECT_NAME", "data-pipeline")
    monkeypatch.setenv("OWNER_TAG", "data-engineering")
    monkeypatch.setenv("API_ENDPOINT", "https://example.com")
    monkeypatch.setenv("LAMBDA_TIMEOUT", "60")
    monkeypatch.setenv("LAMBDA_MEMORY", "256")
    monkeypatch.setenv("OUTPUT_FORMAT", "parquet")
    monkeypatch.setenv("ENABLE_LAKE_FORMATION", "false")
    monkeypatch.setenv("GLUE_DATABASE_NAME", "data_pipeline_db")
    monkeypatch.setenv("GLUE_CRAWLER_NAME", "data_pipeline_crawler")
    monkeypatch.setenv("CRAWLER_SCHEDULE", "cron(0 2 * * ? *)")

    settings = PipelineSettings()
    app = cdk.App()
    stack = DataPipelineStack(app, "Main-Stack", settings=settings)

    t = Template.from_stack(stack)
    t.has_output("ProjectName", {"Value": "data-pipeline"})
    t.has_output("Environment", {"Value": "dev"})
