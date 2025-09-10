import os
import aws_cdk as cdk
from aws_cdk.assertions import Template, Match

from infrastructure.config.settings import PipelineSettings
from infrastructure.stacks.storage_stack import StorageStack
from infrastructure.stacks.compute_stack import ComputeStack


def _mk_settings(monkeypatch):
    # Valid settings for ComputeStack
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("REGION", "us-east-1")
    monkeypatch.setenv("ACCOUNT_ID", "123456789012")
    monkeypatch.setenv("PROJECT_NAME", "data-pipeline")
    monkeypatch.setenv("OWNER_TAG", "data-engineering")

    # Params for the Lambda function
    monkeypatch.setenv("API_ENDPOINT", "https://jsonplaceholder.typicode.com/users")
    monkeypatch.setenv("API_BATCH_SIZE", "100")
    monkeypatch.setenv("LAMBDA_TIMEOUT", "300")
    monkeypatch.setenv("LAMBDA_MEMORY", "1024")
    monkeypatch.setenv("OUTPUT_FORMAT", "parquet")

    # Lake Formation disabled for this test
    monkeypatch.setenv("ENABLE_LAKE_FORMATION", "false")

    acct = os.getenv("ACCOUNT_ID", "123456789012")
    monkeypatch.setenv("DATA_LAKE_ADMIN_ARN", f"arn:aws:iam::{acct}:role/admin-dev-cw")

    return PipelineSettings()


def test_compute_stack_lambda_and_schedule(monkeypatch):
    settings = _mk_settings(monkeypatch)
    app = cdk.App()

    storage = StorageStack(app, "Test-Storage", settings=settings)
    compute = ComputeStack(app, "Test-Compute", settings=settings, storage_stack=storage)

    t = Template.from_stack(compute)

    # Verify Lambda function properties
    t.has_resource_properties("AWS::Lambda::Function", {
        "Handler": "handler.lambda_handler",
        "Runtime": "python3.13",
        "Environment": {
            "Variables": {
                "DATA_BUCKET_NAME": Match.any_value(),
                "API_ENDPOINT": "https://jsonplaceholder.typicode.com/users",
                "OUTPUT_FORMAT": "parquet",
                "LOG_LEVEL": "INFO",
                "ENVIRONMENT": "dev",
            }
        }
    })

    t.has_resource_properties("AWS::Events::Rule", {
        "ScheduleExpression": "cron(0 */6 ? * * *)"
    })
