"""CDK application entry point."""

import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

import aws_cdk as cdk
from infrastructure.stacks.data_pipeline_stack import DataPipelineStack
from infrastructure.config.settings import PipelineSettings


def main():
    """Main entry point for CDK application."""

    # Load settings
    settings = PipelineSettings()

    # Create CDK app
    app = cdk.App()

    # Add context values
    app.node.set_context("environment", settings.environment)
    app.node.set_context("project", settings.project_name)

    # Get account and region from environment or use defaults
    account = os.environ.get("CDK_DEFAULT_ACCOUNT", settings.account_id)
    region = os.environ.get("CDK_DEFAULT_REGION", settings.region)

    # Create stack environment
    env = cdk.Environment(
        account=account,
        region=region
    )

    # Create main pipeline stack
    DataPipelineStack(
        app,
        f"DataPipelineStack-{settings.environment}",
        settings=settings,
        env=env,
        description=f"Data Pipeline Stack for {settings.environment} environment",
        stack_name=f"data-pipeline-{settings.environment}"
    )

    # Synthesize
    app.synth()


if __name__ == "__main__":
    main()
