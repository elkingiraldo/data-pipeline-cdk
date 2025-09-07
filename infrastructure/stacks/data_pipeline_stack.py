"""Main data pipeline stack that orchestrates all sub-stacks."""

from aws_cdk import (
    Stack,
    Stage,
    Tags,
    CfnOutput
)
from constructs import Construct

from infrastructure.config.settings import PipelineSettings
from infrastructure.stacks.analytics_stack import AnalyticsStack
from infrastructure.stacks.catalog_stack import CatalogStack
from infrastructure.stacks.compute_stack import ComputeStack
from infrastructure.stacks.storage_stack import StorageStack


class DataPipelineStack(Stack):
    """Main stack that orchestrates all data pipeline components."""

    def __init__(
            self,
            scope: Construct,
            id: str,
            settings: PipelineSettings,
            **kwargs
    ) -> None:
        """
        Initialize main data pipeline stack.

        Args:
            scope: Parent construct
            id: Stack ID
            settings: Pipeline settings
            **kwargs: Additional stack properties
        """
        super().__init__(scope, id, **kwargs)

        self.settings = settings

        # Create storage stack
        self.storage_stack = StorageStack(
            self,
            f"{id}-Storage",
            settings=settings,
            description="Storage resources for data pipeline"
        )

        # Create compute stack
        self.compute_stack = ComputeStack(
            self,
            f"{id}-Compute",
            settings=settings,
            storage_stack=self.storage_stack,
            description="Compute resources for data pipeline"
        )

        # Create catalog stack
        self.catalog_stack = CatalogStack(
            self,
            f"{id}-Catalog",
            settings=settings,
            storage_stack=self.storage_stack,
            description="Data catalog resources for data pipeline"
        )

        # Create analytics stack
        self.analytics_stack = AnalyticsStack(
            self,
            f"{id}-Analytics",
            settings=settings,
            storage_stack=self.storage_stack,
            catalog_stack=self.catalog_stack,
            compute_stack=self.compute_stack,
            description="Analytics resources for data pipeline"
        )

        # Apply common tags
        self._apply_tags()

        # Create outputs
        self._create_outputs()

    def _apply_tags(self) -> None:
        """Apply common tags to all resources."""
        tags = self.settings.get_common_tags()

        for key, value in tags.items():
            Tags.of(self).add(key, value)

    def _create_outputs(self) -> None:
        """Create main stack outputs."""
        CfnOutput(
            self,
            "ProjectName",
            value=self.settings.project_name,
            description="Project name"
        )

        CfnOutput(
            self,
            "Environment",
            value=self.settings.environment,
            description="Environment"
        )

        CfnOutput(
            self,
            "DataBucketName",
            value=self.storage_stack.data_bucket.bucket_name,
            description="S3 bucket for data storage"
        )

        CfnOutput(
            self,
            "LambdaFunctionName",
            value=self.compute_stack.data_extractor.function_name,
            description="Data extractor Lambda function"
        )

        CfnOutput(
            self,
            "GlueDatabaseName",
            value=self.catalog_stack.glue_database.database_input.name,
            description="Glue database name"
        )

        CfnOutput(
            self,
            "AthenaWorkgroupName",
            value=self.analytics_stack.athena_workgroup.name,
            description="Athena workgroup name"
        )


class DataPipelineStage(Stage):
    """CDK Stage for deploying the data pipeline."""

    def __init__(
            self,
            scope: Construct,
            id: str,
            settings: PipelineSettings,
            **kwargs
    ) -> None:
        """
        Initialize data pipeline stage.

        Args:
            scope: Parent construct
            id: Stage ID
            settings: Pipeline settings
            **kwargs: Additional stage properties
        """
        super().__init__(scope, id, **kwargs)

        # Create main pipeline stack
        DataPipelineStack(
            self,
            f"DataPipeline-{settings.environment}",
            settings=settings
        )
