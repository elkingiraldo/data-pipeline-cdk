"""Compute stack for Lambda functions and related resources."""

from pathlib import Path

from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_logs as logs,
    Duration,
    Tags,
    CfnOutput
)
from constructs import Construct

from infrastructure.config.settings import PipelineSettings
from infrastructure.stacks.storage_stack import StorageStack


class ComputeStack(Stack):
    """Stack for Lambda compute resources."""

    def __init__(
            self,
            scope: Construct,
            id: str,
            settings: PipelineSettings,
            storage_stack: StorageStack,
            **kwargs
    ) -> None:
        """
        Initialize compute stack.

        Args:
            scope: Parent construct
            id: Stack ID
            settings: Pipeline settings
            storage_stack: Reference to storage stack
            **kwargs: Additional stack properties
        """
        super().__init__(scope, id, **kwargs)

        self.settings = settings
        self.storage_stack = storage_stack

        # Create Lambda layer for shared dependencies
        self.common_layer = self._create_lambda_layer()

        # Create Lambda execution role
        self.lambda_role = self._create_lambda_role()

        # Create data extractor Lambda
        self.data_extractor = self._create_data_extractor_lambda()

        # Create scheduled trigger
        self._create_scheduled_trigger()

        # Apply tags
        self._apply_tags()

        # Outputs
        self._create_outputs()

    def _create_lambda_layer(self) -> lambda_.LayerVersion:
        """Create Lambda layer for common dependencies."""
        layer_path = Path("lambdas/layers/common")

        # Create layer
        layer = lambda_.LayerVersion(
            self,
            "CommonUtilsLayer",
            code=lambda_.Code.from_asset(str(layer_path)),
            compatible_runtimes=[
                lambda_.Runtime.PYTHON_3_13,
                lambda_.Runtime.PYTHON_3_12,
                lambda_.Runtime.PYTHON_3_11,
                lambda_.Runtime.PYTHON_3_10
            ],
            description="Common utilities and dependencies for Lambda functions",
            layer_version_name=f"{self.settings.project_name}-common-layer"
        )

        return layer

    def _create_lambda_role(self) -> iam.Role:
        """Create IAM role for Lambda execution."""
        role = iam.Role(
            self,
            "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Execution role for data pipeline Lambda functions",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ]
        )

        # Add S3 permissions
        role.add_to_policy(
            iam.PolicyStatement(
                sid="S3DataAccess",
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:PutObject",
                    "s3:PutObjectAcl",
                    "s3:GetObject",
                    "s3:GetObjectVersion",
                    "s3:DeleteObject",
                    "s3:ListBucket"
                ],
                resources=[
                    self.storage_stack.data_bucket.bucket_arn,
                    f"{self.storage_stack.data_bucket.bucket_arn}/*"
                ]
            )
        )

        # Add CloudWatch Logs permissions
        role.add_to_policy(
            iam.PolicyStatement(
                sid="CloudWatchLogs",
                effect=iam.Effect.ALLOW,
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                resources=[
                    f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/lambda/*"
                ]
            )
        )

        # Add X-Ray tracing permissions
        role.add_to_policy(
            iam.PolicyStatement(
                sid="XRayTracing",
                effect=iam.Effect.ALLOW,
                actions=[
                    "xray:PutTraceSegments",
                    "xray:PutTelemetryRecords"
                ],
                resources=["*"]
            )
        )

        return role

    def _create_data_extractor_lambda(self) -> lambda_.Function:
        """Create Lambda function for data extraction."""
        lambda_path = Path("lambdas/data_extractor")

        # Create Lambda function
        function = lambda_.Function(
            self,
            "DataExtractorFunction",
            runtime=lambda_.Runtime.PYTHON_3_13,
            code=lambda_.Code.from_asset(
                str(lambda_path),
                exclude=[
                    "**/__pycache__/**",
                    "**/*.pyc",
                    ".venv/**", "venv/**",
                    ".pytest_cache/**", "tests/**",
                    "node_modules/**",
                    "*.md", "Dockerfile",
                    "requirements.txt",
                    "requirements-dev.txt",
                ],
            ),
            handler="handler.lambda_handler",
            role=self.lambda_role,
            function_name=f"{self.settings.project_name}-data-extractor",
            description="Extract data from public APIs and store in S3",
            timeout=Duration.seconds(self.settings.lambda_timeout),
            memory_size=self.settings.lambda_memory,
            environment={
                "DATA_BUCKET_NAME": self.storage_stack.data_bucket.bucket_name,
                "API_ENDPOINT": self.settings.api_endpoint,
                "OUTPUT_FORMAT": self.settings.output_format,
                "LOG_LEVEL": "INFO",
                "ENVIRONMENT": self.settings.environment
            },
            layers=[self.common_layer],
            tracing=lambda_.Tracing.ACTIVE,
            retry_attempts=2,
            log_retention=logs.RetentionDays.ONE_WEEK if self.settings.environment == "dev" else logs.RetentionDays.ONE_MONTH,
        )

        # Grant S3 permissions
        self.storage_stack.data_bucket.grant_write(function)

        return function

    def _create_scheduled_trigger(self) -> None:
        """Create EventBridge rule for scheduled execution."""
        # Create schedule rule
        rule = events.Rule(
            self,
            "DataExtractionSchedule",
            rule_name=f"{self.settings.project_name}-extraction-schedule",
            description="Schedule for data extraction pipeline",
            schedule=events.Schedule.cron(
                minute="0",
                hour="*/6",  # Every 6 hours
                month="*",
                week_day="*",
                year="*"
            )
        )

        # Add Lambda as target
        rule.add_target(
            targets.LambdaFunction(
                self.data_extractor,
                retry_attempts=2,
                max_event_age=Duration.hours(1)
            )
        )

        # Grant invoke permission to EventBridge
        self.data_extractor.grant_invoke(iam.ServicePrincipal("events.amazonaws.com"))

    def _apply_tags(self) -> None:
        """Apply tags to all resources in the stack."""
        tags = self.settings.get_common_tags()

        for key, value in tags.items():
            Tags.of(self).add(key, value)

        # Add specific tags for Lambda
        Tags.of(self.data_extractor).add("Type", "DataExtractor")
        Tags.of(self.data_extractor).add("Schedule", "Every6Hours")

    def _create_outputs(self) -> None:
        """Create stack outputs."""
        CfnOutput(
            self,
            "DataExtractorFunctionName",
            value=self.data_extractor.function_name,
            description="Name of the data extractor Lambda function",
            export_name=f"{self.stack_name}-extractor-function-name"
        )

        CfnOutput(
            self,
            "DataExtractorFunctionArn",
            value=self.data_extractor.function_arn,
            description="ARN of the data extractor Lambda function",
            export_name=f"{self.stack_name}-extractor-function-arn"
        )

        CfnOutput(
            self,
            "LambdaRoleArn",
            value=self.lambda_role.role_arn,
            description="ARN of the Lambda execution role",
            export_name=f"{self.stack_name}-lambda-role-arn"
        )
