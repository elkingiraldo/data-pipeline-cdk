"""Storage stack for S3 buckets and related resources."""

from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_iam as iam,
    RemovalPolicy,
    Duration,
    Tags,
    CfnOutput
)
from constructs import Construct

from infrastructure.config.settings import PipelineSettings


class StorageStack(Stack):
    """Stack for S3 storage resources."""

    def __init__(
            self,
            scope: Construct,
            id: str,
            settings: PipelineSettings,
            **kwargs
    ) -> None:
        """
        Initialize storage stack.

        Args:
            scope: Parent construct
            id: Stack ID
            settings: Pipeline settings
            **kwargs: Additional stack properties
        """
        super().__init__(scope, id, **kwargs)

        self.settings = settings

        # Create data lake bucket
        self.data_bucket = self._create_data_bucket()

        # Create Athena results bucket
        self.athena_results_bucket = self._create_athena_results_bucket()

        # Apply tags
        self._apply_tags()

        # Outputs
        self._create_outputs()

    def _create_data_bucket(self) -> s3.Bucket:
        """Create S3 bucket for data storage."""
        bucket = s3.Bucket(
            self,
            "DataBucket",
            bucket_name=self.settings.data_bucket_name,
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN if self.settings.environment == "prod" else RemovalPolicy.DESTROY,
            auto_delete_objects=self.settings.environment != "prod",

            # Lifecycle rules for cost optimization
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="TransitionToIA",
                    enabled=True,
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(30)
                        ),
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=Duration.days(90)
                        )
                    ]
                ),
                s3.LifecycleRule(
                    id="DeleteOldVersions",
                    enabled=True,
                    noncurrent_version_expiration=Duration.days(30)
                )
            ],

            # CORS configuration for potential web access
            cors=[
                s3.CorsRule(
                    allowed_methods=[s3.HttpMethods.GET, s3.HttpMethods.HEAD],
                    allowed_origins=["*"],
                    allowed_headers=["*"],
                    max_age=3000
                )
            ],

            # Enable server access logging
            server_access_logs_prefix="access-logs/"
        )

        # Add bucket policy for secure access
        bucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="DenyInsecureConnections",
                effect=iam.Effect.DENY,
                principals=[iam.AnyPrincipal()],
                actions=["s3:*"],
                resources=[
                    bucket.bucket_arn,
                    f"{bucket.bucket_arn}/*"
                ],
                conditions={
                    "Bool": {"aws:SecureTransport": "false"}
                }
            )
        )

        return bucket

    def _create_athena_results_bucket(self) -> s3.Bucket:
        """Create S3 bucket for Athena query results."""
        bucket = s3.Bucket(
            self,
            "AthenaResultsBucket",
            bucket_name=self.settings.athena_results_bucket,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN if self.settings.environment == "prod" else RemovalPolicy.DESTROY,
            auto_delete_objects=self.settings.environment != "prod",

            # Lifecycle rule to clean up old query results
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="DeleteOldQueryResults",
                    enabled=True,
                    expiration=Duration.days(7)
                )
            ]
        )

        return bucket

    def _apply_tags(self) -> None:
        """Apply tags to all resources in the stack."""
        tags = self.settings.get_common_tags()

        for key, value in tags.items():
            Tags.of(self).add(key, value)

        # Add specific tags for S3 buckets
        Tags.of(self.data_bucket).add("Type", "DataLake")
        Tags.of(self.data_bucket).add("DataClassification", "Internal")
        Tags.of(self.athena_results_bucket).add("Type", "QueryResults")

    def _create_outputs(self) -> None:
        """Create stack outputs."""
        CfnOutput(
            self,
            "DataBucketName",
            value=self.data_bucket.bucket_name,
            description="Name of the data lake bucket",
            export_name=f"{self.stack_name}-data-bucket-name"
        )

        CfnOutput(
            self,
            "DataBucketArn",
            value=self.data_bucket.bucket_arn,
            description="ARN of the data lake bucket",
            export_name=f"{self.stack_name}-data-bucket-arn"
        )

        CfnOutput(
            self,
            "AthenaResultsBucketName",
            value=self.athena_results_bucket.bucket_name,
            description="Name of the Athena results bucket",
            export_name=f"{self.stack_name}-athena-bucket-name"
        )
