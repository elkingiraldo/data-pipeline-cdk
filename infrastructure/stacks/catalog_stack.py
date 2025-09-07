"""Catalog stack for AWS Glue resources."""

from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_iam as iam,
    aws_events as events,
    Tags,
    CfnOutput
)
from constructs import Construct

from infrastructure.config.settings import PipelineSettings
from infrastructure.stacks.storage_stack import StorageStack


class CatalogStack(Stack):
    """Stack for AWS Glue catalog resources."""

    def __init__(
            self,
            scope: Construct,
            id: str,
            settings: PipelineSettings,
            storage_stack: StorageStack,
            **kwargs
    ) -> None:
        """
        Initialize catalog stack.

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

        # Create Glue database
        self.glue_database = self._create_glue_database()

        # Create Glue crawler role
        self.crawler_role = self._create_crawler_role()

        # Create Glue crawler
        self.glue_crawler = self._create_glue_crawler()

        # Create crawler schedule
        self._create_crawler_schedule()

        # Apply tags
        self._apply_tags()

        # Outputs
        self._create_outputs()

    def _create_glue_database(self) -> glue.CfnDatabase:
        """Create Glue database for data catalog."""
        database = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=self.settings.glue_database_name,
                description=f"Data catalog for {self.settings.project_name} pipeline",
                location_uri=f"s3://{self.storage_stack.data_bucket.bucket_name}/",
                parameters={
                    "classification": "parquet",
                    "compressionType": "snappy",
                    "typeOfData": "file",
                    "project": self.settings.project_name,
                    "environment": self.settings.environment
                }
            )
        )

        return database

    def _create_crawler_role(self) -> iam.Role:
        """Create IAM role for Glue crawler."""
        role = iam.Role(
            self,
            "GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description="Role for Glue crawler to catalog data",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ]
        )

        # Add S3 permissions
        role.add_to_policy(
            iam.PolicyStatement(
                sid="S3Access",
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation",
                    "s3:GetBucketAcl",
                    "s3:GetObjectVersion"
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
                    f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws-glue/*"
                ]
            )
        )

        # Add Lake Formation permissions if enabled
        if self.settings.enable_lake_formation:
            role.add_to_policy(
                iam.PolicyStatement(
                    sid="LakeFormationAccess",
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "lakeformation:GetDataAccess",
                        "lakeformation:GrantPermissions",
                        "lakeformation:GetResourceLFTags",
                        "lakeformation:ListLFTags",
                        "lakeformation:GetLFTag"
                    ],
                    resources=["*"]
                )
            )

        return role

    def _create_glue_crawler(self) -> glue.CfnCrawler:
        """Create Glue crawler to catalog data."""
        crawler = glue.CfnCrawler(
            self,
            "GlueCrawler",
            name=self.settings.glue_crawler_name,
            role=self.crawler_role.role_arn,
            database_name=self.glue_database.database_input.name,
            description=f"Crawler for {self.settings.project_name} data pipeline",

            # Target S3 paths
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{self.storage_stack.data_bucket.bucket_name}/raw-data/",
                        exclusions=[
                            "**/*.metadata.json",
                            "**/access-logs/**",
                            "**/_temporary/**"
                        ]
                    )
                ]
            ),

            # Crawler configuration
            configuration="""
            {
                "Version": 1.0,
                "CrawlerOutput": {
                    "Partitions": {
                        "AddOrUpdateBehavior": "InheritFromTable"
                    },
                    "Tables": {
                        "AddOrUpdateBehavior": "MergeNewColumns"
                    }
                },
                "Grouping": {
                    "TableGroupingPolicy": "CombineCompatibleSchemas"
                }
            }
            """,

            # Schema change policy
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="DEPRECATE_IN_DATABASE"
            ),

            # Table prefix for organization
            table_prefix=f"{self.settings.project_name}_",

            # Tags for the crawler
            tags={
                "Environment": self.settings.environment,
                "Project": self.settings.project_name
            }
        )

        # Ensure database is created before crawler
        crawler.add_depends_on(self.glue_database)

        return crawler

    def _create_crawler_schedule(self) -> None:
        """Create EventBridge rule to trigger crawler."""
        if self.settings.crawler_schedule:
            # Parse cron expression for EventBridge
            # Convert AWS Glue cron to EventBridge format if needed
            schedule_expression = self.settings.crawler_schedule

            # Create rule
            rule = events.Rule(
                self,
                "CrawlerScheduleRule",
                rule_name=f"{self.settings.project_name}-crawler-schedule",
                description="Schedule for Glue crawler execution",
                schedule=events.Schedule.expression(schedule_expression)
            )

            # Note: Direct EventBridge to Glue Crawler target is not supported
            # You would typically use a Lambda to trigger the crawler
            # For simplicity, we'll rely on Glue's built-in scheduling

    def _apply_tags(self) -> None:
        """Apply tags to all resources in the stack."""
        tags = self.settings.get_common_tags()

        for key, value in tags.items():
            Tags.of(self).add(key, value)

        # Add specific tags
        Tags.of(self.glue_database).add("Type", "DataCatalog")
        Tags.of(self.glue_crawler).add("Type", "Crawler")

    def _create_outputs(self) -> None:
        """Create stack outputs."""
        CfnOutput(
            self,
            "GlueDatabaseName",
            value=self.glue_database.database_input.name,
            description="Name of the Glue database",
            export_name=f"{self.stack_name}-database-name"
        )

        CfnOutput(
            self,
            "GlueCrawlerName",
            value=self.glue_crawler.name,
            description="Name of the Glue crawler",
            export_name=f"{self.stack_name}-crawler-name"
        )

        CfnOutput(
            self,
            "CrawlerRoleArn",
            value=self.crawler_role.role_arn,
            description="ARN of the Glue crawler role",
            export_name=f"{self.stack_name}-crawler-role-arn"
        )
