"""Analytics stack for Athena and Lake Formation resources."""

from aws_cdk import (
    Stack,
    aws_athena as athena,
    aws_iam as iam,
    aws_lakeformation as lakeformation,
    Tags,
    CfnOutput
)
from constructs import Construct

from infrastructure.config.settings import PipelineSettings
from infrastructure.stacks.catalog_stack import CatalogStack
from infrastructure.stacks.compute_stack import ComputeStack
from infrastructure.stacks.storage_stack import StorageStack


class AnalyticsStack(Stack):
    """Stack for analytics resources including Athena and Lake Formation."""

    def __init__(
            self,
            scope: Construct,
            id: str,
            settings: PipelineSettings,
            storage_stack: StorageStack,
            catalog_stack: CatalogStack,
            compute_stack: ComputeStack,
            **kwargs
    ) -> None:
        """
        Initialize analytics stack.

        Args:
            scope: Parent construct
            id: Stack ID
            settings: Pipeline settings
            storage_stack: Reference to storage stack
            catalog_stack: Reference to catalog stack
            compute_stack: Reference to compute stack
            **kwargs: Additional stack properties
        """
        super().__init__(scope, id, **kwargs)

        self.settings = settings
        self.storage_stack = storage_stack
        self.catalog_stack = catalog_stack
        self.compute_stack = compute_stack

        # Create Athena workgroup
        self.athena_workgroup = self._create_athena_workgroup()

        # Create analytics role
        self.analytics_role = self._create_analytics_role()

        # Setup Lake Formation if enabled
        if self.settings.enable_lake_formation:
            self._setup_lake_formation()

        # Apply tags
        self._apply_tags()

        # Outputs
        self._create_outputs()

    def _create_athena_workgroup(self) -> athena.CfnWorkGroup:
        """Create Athena workgroup for query execution."""
        workgroup = athena.CfnWorkGroup(
            self,
            "AthenaWorkGroup",
            name=f"{self.settings.project_name}-workgroup",
            description=f"Workgroup for {self.settings.project_name} analytics",

            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                # Results configuration
                result_configuration_updates=athena.CfnWorkGroup.ResultConfigurationUpdatesProperty(
                    output_location=f"s3://{self.storage_stack.athena_results_bucket.bucket_name}/query-results/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3"
                    )
                ),

                # Engine version
                engine_version=athena.CfnWorkGroup.EngineVersionProperty(
                    selected_engine_version="Athena engine version 3"
                ),

                # Enforce workgroup configuration
                enforce_work_group_configuration=True,

                # Enable CloudWatch metrics
                publish_cloud_watch_metrics_enabled=True,

                # Bytes scanned cutoff for cost control
                bytes_scanned_cutoff_per_query=10737418240  # 10 GB
            ),

            tags=[
                {
                    "key": "Environment",
                    "value": self.settings.environment
                },
                {
                    "key": "Project",
                    "value": self.settings.project_name
                }
            ]
        )

        return workgroup

    def _create_analytics_role(self) -> iam.Role:
        """Create IAM role for analytics users."""
        role = iam.Role(
            self,
            "AnalyticsRole",
            role_name=f"{self.settings.project_name}-analytics-role",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("athena.amazonaws.com"),
                iam.AccountPrincipal(self.account)
            ),
            description="Role for analytics users to query data"
        )

        # Athena permissions
        role.add_to_policy(
            iam.PolicyStatement(
                sid="AthenaAccess",
                effect=iam.Effect.ALLOW,
                actions=[
                    "athena:GetWorkGroup",
                    "athena:StartQueryExecution",
                    "athena:StopQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "athena:GetDataCatalog",
                    "athena:ListDataCatalogs",
                    "athena:ListWorkGroups"
                ],
                resources=[
                    f"arn:aws:athena:{self.region}:{self.account}:workgroup/{self.athena_workgroup.name}",
                    f"arn:aws:athena:{self.region}:{self.account}:datacatalog/AwsDataCatalog"
                ]
            )
        )

        # Glue catalog permissions
        role.add_to_policy(
            iam.PolicyStatement(
                sid="GlueCatalogAccess",
                effect=iam.Effect.ALLOW,
                actions=[
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:GetDatabases"
                ],
                resources=[
                    f"arn:aws:glue:{self.region}:{self.account}:catalog",
                    f"arn:aws:glue:{self.region}:{self.account}:database/{self.catalog_stack.glue_database.database_input.name}",
                    f"arn:aws:glue:{self.region}:{self.account}:table/{self.catalog_stack.glue_database.database_input.name}/*"
                ]
            )
        )

        # S3 permissions for data and results
        role.add_to_policy(
            iam.PolicyStatement(
                sid="S3DataAccess",
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation"
                ],
                resources=[
                    self.storage_stack.data_bucket.bucket_arn,
                    f"{self.storage_stack.data_bucket.bucket_arn}/*"
                ]
            )
        )

        role.add_to_policy(
            iam.PolicyStatement(
                sid="S3ResultsAccess",
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation"
                ],
                resources=[
                    self.storage_stack.athena_results_bucket.bucket_arn,
                    f"{self.storage_stack.athena_results_bucket.bucket_arn}/*"
                ]
            )
        )

        return role

    def _setup_lake_formation(self) -> None:
        """Setup Lake Formation permissions and governance."""

        # Register S3 location with Lake Formation
        data_lake_location = lakeformation.CfnResource(
            self,
            "DataLakeLocation",
            resource_arn=self.storage_stack.data_bucket.bucket_arn,
            use_service_linked_role=True
        )

        # Grant permissions to Lambda role for writing data
        lakeformation.CfnPermissions(
            self,
            "LambdaDataLocationPermission",
            data_lake_principal=lakeformation.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=self.compute_stack.lambda_role.role_arn
            ),
            resource=lakeformation.CfnPermissions.ResourceProperty(
                data_location_resource=lakeformation.CfnPermissions.DataLocationResourceProperty(
                    s3_resource=self.storage_stack.data_bucket.bucket_arn
                )
            ),
            permissions=["DATA_LOCATION_ACCESS"]
        )

        # Grant permissions to Crawler role for cataloging
        lakeformation.CfnPermissions(
            self,
            "CrawlerDatabasePermission",
            data_lake_principal=lakeformation.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=self.catalog_stack.crawler_role.role_arn
            ),
            resource=lakeformation.CfnPermissions.ResourceProperty(
                database_resource=lakeformation.CfnPermissions.DatabaseResourceProperty(
                    name=self.catalog_stack.glue_database.database_input.name
                )
            ),
            permissions=["CREATE_TABLE", "ALTER", "DROP"]
        )

        # Grant permissions to Analytics role for querying
        lakeformation.CfnPermissions(
            self,
            "AnalyticsTablePermission",
            data_lake_principal=lakeformation.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=self.analytics_role.role_arn
            ),
            resource=lakeformation.CfnPermissions.ResourceProperty(
                table_resource=lakeformation.CfnPermissions.TableResourceProperty(
                    database_name=self.catalog_stack.glue_database.database_input.name,
                    table_wildcard={}  # Grant access to all tables
                )
            ),
            permissions=["SELECT", "DESCRIBE"],
            permissions_with_grant_option=[]
        )

        # Create Lake Formation tags for data classification
        data_classification_tag = lakeformation.CfnTag(
            self,
            "DataClassificationTag",
            tag_key="DataClassification",
            tag_values=["Public", "Internal", "Confidential", "Restricted"]
        )

        environment_tag = lakeformation.CfnTag(
            self,
            "EnvironmentTag",
            tag_key="Environment",
            tag_values=["dev", "staging", "prod"]
        )

    def _apply_tags(self) -> None:
        """Apply tags to all resources in the stack."""
        tags = self.settings.get_common_tags()

        for key, value in tags.items():
            Tags.of(self).add(key, value)

        Tags.of(self.athena_workgroup).add("Type", "Analytics")
        Tags.of(self.analytics_role).add("Type", "AnalyticsRole")

    def _create_outputs(self) -> None:
        """Create stack outputs."""
        CfnOutput(
            self,
            "AthenaWorkgroupName",
            value=self.athena_workgroup.name,
            description="Name of the Athena workgroup",
            export_name=f"{self.stack_name}-workgroup-name"
        )

        CfnOutput(
            self,
            "AnalyticsRoleArn",
            value=self.analytics_role.role_arn,
            description="ARN of the analytics role",
            export_name=f"{self.stack_name}-analytics-role-arn"
        )

        CfnOutput(
            self,
            "QueryResultsLocation",
            value=f"s3://{self.storage_stack.athena_results_bucket.bucket_name}/query-results/",
            description="S3 location for Athena query results",
            export_name=f"{self.stack_name}-query-results-location"
        )
