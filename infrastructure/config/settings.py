"""Configuration settings for the data pipeline."""

from typing import Dict, Optional

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineSettings(BaseSettings):
    """Pipeline configuration settings."""

    # Environment
    environment: str = Field(default="dev", description="Development Environment")
    region: str = Field(default="us-east-1", description="AWS region")
    account_id: Optional[str] = Field(default=None, description="AWS account ID")

    # Project
    project_name: str = Field(default="data-pipeline-cdk", description="Project name")
    owner_tag: str = Field(default="data-engineering", description="Owner tag")

    # S3 Configuration
    data_bucket_name: Optional[str] = Field(default="data-pipeline-cdk-dev-data-bucket", description="S3 bucket name")
    athena_results_bucket: Optional[str] = Field(default=None)

    # Lambda Configuration
    lambda_timeout: int = Field(default=300, description="Lambda timeout in seconds")
    lambda_memory: int = Field(default=1024, description="Lambda memory in MB")
    lambda_runtime: str = Field(default="python3.13", description="Lambda runtime")

    # API Configuration
    api_endpoint: str = Field(
        default="https://jsonplaceholder.typicode.com/users",
        description="API endpoint to fetch data from"
    )
    api_batch_size: int = Field(default=100, description="API batch size")

    # Glue Configuration
    glue_database_name: str = Field(default="data_pipeline_db", description="Glue database name")
    glue_crawler_name: str = Field(default="data_pipeline_crawler", description="Glue crawler name")
    crawler_schedule: str = Field(default="cron(0 2 * * ? *)", description="Crawler schedule")

    # Data Format
    output_format: str = Field(default="parquet", description="Output format (parquet, csv, json)")
    partition_keys: list = Field(default_factory=lambda: ["year", "month", "day"])

    # Lake Formation
    enable_lake_formation: bool = Field(default=True, description="Enable Lake Formation")
    data_lake_admin_arn: Optional[str] = Field(default=None)

    # Pydantic Settings (v2)
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    @model_validator(mode="after")
    def _fill_derived_defaults(self):
        if not self.data_bucket_name:
            self.data_bucket_name = f"{self.project_name}-{self.environment}-data-bucket"
        if not self.athena_results_bucket:
            self.athena_results_bucket = f"{self.project_name}-{self.environment}-athena-results"
        return self

    def get_common_tags(self) -> Dict[str, str]:
        """Get common tags for all resources."""
        return {
            "Environment": self.environment,
            "Project": self.project_name,
            "Owner": self.owner_tag,
            "ManagedBy": "CDK",
            "CostCenter": f"{self.project_name}-{self.environment}"
        }
