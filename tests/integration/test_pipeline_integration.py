"""Integration tests for the complete pipeline."""

import json
from datetime import datetime, UTC

import boto3
import pytest
from moto import mock_aws


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    import os
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def moto_env(aws_credentials):
    with mock_aws():
        yield


@pytest.fixture
def s3_client(moto_env):
    return boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def lambda_client(moto_env):
    return boto3.client("lambda", region_name="us-east-1")


@pytest.fixture
def glue_client(moto_env):
    return boto3.client("glue", region_name="us-east-1")


class TestPipelineIntegration:
    """Integration tests for data pipeline."""

    def test_end_to_end_data_flow(self, s3_client, lambda_client, glue_client):
        """Test complete data flow from API to S3."""
        # Create test bucket
        bucket_name = "test-data-bucket"
        s3_client.create_bucket(Bucket=bucket_name)

        # Create test database
        database_name = "test_database"
        glue_client.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': 'Test database'
            }
        )

        # Simulate Lambda execution
        from lambdas.data_extractor.s3_writer import S3Writer

        # Create test data
        test_data = [
            {"id": 1, "name": "Test 1", "value": 100},
            {"id": 2, "name": "Test 2", "value": 200}
        ]

        # Write data to S3
        writer = S3Writer(bucket_name)
        s3_key = writer.write_data(
            data=test_data,
            prefix="raw-data/year=2024/month=01/day=01",
            format="json"
        )

        # Verify data was written
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        stored_data = json.loads(response['Body'].read())

        assert len(stored_data) == 2
        assert stored_data[0]["id"] == 1

    def test_data_partitioning(self, s3_client):
        """Test that data is properly partitioned in S3."""
        bucket_name = "test-partition-bucket"
        s3_client.create_bucket(Bucket=bucket_name)

        from lambdas.data_extractor.utils import get_partition_path

        # Get partition path
        partition_path = get_partition_path()

        # Verify format
        assert "year=" in partition_path
        assert "month=" in partition_path
        assert "day=" in partition_path

        # Parse and verify values
        parts = partition_path.split("/")
        assert len(parts) == 3

        year = int(parts[0].split("=")[1])
        month = int(parts[1].split("=")[1])
        day = int(parts[2].split("=")[1])

        now = datetime.now(UTC)
        assert year == now.year
        assert month == now.month
        assert day == now.day
