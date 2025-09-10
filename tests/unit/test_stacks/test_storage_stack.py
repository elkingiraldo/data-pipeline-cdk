import pytest
from aws_cdk import App
from aws_cdk.assertions import Template, Match

from infrastructure.config.settings import PipelineSettings
from infrastructure.stacks.storage_stack import StorageStack


class TestStorageStack:
    """Test storage stack resources."""

    @pytest.fixture
    def app(self):
        """Create CDK app."""
        return App()

    @pytest.fixture
    def settings(self):
        """Create test settings."""
        return PipelineSettings(
            environment="test",
            project_name="test-pipeline",
            data_bucket_name="test-pipeline-data",
            athena_results_bucket="test-pipeline-athena"
        )

    def test_s3_buckets_created(self, app, settings):
        """Test that S3 buckets are created with correct properties."""
        # Create stack
        stack = StorageStack(app, "TestStorageStack", settings=settings)

        # Get template
        template = Template.from_stack(stack)

        # Assert data bucket exists
        template.resource_count_is("AWS::S3::Bucket", 2)

        # Check data bucket properties
        template.has_resource_properties("AWS::S3::Bucket", {
            "BucketName": "test-pipeline-data",
            "VersioningConfiguration": {
                "Status": "Enabled"
            },
            "BucketEncryption": {
                "ServerSideEncryptionConfiguration": [
                    {
                        "ServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "AES256"
                        }
                    }
                ]
            },
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True
            }
        })

    def test_lifecycle_rules(self, app, settings):
        """Test that lifecycle rules are configured."""
        stack = StorageStack(app, "TestStorageStack", settings=settings)
        template = Template.from_stack(stack)

        # Check lifecycle rules
        template.has_resource_properties("AWS::S3::Bucket", {
            "LifecycleConfiguration": {
                "Rules": Match.array_with([
                    Match.object_like({
                        "Id": "TransitionToIA",
                        "Status": "Enabled"
                    }),
                    Match.object_like({
                        "Id": "DeleteOldVersions",
                        "Status": "Enabled"
                    })
                ])
            }
        })

    def test_bucket_policy(self, app, settings):
        """Test that bucket policy denies insecure connections."""
        stack = StorageStack(app, "TestStorageStack", settings=settings)
        template = Template.from_stack(stack)

        # Check bucket policy
        template.has_resource_properties("AWS::S3::BucketPolicy", {
            "PolicyDocument": Match.object_like({
                "Statement": Match.array_with([
                    Match.object_like({
                        "Sid": "DenyInsecureConnections",
                        "Effect": "Deny",
                        "Condition": {
                            "Bool": {
                                "aws:SecureTransport": "false"
                            }
                        }
                    })
                ])
            })
        })

    def test_stack_outputs(self, app, settings):
        """Test that stack outputs are created."""
        stack = StorageStack(app, "TestStorageStack", settings=settings)
        template = Template.from_stack(stack)

        # Check outputs
        template.has_output("DataBucketName", {
            "Value": Match.any_value(),
            "Description": "Name of the data lake bucket"
        })

        template.has_output("DataBucketArn", {
            "Value": Match.any_value(),
            "Description": "ARN of the data lake bucket"
        })
