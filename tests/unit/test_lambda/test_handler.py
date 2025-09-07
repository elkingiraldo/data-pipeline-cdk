"""Unit tests for Lambda handler."""

import json
from unittest.mock import Mock, patch

import pytest


# Mock the Lambda context
@pytest.fixture
def lambda_context():
    """Create mock Lambda context."""
    context = Mock()
    context.request_id = "test-request-123"
    context.function_name = "test-function"
    context.memory_limit_in_mb = 128
    context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test"
    return context


@pytest.fixture
def lambda_event():
    """Create sample Lambda event."""
    return {
        "force_refresh": False,
        "api_params": {}
    }


@pytest.fixture
def environment_variables(monkeypatch):
    """Set environment variables for testing."""
    monkeypatch.setenv("DATA_BUCKET_NAME", "test-bucket")
    monkeypatch.setenv("API_ENDPOINT", "https://api.example.com/data")
    monkeypatch.setenv("OUTPUT_FORMAT", "parquet")
    monkeypatch.setenv("LOG_LEVEL", "INFO")


class TestLambdaHandler:
    """Test Lambda handler functionality."""

    @patch('lambda.data_extractor.handler.APIClient')
    @patch('lambda.data_extractor.handler.DataProcessor')
    @patch('lambda.data_extractor.handler.S3Writer')
    def test_successful_data_extraction(
            self,
            mock_s3_writer,
            mock_processor,
            mock_api_client,
            lambda_event,
            lambda_context,
            environment_variables
    ):
        """Test successful data extraction and processing."""
        # Setup mocks
        mock_api_instance = Mock()
        mock_api_instance.fetch_data.return_value = [
            {"id": 1, "name": "Test User 1"},
            {"id": 2, "name": "Test User 2"}
        ]
        mock_api_client.return_value = mock_api_instance

        mock_processor_instance = Mock()
        mock_processor_instance.process.return_value = [
            {"id": "1", "name": "Test User 1", "processed_at": "2024-01-01T00:00:00"},
            {"id": "2", "name": "Test User 2", "processed_at": "2024-01-01T00:00:00"}
        ]
        mock_processor_instance.add_metadata.return_value = {"record_count": 2}
        mock_processor.return_value = mock_processor_instance

        mock_s3_instance = Mock()
        mock_s3_instance.write_data.return_value = "raw-data/year=2024/month=01/day=01/data_20240101_120000.parquet"
        mock_s3_writer.return_value = mock_s3_instance

        # Import handler after mocks are set up
        from lambdas.data_extractor.handler import lambda_handler

        # Execute
        response = lambda_handler(lambda_event, lambda_context)

        # Assertions
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["message"] == "Data extraction completed successfully"
        assert body["details"]["records_processed"] == 2
        assert "s3_location" in body["details"]

        # Verify mock calls
        mock_api_instance.fetch_data.assert_called_once()
        mock_processor_instance.process.assert_called_once()
        mock_s3_instance.write_data.assert_called_once()

    @patch('lambda.data_extractor.handler.APIClient')
    def test_no_data_from_api(
            self,
            mock_api_client,
            lambda_event,
            lambda_context,
            environment_variables
    ):
        """Test handling when API returns no data."""
        # Setup mock
        mock_api_instance = Mock()
        mock_api_instance.fetch_data.return_value = []
        mock_api_client.return_value = mock_api_instance

        from lambdas.data_extractor.handler import lambda_handler

        # Execute
        response = lambda_handler(lambda_event, lambda_context)

        # Assertions
        assert response["statusCode"] == 204
        body = json.loads(response["body"])
        assert body["message"] == "No data to process"

    def test_missing_environment_variable(
            self,
            lambda_event,
            lambda_context
    ):
        """Test handling of missing environment variables."""
        from lambdas.data_extractor.handler import lambda_handler

        # Execute without setting environment variables
        response = lambda_handler(lambda_event, lambda_context)

        # Assertions
        assert response["statusCode"] == 500
        body = json.loads(response["body"])
        assert "Configuration error" in body["error"]
