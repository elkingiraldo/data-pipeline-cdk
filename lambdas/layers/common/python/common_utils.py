"""Common utilities for Lambda functions."""

import hashlib
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Dict, Optional, List

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


class LambdaResponse:
    """Helper class for creating Lambda responses."""

    @staticmethod
    def success(
            message: str,
            data: Optional[Dict[str, Any]] = None,
            status_code: int = 200
    ) -> Dict[str, Any]:
        """Create a success response."""
        body = {"message": message}
        if data:
            body["data"] = data

        return {
            "statusCode": status_code,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps(body, default=str)
        }

    @staticmethod
    def error(
            message: str,
            error_type: str = "Error",
            status_code: int = 500,
            details: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create an error response."""
        body = {
            "error": error_type,
            "message": message
        }
        if details:
            body["details"] = details

        return {
            "statusCode": status_code,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps(body, default=str)
        }


class DataValidator:
    """Validate and sanitize data."""

    @staticmethod
    def validate_required_fields(
            data: Dict[str, Any],
            required_fields: List[str]
    ) -> tuple[bool, Optional[str]]:
        """
        Validate that required fields are present.

        Returns:
            Tuple of (is_valid, error_message)
        """
        missing_fields = [
            field for field in required_fields
            if field not in data or data[field] is None
        ]

        if missing_fields:
            return False, f"Missing required fields: {', '.join(missing_fields)}"

        return True, None

    @staticmethod
    def sanitize_string(value: str, max_length: int = 1000) -> str:
        """Sanitize string input."""
        if not isinstance(value, str):
            value = str(value)

        # Remove control characters
        value = ''.join(char for char in value if ord(char) >= 32 or char == '\n')

        # Truncate if too long
        if len(value) > max_length:
            value = value[:max_length]

        return value.strip()

    @staticmethod
    def validate_email(email: str) -> bool:
        """Basic email validation."""
        import re
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))


class MetricsCollector:
    """Collect and log metrics."""

    def __init__(self, namespace: str = "DataPipeline"):
        """Initialize metrics collector."""
        self.namespace = namespace
        self.metrics = {}

    def record_metric(self, name: str, value: float, unit: str = "None") -> None:
        """Record a metric."""
        self.metrics[name] = {
            "value": value,
            "unit": unit,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    def record_duration(self, name: str):
        """Decorator to record function execution duration."""

        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    duration = time.time() - start_time
                    self.record_metric(f"{name}_duration", duration, "Seconds")
                    self.record_metric(f"{name}_success", 1, "Count")
                    return result
                except Exception as e:
                    duration = time.time() - start_time
                    self.record_metric(f"{name}_duration", duration, "Seconds")
                    self.record_metric(f"{name}_error", 1, "Count")
                    raise

            return wrapper

        return decorator

    def log_metrics(self) -> None:
        """Log all collected metrics."""
        logger.info(f"Metrics: {json.dumps(self.metrics, indent=2)}")


class RetryHandler:
    """Handle retries with exponential backoff."""

    @staticmethod
    def with_retry(
            max_attempts: int = 3,
            backoff_base: float = 2.0,
            max_delay: float = 60.0
    ):
        """
        Decorator for retrying functions with exponential backoff.

        Args:
            max_attempts: Maximum number of retry attempts
            backoff_base: Base for exponential backoff
            max_delay: Maximum delay between retries
        """

        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                attempt = 0
                delay = 1.0

                while attempt < max_attempts:
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        attempt += 1

                        if attempt >= max_attempts:
                            logger.error(f"Max retries ({max_attempts}) exceeded for {func.__name__}")
                            raise

                        logger.warning(
                            f"Attempt {attempt} failed for {func.__name__}: {str(e)}. "
                            f"Retrying in {delay} seconds..."
                        )

                        time.sleep(delay)
                        delay = min(delay * backoff_base, max_delay)

            return wrapper

        return decorator


class S3Helper:
    """Helper functions for S3 operations."""

    @staticmethod
    def generate_s3_key(
            prefix: str,
            filename: str,
            partition_by_date: bool = True,
            include_timestamp: bool = True
    ) -> str:
        """
        Generate S3 key with optional partitioning.

        Args:
            prefix: S3 key prefix
            filename: Base filename
            partition_by_date: Add date partitioning
            include_timestamp: Include timestamp in filename

        Returns:
            Complete S3 key
        """
        parts = [prefix.rstrip("/")]

        if partition_by_date:
            now = datetime.now(timezone.utc)
            parts.extend([
                f"year={now.year}",
                f"month={now.month:02d}",
                f"day={now.day:02d}"
            ])

        if include_timestamp:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            name, ext = os.path.splitext(filename)
            filename = f"{name}_{timestamp}{ext}"

        parts.append(filename)

        return "/".join(parts)

    @staticmethod
    def parse_s3_path(s3_path: str) -> tuple[str, str]:
        """
        Parse S3 path into bucket and key.

        Args:
            s3_path: S3 path (s3://bucket/key or bucket/key)

        Returns:
            Tuple of (bucket, key)
        """
        if s3_path.startswith("s3://"):
            s3_path = s3_path[5:]

        parts = s3_path.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""

        return bucket, key


class DataQualityChecker:
    """Check data quality metrics."""

    @staticmethod
    def calculate_completeness(
            data: List[Dict[str, Any]],
            required_fields: List[str]
    ) -> float:
        """
        Calculate data completeness percentage.

        Args:
            data: List of records
            required_fields: Fields to check

        Returns:
            Completeness percentage (0-100)
        """
        if not data:
            return 0.0

        total_fields = len(data) * len(required_fields)
        filled_fields = 0

        for record in data:
            for field in required_fields:
                if record.get(field) is not None and record.get(field) != "":
                    filled_fields += 1

        return (filled_fields / total_fields) * 100 if total_fields > 0 else 0.0

    @staticmethod
    def detect_duplicates(
            data: List[Dict[str, Any]],
            key_field: str
    ) -> List[Any]:
        """
        Detect duplicate records based on key field.

        Args:
            data: List of records
            key_field: Field to use as unique key

        Returns:
            List of duplicate key values
        """
        seen = set()
        duplicates = []

        for record in data:
            key_value = record.get(key_field)
            if key_value in seen:
                duplicates.append(key_value)
            else:
                seen.add(key_value)

        return duplicates

    @staticmethod
    def validate_schema(
            data: List[Dict[str, Any]],
            expected_schema: Dict[str, type]
    ) -> tuple[bool, List[str]]:
        """
        Validate data against expected schema.

        Args:
            data: List of records
            expected_schema: Expected field types

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []

        for i, record in enumerate(data):
            for field, expected_type in expected_schema.items():
                if field in record:
                    actual_value = record[field]
                    if actual_value is not None and not isinstance(actual_value, expected_type):
                        errors.append(
                            f"Record {i}: Field '{field}' has type {type(actual_value).__name__}, "
                            f"expected {expected_type.__name__}"
                        )

        return len(errors) == 0, errors


def generate_request_id() -> str:
    """Generate unique request ID."""
    return str(uuid.uuid4())


def get_current_timestamp() -> str:
    """Get current timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def calculate_checksum(data: str) -> str:
    """Calculate SHA256 checksum of data."""
    return hashlib.sha256(data.encode()).hexdigest()


def safe_json_dumps(obj: Any) -> str:
    """Safely convert object to JSON string."""
    return json.dumps(obj, default=str, ensure_ascii=False)


def log_lambda_event(event: Dict[str, Any], context: Any) -> None:
    """Log Lambda invocation details."""
    logger.info(f"Lambda invoked: {context.function_name}")
    logger.info(f"Request ID: {context.request_id}")
    logger.info(f"Event: {json.dumps(event, default=str)}")
    logger.info(f"Remaining time: {context.get_remaining_time_in_millis()}ms")
