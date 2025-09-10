"""Lambda handler for data extraction from public APIs."""

import json
import os
import traceback
from datetime import datetime, UTC
from typing import Dict, Any

from api_client import APIClient
from data_processor import DataProcessor
from s3_writer import S3Writer
from utils import setup_logging, get_partition_path

# Setup logging
logger = setup_logging()


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for data extraction pipeline.

    Args:
        event: Lambda event payload
        context: Lambda context object

    Returns:
        Response with status and processing details
    """
    request_id = getattr(context, "aws_request_id", "local-test")
    ctx = {
        "request_id": request_id,
        "function": getattr(context, "function_name", "local"),
        "version": getattr(context, "function_version", "local"),
    }
    logger.info(f"Starting data extraction - Context: {ctx}")

    try:
        # Get configuration from environment variables
        api_endpoint = os.environ.get("API_ENDPOINT", "https://jsonplaceholder.typicode.com/users")
        bucket_name = os.environ["DATA_BUCKET_NAME"]
        output_format = os.environ.get("OUTPUT_FORMAT", "parquet")

        # Extract optional parameters from event
        force_refresh = event.get("force_refresh", False)
        api_params = event.get("api_params", {})

        logger.info(f"Configuration - Endpoint: {api_endpoint}, Bucket: {bucket_name}, Format: {output_format}")

        # Step 1: Fetch data from API
        api_client = APIClient(api_endpoint)
        raw_data = api_client.fetch_data(params=api_params)

        if not raw_data:
            logger.warning("No data received from API")
            return {
                "statusCode": 204,
                "body": json.dumps({"message": "No data to process"})
            }

        logger.info(f"Fetched {len(raw_data)} records from API")

        # Step 2: Process and transform data
        processor = DataProcessor()
        processed_data = processor.process(raw_data)

        # Add metadata
        metadata = processor.add_metadata(processed_data, {
            "source": api_endpoint,
            "extraction_time": datetime.now(UTC).isoformat(),
            "request_id": request_id
        })

        logger.info(f"Processed {len(processed_data)} records")

        # Step 3: Write to S3
        s3_writer = S3Writer(bucket_name)
        partition_path = get_partition_path()

        s3_key = s3_writer.write_data(
            data=processed_data,
            prefix=f"raw-data/{partition_path}",
            format=output_format,
            metadata=metadata
        )

        logger.info(f"Data written to S3: s3://{bucket_name}/{s3_key}")

        # Return success response
        response = {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Data extraction completed successfully",
                "details": {
                    "records_processed": len(processed_data),
                    "s3_location": f"s3://{bucket_name}/{s3_key}",
                    "format": output_format,
                    "request_id": request_id
                }
            })
        }

        return response

    except KeyError as e:
        logger.error(f"Missing required configuration: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Configuration error: {str(e)}"})
        }
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}\n{traceback.format_exc()}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Processing failed: {str(e)}"})
        }
