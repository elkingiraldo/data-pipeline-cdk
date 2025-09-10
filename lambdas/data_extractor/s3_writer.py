"""S3 writer for storing processed data."""

import json
import logging
from datetime import datetime, UTC
from typing import Dict, Any, List, Optional

import boto3

logger = logging.getLogger(__name__)


class S3Writer:
    """Write data to S3 in various formats."""

    def __init__(self, bucket_name: str):
        """Initialize S3 writer."""
        self.bucket_name = bucket_name
        self.s3_client = boto3.client("s3")

    def write_data(
            self,
            data: List[Dict[str, Any]],
            prefix: str,
            format: str = "json",  # Cambiar default a JSON
            metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Write data to S3 in specified format."""
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

        # Por ahora solo soportar JSON y CSV sin pandas
        if format in ["parquet", "json"]:
            # Usar JSON como fallback para parquet
            s3_key = f"{prefix}/data_{timestamp}.json"
            content = self._to_json(data)
            content_type = "application/json"
        elif format == "csv":
            s3_key = f"{prefix}/data_{timestamp}.csv"
            content = self._to_csv_simple(data)
            content_type = "text/csv"
        else:
            raise ValueError(f"Unsupported format: {format}")

        # Prepare S3 metadata
        s3_metadata = {
            "record_count": str(len(data)),
            "format": "json" if format == "parquet" else format,  # Temporal
            "timestamp": timestamp
        }

        if metadata:
            for key, value in metadata.items():
                if len(key) + len(str(value)) < 1024:
                    s3_metadata[key.replace(" ", "_").lower()] = str(value)

        # Upload to S3
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=content,
                ContentType=content_type,
                Metadata=s3_metadata,
                ServerSideEncryption="AES256"
            )

            logger.info(f"Successfully wrote {len(data)} records to s3://{self.bucket_name}/{s3_key}")

            # Write metadata file
            self._write_metadata_file(s3_key, data, metadata)

            return s3_key

        except Exception as e:
            logger.error(f"Failed to write to S3: {str(e)}")
            raise

    def _to_json(self, data: List[Dict[str, Any]]) -> bytes:
        """Convert data to JSON format."""
        return json.dumps(data, indent=2, default=str).encode('utf-8')

    def _to_csv_simple(self, data: List[Dict[str, Any]]) -> bytes:
        """Convert data to CSV format without pandas."""
        if not data:
            return b""

        # Get headers from first record
        headers = list(data[0].keys())

        # Build CSV
        lines = []
        lines.append(",".join(headers))

        for record in data:
            values = [str(record.get(h, "")) for h in headers]
            lines.append(",".join(values))

        return "\n".join(lines).encode('utf-8')

    def _write_metadata_file(self, data_key: str, data: List[Dict[str, Any]],
                             metadata: Optional[Dict[str, Any]]) -> None:
        """Write metadata file alongside data file."""
        metadata_key = f"{data_key}.metadata.json"

        metadata_content = {
            "data_file": data_key,
            "created_at": datetime.now(UTC).isoformat(),
            "record_count": len(data),
            "schema": self._infer_schema(data),
            "custom_metadata": metadata or {}
        }

        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=metadata_key,
                Body=json.dumps(metadata_content, indent=2).encode('utf-8'),
                ContentType="application/json"
            )
            logger.info(f"Metadata written to {metadata_key}")
        except Exception as e:
            logger.warning(f"Failed to write metadata file: {str(e)}")

    def _infer_schema(self, data: List[Dict[str, Any]]) -> Dict[str, str]:
        """Infer schema from data."""
        if not data:
            return {}

        schema = {}
        sample = data[0]

        for key, value in sample.items():
            schema[key] = type(value).__name__

        return schema
