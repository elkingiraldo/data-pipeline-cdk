"""Data processing and transformation logic."""

import hashlib
import json
import logging
from datetime import datetime, UTC
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class DataProcessor:
    """Process and transform raw data."""

    def process(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process raw data from API.

        Args:
            raw_data: Raw data from API

        Returns:
            Processed data ready for storage
        """
        processed_data = []

        for record in raw_data:
            try:
                processed_record = self._process_record(record)
                if processed_record:
                    processed_data.append(processed_record)
            except Exception as e:
                logger.error(f"Failed to process record: {str(e)}, Record: {record}")
                continue

        logger.info(f"Successfully processed {len(processed_data)}/{len(raw_data)} records")
        return processed_data

    def _process_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process individual record.

        Args:
            record: Raw record

        Returns:
            Processed record or None if invalid
        """
        # Validate required fields (customize based on your API)
        if not record:
            return None

        # Create processed record with standardized fields
        processed = {
            # Preserve original ID or generate one
            "id": str(record.get("id", self._generate_id(record))),

            # Standardize timestamps
            "created_at": datetime.now(UTC).isoformat(),
            "processed_at": datetime.now(UTC).isoformat(),

            # Flatten nested structures if present
            **self._flatten_record(record),

            # Add data quality indicators
            "data_quality_score": self._calculate_quality_score(record),
            "is_complete": self._check_completeness(record)
        }

        return processed

    def _flatten_record(self, record: Dict[str, Any], parent_key: str = "", sep: str = "_") -> Dict[str, Any]:
        """
        Flatten nested dictionary structures.

        Args:
            record: Record to flatten
            parent_key: Parent key for nested fields
            sep: Separator for flattened keys

        Returns:
            Flattened dictionary
        """
        items = []

        for key, value in record.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key

            if isinstance(value, dict):
                items.extend(self._flatten_record(value, new_key, sep=sep).items())
            elif isinstance(value, list):
                # Convert lists to JSON strings for storage
                items.append((new_key, json.dumps(value)))
            else:
                items.append((new_key, value))

        return dict(items)

    def _generate_id(self, record: Dict[str, Any]) -> str:
        """Generate unique ID for record."""
        content = json.dumps(record, sort_keys=True)
        return hashlib.md5(content.encode()).hexdigest()

    def _calculate_quality_score(self, record: Dict[str, Any]) -> float:
        """
        Calculate data quality score.

        Args:
            record: Record to evaluate

        Returns:
            Quality score between 0 and 1
        """
        total_fields = len(record)
        if total_fields == 0:
            return 0.0

        filled_fields = sum(1 for v in record.values() if v is not None and v != "")
        return round(filled_fields / total_fields, 2)

    def _check_completeness(self, record: Dict[str, Any]) -> bool:
        """Check if record has all required fields."""
        # Define required fields based on your data model
        required_fields = ["id"]  # Add more as needed
        return all(record.get(field) is not None for field in required_fields)

    def add_metadata(self, data: List[Dict[str, Any]], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add metadata to processed data.

        Args:
            data: Processed data
            metadata: Metadata to add

        Returns:
            Metadata dictionary
        """
        return {
            **metadata,
            "record_count": len(data),
            "processing_timestamp": datetime.now(UTC).isoformat(),
            "schema_version": "1.0.0"
        }
