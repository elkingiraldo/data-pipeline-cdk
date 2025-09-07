"""Constants for the data pipeline."""

from enum import Enum


class DataFormat(str, Enum):
    """Supported data formats."""
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
    AVRO = "avro"


class CrawlerState(str, Enum):
    """Glue crawler states."""
    READY = "READY"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"


class LakeFormationPermission(str, Enum):
    """Lake Formation permissions."""
    ALL = "ALL"
    SELECT = "SELECT"
    ALTER = "ALTER"
    DROP = "DROP"
    DELETE = "DELETE"
    INSERT = "INSERT"
    CREATE_DATABASE = "CREATE_DATABASE"
    CREATE_TABLE = "CREATE_TABLE"
    DATA_LOCATION_ACCESS = "DATA_LOCATION_ACCESS"


# S3 paths
S3_RAW_DATA_PREFIX = "raw-data"
S3_PROCESSED_DATA_PREFIX = "processed-data"
S3_ARCHIVE_PREFIX = "archive"

# Glue configuration
GLUE_VERSION = "4.0"
GLUE_PYTHON_VERSION = "3.9"

# Lambda configuration
LAMBDA_LAYER_NAME = "common-utils-layer"

# Athena configuration
ATHENA_WORKGROUP = "primary"

# API endpoints (multiple options)
API_ENDPOINTS = {
    "jsonplaceholder": "https://jsonplaceholder.typicode.com/users",
    "randomuser": "https://randomuser.me/api/?results=100",
    "reqres": "https://reqres.in/api/users?page=1&per_page=100"
}
