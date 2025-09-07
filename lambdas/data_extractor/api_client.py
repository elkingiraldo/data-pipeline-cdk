"""API client for fetching data from external sources."""

import logging
import time
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

import backoff
import requests

logger = logging.getLogger(__name__)


class APIClient:
    """Client for interacting with external APIs."""

    def __init__(self, base_url: str, timeout: int = 30, max_retries: int = 3):
        """
        Initialize API client.

        Args:
            base_url: Base URL of the API
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
        """
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "DataPipeline/1.0",
            "Accept": "application/json"
        })

    @backoff.on_exception(
        backoff.expo,
        (requests.RequestException, requests.Timeout),
        max_tries=3,
        max_time=60
    )
    def fetch_data(self, endpoint: str = "", params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Fetch data from the API with retry logic.

        Args:
            endpoint: API endpoint path
            params: Query parameters

        Returns:
            List of data records
        """
        url = urljoin(self.base_url, endpoint) if endpoint else self.base_url

        logger.info(f"Fetching data from: {url}")

        try:
            response = self.session.get(
                url,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()

            data = response.json()

            # Handle different response structures
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                # Try common patterns for data in response
                if "data" in data:
                    return data["data"]
                elif "results" in data:
                    return data["results"]
                elif "items" in data:
                    return data["items"]
                else:
                    # Wrap single object in list
                    return [data]
            else:
                logger.warning(f"Unexpected data type: {type(data)}")
                return []

        except requests.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            raise
        except ValueError as e:
            logger.error(f"Failed to parse JSON response: {str(e)}")
            raise

    def fetch_paginated_data(
            self,
            endpoint: str = "",
            page_size: int = 100,
            max_pages: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch paginated data from the API.

        Args:
            endpoint: API endpoint
            page_size: Number of records per page
            max_pages: Maximum number of pages to fetch

        Returns:
            Combined list of all records
        """
        all_data = []
        page = 1

        while True:
            if max_pages and page > max_pages:
                break

            params = {"page": page, "per_page": page_size}
            data = self.fetch_data(endpoint, params)

            if not data:
                break

            all_data.extend(data)
            logger.info(f"Fetched page {page} with {len(data)} records")

            # Simple rate limiting
            time.sleep(0.5)
            page += 1

        logger.info(f"Total records fetched: {len(all_data)}")
        return all_data

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
