#!/usr/bin/env python3
"""Test the deployed data pipeline."""

import json
import sys
import time
from typing import Dict

import boto3


class PipelineTester:
    """Test the deployed data pipeline."""

    def __init__(self, environment: str = "dev"):
        """Initialize pipeline tester."""
        self.environment = environment
        self.project_name = "data-pipeline"

        # Initialize AWS clients
        self.lambda_client = boto3.client("lambda")
        self.s3_client = boto3.client("s3")
        self.glue_client = boto3.client("glue")
        self.athena_client = boto3.client("athena")

        # Get stack outputs
        self.stack_outputs = self._get_stack_outputs()

    def _get_stack_outputs(self) -> Dict[str, str]:
        """Get CloudFormation stack outputs."""
        cf_client = boto3.client("cloudformation")
        stack_name = f"{self.project_name}-{self.environment}"

        try:
            response = cf_client.describe_stacks(StackName=stack_name)
            outputs = response["Stacks"][0].get("Outputs", [])

            return {
                output["OutputKey"]: output["OutputValue"]
                for output in outputs
            }
        except Exception as e:
            print(f"❌ Failed to get stack outputs: {e}")
            return {}

    def test_lambda_invocation(self) -> bool:
        """Test Lambda function invocation."""
        print("🔄 Testing Lambda function...")

        function_name = f"{self.project_name}-data-extractor"

        try:
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType="RequestResponse",
                Payload=json.dumps({"force_refresh": True})
            )

            status_code = response["StatusCode"]
            payload = json.loads(response["Payload"].read())

            if status_code == 200:
                print(f"✅ Lambda invocation successful")
                print(f"   Response: {json.dumps(payload, indent=2)}")
                return True
            else:
                print(f"❌ Lambda invocation failed with status {status_code}")
                return False

        except Exception as e:
            print(f"❌ Lambda invocation error: {e}")
            return False

    def test_s3_data_exists(self) -> bool:
        """Test if data exists in S3."""
        print("🔄 Checking S3 for data...")

        bucket_name = self.stack_outputs.get("DataBucketName")
        if not bucket_name:
            print("❌ Could not find data bucket name")
            return False

        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix="raw-data/",
                MaxKeys=10
            )

            if "Contents" in response:
                print(f"✅ Found {len(response['Contents'])} objects in S3")
                for obj in response["Contents"][:3]:
                    print(f"   - {obj['Key']} ({obj['Size']} bytes)")
                return True
            else:
                print("⚠️  No data found in S3 yet")
                return False

        except Exception as e:
            print(f"❌ S3 check error: {e}")
            return False

    def test_glue_crawler(self) -> bool:
        """Test Glue crawler."""
        print("🔄 Testing Glue crawler...")

        crawler_name = f"{self.project_name}_crawler"

        try:
            # Get crawler status
            response = self.glue_client.get_crawler(Name=crawler_name)
            crawler = response["Crawler"]

            print(f"✅ Crawler found: {crawler_name}")
            print(f"   State: {crawler['State']}")
            print(f"   Last run: {crawler.get('LastCrawl', {}).get('Status', 'Never run')}")

            # Start crawler if not running
            if crawler["State"] == "READY":
                print("🔄 Starting crawler...")
                self.glue_client.start_crawler(Name=crawler_name)
                print("✅ Crawler started")

            return True

        except Exception as e:
            print(f"❌ Glue crawler error: {e}")
            return False

    def test_athena_query(self) -> bool:
        """Test Athena query."""
        print("🔄 Testing Athena query...")

        database_name = self.stack_outputs.get("GlueDatabaseName", f"{self.project_name}_db")
        workgroup_name = self.stack_outputs.get("AthenaWorkgroupName", f"{self.project_name}-workgroup")
        results_location = self.stack_outputs.get("QueryResultsLocation")

        if not results_location:
            print("⚠️  Could not find query results location")
            return False

        # Simple test query
        query = f"SHOW TABLES IN {database_name}"

        try:
            # Start query
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": database_name},
                ResultConfiguration={"OutputLocation": results_location},
                WorkGroup=workgroup_name
            )

            query_id = response["QueryExecutionId"]
            print(f"   Query ID: {query_id}")

            # Wait for completion
            max_attempts = 30
            for i in range(max_attempts):
                response = self.athena_client.get_query_execution(
                    QueryExecutionId=query_id
                )

                status = response["QueryExecution"]["Status"]["State"]

                if status == "SUCCEEDED":
                    print("✅ Athena query succeeded")

                    # Get results
                    results = self.athena_client.get_query_results(
                        QueryExecutionId=query_id
                    )

                    rows = results.get("ResultSet", {}).get("Rows", [])
                    if len(rows) > 1:  # First row is header
                        print(f"   Found {len(rows) - 1} tables")
                        for row in rows[1:]:
                            table_name = row["Data"][0].get("VarCharValue", "")
                            print(f"   - {table_name}")

                    return True

                elif status == "FAILED":
                    error = response["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
                    print(f"❌ Query failed: {error}")
                    return False

                time.sleep(1)

            print("⚠️  Query timeout")
            return False

        except Exception as e:
            print(f"❌ Athena query error: {e}")
            return False

    def run_all_tests(self) -> bool:
        """Run all pipeline tests."""
        print("=" * 50)
        print(f"🧪 Testing Data Pipeline - {self.environment}")
        print("=" * 50)
        print()

        tests = [
            self.test_lambda_invocation,
            self.test_s3_data_exists,
            self.test_glue_crawler,
            self.test_athena_query
        ]

        results = []
        for test in tests:
            result = test()
            results.append(result)
            print()

        # Summary
        print("=" * 50)
        print("📊 Test Summary")
        print("=" * 50)

        passed = sum(results)
        total = len(results)

        if passed == total:
            print(f"✅ All tests passed ({passed}/{total})")
            return True
        else:
            print(f"⚠️  {passed}/{total} tests passed")
            return False


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Test the deployed data pipeline")
    parser.add_argument(
        "--environment",
        default="dev",
        help="Environment to test (default: dev)"
    )

    args = parser.parse_args()

    tester = PipelineTester(environment=args.environment)
    success = tester.run_all_tests()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
