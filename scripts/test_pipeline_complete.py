#!/usr/bin/env python3
"""End-to-end test for the deployed data pipeline."""

import json
import sys
import time
from typing import Dict

import boto3
from botocore.exceptions import ClientError


def test_pipeline() -> bool:
    print("🧪 STARTING PIPELINE TESTS")
    print("=" * 50)

    # AWS clients (use your default AWS profile/region)
    lambda_client = boto3.client("lambda")
    s3_client = boto3.client("s3")
    glue_client = boto3.client("glue")
    athena_client = boto3.client("athena")
    cf_client = boto3.client("cloudformation")

    # ---- Stack outputs ----
    stack_name = "data-pipeline-dev"
    stacks = cf_client.describe_stacks(StackName=stack_name)["Stacks"]
    outputs: Dict[str, str] = {o["OutputKey"]: o["OutputValue"] for o in stacks[0]["Outputs"]}

    bucket_name = outputs["DataBucketName"]
    function_name = outputs["LambdaFunctionName"]
    database_name = outputs["GlueDatabaseName"]
    workgroup_name = outputs.get("AthenaWorkgroupName", "primary")
    crawler_name = outputs.get("GlueCrawlerName", "data_pipeline_crawler")  # fallback

    print(f"📦 Bucket:   {bucket_name}")
    print(f"⚡ Lambda:   {function_name}")
    print(f"📊 Database: {database_name}")
    print(f"🧰 Workgroup:{workgroup_name}")
    print()

    # ---- Test 1: Invoke Lambda ----
    print("1️⃣ Invoking Lambda...")
    try:
        resp = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps({"force_refresh": True}).encode("utf-8"),
        )
        if resp["StatusCode"] != 200:
            print(f"❌ Lambda invoke returned HTTP {resp['StatusCode']}")
            return False

        payload = json.loads(resp["Payload"].read() or "{}")
        print("✅ Lambda executed")
        print(f"   Response: {payload}")
    except ClientError as e:
        print(f"❌ Lambda invoke error: {e}")
        return False

    # ---- Test 2: Verify data in S3 ----
    print("\n2️⃣ Verifying data in S3 (prefix raw-data/)...")
    time.sleep(5)  # small delay for eventual consistency
    s3_list = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="raw-data/", MaxKeys=10)
    contents = s3_list.get("Contents", [])
    if not contents:
        print("❌ No files found under raw-data/")
        # Not fatal, but queries will fail without data
    else:
        print(f"✅ Found {len(contents)} objects (showing up to 5):")
        for obj in contents[:5]:
            print(f"   - {obj['Key']}")

    # ---- Test 3: Run/Wait Glue Crawler ----
    print("\n3️⃣ Running Glue Crawler...")
    try:
        glue_client.start_crawler(Name=crawler_name)
        print("   Crawler started.")
    except ClientError as e:
        # If already running, keep waiting
        if "CrawlerRunningException" in str(e):
            print("   Crawler already running, waiting instead...")
        else:
            print(f"⚠️  start_crawler error: {e}")

    print("   Waiting until READY...")
    for _ in range(90):  # up to ~7.5 minutes
        state = glue_client.get_crawler(Name=crawler_name)["Crawler"]["State"]
        if state == "READY":
            print("✅ Crawler completed")
            break
        time.sleep(5)
    else:
        print("❌ Crawler did not reach READY in time")
        return False

    # ---- Test 4: Query in Athena (use WorkGroup & escape table name) ----
    print("\n4️⃣ Executing Athena query...")
    # Table name has a dash -> must be quoted
    query = 'SELECT COUNT(*) AS total FROM "data-pipeline_raw_data";'

    try:
        start = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database_name, "Catalog": "AwsDataCatalog"},
            WorkGroup=workgroup_name,  # use WG OutputLocation; do NOT pass ResultConfiguration here
        )
        query_id = start["QueryExecutionId"]
        print(f"   Query ID: {query_id}")
    except ClientError as e:
        print(f"❌ start_query_execution error: {e}")
        return False

    # Wait for completion
    final_status = None
    reason = ""
    output_loc = ""
    for _ in range(60):  # up to ~2 minutes
        q = athena_client.get_query_execution(QueryExecutionId=query_id)
        st = q["QueryExecution"]["Status"]
        final_status = st["State"]
        output_loc = q["QueryExecution"]["ResultConfiguration"].get("OutputLocation", "")
        if final_status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            reason = st.get("StateChangeReason", "")
            break
        time.sleep(2)

    if final_status != "SUCCEEDED":
        print(f"❌ Query failed -> {final_status}")
        if reason:
            print(f"   Reason: {reason}")
        if output_loc:
            print(f"   OutputLocation: {output_loc}")
        return False

    # Fetch results
    results = athena_client.get_query_results(QueryExecutionId=query_id)
    rows = results.get("ResultSet", {}).get("Rows", [])
    # first row is header
    total = rows[1]["Data"][0]["VarCharValue"] if len(rows) > 1 else "0"
    print("✅ Query succeeded")
    print(f"   total = {total}")

    print("\n" + "=" * 50)
    print("🎉 TESTS COMPLETED")

    return True


if __name__ == "__main__":
    ok = test_pipeline()
    sys.exit(0 if ok else 1)
