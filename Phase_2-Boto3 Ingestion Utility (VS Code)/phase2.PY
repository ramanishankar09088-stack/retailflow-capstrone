#!/usr/bin/env python
"""Phase 2 — Boto3 Ingestion Utility"""

import os
import time
import json
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Load environment variables
load_dotenv()

# Get configuration from environment
my_bucket_name = os.getenv("AWS_BUCKET_NAME")
TARGET_BUCKET = os.getenv("AWS_TARGET_BUCKET")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

print(f"Bucket Name: {my_bucket_name}")
print(f"Target Bucket: {TARGET_BUCKET}")
print(f"Region: {AWS_REGION}\n")

# Initialize boto3 clients
iam = boto3.client("iam")
sts = boto3.client("sts")
s3_client = boto3.client('s3', region_name=AWS_REGION)
logs_client = boto3.client('logs', region_name=AWS_REGION)

LOG_GROUP = "/aws/custom/s3-utility-logs"
LOG_STREAM = "ExecutionStream"

# Define IAM Policy
policy_name = "boto3-S3logs-mohank-Policy"
account_id = sts.get_caller_identity()["Account"]

policy_doc = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": ["s3:*", "logs:*"],
        "Resource": "*"
    }]
}

def send_log_to_cloudwatch(message):
    """Print locally and send log message to AWS CloudWatch Logs."""
    print(f"INFO: {message}")
    timestamp = int(round(time.time() * 1000))
    try:
        logs_client.put_log_events(
            logGroupName=LOG_GROUP,
            logStreamName=LOG_STREAM,
            logEvents=[{'timestamp': timestamp, 'message': message}]
        )
    except Exception as e:
        print(f"Failed to send log to CloudWatch: {e}")

def setup_cloudwatch_logs():
    """Create Log Group and Stream in CloudWatch if they don't exist."""
    try:
        logs_client.create_log_group(logGroupName=LOG_GROUP)
    except ClientError:
        pass
    try:
        logs_client.create_log_stream(logGroupName=LOG_GROUP, logStreamName=LOG_STREAM)
    except ClientError:
        pass

def list_custom_iam_policies():
    """List customer-managed IAM policies"""
    result = []
    response = iam.list_policies(Scope='Local')
    result = [policy['PolicyName'] for policy in response['Policies']]
    return result

def list_s3_buckets():
    """List all S3 bucket names"""
    result = []
    response = s3_client.list_buckets()
    result = [bucket['Name'] for bucket in response['Buckets']]
    return result

def list_s3_objects(target_bucket, prefix=""):
    """Lists objects in the S3 bucket to verify the upload."""
    try:
        response = s3_client.list_objects_v2(Bucket=target_bucket, Prefix=prefix)
        if 'Contents' in response:
            for obj in response['Contents']:
                send_log_to_cloudwatch(f"Found object in S3: {obj['Key']} (Size: {obj['Size']} bytes)")
        else:
            send_log_to_cloudwatch("No objects found with the specified prefix.")
    except ClientError as e:
        send_log_to_cloudwatch(f"Listing objects failed: {e}")

def upload_file_to_s3(files, target_bucket, object_name):
    """Uploads the dataset to your S3 bucket."""
    failed_files = []
    for file in files:
        try:
            s3_client.upload_file(file, target_bucket, f"raw/{file}")
            send_log_to_cloudwatch(f"File successfully uploaded to {target_bucket}/{object_name}")
        except ClientError as e:
            if "AccessDenied" in str(e):
                print(f"WARNING: Could not upload {file} - Access Denied")
                failed_files.append(file)
            else:
                send_log_to_cloudwatch(f"Upload failed for {file}: {e}")
                failed_files.append(file)
        except Exception as e:
            print(f"ERROR uploading {file}: {e}")
            failed_files.append(file)
    
    return failed_files

# ============ MAIN EXECUTION ============

print("=" * 60)
print("PHASE 2 - AWS Boto3 Ingestion Utility")
print("=" * 60 + "\n")

# Step 1: Setup CloudWatch
print("[STEP 1] Setting up CloudWatch Logs...")
setup_cloudwatch_logs()
send_log_to_cloudwatch("CloudWatch setup complete. Ready for pipeline execution.")
print()

# Step 2: Create/Check IAM Policy
print("[STEP 2] Creating/Checking IAM Policy...")
try:
    res = iam.create_policy(
        PolicyName=policy_name,
        PolicyDocument=json.dumps(policy_doc)
    )
    print("INFO: IAM Policy Created Successfully.")
    print("ARN:", res["Policy"]["Arn"])
except ClientError as e:
    if e.response["Error"]["Code"] == "EntityAlreadyExists":
        print("INFO: IAM Policy already exists:")
        print(f"ARN: arn:aws:iam::{account_id}:policy/{policy_name}")
    else:
        print("ERROR:", e)
print()

# Step 3: List Custom IAM Policies
print("[STEP 3] Listing Custom IAM Policies...")
custom_policies = list_custom_iam_policies()
print("Custom IAM Policies:", custom_policies)
print()

# Step 4: Disable Block Public Access and Attach Policy
print("[STEP 4] Configuring S3 bucket access...")
try:
    s3_client.put_public_access_block(
        Bucket=TARGET_BUCKET,
        PublicAccessBlockConfiguration={
            'BlockPublicAcls': False,
            'IgnorePublicAcls': False,
            'BlockPublicPolicy': False,
            'RestrictPublicBuckets': False
        }
    )
    print("INFO: Disabled Block Public Access via Boto3")

    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{TARGET_BUCKET}/*"
            }
        ]
    }

    s3_client.put_bucket_policy(
        Bucket=TARGET_BUCKET,
        Policy=json.dumps(bucket_policy)
    )
    print("INFO: Policy attached successfully to", TARGET_BUCKET)
except ClientError as e:
    if "AccessDenied" in str(e):
        print("WARNING: You don't have permissions to configure bucket access policies.")
        print("This is OK - you can still read/write to the bucket with your current permissions.")
    else:
        print("ERROR:", e)
print()

# Step 5: List S3 Buckets
print("[STEP 5] Listing S3 Buckets...")
bucket_names = list_s3_buckets()
print("My S3 Buckets:", bucket_names)
print()

# Step 6: Upload Files
print("[STEP 6] Uploading files to S3...")
files = [
    "data/clickstream_day1.json",
    "data/clickstream_day2.json",
    "data/customers.csv",
    "data/order_items_day1.json",
    "data/order_items_day2.json",
    "data/orders_day1.json",
    "data/orders_day2.json",
    "data/products.csv"
]

s3_destination_key = "raw/"
failed_uploads = upload_file_to_s3(files, TARGET_BUCKET, s3_destination_key)
if failed_uploads:
    print(f"\nWARNING: Failed to upload {len(failed_uploads)} file(s): {failed_uploads}")
print()

# Step 7: Verify Upload
print("[STEP 7] Verifying uploaded files...")
list_s3_objects(TARGET_BUCKET, prefix="raw/data/")
send_log_to_cloudwatch("S3 Utility script execution completed successfully.")

print("\n" + "=" * 60)
print("EXECUTION COMPLETED")
print("=" * 60)
