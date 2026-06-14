#!/bin/bash
echo "Creating kairos-raw S3 bucket ..."
awslocal s3 mb s3://kairos-raw
awslocal s3api put-bucket-versioning \
    --bucket kairos-raw \
    --versioning-configuration Status=Enabled
echo "Done. Bucket kairos-aw ready."
