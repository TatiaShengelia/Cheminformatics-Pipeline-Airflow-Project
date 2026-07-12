import boto3
from moto import mock_aws
from chem_pipeline.s3_utils import discover_all_pairs, filter_new_or_overwrite
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

@mock_aws
def test_discover_all_pairs_finds_matching_files():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(Bucket="test-bucket", Key="incoming/run1_scaffolds.csv", Body=b"id,smiles\n1,CC*")
    s3.put_object(Bucket="test-bucket", Key="incoming/run1_r_groups.csv", Body=b"id,smiles\n1,*O")

    hook = S3Hook(aws_conn_id="aws_default")
    pairs = discover_all_pairs(hook, "test-bucket", "incoming/", "_scaffolds.csv", "_r_groups.csv")

    assert "run1" in pairs