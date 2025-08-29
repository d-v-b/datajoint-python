import pytest
import boto3
from moto import mock_aws

from datajoint.blob import pack
from datajoint.errors import DataJointError
from datajoint.hash import uuid_from_buffer

from .schema_external import SimpleRemote


def test_connection(http_client, s3_client, s3_creds):
    try:
        s3_client.head_bucket(Bucket=s3_creds["bucket"])
        assert True
    except Exception:
        assert False


def test_connection_secure(s3_client, s3_creds):
    try:
        s3_client.head_bucket(Bucket=s3_creds["bucket"])
        assert True
    except Exception:
        assert False


def test_remove_object_exception(schema_ext, s3_creds):
    # https://github.com/datajoint/datajoint-python/issues/952

    # Insert some test data and remove it so that the external table is populated
    test = [1, [1, 2, 3]]
    SimpleRemote.insert1(test)
    SimpleRemote.delete()

    # Save the old external table minio client
    old_client = schema_ext.external["share"].s3.client

    # Apply our new S3 client which has invalid credentials
    schema_ext.external["share"].s3.client = boto3.client(
        's3',
        endpoint_url=f"http://{s3_creds['endpoint']}",
        aws_access_key_id="jeffjeff",
        aws_secret_access_key="jeffjeff",
        region_name='us-east-1'
    )

    # This method returns a list of errors
    error_list = schema_ext.external["share"].delete(
        delete_external_files=True, errors_as_string=False
    )

    # Teardown
    schema_ext.external["share"].s3.client = old_client
    schema_ext.external["share"].delete(delete_external_files=True)

    with pytest.raises(DataJointError):
        # Raise the error we want if the error matches the expected uuid
        if str(error_list[0][0]) == str(uuid_from_buffer(pack(test[1]))):
            raise error_list[0][2]
