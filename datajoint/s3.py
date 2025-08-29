"""
AWS S3 operations
"""

import logging
import uuid
from io import BytesIO
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
import urllib3

from . import errors

logger = logging.getLogger(__name__.split(".")[0])


class Folder:
    """
    A Folder instance manipulates a flat folder of objects within an S3-compatible object store
    """

    def __init__(
        self,
        endpoint,
        bucket,
        access_key,
        secret_key,
        *,
        secure=False,
        proxy_server=None,
        **_,
    ):
        # Configure boto3 client for S3-compatible storage
        endpoint_url = f"http{'s' if secure else ''}://{endpoint}"
        
        # Configure session with proxy if provided
        session_config = {}
        if proxy_server:
            session_config['proxies'] = {
                'http': proxy_server,
                'https': proxy_server
            }
            
        self.client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(
                retries={
                    'max_attempts': 5,
                    'mode': 'adaptive'
                },
                **session_config
            )
        )
        self.bucket = bucket
        try:
            self.client.head_bucket(Bucket=bucket)
        except ClientError:
            raise errors.BucketInaccessible("Inaccessible s3 bucket %s" % bucket)

    def put(self, name, buffer):
        logger.debug("put: {}:{}".format(self.bucket, name))
        return self.client.put_object(
            Bucket=self.bucket, Key=str(name), Body=buffer
        )

    def fput(self, local_file, name, metadata=None):
        logger.debug("fput: {} -> {}:{}".format(self.bucket, local_file, name))
        extra_args = {}
        if metadata:
            extra_args['Metadata'] = metadata
        return self.client.upload_file(
            str(local_file), self.bucket, str(name), ExtraArgs=extra_args
        )

    def get(self, name):
        logger.debug("get: {}:{}".format(self.bucket, name))
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=str(name))
            return response['Body'].read()
        except ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                raise errors.MissingExternalFile("Missing s3 key %s" % name)
            else:
                raise e

    def fget(self, name, local_filepath):
        """get file from object name to local filepath"""
        logger.debug("fget: {}:{}".format(self.bucket, name))
        name = str(name)
        try:
            # Get object metadata
            head_response = self.client.head_object(Bucket=self.bucket, Key=name)
            meta = {k.lower().lstrip("x-amz-meta-"): v for k, v in (head_response.get('Metadata', {})).items()}
            
            # Download file
            local_filepath = Path(local_filepath)
            local_filepath.parent.mkdir(parents=True, exist_ok=True)
            self.client.download_file(self.bucket, name, str(local_filepath))
            
            if "contents_hash" in meta:
                return uuid.UUID(meta["contents_hash"])
        except ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                raise errors.MissingExternalFile("Missing s3 key %s" % name)
            else:
                raise e

    def exists(self, name):
        logger.debug("exists: {}:{}".format(self.bucket, name))
        try:
            self.client.head_object(Bucket=self.bucket, Key=str(name))
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                return False
            else:
                raise e

    def get_size(self, name):
        logger.debug("get_size: {}:{}".format(self.bucket, name))
        try:
            response = self.client.head_object(Bucket=self.bucket, Key=str(name))
            return response['ContentLength']
        except ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                raise errors.MissingExternalFile
            raise e

    def remove_object(self, name):
        logger.debug("remove_object: {}:{}".format(self.bucket, name))
        try:
            self.client.delete_object(Bucket=self.bucket, Key=str(name))
        except ClientError:
            raise errors.DataJointError("Failed to delete %s from s3 storage" % name)
