import unittest
import uuid
from mypy_boto3_s3 import S3ServiceResource
from testcontainers.localstack import LocalStackContainer
import boto3
from botocore.client import Config

CONTAINER = LocalStackContainer('localstack/localstack:4.2')

class LocalStack:
    _container: LocalStackContainer = None

    @classmethod
    def resource(cls, name: str, **kwargs):
        container = cls.container()
        kwargs_ = {
            "endpoint_url": container.get_url(),
            "region_name": container.region_name,
            "aws_access_key_id": "testcontainers-localstack",
            "aws_secret_access_key": "testcontainers-localstack",
            "config": Config(s3={'addressing_style': 'path'})
        }
        kwargs_.update(kwargs)
        return boto3.resource(name, **kwargs_)

    @classmethod
    def region_name(cls) -> str:
        return cls.container().region_name

    @classmethod
    def container(cls) -> LocalStackContainer:
        if cls._container is None:
            cls._container = LocalStackContainer('localstack/localstack:4.2')
            cls._container.start()
            cls._container.get_logs()
        return cls._container


class LocalStackTestBase(unittest.TestCase):
    def setUp(self):
        self.s3: S3ServiceResource = LocalStack.resource('s3')
        self.region_name = LocalStack.region_name()

    def create_bucket(self) -> str:
        bucket_name = str(uuid.uuid4())
        self.s3.Bucket(bucket_name).create(CreateBucketConfiguration={'LocationConstraint': self.region_name})
        return bucket_name
