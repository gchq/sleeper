import uuid

import boto3
from botocore.client import Config
from mypy_boto3_s3 import S3ServiceResource
from mypy_boto3_s3.service_resource import Bucket
from mypy_boto3_sqs import SQSServiceResource
from mypy_boto3_sqs.service_resource import Queue
from testcontainers.localstack import LocalStackContainer

CONTAINER = LocalStackContainer("localstack/localstack:4.2")


class LocalStack:
    _container: LocalStackContainer = None
    _s3_resource: S3ServiceResource = None
    _sqs_resource: SQSServiceResource = None

    @classmethod
    def container(cls) -> LocalStackContainer:
        if cls._container is None:
            cls._container = LocalStackContainer("localstack/localstack:4.2")
            cls._container.start()
        return cls._container

    @classmethod
    def resource(cls, name: str, **kwargs):
        container = cls.container()
        kwargs_ = {
            "endpoint_url": container.get_url(),
            "region_name": container.region_name,
            "aws_access_key_id": "testcontainers-localstack",
            "aws_secret_access_key": "testcontainers-localstack",
            "config": Config(s3={"addressing_style": "path"}),
        }
        kwargs_.update(kwargs)
        return boto3.resource(name, **kwargs_)

    @classmethod
    def region_name(cls) -> str:
        return cls.container().region_name

    @classmethod
    def s3_resource(cls) -> S3ServiceResource:
        if cls._s3_resource is None:
            cls._s3_resource = cls.resource("s3")
        return cls._s3_resource

    @classmethod
    def sqs_resource(cls) -> SQSServiceResource:
        if cls._sqs_resource is None:
            cls._sqs_resource = cls.resource("sqs")
        return cls._sqs_resource

    @classmethod
    def create_bucket(cls) -> Bucket:
        bucket_name = str(uuid.uuid4())
        bucket = cls.s3_resource().Bucket(bucket_name)
        bucket.create(CreateBucketConfiguration={"LocationConstraint": cls.region_name()})
        return bucket

    @classmethod
    def create_queue(cls) -> Queue:
        queue_name = str(uuid.uuid4())
        return cls.sqs_resource().create_queue(QueueName=queue_name)
