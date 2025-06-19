import uuid

import boto3
import s3fs
from mypy_boto3_dynamodb import DynamoDBServiceResource
from mypy_boto3_s3 import S3Client, S3ServiceResource
from mypy_boto3_s3.service_resource import Bucket
from mypy_boto3_sqs import SQSServiceResource
from mypy_boto3_sqs.service_resource import Queue
from testcontainers.localstack import LocalStackContainer

CONTAINER = LocalStackContainer("localstack/localstack:4.2")


class LocalStack:
    _container: LocalStackContainer = None
    _s3_resource: S3ServiceResource = None
    _s3_client: S3Client = None
    _s3fs: s3fs.S3FileSystem = None
    _sqs_resource: SQSServiceResource = None
    _dynamo_resource: DynamoDBServiceResource = None

    @classmethod
    def container(cls) -> LocalStackContainer:
        if cls._container is None:
            cls._container = LocalStackContainer("localstack/localstack:4.2")
            cls._container.start()
        return cls._container

    @classmethod
    def resource(cls, name: str, **kwargs):
        kwargs_ = cls.client_kwargs()
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
    def s3_client(cls) -> S3Client:
        if cls._s3_client is None:
            cls._s3_client = cls.container().get_client("s3")
        return cls._s3_client

    @classmethod
    def s3fs(cls) -> s3fs.S3FileSystem:
        if cls._s3fs is None:
            args = cls.client_kwargs()
            key = args.pop("aws_access_key_id")
            secret = args.pop("aws_secret_access_key")
            cls._s3fs = s3fs.S3FileSystem(key=key, secret=secret, client_kwargs=args)
        return cls._s3fs

    @classmethod
    def sqs_resource(cls) -> SQSServiceResource:
        if cls._sqs_resource is None:
            cls._sqs_resource = cls.resource("sqs")
        return cls._sqs_resource

    @classmethod
    def dynamo_resource(cls) -> DynamoDBServiceResource:
        if cls._dynamo_resource is None:
            cls._dynamo_resource = cls.resource("dynamodb")
        return cls._dynamo_resource

    @classmethod
    def client_kwargs(cls) -> dict:
        container = cls.container()
        return {
            "endpoint_url": container.get_url(),
            "region_name": container.region_name,
            "aws_access_key_id": "testcontainers-localstack",
            "aws_secret_access_key": "testcontainers-localstack",
        }

    @classmethod
    def create_bucket(cls, bucket_name=None) -> Bucket:
        if bucket_name is None:
            bucket_name = str(uuid.uuid4())
        bucket = cls.s3_resource().Bucket(bucket_name)
        bucket.create(CreateBucketConfiguration={"LocationConstraint": cls.region_name()})
        return bucket

    @classmethod
    def create_queue(cls) -> Queue:
        queue_name = str(uuid.uuid4())
        return cls.sqs_resource().create_queue(QueueName=queue_name)
