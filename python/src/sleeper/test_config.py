import unittest
from testcontainers.localstack import LocalStackContainer
from mypy_boto3_s3.service_resource import S3ServiceResource
import boto3
from botocore.client import Config
import uuid
from sleeper.config import load_instance_properties_from_bucket

class TestConfig(unittest.TestCase):

    def setUp(self):
        self.container

    def test_load_instance_properties(self):
        with LocalStackContainer('localstack/localstack:4.2').with_services('s3') as localstack:
            s3: S3ServiceResource = resource(localstack, 's3')
            bucket_name = str(uuid.uuid4())
            s3.Bucket(bucket_name).create(CreateBucketConfiguration={'LocationConstraint': localstack.region_name})
            s3.Object(bucket_name, 'instance.properties').put(Body="a.b.c=value")
            properties = load_instance_properties_from_bucket(bucket_name, s3)
            self.assertEqual({'a.b.c': 'value'}, properties)

def resource(localstack: LocalStackContainer, name: str, **kwargs):
        kwargs_ = {
            "endpoint_url": localstack.get_url(),
            "region_name": localstack.region_name,
            "aws_access_key_id": "testcontainers-localstack",
            "aws_secret_access_key": "testcontainers-localstack",
            "config": Config(s3={'addressing_style': 'path'})
        }
        kwargs_.update(kwargs)
        return boto3.resource(name, **kwargs_)

if __name__ == '__main__':
    unittest.main()
