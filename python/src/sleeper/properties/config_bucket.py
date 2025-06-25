import logging

from mypy_boto3_s3 import S3ServiceResource

from sleeper.properties.cdk_defined_properties import CommonCdkProperty
from sleeper.properties.instance_properties import InstanceProperties, load_instance_properties_from_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def load_instance_properties(s3: S3ServiceResource, instance_id: str) -> InstanceProperties:
    bucket_name = config_bucket_for_instance(instance_id)
    return load_instance_properties_from_bucket(s3, bucket_name)


def load_instance_properties_from_bucket(s3: S3ServiceResource, bucket_name: str) -> InstanceProperties:
    config_obj = s3.Object(bucket_name, "instance.properties")
    config_str = config_obj.get()["Body"].read().decode("utf-8")
    return load_instance_properties_from_string(config_str)


def save_instance_properties(s3: S3ServiceResource, properties: InstanceProperties):
    bucket_name = properties.get(CommonCdkProperty.CONFIG_BUCKET)
    config_obj = s3.Object(bucket_name, "instance.properties")
    config_obj.put(Body=properties.as_properties_str())


def config_bucket_for_instance(instance_id: str) -> str:
    return "sleeper-" + instance_id + "-config"
