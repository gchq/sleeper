import configparser
import logging
from typing import Self

from mypy_boto3_s3.service_resource import S3ServiceResource

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class InstanceProperties:
    def __init__(self, properties: dict = None):
        if properties is None:
            properties = {}
        self._properties = properties

    @classmethod
    def load_for_instance(cls, s3: S3ServiceResource, instance_id: str) -> Self:
        return cls.load_from_bucket(s3, "sleeper-" + instance_id + "-config")

    @classmethod
    def load_from_bucket(cls, s3: S3ServiceResource, bucket_name: str) -> Self:
        config_obj = s3.Object(bucket_name, "instance.properties")
        config_str = config_obj.get()["Body"].read().decode("utf-8")
        config_str = "[asection]\n" + config_str
        config = configparser.ConfigParser(allow_no_value=True)
        config.read_string(config_str)
        properties = dict(config["asection"])
        logger.debug("Loaded properties from config bucket ${bucket_name}")
        return cls(properties)

    def as_dict(self) -> dict:
        return self._properties

    def get(self, property) -> str:
        if isinstance(property, InstanceProperty):
            property = property.property_name
        return self._properties[property]

    def set(self, property, value: str):
        if isinstance(property, InstanceProperty):
            property = property.property_name
        self._properties[property] = value


class InstanceProperty:
    def __init__(self, property_name):
        self.property_name = property_name


class QueueProperty(InstanceProperty):
    def queue_url(self, properties: InstanceProperties):
        return properties.get(self.property_name)

    def queue_name(self, properties: InstanceProperties):
        return self.queue_url(properties).rsplit("/", 1)[1]


class BucketProperty(InstanceProperty):
    def bucket_name(self, properties: InstanceProperties):
        return properties.get(self.property_name)


class DynamoTableProperty(InstanceProperty):
    def table_name(self, properties: InstanceProperties):
        return properties.get(self.property_name)
