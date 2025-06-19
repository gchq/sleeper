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

    def get(self, property) -> str:
        return self._properties[_property_name(property)]

    def set(self, property, value: str):
        self._properties[_property_name(property)] = value

    def as_dict(self) -> dict:
        return self._properties

    def as_properties_str(self) -> str:
        return "\n".join(f"{k}={v}" for k, v in self._properties.items())


def _property_name(property):
    if isinstance(property, InstanceProperty):
        return property.property_name
    else:
        return property


class InstanceProperty:
    def __init__(self, property_name):
        self.property_name = property_name


class QueueProperty(InstanceProperty):
    def queue_url(self, properties: InstanceProperties):
        return properties.get(self)

    def queue_name(self, properties: InstanceProperties):
        return self.queue_url(properties).rsplit("/", 1)[1]


class BucketProperty(InstanceProperty):
    def bucket_name(self, properties: InstanceProperties):
        return properties.get(self)


class DynamoTableProperty(InstanceProperty):
    def table_name(self, properties: InstanceProperties):
        return properties.get(self)
