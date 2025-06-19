import configparser
import logging
from typing import Self

from mypy_boto3_s3.service_resource import S3ServiceResource

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class InstanceProperties:
    def __init__(self, properties: dict):
        self._properties = properties

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

    def get(self, property_name) -> str:
        return self._properties[property_name]
