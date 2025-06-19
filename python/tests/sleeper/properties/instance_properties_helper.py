import uuid

from sleeper.properties.cdk_defined_properties import CommonCdkProperty
from sleeper.properties.config_bucket import config_bucket_for_instance
from sleeper.properties.instance_properties import InstanceProperties
from sleeper.properties.user_defined_properties import CommonProperty


def create_test_instance_properties() -> InstanceProperties:
    properties = InstanceProperties()
    instance_id = str(uuid.uuid4())[:20]
    properties.set(CommonProperty.ID, instance_id)
    properties.set(CommonCdkProperty.CONFIG_BUCKET, config_bucket_for_instance(instance_id))
    return properties
