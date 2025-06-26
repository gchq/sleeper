from sleeper.properties.cdk_defined_properties import CommonCdkProperty
from sleeper.properties.config_bucket import load_instance_properties, load_instance_properties_from_bucket, save_instance_properties
from sleeper.properties.user_defined_properties import CommonProperty
from tests.sleeper.localstack import LocalStack
from tests.sleeper.properties.instance_properties_helper import create_test_instance_properties


def should_load_instance_properties():
    # Given
    bucket = LocalStack.create_bucket()
    LocalStack.s3_resource().Object(bucket.name, "instance.properties").put(Body="a.b.c=value")

    # When
    properties = load_instance_properties_from_bucket(LocalStack.s3_resource(), bucket.name)

    # Then
    assert properties.as_dict() == {"a.b.c": "value"}


def should_save_load_instance_properties():
    # Given
    properties = create_test_instance_properties()
    LocalStack.create_bucket(properties.get(CommonCdkProperty.CONFIG_BUCKET))
    save_instance_properties(LocalStack.s3_resource(), properties)

    # When
    loaded = load_instance_properties(LocalStack.s3_resource(), properties.get(CommonProperty.ID))

    # Then
    assert properties.as_dict() == loaded.as_dict()
