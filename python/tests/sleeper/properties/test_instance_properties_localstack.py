from sleeper.properties.instance_properties import InstanceProperties
from tests.sleeper.localstack import LocalStack


def test_load_instance_properties():
    # Given
    bucket = LocalStack.create_bucket()
    LocalStack.s3_resource().Object(bucket.name, "instance.properties").put(Body="a.b.c=value")

    # When
    properties = InstanceProperties.load_from_bucket(LocalStack.s3_resource(), bucket.name)

    # Then
    assert properties.as_dict() == {"a.b.c": "value"}
