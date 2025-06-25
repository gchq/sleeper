import pytest

from sleeper.properties.cdk_defined_properties import queue_name_from_url
from sleeper.properties.instance_properties import InstanceProperties, InstanceProperty, load_instance_properties_from_string
from sleeper.properties.user_defined_properties import CommonProperty
from tests.sleeper.repository_path import get_repository_path


def test_read_set_field():
    # Given
    properties = InstanceProperties({"a.b.c": "value"})

    # When / Then
    assert properties.get("a.b.c") == "value"


def test_read_unset_field():
    # Given
    properties = InstanceProperties({})

    # When / Then
    with pytest.raises(KeyError):
        properties.get("a.b.c")


def test_read_property():
    # Given
    property = InstanceProperty("queue.url")
    properties = InstanceProperties({"queue.url": "https://sqs.eu-west-2.amazonaws.com/123456789/MyQueue"})

    # When / Then
    assert properties.get(property) == "https://sqs.eu-west-2.amazonaws.com/123456789/MyQueue"
    assert queue_name_from_url(properties.get(property)) == "MyQueue"


def test_read_full_example_properties():
    # Given
    file = get_repository_path() / "example/full/instance.properties"
    content = file.read_text()

    # When
    properties = load_instance_properties_from_string(content)

    # Then
    assert properties.get(CommonProperty.ID) == "full-example"
