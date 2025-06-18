import pytest

from sleeper.properties.bucket_property import BucketProperty
from sleeper.properties.dynamo_table_property import DynamoTableProperty
from sleeper.properties.instance_properties import InstanceProperties
from sleeper.properties.queue_property import QueueProperty


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


def test_read_queue():
    # Given
    property = QueueProperty("queue.url")
    properties = InstanceProperties({"queue.url": "https://sqs.eu-west-2.amazonaws.com/123456789/MyQueue"})

    # When / Then
    assert property.queue_url(properties) == "https://sqs.eu-west-2.amazonaws.com/123456789/MyQueue"
    assert property.queue_name(properties) == "MyQueue"


def test_read_table():
    # Given
    property = DynamoTableProperty("table.name")
    properties = InstanceProperties({"table.name": "test-dynamo-table"})

    # When / Then
    assert property.table_name(properties) == "test-dynamo-table"


def test_read_bucket():
    # Given
    property = BucketProperty("bucket.name")
    properties = InstanceProperties({"bucket.name": "test-bucket"})

    # When / Then
    assert property.bucket_name(properties) == "test-bucket"
