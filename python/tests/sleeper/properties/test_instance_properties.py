
import unittest

from sleeper.properties.bucket_property import BucketProperty
from sleeper.properties.dynamo_table_property import DynamoTableProperty
from sleeper.properties.instance_properties import InstanceProperties
from sleeper.properties.queue_property import QueueProperty


class TestInstanceProperties(unittest.TestCase):


    def test_read_set_field(self):
        # Given
        properties = InstanceProperties({'a.b.c': 'value'})

        # When / Then
        self.assertEqual('value', properties.get('a.b.c'))


    def test_read_unset_field(self):
        # Given
        properties = InstanceProperties({})

        # When / Then
        with self.assertRaises(KeyError):
            properties.get('a.b.c')

    def test_read_queue(self):
        # Given
        property = QueueProperty('queue.url')
        properties = InstanceProperties({'queue.url': 'https://sqs.eu-west-2.amazonaws.com/123456789/MyQueue'})

        # When / Then
        self.assertEqual('https://sqs.eu-west-2.amazonaws.com/123456789/MyQueue', property.queue_url(properties))
        self.assertEqual('MyQueue', property.queue_name(properties))

    def test_read_table(self):
        # Given
        property = DynamoTableProperty('table.name')
        properties = InstanceProperties({'table.name': 'test-dynamo-table'})

        # When / Then
        self.assertEqual('test-dynamo-table', property.table_name(properties))

    def test_read_bucket(self):
        # Given
        property = BucketProperty('bucket.name')
        properties = InstanceProperties({'bucket.name': 'test-bucket'})

        # When / Then
        self.assertEqual('test-bucket', property.bucket_name(properties))
