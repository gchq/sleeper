import unittest
from tests.sleeper.localstack import LocalStackTestBase
from sleeper.properties.instance_properties import InstanceProperties

class TestInstancePropertiesLocalStack(LocalStackTestBase):

    def test_load_instance_properties(self):
        # Given
        bucket_name = self.create_bucket()
        self.s3.Object(bucket_name, 'instance.properties').put(Body="a.b.c=value")

        # When
        properties = InstanceProperties.load_from_bucket(self.s3, bucket_name)

        # Then
        self.assertEqual({'a.b.c': 'value'}, properties.as_dict())
