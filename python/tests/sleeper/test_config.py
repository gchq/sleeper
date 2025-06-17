import unittest
from tests.sleeper.localstack import LocalStackTestBase
from sleeper.config import load_instance_properties_from_bucket

class TestConfig(LocalStackTestBase):

    def test_load_instance_properties(self):
        # Given
        bucket_name = self.create_bucket()
        self.s3.Object(bucket_name, 'instance.properties').put(Body="a.b.c=value")

        # When
        properties = load_instance_properties_from_bucket(bucket_name, self.s3)

        # Then
        self.assertEqual({'a.b.c': 'value'}, properties)

if __name__ == '__main__':
    unittest.main()
