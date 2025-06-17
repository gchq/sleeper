
import unittest

from sleeper.instance_properties import InstanceProperties


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
