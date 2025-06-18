

from sleeper.properties.instance_properties import InstanceProperties


class DynamoTableProperty:

    def __init__(self, table_name_property):
        self.table_name_property = table_name_property

    def table_name(self, properties: InstanceProperties):
        return properties.get(self.table_name_property)
