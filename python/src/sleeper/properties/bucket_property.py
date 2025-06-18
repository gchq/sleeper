from sleeper.properties.instance_properties import InstanceProperties


class BucketProperty:
    def __init__(self, bucket_name_property):
        self.bucket_name_property = bucket_name_property

    def bucket_name(self, properties: InstanceProperties):
        return properties.get(self.bucket_name_property)
