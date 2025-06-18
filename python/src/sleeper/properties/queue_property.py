

from sleeper.properties.instance_properties import InstanceProperties


class QueueProperty:

    def __init__(self, queue_url_property):
        self.queue_url_property = queue_url_property

    def queue_url(self, properties: InstanceProperties):
        return properties.get(self.queue_url_property)

    def queue_name(self, properties: InstanceProperties):
        return self.queue_url(properties).rsplit('/', 1)[1]
