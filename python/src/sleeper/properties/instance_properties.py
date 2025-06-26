import io

from jproperties import Properties


class InstanceProperties:
    def __init__(self, properties: dict = None):
        if properties is None:
            properties = {}
        self._properties = properties

    def get(self, property) -> str:
        return self._properties[_property_name(property)]

    def set(self, property, value: str):
        self._properties[_property_name(property)] = value

    def as_dict(self) -> dict:
        return self._properties

    def as_properties_str(self) -> str:
        properties = Properties()
        for key, value in self._properties.items():
            properties[key] = value
        with io.BytesIO() as data:
            properties.store(data, encoding="utf-8", timestamp=False)
            return data.getvalue().decode("utf-8")


def _property_name(property):
    if isinstance(property, InstanceProperty):
        return property.property_name
    else:
        return property


class InstanceProperty:
    def __init__(self, property_name):
        self.property_name = property_name


def load_instance_properties_from_string(properties_str: str) -> InstanceProperties:
    properties = Properties()
    properties.load(properties_str, encoding="utf-8")
    return InstanceProperties(properties.properties)
