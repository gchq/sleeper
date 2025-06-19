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
        return "\n".join(f"{k}={v}" for k, v in self._properties.items())


def _property_name(property):
    if isinstance(property, InstanceProperty):
        return property.property_name
    else:
        return property


class InstanceProperty:
    def __init__(self, property_name):
        self.property_name = property_name
