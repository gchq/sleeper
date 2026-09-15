from sleeper.properties.instance_properties import InstanceProperty


class CommonProperty:
    ID = InstanceProperty("sleeper.id")
    ADD_TABLE_PATH = "sleeper/tables"  # Legacy path for deployments without route URLs.
