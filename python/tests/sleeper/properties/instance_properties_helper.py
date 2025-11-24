#  Copyright 2022-2025 Crown Copyright
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import uuid

from sleeper.properties.cdk_defined_properties import CommonCdkProperty
from sleeper.properties.config_bucket import config_bucket_for_instance
from sleeper.properties.instance_properties import InstanceProperties
from sleeper.properties.user_defined_properties import CommonProperty


def create_test_instance_properties() -> InstanceProperties:
    properties = InstanceProperties()
    instance_id = str(uuid.uuid4())[:20]
    properties.set(CommonProperty.ID, instance_id)
    properties.set(CommonCdkProperty.CONFIG_BUCKET, config_bucket_for_instance(instance_id))
    properties.set(CommonCdkProperty.DATA_BUCKET, f"sleeper-{instance_id}-table-data")
    return properties
