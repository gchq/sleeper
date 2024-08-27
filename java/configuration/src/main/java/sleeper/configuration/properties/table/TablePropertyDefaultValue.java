/*
 * Copyright 2022-2024 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.configuration.properties.table;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.configuration.properties.instance.SleeperProperty;

@FunctionalInterface
public interface TablePropertyDefaultValue {

    String getDefaultValue(InstanceProperties instanceProperties, TableProperties tableProperties);

    static TablePropertyDefaultValue fixed(String value) {
        return (instanceProperties, tableProperties) -> value;
    }

    static TablePropertyDefaultValue none() {
        return (instanceProperties, tableProperties) -> null;
    }

    static TablePropertyDefaultValue defaultProperty(SleeperProperty property) {
        if (property instanceof InstanceProperty) {
            InstanceProperty instanceProperty = (InstanceProperty) property;
            return (instanceProperties, tableProperties) -> instanceProperties.get(instanceProperty);
        } else if (property instanceof TableProperty) {
            TableProperty tableProperty = (TableProperty) property;
            return (instanceProperties, tableProperties) -> tableProperties.get(tableProperty);
        } else {
            throw new IllegalArgumentException("Unexpected default property type: " + property);
        }
    }
}
