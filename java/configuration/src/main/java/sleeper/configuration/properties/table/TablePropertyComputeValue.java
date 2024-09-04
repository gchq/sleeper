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

import sleeper.configuration.properties.SleeperProperty;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.InstanceProperty;

import java.util.function.BiFunction;
import java.util.function.Supplier;

@FunctionalInterface
public interface TablePropertyComputeValue {

    String computeValue(String value, InstanceProperties instanceProperties, TableProperties tableProperties);

    static TablePropertyComputeValue fixedDefault(String defaultValue) {
        return applyDefaultValue(() -> defaultValue);
    }

    static TablePropertyComputeValue none() {
        return applyDefaultValue(() -> null);
    }

    static TablePropertyComputeValue defaultProperty(SleeperProperty property) {
        if (property instanceof InstanceProperty) {
            InstanceProperty instanceProperty = (InstanceProperty) property;
            return applyDefaultValue((instanceProperties, tableProperties) -> instanceProperties.get(instanceProperty));
        } else if (property instanceof TableProperty) {
            TableProperty tableProperty = (TableProperty) property;
            return applyDefaultValue((instanceProperties, tableProperties) -> tableProperties.get(tableProperty));
        } else {
            throw new IllegalArgumentException("Unexpected default property type: " + property);
        }
    }

    static TablePropertyComputeValue enabledBy(TableProperty property, TablePropertyComputeValue compute) {
        return (value, instanceProperties, tableProperties) -> {
            if (tableProperties.getBoolean(property)) {
                return compute.computeValue(value, instanceProperties, tableProperties);
            } else {
                return "false";
            }
        };
    }

    static TablePropertyComputeValue applyDefaultValue(Supplier<String> getDefault) {
        return applyDefaultValue((instanceProperties, tableProperties) -> getDefault.get());
    }

    static TablePropertyComputeValue applyDefaultValue(BiFunction<InstanceProperties, TableProperties, String> getDefault) {
        return (value, instanceProperties, tableProperties) -> {
            if (value != null) {
                return value;
            } else {
                return getDefault.apply(instanceProperties, tableProperties);
            }
        };
    }
}
