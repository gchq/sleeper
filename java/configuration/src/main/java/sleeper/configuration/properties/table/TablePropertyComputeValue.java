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

import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperProperty;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.InstanceProperty;

import java.util.function.Supplier;
import java.util.function.UnaryOperator;

@FunctionalInterface
public interface TablePropertyComputeValue {

    String computeValue(String value, InstanceProperties instanceProperties, TableProperties tableProperties);

    default TablePropertyComputeValue andThen(TablePropertyComputeValue after) {
        return (value, instanceProperties, tableProperties) -> {
            String intermediate = computeValue(value, instanceProperties, tableProperties);
            return after.computeValue(intermediate, instanceProperties, tableProperties);
        };
    }

    default TablePropertyComputeValue andThen(UnaryOperator<String> after) {
        return (value, instanceProperties, tableProperties) -> {
            String intermediate = computeValue(value, instanceProperties, tableProperties);
            return after.apply(intermediate);
        };
    }

    static TablePropertyComputeValue fixedDefault(String defaultValue) {
        return applyDefaultValue(() -> defaultValue);
    }

    static TablePropertyComputeValue none() {
        return applyDefaultValue(() -> null);
    }

    static TablePropertyComputeValue applyDefaultValue(Supplier<String> getDefault) {
        return (value, instanceProperties, tableProperties) -> SleeperProperties.applyDefaultValue(value, getDefault);
    }

    static TablePropertyComputeValue defaultProperty(SleeperProperty property) {
        if (property instanceof InstanceProperty) {
            InstanceProperty instanceProperty = (InstanceProperty) property;
            return (value, instanceProperties, tableProperties) -> SleeperProperties.applyDefaultValue(value, () -> instanceProperties.get(instanceProperty));
        } else if (property instanceof TableProperty) {
            TableProperty tableProperty = (TableProperty) property;
            return (value, instanceProperties, tableProperties) -> SleeperProperties.applyDefaultValue(value, () -> tableProperties.get(tableProperty));
        } else {
            throw new IllegalArgumentException("Unexpected default property type: " + property);
        }
    }
}
