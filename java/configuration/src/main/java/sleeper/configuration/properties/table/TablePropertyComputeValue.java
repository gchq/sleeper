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
import sleeper.core.properties.SleeperProperty;

import java.util.function.BiFunction;

/**
 * Behaviour to post-process the value of a table property. This is used to set a default value for the property, to
 * default the property to the value of another property, or to apply some behaviour based on the values of other
 * properties. See {@link sleeper.configuration.properties.validation.DefaultAsyncCommitBehaviour} for examples.
 */
@FunctionalInterface
public interface TablePropertyComputeValue {

    /**
     * Applies post-processing to a property value. May read values of other properties. Must not query the value of
     * the property that the post-processing is applied to, as this would produce an infinite loop.
     *
     * @param  value              the value before post-processing
     * @param  instanceProperties the instance properties
     * @param  tableProperties    the table properties
     * @return                    the value
     */
    String computeValue(String value, InstanceProperties instanceProperties, TableProperties tableProperties);

    /**
     * Creates behaviour to apply a fixed default value for a property.
     *
     * @param  defaultValue the default value
     * @return              the behaviour
     */
    static TablePropertyComputeValue fixedDefault(String defaultValue) {
        return applyDefaultValue((instanceProperties, tableProperties) -> defaultValue);
    }

    /**
     * Creates behaviour for a property with no default value and no other pre-processing.
     *
     * @return the behaviour
     */
    static TablePropertyComputeValue none() {
        return fixedDefault(null);
    }

    /**
     * Creates behaviour to retrieve the default value for a property from another configuration property.
     *
     * @param  property the property to default the property's value to
     * @return          the behaviour
     */
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

    /**
     * Creates behaviour to retrieve the default value for a property when it is unset.
     *
     * @param  getDefault a function to get the default value based on the instance and table properties
     * @return            the behaviour
     */
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
