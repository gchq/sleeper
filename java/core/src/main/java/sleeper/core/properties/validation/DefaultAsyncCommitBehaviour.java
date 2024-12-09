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
package sleeper.core.properties.validation;

import org.apache.commons.lang3.EnumUtils;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertyComputeValue;

import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_ASYNC_COMMIT_BEHAVIOUR;
import static sleeper.core.properties.table.TableProperty.STATESTORE_ASYNC_COMMITS_ENABLED;
import static sleeper.core.properties.table.TableProperty.STATESTORE_CLASSNAME;

/**
 * Valid values for default behaviour to determine whether commits to the state store should be done asynchronously.
 */
public enum DefaultAsyncCommitBehaviour {
    DISABLED,
    PER_IMPLEMENTATION,
    ALL_IMPLEMENTATIONS;

    /**
     * Checks if the value is a valid default behaviour for asynchronous state store commits.
     *
     * @param  behaviour the value
     * @return           true if it is valid
     */
    public static boolean isValid(String behaviour) {
        return EnumUtils.isValidEnumIgnoreCase(DefaultAsyncCommitBehaviour.class, behaviour);
    }

    /**
     * Determines the default value for whether asynchronous commit should be enabled for a given Sleeper table. This
     * depends on the default behaviour set in the Sleeper instance.
     *
     * @param  instanceProperties the instance properties
     * @param  tableProperties    the table properties
     * @return                    true if asynchronous by default, false otherwise
     */
    public static String getDefaultAsyncCommitEnabled(InstanceProperties instanceProperties, TableProperties tableProperties) {
        DefaultAsyncCommitBehaviour behaviour = instanceProperties.getEnumValue(DEFAULT_ASYNC_COMMIT_BEHAVIOUR, DefaultAsyncCommitBehaviour.class);
        switch (behaviour) {
            case DISABLED:
                return "false";
            case ALL_IMPLEMENTATIONS:
                return "true";
            case PER_IMPLEMENTATION:
            default:
                String classname = tableProperties.get(STATESTORE_CLASSNAME);
                return "" + classname.contains("TransactionLog");
        }
    }

    /**
     * Creates behaviour for a table property that sets whether asynchronous commit is enabled for a specific state
     * store update type. May be disabled at the table level. Applies defaults from the instance level.
     *
     * @param  defaultUpdateEnabledProperty the instance property that sets whether asynchronous commit is enabled by
     *                                      default for the same state store update type
     * @return                              the behaviour
     */
    public static TablePropertyComputeValue computeAsyncCommitForUpdate(InstanceProperty defaultUpdateEnabledProperty) {
        return (typeEnabledStr, instanceProperties, tableProperties) -> {
            if (typeEnabledStr == null) {
                boolean typeEnabledByDefault = instanceProperties.getBoolean(defaultUpdateEnabledProperty);
                boolean tableEnabled = tableProperties.getBoolean(STATESTORE_ASYNC_COMMITS_ENABLED);
                return "" + (typeEnabledByDefault && tableEnabled);
            } else if (tableProperties.isSet(STATESTORE_ASYNC_COMMITS_ENABLED)) {
                boolean typeEnabled = Boolean.parseBoolean(typeEnabledStr);
                boolean tableEnabled = tableProperties.getBoolean(STATESTORE_ASYNC_COMMITS_ENABLED);
                return "" + (typeEnabled && tableEnabled);
            } else {
                return typeEnabledStr;
            }
        };
    }
}
