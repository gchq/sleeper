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
package sleeper.configuration.properties.validation;

import org.apache.commons.lang3.EnumUtils;

import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.configuration.properties.table.TablePropertyComputeValue;

import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_ASYNC_COMMIT_BEHAVIOUR;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_ASYNC_COMMITS_ENABLED;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;

public enum DefaultAsyncCommitBehaviour {
    DISABLED,
    PER_IMPLEMENTATION,
    ALL_IMPLEMENTATIONS;

    public static boolean isValid(String behaviour) {
        return EnumUtils.isValidEnumIgnoreCase(DefaultAsyncCommitBehaviour.class, behaviour);
    }

    public static TablePropertyComputeValue defaultAsyncCommitEnabled() {
        return TablePropertyComputeValue.applyDefaultValue((instanceProperties, tableProperties) -> {
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
        });
    }

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
