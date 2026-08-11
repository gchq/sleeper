/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.clients.admin.properties;

import sleeper.core.properties.SleeperProperty;

import java.util.Set;

/**
 * A validation result from an update to configuration properties.
 */
public class UpdatePropertiesValidationResult {

    private final Set<SleeperProperty> invalidValueProperties;
    private final Set<SleeperProperty> nonUpdateableProperties;

    public UpdatePropertiesValidationResult(Set<SleeperProperty> invalidValueProperties, Set<SleeperProperty> nonUpdateableProperties) {
        this.invalidValueProperties = invalidValueProperties;
        this.nonUpdateableProperties = nonUpdateableProperties;
    }

    /**
     * Checks if all of the properties are valid. They are considered valid if there are no
     * properties with invalid values and no properties that have been updated but are marked
     * as non-updateable.
     *
     * @return true if all of the properties are valid, false otherwise
     */
    public boolean isValid() {
        return invalidValueProperties.isEmpty() && nonUpdateableProperties.isEmpty();
    }

    /**
     * Checks if a specific property is invalid. A property is invalid if its value is
     * malformed or if it is a non-updateable property that has been modified.
     *
     * @param  property the property to check
     * @return          true if the property is invalid, false otherwise
     */
    public boolean isInvalid(SleeperProperty property) {
        return invalidValueProperties.contains(property) || nonUpdateableProperties.contains(property);
    }

    /**
     * Checks if a specific property has been identified as non-updateable.
     *
     * @param  property the property to check
     * @return          true if the property cannot be updated, false otherwise
     */
    public boolean isNonUpdateable(SleeperProperty property) {
        return nonUpdateableProperties.contains(property);
    }

    /**
     * Checks if a specific property has an invalid value.
     *
     * @param  property the property to check
     * @return          true if the value of the property is invalid, false otherwise
     */
    public boolean isValueInvalid(SleeperProperty property) {
        return invalidValueProperties.contains(property);
    }

}
