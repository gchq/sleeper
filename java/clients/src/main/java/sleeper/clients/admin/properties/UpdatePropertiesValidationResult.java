/*
 * Copyright 2022-2025 Crown Copyright
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
 * Class to hold the validation result for properties both before and after any changes.
 */
public class UpdatePropertiesValidationResult {

    private final Set<SleeperProperty> invalidProperties;
    private final Set<SleeperProperty> invalidBeforeProperties;

    public UpdatePropertiesValidationResult(Set<SleeperProperty> invalidProperties, Set<SleeperProperty> invalidBeforeProperties) {
        // this.invalidProperties = Collections.emptySet();
        // this.invalidBeforeProperties = Collections.emptySet();
        this.invalidProperties = invalidProperties;
        this.invalidBeforeProperties = invalidBeforeProperties;
    }

    public Set<SleeperProperty> getInvalidProperties() {
        return invalidProperties;
    }

    /**
     * Checks if a property was invalid and non editable before it was changed.
     *
     * @param  property a Sleeper property
     * @return          the result of the check
     */
    public boolean checkInvalidBeforeProperty(SleeperProperty property) {
        return checkInvalidBeforeProperty(property, invalidBeforeProperties);
    }

    /**
     * Checks if a property was invalid and non editable before it was changed.
     *
     * @param  property                a Sleeper property
     * @param  invalidBeforeProperties set of invalid properties before update
     * @return                         the result of the check
     */
    public static boolean checkInvalidBeforeProperty(SleeperProperty property, Set<SleeperProperty> invalidBeforeProperties) {
        if (!property.isEditable() && invalidBeforeProperties.contains(property)) {
            return true;
        }
        return false;
    }

    public boolean isInvalidPropertiesEmpty() {
        return invalidProperties.isEmpty();
    }

    // public void setInvalidBeforeProperties(Set<SleeperProperty> invalidBeforeProperties) {
    //     this.invalidBeforeProperties = invalidBeforeProperties;
    // }

    // public void setInvalidProperties(Set<SleeperProperty> invalidProperties) {
    //     this.invalidProperties = invalidProperties;
    // }

}
