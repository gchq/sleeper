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

    private final Set<SleeperProperty> invalidValueProperties;
    private final Set<SleeperProperty> nonUpdateableProperties;

    public UpdatePropertiesValidationResult(Set<SleeperProperty> invalidValueProperties, Set<SleeperProperty> nonUpdateableProperties) {
        this.invalidValueProperties = invalidValueProperties;
        this.nonUpdateableProperties = nonUpdateableProperties;
    }

    public boolean isValid() {
        return invalidValueProperties.isEmpty() && nonUpdateableProperties.isEmpty();
    }

    public boolean isInvalid(SleeperProperty property) {
        return invalidValueProperties.contains(property) || nonUpdateableProperties.contains(property);
    }

    public boolean isNonUpdateable(SleeperProperty property) {
        return nonUpdateableProperties.contains(property);
    }

    public boolean isValueInvalid(SleeperProperty property) {
        return invalidValueProperties.contains(property);
    }

}
