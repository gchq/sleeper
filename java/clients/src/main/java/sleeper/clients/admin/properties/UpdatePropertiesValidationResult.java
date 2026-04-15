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

import java.util.Collections;
import java.util.Set;

public class UpdatePropertiesValidationResult {

    private Set<SleeperProperty> invalidProperties;
    private Set<SleeperProperty> invalidBeforeProperties;

    public UpdatePropertiesValidationResult(Set<SleeperProperty> invalidProperties, Set<SleeperProperty> invalidBeforeProperties) {
        this.invalidProperties = invalidProperties;
        this.invalidBeforeProperties = invalidBeforeProperties;
    }

    public UpdatePropertiesValidationResult() {
        this.invalidProperties = Collections.emptySet();
        this.invalidBeforeProperties = Collections.emptySet();
    }

    public Set<SleeperProperty> getInvalidProperties() {
        return invalidProperties;
    }

    public Set<SleeperProperty> getInvalidBeforeProperties() {
        return invalidBeforeProperties;
    }

    public Boolean checkInvalidBeforeProperty(SleeperProperty property) {
        if (!property.isEditable() && invalidBeforeProperties.contains(property)) {
            return true;
        }
        return false;
    }

    public Boolean isInvalidPropertiesEmpty() {
        return invalidProperties.isEmpty();
    }

    public void setInvalidBeforeProperties(Set<SleeperProperty> invalidBeforeProperties) {
        this.invalidBeforeProperties = invalidBeforeProperties;
    }

    public void setInvalidProperties(Set<SleeperProperty> invalidProperties) {
        this.invalidProperties = invalidProperties;
    }

}
