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

import sleeper.core.properties.SleeperProperties;
import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.SleeperProperty;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UpdatePropertiesRequest<T extends SleeperProperties<?>> {

    private final PropertiesDiff diff;
    private final T beforeProperties;
    private final T updatedProperties;
    private final Set<SleeperProperty> invalidBeforeProperties;
    private final UpdatePropertiesValidationResult updatePropertiesValidationResult;

    private UpdatePropertiesRequest(PropertiesDiff diff, T beforeProperties, T updatedProperties,
            UpdatePropertiesValidationResult updatePropertiesValidationResult) {
        this.diff = diff;
        this.beforeProperties = beforeProperties;
        this.updatedProperties = updatedProperties;
        this.updatePropertiesValidationResult = updatePropertiesValidationResult;
        this.invalidBeforeProperties = getInvalidBeforeProperty();
    }

    public static <T extends SleeperProperties<?>> UpdatePropertiesRequest<T> buildRequest(T beforeProperties,
            T updatedProperties) {
        PropertiesDiff propertiesDiff;
        if (beforeProperties.equals(updatedProperties)) {
            propertiesDiff = PropertiesDiff.noChanges();
        } else {
            propertiesDiff = new PropertiesDiff(beforeProperties, updatedProperties);
        }
        UpdatePropertiesValidationResult updatePropertiesValidationResult = new UpdatePropertiesValidationResult();
        return new UpdatePropertiesRequest<>(propertiesDiff, beforeProperties, updatedProperties,
                updatePropertiesValidationResult);
    }

    public PropertiesDiff getDiff() {
        return diff;
    }

    public T getBeforeProperties() {
        return beforeProperties;
    }

    public T getUpdatedProperties() {
        return updatedProperties;
    }

    public UpdatePropertiesValidationResult getUpdatePropertiesValidationResult() {
        return updatePropertiesValidationResult;
    }

    public Set<SleeperProperty> getInvalidBeforeProperties() {
        return invalidBeforeProperties;
    }

    public Set<SleeperProperty> getInvalidProperties() {
        try {
            updatedProperties.validate();
            updatePropertiesValidationResult.setInvalidProperties(getUneditableProperties()
                    .collect(Collectors.toSet()));
            return getUneditableProperties()
                    .collect(Collectors.toSet());
        } catch (SleeperPropertiesInvalidException e) {
            updatePropertiesValidationResult.setInvalidProperties(Stream.concat(getUneditableProperties(), e.getInvalidValues().keySet().stream())
                    .collect(Collectors.toSet()));
            return Stream.concat(getUneditableProperties(), e.getInvalidValues().keySet().stream())
                    .collect(Collectors.toSet());
        }
    }

    private Stream<? extends SleeperProperty> getUneditableProperties() {
        getInvalidBeforeProperty();
        return diff.getChanges().stream()
                .flatMap(d -> d.getProperty(updatedProperties.getPropertiesIndex()).stream())
                .filter(prop -> isEligibleForStream(prop));

    }

    private Set<SleeperProperty> getInvalidBeforeProperty() {
        try {
            beforeProperties.validate();
            updatePropertiesValidationResult.setInvalidBeforeProperties(Collections.emptySet());
            return Collections.emptySet();
        } catch (SleeperPropertiesInvalidException e) {
            updatePropertiesValidationResult.setInvalidBeforeProperties(e.getInvalidValues().keySet());
            return e.getInvalidValues().keySet();
        }
    }

    private boolean isEligibleForStream(SleeperProperty property) {
        if (updatePropertiesValidationResult.checkInvalidBeforeProperty(property)) {
            return false;
        }
        return !property.isEditable();
    }
}
