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

import sleeper.core.properties.SleeperProperties;
import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.SleeperProperty;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;

public class UpdatePropertiesRequest<T extends SleeperProperties<?>> {

    private final PropertiesDiff diff;
    private final T beforeProperties;
    private final T updatedProperties;
    // private final UpdatePropertiesValidationResult updatePropertiesValidationResult;

    private UpdatePropertiesRequest(PropertiesDiff diff, T beforeProperties, T updatedProperties) {
        this.diff = diff;
        this.beforeProperties = beforeProperties;
        this.updatedProperties = updatedProperties;
        // this.updatePropertiesValidationResult = updatePropertiesValidationResult;
    }

    public static <T extends SleeperProperties<?>> UpdatePropertiesRequest<T> fromBeforeAndAfter(T beforeProperties,
            T updatedProperties) {
        PropertiesDiff propertiesDiff;
        if (beforeProperties.equals(updatedProperties)) {
            propertiesDiff = PropertiesDiff.noChanges();
        } else {
            propertiesDiff = new PropertiesDiff(beforeProperties, updatedProperties);
        }
        // UpdatePropertiesValidationResult updatePropertiesValidationResult = new UpdatePropertiesValidationResult();
        return new UpdatePropertiesRequest<>(propertiesDiff, beforeProperties, updatedProperties);
    }

    public PropertiesDiff getDiff() {
        return diff;
    }

    public T getUpdatedProperties() {
        return updatedProperties;
    }

    public UpdatePropertiesValidationResult validateProperties() {
        Set<SleeperProperty> invalidBeforeProperties = getInvalidBeforeProperty();
        try {
            updatedProperties.validate();
            return new UpdatePropertiesValidationResult(getUneditableProperties(invalidBeforeProperties)
                    .collect(Collectors.toSet()), invalidBeforeProperties);
        } catch (SleeperPropertiesInvalidException e) {
            return new UpdatePropertiesValidationResult(Stream.concat(getUneditableProperties(invalidBeforeProperties), e.getInvalidValues().keySet().stream())
                    .collect(Collectors.toSet()), invalidBeforeProperties);

        }
    }

    private Stream<? extends SleeperProperty> getUneditableProperties(Set<SleeperProperty> invalidBeforeProperties) {
        return diff.getChanges().stream()
                .flatMap(d -> d.getProperty(updatedProperties.getPropertiesIndex()).stream())
                .filter(not(SleeperProperty::isEditable))
                .filter(not(invalidBeforeProperties::contains));
    }

    private Set<SleeperProperty> getInvalidBeforeProperty() {
        try {
            beforeProperties.validate();
            return Collections.emptySet();
        } catch (SleeperPropertiesInvalidException e) {
            return e.getInvalidValues().keySet();
        }
    }
}
