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
import sleeper.core.properties.SleeperPropertyIndex;

import java.util.Collections;
import java.util.Set;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableSet;

public class UpdatePropertiesRequest<T extends SleeperProperties<?>> {

    private final PropertiesDiff diff;
    private final T propertiesBefore;
    private final T propertiesAfter;

    private UpdatePropertiesRequest(PropertiesDiff diff, T propertiesBefore, T propertiesAfter) {
        this.diff = diff;
        this.propertiesBefore = propertiesBefore;
        this.propertiesAfter = propertiesAfter;
    }

    public static <T extends SleeperProperties<?>> UpdatePropertiesRequest<T> fromBeforeAndAfter(T beforeProperties,
            T updatedProperties) {
        return new UpdatePropertiesRequest<>(new PropertiesDiff(beforeProperties, updatedProperties), beforeProperties, updatedProperties);
    }

    public PropertiesDiff getDiff() {
        return diff;
    }

    public T getPropertiesBefore() {
        return propertiesBefore;
    }

    public T getPropertiesAfter() {
        return propertiesAfter;
    }

    public UpdatePropertiesValidationResult validateProperties() {
        try {
            propertiesAfter.validate();
            return new UpdatePropertiesValidationResult(Set.of(), getNonUpdateableProperties());
        } catch (SleeperPropertiesInvalidException e) {
            return new UpdatePropertiesValidationResult(e.getInvalidValues().keySet(), getNonUpdateableProperties());

        }
    }

    private Set<SleeperProperty> getNonUpdateableProperties() {
        Set<SleeperProperty> invalidBeforeProperties = getInvalidBeforeProperties();
        SleeperPropertyIndex<?> propertyIndex = propertiesAfter.getPropertiesIndex();
        return diff.streamChanges()
                .flatMap(d -> propertyIndex.getByName(d.getPropertyName()).stream())
                .filter(not(SleeperProperty::isEditable))
                .filter(not(invalidBeforeProperties::contains)) // If an uneditable property was invalid before, allow editing
                .collect(toUnmodifiableSet());
    }

    private Set<SleeperProperty> getInvalidBeforeProperties() {
        try {
            propertiesBefore.validate();
            return Collections.emptySet();
        } catch (SleeperPropertiesInvalidException e) {
            return e.getInvalidValues().keySet();
        }
    }
}
