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
package sleeper.clients.admin.properties;

import sleeper.configuration.properties.SleeperProperties;
import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.SleeperProperty;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;

public class UpdatePropertiesRequest<T extends SleeperProperties<?>> {

    private final PropertiesDiff diff;
    private final T updatedProperties;

    public UpdatePropertiesRequest(PropertiesDiff diff, T updatedProperties) {
        this.diff = diff;
        this.updatedProperties = updatedProperties;
    }

    public PropertiesDiff getDiff() {
        return diff;
    }

    public T getUpdatedProperties() {
        return updatedProperties;
    }

    public Set<SleeperProperty> getInvalidProperties() {
        try {
            updatedProperties.validate();
            return getUneditableChangedProperties()
                    .collect(Collectors.toSet());
        } catch (SleeperPropertiesInvalidException e) {
            return Stream.concat(getUneditableChangedProperties(), e.getInvalidValues().keySet().stream())
                    .collect(Collectors.toSet());
        }
    }

    private Stream<? extends SleeperProperty> getUneditableChangedProperties() {
        return diff.getChanges().stream()
                .flatMap(d -> d.getProperty(updatedProperties.getPropertiesIndex()).stream())
                .filter(not(SleeperProperty::isEditable));
    }
}
