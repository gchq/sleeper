/*
 * Copyright 2022 Crown Copyright
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
package sleeper.clients.admin;

import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperProperty;
import sleeper.configuration.properties.SystemDefinedInstanceProperty;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TableProperty;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

public class SleeperPropertyUtils {

    private SleeperPropertyUtils() {
    }

    public static <T extends SleeperProperty> NavigableMap<Object, Object> orderedPropertyMapWithIncludes(
            SleeperProperties<T> properties, List<T> includeIfMissing) {
        TreeMap<Object, Object> orderedMap = new TreeMap<>();
        Iterator<Map.Entry<Object, Object>> iterator = properties.getPropertyIterator();
        while (iterator.hasNext()) {
            Map.Entry<Object, Object> entry = iterator.next();
            orderedMap.put(entry.getKey(), entry.getValue());
        }
        for (T property : includeIfMissing) {
            if (!orderedMap.containsKey(property.getPropertyName())) {
                orderedMap.put(property.getPropertyName(), properties.get(property));
            }
        }
        return orderedMap;
    }

    public static UserDefinedInstanceProperty getValidInstanceProperty(String propertyName, String propertyValue) {
        getProperty(propertyName, SystemDefinedInstanceProperty.values())
                .ifPresent(property -> {
                    throw new IllegalArgumentException(String.format(
                            "Sleeper property %s is a system-defined property and cannot be updated", propertyName));
                });
        UserDefinedInstanceProperty property = getProperty(propertyName, UserDefinedInstanceProperty.values())
                .orElseThrow(() -> new IllegalArgumentException(String.format(
                        "Sleeper property %s does not exist and cannot be updated", propertyName)));
        if (!property.validationPredicate().test(propertyValue)) {
            throw new IllegalArgumentException(String.format("Sleeper property %s is invalid", propertyName));
        }
        return property;
    }

    public static TableProperty getValidTableProperty(String propertyName, String propertyValue) {
        TableProperty property = getProperty(propertyName, TableProperty.values())
                .orElseThrow(() -> new IllegalArgumentException(String.format(
                        "Sleeper property %s does not exist and cannot be updated", propertyName)));
        if (!property.validationPredicate().test(propertyValue)) {
            throw new IllegalArgumentException(String.format("Sleeper property %s is invalid", propertyName));
        }
        return property;
    }

    private static <T extends SleeperProperty> Optional<T> getProperty(String propertyName, T[] properties) {
        for (T property : properties) {
            if (property.getPropertyName().equals(propertyName)) {
                return Optional.of(property);
            }
        }
        return Optional.empty();
    }

}
