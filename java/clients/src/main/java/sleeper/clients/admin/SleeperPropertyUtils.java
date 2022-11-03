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
        UserDefinedInstanceProperty property = getUserDefinedInstanceProperty(propertyName)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Sleeper property " + propertyName + " does not exist and cannot be updated"));
        if (!property.validationPredicate().test(propertyValue)) {
            throw new IllegalArgumentException("Sleeper property " + propertyName + " is invalid");
        }
        return property;
    }

    public static TableProperty getValidTableProperty(String propertyName, String propertyValue) {
        TableProperty property = getTableProperty(propertyName)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Sleeper property " + propertyName + " does not exist and cannot be updated"));
        if (!property.validationPredicate().test(propertyValue)) {
            throw new IllegalArgumentException("Sleeper property " + propertyName + " is invalid");
        }
        return property;
    }

    private static Optional<UserDefinedInstanceProperty> getUserDefinedInstanceProperty(String propertyName) {
        for (UserDefinedInstanceProperty property : UserDefinedInstanceProperty.values()) {
            if (property.getPropertyName().equals(propertyName)) {
                return Optional.of(property);
            }
        }
        return Optional.empty();
    }

    private static Optional<TableProperty> getTableProperty(String propertyName) {
        for (TableProperty property : TableProperty.values()) {
            if (property.getPropertyName().equals(propertyName)) {
                return Optional.of(property);
            }
        }
        return Optional.empty();
    }

}
