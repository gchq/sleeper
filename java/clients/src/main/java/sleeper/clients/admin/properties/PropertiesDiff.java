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

import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperPropertyIndex;
import sleeper.core.properties.SleeperProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PropertiesDiff {
    private final Map<String, PropertyDiff> changes;

    public PropertiesDiff(SleeperProperties<?> before, SleeperProperties<?> after) {
        this(before.toMap(), after.toMap());
    }

    public PropertiesDiff(Map<String, String> before, Map<String, String> after) {
        this(calculateChanges(before, after));
    }

    public PropertiesDiff(SleeperProperty property, String before, String after) {
        this(Map.of(property.getPropertyName(),
                new PropertyDiff(property.getPropertyName(), before, after)));
    }

    private PropertiesDiff(Map<String, PropertyDiff> changes) {
        this.changes = changes;
    }

    public static PropertiesDiff noChanges() {
        return new PropertiesDiff(Collections.emptyMap());
    }

    public void print(
            ConsoleOutput out, SleeperPropertyIndex<?> propertyIndex, Set<SleeperProperty> invalidProperties) {
        out.println("Found changes to properties:");
        out.println();

        // Print known properties
        propertyIndex.getAll().stream()
                .filter(property -> changes.containsKey(property.getPropertyName()))
                .map(property -> changes.get(property.getPropertyName()))
                .forEach(diff -> diff.print(out, propertyIndex, invalidProperties));

        // Print unknown properties
        List<String> unknownPropertyNames = changes.keySet().stream()
                .filter(property -> propertyIndex.getByName(property).isEmpty())
                .sorted().collect(Collectors.toList());
        for (String propertyName : unknownPropertyNames) {
            changes.get(propertyName).print(out, propertyIndex, invalidProperties);
        }

        if (!invalidProperties.isEmpty()) {
            out.println("Found invalid properties:");
            propertyIndex.getAll().stream()
                    .filter(invalidProperties::contains)
                    .forEach(property -> out.println(property.getPropertyName()));
            out.println();
        }
    }

    public <T extends SleeperProperty> List<T> getChangedPropertiesDeployedByCDK(SleeperPropertyIndex<T> propertyIndex) {
        return changes.values().stream()
                .flatMap(diff -> diff.getProperty(propertyIndex).stream())
                .filter(SleeperProperty::isRunCdkDeployWhenChanged)
                .collect(Collectors.toList());
    }

    private static Map<String, PropertyDiff> calculateChanges(Map<String, String> before, Map<String, String> after) {
        return getAllSetPropertyNames(before, after)
                .flatMap(propertyName -> PropertyDiff.forProperty(propertyName, before, after).stream())
                .collect(Collectors.toMap(PropertyDiff::getPropertyName, diff -> diff));
    }

    private static Stream<String> getAllSetPropertyNames(Map<String, String> before, Map<String, String> after) {
        Set<String> propertyNames = new HashSet<>();
        propertyNames.addAll(before.keySet());
        propertyNames.addAll(after.keySet());
        return propertyNames.stream();
    }

    public PropertiesDiff andThen(PropertiesDiff diff) {
        return new PropertiesDiff(combine(changes, diff.changes));
    }

    private static Map<String, PropertyDiff> combine(
            Map<String, PropertyDiff> firstMap, Map<String, PropertyDiff> secondMap) {

        return Stream.concat(firstMap.keySet().stream(), secondMap.keySet().stream())
                .distinct()
                .flatMap(propertyName -> combineProperty(propertyName, firstMap, secondMap).stream())
                .collect(Collectors.toMap(PropertyDiff::getPropertyName, diff -> diff));
    }

    private static Optional<PropertyDiff> combineProperty(
            String propertyName, Map<String, PropertyDiff> firstMap, Map<String, PropertyDiff> secondMap) {

        PropertyDiff first = firstMap.get(propertyName);
        PropertyDiff second = secondMap.get(propertyName);

        if (first == null) {
            return Optional.of(second);
        } else if (second == null) {
            return Optional.of(first);
        } else {
            return first.andThen(second);
        }
    }

    public List<PropertyDiff> getChanges() {
        return new ArrayList<>(changes.values());
    }

    public boolean isChanged() {
        return !changes.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PropertiesDiff that = (PropertiesDiff) o;
        return Objects.equals(changes, that.changes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(changes);
    }
}
