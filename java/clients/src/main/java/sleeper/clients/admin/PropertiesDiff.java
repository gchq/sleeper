/*
 * Copyright 2022-2023 Crown Copyright
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

import sleeper.configuration.properties.SleeperProperty;
import sleeper.configuration.properties.SleeperPropertyIndex;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PropertiesDiff {
    private final List<PropertyDiff> changes;

    public PropertiesDiff(Map<String, String> before, Map<String, String> after) {
        this.changes = calculateChanges(before, after);
    }

    public PropertiesDiff(SleeperProperty property, String before, String after) {
        this.changes = List.of(new PropertyDiff(property.getPropertyName(), before, after));
    }

    public List<PropertyDiff> getChanges() {
        return changes;
    }

    public <T extends SleeperProperty> List<T> getChangedPropertiesDeployedByCDK(SleeperPropertyIndex<T> propertyIndex) {
        return getChanges().stream()
                .flatMap(diff -> diff.getProperty(propertyIndex).stream())
                .filter(SleeperProperty::isRunCDKDeployWhenChanged)
                .collect(Collectors.toList());
    }

    private static List<PropertyDiff> calculateChanges(Map<String, String> before, Map<String, String> after) {
        return getAllSetPropertyNames(before, after)
                .flatMap(propertyName -> PropertyDiff.forProperty(propertyName, before, after).stream())
                .collect(Collectors.toList());
    }

    private static Stream<String> getAllSetPropertyNames(Map<String, String> before, Map<String, String> after) {
        Set<String> propertyNames = new HashSet<>();
        propertyNames.addAll(before.keySet());
        propertyNames.addAll(after.keySet());
        return propertyNames.stream();
    }
}
