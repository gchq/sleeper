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

import sleeper.configuration.properties.SleeperPropertyIndex;
import sleeper.console.ConsoleOutput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PropertiesDiff {
    private final Map<String, PropertyDiff> changes;

    public PropertiesDiff(Map<String, String> before, Map<String, String> after) {
        this(calculateChanges(before, after));
    }

    private PropertiesDiff(Map<String, PropertyDiff> changes) {
        this.changes = changes;
    }

    public static PropertiesDiff noChanges() {
        return new PropertiesDiff(Collections.emptyMap());
    }

    public List<PropertyDiff> getChanges() {
        return new ArrayList<>(changes.values());
    }

    public void print(ConsoleOutput out, SleeperPropertyIndex<?> propertyIndex) {
        out.println("Found changes to properties:");
        out.println();
        for (PropertyDiff diff : changes.values()) {
            diff.print(out, propertyIndex);
        }
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

    public boolean isChanged() {
        return !changes.isEmpty();
    }

    public PropertiesDiff andThen(PropertiesDiff diff) {
        Map<String, PropertyDiff> combined = new HashMap<>(changes);
        diff.changes.values().forEach(then -> {
            String propertyName = then.getPropertyName();
            PropertyDiff first = changes.get(propertyName);
            if (first != null && Objects.equals(first.getOldValue(), then.getNewValue())) {
                combined.remove(propertyName);
            } else {
                combined.put(propertyName, then);
            }
        });
        return new PropertiesDiff(combined);
    }
}
