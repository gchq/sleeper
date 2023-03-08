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

import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperProperty;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PropertiesDiff<T extends SleeperProperty> {
    private final List<PropertyDiff> propertyDiffs;

    public PropertiesDiff(SleeperProperties<T> before, SleeperProperties<T> after) {
        this.propertyDiffs = calculateDiffs(before, after);
    }

    private static <T extends SleeperProperty> List<PropertyDiff> calculateDiffs(
            SleeperProperties<T> before, SleeperProperties<T> after) {
        return Stream.concat(
                        calculateKnownPropertyDiffs(before, after),
                        calculateUnknownPropertyDiffs(before, after))
                .collect(Collectors.toList());
    }

    private static <T extends SleeperProperty> Stream<PropertyDiff> calculateKnownPropertyDiffs(
            SleeperProperties<T> before, SleeperProperties<T> after) {
        Set<T> setProperties = new HashSet<>();
        before.getKnownSetProperties().forEach(setProperties::add);
        after.getKnownSetProperties().forEach(setProperties::add);
        return setProperties.stream()
                .flatMap(property -> PropertyDiff.compare(property, before, after).stream());
    }

    private static <T extends SleeperProperty> Stream<PropertyDiff> calculateUnknownPropertyDiffs(
            SleeperProperties<T> before, SleeperProperties<T> after) {
        Set<String> unknownProperties = new HashSet<>();
        Map<String, String> beforeMap = before.getUnknownPropertyValues()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, String> afterMap = after.getUnknownPropertyValues()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        unknownProperties.addAll(beforeMap.keySet());
        unknownProperties.addAll(afterMap.keySet());


        return unknownProperties.stream()
                .flatMap(propertyName -> PropertyDiff.compare(propertyName, beforeMap, afterMap).stream());
    }

    public List<PropertyDiff> getChanges() {
        return propertyDiffs;
    }
}
