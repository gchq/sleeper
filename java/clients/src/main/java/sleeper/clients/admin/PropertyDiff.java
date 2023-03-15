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

import sleeper.console.ConsoleOutput;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class PropertyDiff {
    private final String propertyName;
    private final String oldValue;
    private final String newValue;

    public PropertyDiff(String propertyName, String oldValue, String newValue) {
        this.propertyName = Objects.requireNonNull(propertyName, "propertyName must not be null");
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    public static Optional<PropertyDiff> forProperty(String propertyName, Map<String, String> beforeMap, Map<String, String> afterMap) {
        String oldValue = beforeMap.get(propertyName);
        String newValue = afterMap.get(propertyName);
        if (Objects.equals(oldValue, newValue)) {
            return Optional.empty();
        } else {
            return Optional.of(new PropertyDiff(propertyName, oldValue, newValue));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PropertyDiff that = (PropertyDiff) o;
        return propertyName.equals(that.propertyName)
                && Objects.equals(oldValue, that.oldValue)
                && Objects.equals(newValue, that.newValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(propertyName, oldValue, newValue);
    }

    @Override
    public String toString() {
        return "PropertyDiff{" +
                "propertyName='" + propertyName + '\'' +
                ", oldValue='" + oldValue + '\'' +
                ", newValue='" + newValue + '\'' +
                '}';
    }

    public void print(ConsoleOutput out) {
        out.println(propertyName);
        out.printf("Before: %s%n", oldValue);
        out.printf("After: %s%n", newValue);
        out.println();
    }
}
