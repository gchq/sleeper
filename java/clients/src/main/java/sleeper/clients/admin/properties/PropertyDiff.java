/*
 * Copyright 2022-2025 Crown Copyright
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
import sleeper.core.properties.SleeperProperty;
import sleeper.core.properties.SleeperPropertyIndex;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static sleeper.core.properties.SleeperPropertiesPrettyPrinter.formatDescription;

/**
 * A diff describing a single change to a Sleeper configuration property.
 */
public class PropertyDiff {
    private final String propertyName;
    private final String oldValue;
    private final String newValue;

    public PropertyDiff(String propertyName, String oldValue, String newValue) {
        this.propertyName = Objects.requireNonNull(propertyName, "propertyName must not be null");
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    /**
     * Creates a diff for a single property based on a set of all values before and after a change.
     *
     * @param  propertyName the name of the property to diff
     * @param  beforeMap    the values of all properties before the change
     * @param  afterMap     the values of all properties after the change
     * @return              the diff, if the property changed
     */
    public static Optional<PropertyDiff> forProperty(String propertyName, Map<String, String> beforeMap, Map<String, String> afterMap) {
        String oldValue = beforeMap.get(propertyName);
        String newValue = afterMap.get(propertyName);
        if (Objects.equals(oldValue, newValue)) {
            return Optional.empty();
        } else {
            return Optional.of(new PropertyDiff(propertyName, oldValue, newValue));
        }
    }

    /**
     * Writes a description of the change to the console.
     *
     * @param out               the console output
     * @param propertyIndex     an index of all properties of the type being changed (e.g. instance properties)
     * @param invalidProperties which properties have validation failures caused by the larger update to all values
     */
    public void print(ConsoleOutput out,
            SleeperPropertyIndex<?> propertyIndex,
            Set<SleeperProperty> invalidProperties) {
        out.println(propertyName);
        var propertyOpt = propertyIndex.getByName(propertyName);
        String description = propertyOpt.map(SleeperProperty::getDescription)
                .orElse("Unknown property, no description available");
        out.println(formatDescription("", description));
        if (oldValue == null) {
            String defaultValue = propertyOpt.map(SleeperProperty::getDefaultValue).orElse(null);
            if (defaultValue != null) {
                out.printf("Unset before, default value: %s%n", defaultValue);
            } else {
                out.println("Unset before");
            }
        } else {
            out.printf("Before: %s%n", oldValue);
        }
        out.printf("After%s: %s%n", invalidNote(propertyOpt.orElse(null), invalidProperties), newValue);
        out.println();
    }

    private String invalidNote(SleeperProperty property, Set<SleeperProperty> invalidProperties) {
        if (property == null) {
            return "";
        }
        if (!property.isEditable()) {
            return " (cannot be changed, please undo)";
        }
        if (invalidProperties.contains(property)) {
            return " (not valid, please change)";
        }
        return "";
    }

    /**
     * Combines this diff with another change that happens after this one. This should only be used with a diff to the
     * same property. This will discard information about the intermediate state.
     *
     * @param  then the diff to apply after this one
     * @return      the combined diff, if the start and end values are different
     */
    public Optional<PropertyDiff> andThen(PropertyDiff then) {
        if (Objects.equals(oldValue, then.newValue)) {
            return Optional.empty();
        } else {
            return Optional.of(new PropertyDiff(propertyName, oldValue, then.newValue));
        }
    }

    public String getPropertyName() {
        return propertyName;
    }

    public String getOldValue() {
        return oldValue;
    }

    public String getNewValue() {
        return newValue;
    }

    /**
     * Retrieves the declaration of the property changed in this diff from the index.
     *
     * @param  <T>           the type of properties changed by this diff (e.g. instance property)
     * @param  propertyIndex the index of all properties of the type being changed
     * @return               the definition of the property changed by this diff, if it is in the index
     */
    public <T extends SleeperProperty> Optional<T> getProperty(SleeperPropertyIndex<T> propertyIndex) {
        return propertyIndex.getByName(propertyName);
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
}
