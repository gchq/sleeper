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
 * This class is used to map a property's old value to it's new value.
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
     * This method takes in a property name and two property maps.
     * It compapres the value of the property in each map.
     * It returns an Optional empty if they match and an Optional PropertyDiff if they don't.
     *
     * @param  propertyName String of the property to be searched for.
     * @param  beforeMap    Map of String to String properties of the before state.
     * @param  afterMap     Map of String to String properties of the after state.
     * @return              Optional Empty if the value of the property matches in both maps, an Optinal PropertyDiff if
     *                      they don't match.
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
     * This method prints this object to the console in a nice format.
     *
     * @param out               This ConsoleOutput object to use to print with.
     * @param propertyIndex     An Index of sleeperProperties used to get the property description and default value.
     * @param invalidProperties A set of SleeperProperties that can't be changed.
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
     * This method comapres the old value of this object to the new value of the input object 'then'.
     * It returns an Optional Empty if they match and new Optional Property diff if they don't.
     *
     * @param  then PropertyDiff containing the newValue to compare to this object's old value.
     * @return      an Optional Empty if they match and new Optional Property diff if they don't.
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
     * This method gets the property from the input propertyIndex that matches the propertyName of this object.
     *
     * @param  <T>           A SleeperPropertyIndex of type T that should have the property in.
     * @param  propertyIndex A SleeperPropertyIndex of type T that should have the property in.
     * @return               A T that extends SleeperProperty taking from the inputted propertyIndex.
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
