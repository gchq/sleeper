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

import java.util.Objects;
import java.util.Optional;

public class PropertyDiff {
    private final SleeperProperty property;
    private final String oldValue;
    private final String newValue;

    public static <T extends SleeperProperty> Optional<PropertyDiff> compare(T property, SleeperProperties<T> before, SleeperProperties<T> after) {
        String oldValue = before.get(property);
        String newValue = after.get(property);
        if (Objects.equals(oldValue, newValue)) {
            return Optional.empty();
        } else {
            return Optional.of(new PropertyDiff(property, oldValue, newValue));
        }
    }

    public PropertyDiff(SleeperProperty property, String oldValue, String newValue) {
        this.property = Objects.requireNonNull(property, "property must not be null");
        this.oldValue = oldValue;
        this.newValue = newValue;
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
        return property.equals(that.property)
                && Objects.equals(oldValue, that.oldValue)
                && Objects.equals(newValue, that.newValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(property, oldValue, newValue);
    }

    @Override
    public String toString() {
        return "PropertyDiff{" +
                "property=" + property +
                ", oldValue='" + oldValue + '\'' +
                ", newValue='" + newValue + '\'' +
                '}';
    }
}
