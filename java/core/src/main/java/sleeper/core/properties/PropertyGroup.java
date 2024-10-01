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

package sleeper.core.properties;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * A group used to organise Sleeper configuration properties.
 */
public class PropertyGroup {
    private final String name;
    private final String description;
    private final String details;

    private PropertyGroup(Builder builder) {
        name = Objects.requireNonNull(builder.name, "name must not be null");
        description = Objects.requireNonNull(builder.description, "description must not be null");
        details = builder.details;
    }

    /**
     * Creates a builder for a group with a given name.
     *
     * @param  name the group name
     * @return      a builder
     */
    public static Builder group(String name) {
        return new Builder().name(name);
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getDetails() {
        return details;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PropertyGroup that = (PropertyGroup) o;
        return name.equals(that.name) && description.equals(that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, description);
    }

    @Override
    public String toString() {
        return description;
    }

    /**
     * A builder for instances of this class.
     */
    public static final class Builder {
        private String name;
        private String description;
        private String details;
        private Consumer<PropertyGroup> afterBuild = group -> {
        };

        private Builder() {
        }

        /**
         * Sets the group name.
         *
         * @param  name the name
         * @return      this builder
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the group description.
         *
         * @param  description the description
         * @return             this builder
         */
        public Builder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * Sets the group details.
         *
         * @param  details the details
         * @return         this builder
         */
        public Builder details(String details) {
            this.details = details;
            return this;
        }

        /**
         * Sets an operation to perform on the group after it is built.
         *
         * @param  afterBuild the operation
         * @return            this builder
         */
        public Builder afterBuild(Consumer<PropertyGroup> afterBuild) {
            this.afterBuild = afterBuild;
            return this;
        }

        public PropertyGroup build() {
            PropertyGroup group = new PropertyGroup(this);
            afterBuild.accept(group);
            return group;
        }
    }

    /**
     * Sorts a list of Sleeper configuration properties by their group. Orders groups of properties in the order that
     * the property groups are specified in the list.
     *
     * @param  <T>        the type of property to sort
     * @param  properties the properties
     * @param  groups     the order of groups to apply to the properties
     * @return            a new list of the properties with groups in the given order
     */
    public static <T extends SleeperProperty> List<T> sortPropertiesByGroup(List<T> properties, List<PropertyGroup> groups) {
        List<T> sorted = new ArrayList<>(properties);
        sorted.sort(Comparator.comparingInt(p -> groups.indexOf(p.getPropertyGroup())));
        return sorted;
    }
}
