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

package sleeper.configuration.properties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class PropertyGroup {
    private static final List<PropertyGroup> ALL = new ArrayList<>();
    private final String name;
    private final String description;

    private PropertyGroup(Builder builder) {
        name = Objects.requireNonNull(builder.name, "name must not be null");
        description = Objects.requireNonNull(builder.description, "description must not be null");
    }

    public static Builder group(String name) {
        return new Builder().name(name);
    }

    public static List<PropertyGroup> all() {
        return Collections.unmodifiableList(ALL);
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
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

    static final class Builder {
        private String name;
        private String description;
        private Consumer<PropertyGroup> addToList = Builder::addToAllList;

        private Builder() {
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder addToList(Consumer<PropertyGroup> addToList) {
            this.addToList = addToList;
            return this;
        }

        public PropertyGroup build() {
            PropertyGroup group = new PropertyGroup(this);
            addToList.accept(group);
            return group;
        }

        private static void addToAllList(PropertyGroup property) {
            ALL.add(property);
        }
    }
}
