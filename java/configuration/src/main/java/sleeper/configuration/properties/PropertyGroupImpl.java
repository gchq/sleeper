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

public class PropertyGroupImpl implements PropertyGroup {
    private static final List<PropertyGroup> ALL = new ArrayList<>();
    private final String name;
    private final String description;

    private PropertyGroupImpl(Builder builder) {
        name = builder.name;
        description = builder.description;
    }

    static Builder group(String name) {
        return builder().name(name);
    }

    private static Builder builder() {
        return new Builder();
    }

    @Override
    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public static List<PropertyGroup> all() {
        return Collections.unmodifiableList(ALL);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PropertyGroupImpl that = (PropertyGroupImpl) o;
        return Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(description);
    }

    @Override
    public String toString() {
        return description;
    }

    static final class Builder {
        private String name;
        private String description;

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

        public PropertyGroup build() {
            return addToAllList(new PropertyGroupImpl(this));
        }

        private static PropertyGroup addToAllList(PropertyGroup property) {
            ALL.add(property);
            return property;
        }
    }
}
