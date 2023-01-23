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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

class SystemDefinedInstancePropertyImpl implements SystemDefinedInstanceProperty {

    private static final Map<String, SystemDefinedInstanceProperty> ALL_MAP = new HashMap<>();
    private static final List<SystemDefinedInstanceProperty> ALL = new ArrayList<>();

    private final String propertyName;
    private final String description;

    private SystemDefinedInstancePropertyImpl(Builder builder) {
        propertyName = Objects.requireNonNull(builder.propertyName, "propertyName must not be null");
        description = Objects.requireNonNull(builder.description, "description must not be null");
    }

    @Override
    public String getDefaultValue() {
        return null;
    }

    @Override
    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder named(String propertyName) {
        return builder().propertyName(propertyName);
    }

    public static SystemDefinedInstanceProperty get(String propertyName) {
        return ALL_MAP.get(propertyName);
    }

    public static List<SystemDefinedInstanceProperty> all() {
        return Collections.unmodifiableList(ALL);
    }

    public static final class Builder {
        private String propertyName;
        private String description = "No description available";

        private Builder() {
        }

        public Builder propertyName(String propertyName) {
            this.propertyName = propertyName;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public SystemDefinedInstanceProperty build() {
            return addToAllList(new SystemDefinedInstancePropertyImpl(this));
        }

        private static SystemDefinedInstanceProperty addToAllList(SystemDefinedInstanceProperty property) {
            ALL_MAP.put(property.getPropertyName(), property);
            ALL.add(property);
            return property;
        }
    }
}
