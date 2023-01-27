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

package sleeper.configuration.properties.table;

import sleeper.configuration.properties.SleeperProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

class TablePropertyImpl implements TableProperty {

    private static final Map<String, TableProperty> ALL_MAP = new HashMap<>();
    private static final List<TableProperty> ALL = new ArrayList<>();

    private final String propertyName;
    private final String defaultValue;
    private final Predicate<String> validationPredicate;
    private final SleeperProperty defaultProperty;
    private final String description;

    private TablePropertyImpl(Builder builder) {
        propertyName = Objects.requireNonNull(builder.propertyName, "propertyName must not be null");
        defaultValue = builder.defaultValue;
        validationPredicate = Objects.requireNonNull(builder.validationPredicate, "validationPredicate must not be null");
        defaultProperty = builder.defaultProperty;
        description = Objects.requireNonNull(builder.description, "description must not be null");
    }

    static Builder builder() {
        return new Builder();
    }

    public static Builder named(String name) {
        return builder().propertyName(name);
    }

    public static List<TableProperty> all() {
        return Collections.unmodifiableList(ALL);
    }

    public static TableProperty get(String propertyName) {
        return ALL_MAP.get(propertyName);
    }

    @Override
    public Predicate<String> validationPredicate() {
        return validationPredicate;
    }

    @Override
    public String getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public SleeperProperty getDefaultProperty() {
        return defaultProperty;
    }

    @Override
    public String getDescription() {
        return description;
    }

    static final class Builder {
        private String propertyName;
        private String defaultValue;
        private Predicate<String> validationPredicate = s -> true;
        private SleeperProperty defaultProperty;
        private String description = "No description available";

        private Builder() {
        }

        public Builder propertyName(String propertyName) {
            this.propertyName = propertyName;
            return this;
        }

        public Builder defaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder validationPredicate(Predicate<String> validationPredicate) {
            this.validationPredicate = validationPredicate;
            return this;
        }

        public Builder defaultProperty(SleeperProperty defaultProperty) {
            this.defaultProperty = defaultProperty;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public TableProperty build() {
            return addToAllList(new TablePropertyImpl(this));
        }

        private static TableProperty addToAllList(TableProperty property) {
            ALL_MAP.put(property.getPropertyName(), property);
            ALL.add(property);
            return property;
        }
    }
}
