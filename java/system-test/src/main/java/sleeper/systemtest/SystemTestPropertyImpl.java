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

package sleeper.systemtest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class SystemTestPropertyImpl implements SystemTestProperty {
    private static final Map<String, SystemTestProperty> ALL_MAP = new HashMap<>();
    private static final List<SystemTestProperty> ALL = new ArrayList<>();
    private final String propertyName;
    private final String defaultValue;
    private final Predicate<String> validationPredicate;
    private final String description;

    private SystemTestPropertyImpl(Builder builder) {
        propertyName = builder.propertyName;
        defaultValue = builder.defaultValue;
        validationPredicate = builder.validationPredicate;
        description = builder.description;
    }

    @Override
    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public String getDefaultValue() {
        return defaultValue;
    }

    @Override
    public Predicate<String> validationPredicate() {
        return validationPredicate;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder named(String name) {
        return builder().propertyName(name);
    }

    public static SystemTestProperty get(String propertyName) {
        return ALL_MAP.get(propertyName);
    }

    public static List<SystemTestProperty> all() {
        return Collections.unmodifiableList(ALL);
    }

    public static final class Builder {
        private String propertyName;
        private String defaultValue;
        private Predicate<String> validationPredicate = (s) -> true;
        private String description;

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

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public SystemTestProperty build() {
            return addToAllList(new SystemTestPropertyImpl(this));
        }

        private static SystemTestProperty addToAllList(SystemTestProperty property) {
            ALL_MAP.put(property.getPropertyName(), property);
            ALL.add(property);
            return property;
        }
    }
}
