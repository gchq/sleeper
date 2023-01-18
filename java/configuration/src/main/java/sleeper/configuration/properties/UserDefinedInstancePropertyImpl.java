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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

class UserDefinedInstancePropertyImpl implements UserDefinedInstancePropertyConstant {

    public static final List<UserDefinedInstancePropertyConstant> ALL = getAll();

    private final String propertyName;
    private final String defaultValue;
    private final Predicate<String> validationPredicate;

    private UserDefinedInstancePropertyImpl(Builder builder) {
        propertyName = builder.propertyName;
        defaultValue = builder.defaultValue;
        validationPredicate = builder.validationPredicate;
    }

    static Builder builder() {
        return new Builder();
    }

    static UserDefinedInstancePropertyImpl.Builder named(String name) {
        return builder().propertyName(name);
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

    private static List<UserDefinedInstancePropertyConstant> getAll() {
        Field[] fields = UserDefinedInstancePropertyConstant.class.getDeclaredFields();
        List<UserDefinedInstancePropertyConstant> properties = new ArrayList<>(fields.length);
        for (Field field : fields) {
            try {
                properties.add((UserDefinedInstancePropertyConstant) field.get(null));
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(
                        "Could not instantiate list of all user defined instance properties, failed reading " + field.getName(), e);
            }
        }
        return Collections.unmodifiableList(properties);
    }

    static final class Builder {
        private String propertyName;
        private String defaultValue;
        private Predicate<String> validationPredicate = s -> true;

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

        public UserDefinedInstancePropertyImpl build() {
            return new UserDefinedInstancePropertyImpl(this);
        }
    }
}
