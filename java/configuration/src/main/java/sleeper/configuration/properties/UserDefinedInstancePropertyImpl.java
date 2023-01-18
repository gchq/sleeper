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

import java.util.function.Predicate;

class UserDefinedInstancePropertyImpl implements InstanceProperty {
    private final String propertyName;
    private final String defaultValue;
    private final Predicate<String> validationPredicate;

    UserDefinedInstancePropertyImpl(String propertyName) {
        this(propertyName, (String) null);
    }

    UserDefinedInstancePropertyImpl(String propertyName, Predicate<String> validationPredicate) {
        this(propertyName, null, validationPredicate);
    }

    UserDefinedInstancePropertyImpl(String propertyName, String defaultValue) {
        this(propertyName, defaultValue, (s) -> true);
    }

    UserDefinedInstancePropertyImpl(String propertyName, String defaultValue, Predicate<String> validationPredicate) {
        this.propertyName = propertyName;
        this.defaultValue = defaultValue;
        this.validationPredicate = validationPredicate;
    }

    private UserDefinedInstancePropertyImpl(Builder builder) {
        propertyName = builder.propertyName;
        defaultValue = builder.defaultValue;
        validationPredicate = builder.validationPredicate;
    }

    static Builder builder() {
        return new Builder();
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


    static final class Builder {
        private String propertyName;
        private String defaultValue;
        private Predicate<String> validationPredicate = (s) -> true;

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
