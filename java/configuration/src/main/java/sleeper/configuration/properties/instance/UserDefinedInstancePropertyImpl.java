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

package sleeper.configuration.properties.instance;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import sleeper.configuration.properties.PropertyGroup;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

class UserDefinedInstancePropertyImpl implements UserDefinedInstanceProperty {
    private final String propertyName;
    private final String defaultValue;
    private final Predicate<String> validationPredicate;
    private final String description;
    private final PropertyGroup propertyGroup;
    private final boolean runCdkDeployWhenChanged;
    private final boolean editable;
    private final boolean includedInTemplate;
    private final boolean includedInBasicTemplate;
    private final boolean ignoreEmptyValue;

    private UserDefinedInstancePropertyImpl(Builder builder) {
        propertyName = Objects.requireNonNull(builder.propertyName, "propertyName must not be null");
        defaultValue = builder.defaultValue;
        validationPredicate = Objects.requireNonNull(builder.validationPredicate, "validationPredicate must not be null");
        description = Objects.requireNonNull(builder.description, "description must not be null");
        propertyGroup = Objects.requireNonNull(builder.propertyGroup, "propertyGroup must not be null");
        runCdkDeployWhenChanged = builder.runCdkDeployWhenChanged;
        editable = builder.editable;
        includedInTemplate = builder.includedInTemplate;
        includedInBasicTemplate = Optional.ofNullable(builder.includedInBasicTemplate)
                .orElseGet(UserDefinedInstanceProperty.super::isIncludedInBasicTemplate);
        ignoreEmptyValue = builder.ignoreEmptyValue;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder named(String name) {
        return builder().propertyName(name);
    }

    @Override
    public Predicate<String> validationPredicate() {
        return validationPredicate;
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
    public PropertyGroup getPropertyGroup() {
        return propertyGroup;
    }

    public String toString() {
        return propertyName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public boolean isRunCdkDeployWhenChanged() {
        return runCdkDeployWhenChanged;
    }

    @Override
    public boolean isEditable() {
        return editable;
    }

    @Override
    public boolean isIncludedInTemplate() {
        return includedInTemplate;
    }

    @Override
    public boolean isIncludedInBasicTemplate() {
        return includedInBasicTemplate;
    }

    @Override
    public boolean isIgnoreEmptyValue() {
        return ignoreEmptyValue;
    }

    static final class Builder {
        private String propertyName;
        private String defaultValue;
        private Predicate<String> validationPredicate = s -> true;
        private String description;
        private PropertyGroup propertyGroup;
        private boolean runCdkDeployWhenChanged;
        private boolean editable = true;
        private boolean includedInTemplate = true;
        private Boolean includedInBasicTemplate;
        private boolean ignoreEmptyValue = true;
        private Consumer<UserDefinedInstanceProperty> addToIndex;

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

        public Builder propertyGroup(PropertyGroup propertyGroup) {
            this.propertyGroup = propertyGroup;
            return this;
        }

        public Builder runCdkDeployWhenChanged(boolean runCdkDeployWhenChanged) {
            this.runCdkDeployWhenChanged = runCdkDeployWhenChanged;
            return this;
        }

        public Builder editable(boolean editable) {
            this.editable = editable;
            return this;
        }

        public Builder includedInTemplate(boolean includedInTemplate) {
            this.includedInTemplate = includedInTemplate;
            return this;
        }

        public Builder includedInBasicTemplate(boolean includedInBasicTemplate) {
            this.includedInBasicTemplate = includedInBasicTemplate;
            return this;
        }

        public Builder ignoreEmptyValue(boolean ignoreEmptyValue) {
            this.ignoreEmptyValue = ignoreEmptyValue;
            return this;
        }

        public Builder addToIndex(Consumer<UserDefinedInstanceProperty> addToIndex) {
            this.addToIndex = addToIndex;
            return this;
        }

        // We want an exception to be thrown if addToIndex is null
        @SuppressFBWarnings("UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR")
        public UserDefinedInstanceProperty build() {
            UserDefinedInstanceProperty property = new UserDefinedInstancePropertyImpl(this);
            addToIndex.accept(property);
            return property;
        }
    }
}
