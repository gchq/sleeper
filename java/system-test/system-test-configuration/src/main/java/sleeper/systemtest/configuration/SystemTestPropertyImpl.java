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

package sleeper.systemtest.configuration;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.core.properties.PropertyGroup;
import sleeper.core.properties.SleeperPropertyValues;
import sleeper.core.properties.instance.InstancePropertyGroup;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class SystemTestPropertyImpl implements SystemTestProperty {
    private final String propertyName;
    private final String defaultValue;
    private final Predicate<String> validationPredicate;
    private final String description;
    private final boolean runCdkDeployWhenChanged;
    private final boolean setByCdk;
    private final boolean editable;

    private SystemTestPropertyImpl(Builder builder) {
        propertyName = Objects.requireNonNull(builder.propertyName, "propertyName must not be null");
        defaultValue = builder.defaultValue;
        validationPredicate = Objects.requireNonNull(builder.validationPredicate, "validationPredicate must not be null");
        description = Objects.requireNonNull(builder.description, "description must not be null");
        runCdkDeployWhenChanged = builder.runCdkDeployWhenChanged;
        setByCdk = builder.setByCdk;
        editable = builder.editable;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder named(String name) {
        return builder().propertyName(name);
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
    public Predicate<String> getValidationPredicate() {
        return validationPredicate;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public PropertyGroup getPropertyGroup() {
        return InstancePropertyGroup.COMMON;
    }

    @Override
    public boolean isRunCdkDeployWhenChanged() {
        return runCdkDeployWhenChanged;
    }

    @Override
    public boolean isSetByCdk() {
        return setByCdk;
    }

    @Override
    public boolean isEditable() {
        return editable && !setByCdk;
    }

    public String toString() {
        return propertyName;
    }

    @Override
    public String computeValue(String value, SleeperPropertyValues<InstanceProperty> instanceProperties) {
        if (value != null) {
            return value;
        } else {
            return getDefaultValue();
        }
    }

    public static final class Builder {
        private String propertyName;
        private String defaultValue;
        private Predicate<String> validationPredicate = s -> true;
        private String description;
        private boolean runCdkDeployWhenChanged;
        private boolean setByCdk;
        private boolean editable;
        private Consumer<SystemTestProperty> addToIndex;

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

        public Builder runCdkDeployWhenChanged(boolean runCdkDeployWhenChanged) {
            this.runCdkDeployWhenChanged = runCdkDeployWhenChanged;
            return this;
        }

        public Builder setByCdk(boolean setByCdk) {
            this.setByCdk = setByCdk;
            return this;
        }

        public Builder editable(boolean editable) {
            this.editable = editable;
            return this;
        }

        public Builder addToIndex(Consumer<SystemTestProperty> addToIndex) {
            this.addToIndex = addToIndex;
            return this;
        }

        // We want an exception to be thrown if addToIndex is null
        @SuppressFBWarnings("UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR")
        public SystemTestProperty build() {
            SystemTestProperty property = new SystemTestPropertyImpl(this);
            addToIndex.accept(property);
            return property;
        }
    }
}
