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
import sleeper.configuration.properties.SleeperProperty;

import java.util.Objects;
import java.util.function.Consumer;

class CdkDefinedInstancePropertyImpl implements CdkDefinedInstanceProperty {

    private final String propertyName;
    private final String description;
    private final PropertyGroup group;
    private final SleeperProperty defaultProperty;

    private CdkDefinedInstancePropertyImpl(Builder builder) {
        propertyName = Objects.requireNonNull(builder.propertyName, "propertyName must not be null");
        description = Objects.requireNonNull(builder.description, "description must not be null");
        group = Objects.requireNonNull(builder.group, "group must not be null");
        defaultProperty = builder.defaultProperty;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder named(String propertyName) {
        return builder().propertyName(propertyName);
    }

    @Override
    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public String getDefaultValue() {
        return null;
    }

    @Override
    public SleeperProperty getDefaultProperty() {
        return defaultProperty;
    }

    public String toString() {
        return propertyName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public PropertyGroup getPropertyGroup() {
        return group;
    }

    @Override
    public boolean isRunCdkDeployWhenChanged() {
        return true;
    }

    public static final class Builder {
        private String propertyName;
        private String description;
        private PropertyGroup group;
        private Consumer<CdkDefinedInstanceProperty> addToIndex;
        private SleeperProperty defaultProperty;

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

        public Builder propertyGroup(PropertyGroup group) {
            this.group = group;
            return this;
        }

        public Builder defaultProperty(SleeperProperty property) {
            this.defaultProperty = property;
            return this;
        }

        public Builder addToIndex(Consumer<CdkDefinedInstanceProperty> addToIndex) {
            this.addToIndex = addToIndex;
            return this;
        }

        // We want an exception to be thrown if addToIndex is null
        @SuppressFBWarnings("UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR")
        public CdkDefinedInstanceProperty build() {
            CdkDefinedInstanceProperty property = new CdkDefinedInstancePropertyImpl(this);
            addToIndex.accept(property);
            return property;
        }
    }
}
