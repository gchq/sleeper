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

package sleeper.configuration.properties.table;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import sleeper.configuration.properties.PropertyGroup;
import sleeper.configuration.properties.SleeperProperty;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

class TablePropertyImpl implements TableProperty {

    private final String propertyName;
    private final String description;
    private final PropertyGroup propertyGroup;
    private final TablePropertyComputeValue computeValue;
    private final String defaultValue;
    private final SleeperProperty defaultProperty;
    private final Predicate<String> validationPredicate;
    private final boolean editable;
    private final boolean includedInTemplate;
    private final boolean setBySleeper;

    private TablePropertyImpl(Builder builder) {
        propertyName = Objects.requireNonNull(builder.propertyName, "propertyName must not be null");
        description = Objects.requireNonNull(builder.description, "description must not be null");
        propertyGroup = Objects.requireNonNull(builder.propertyGroup, "propertyGroup must not be null");
        computeValue = Optional.ofNullable(builder.computeValue).orElseGet(TablePropertyComputeValue::none);
        defaultValue = builder.defaultValue;
        defaultProperty = builder.defaultProperty;
        validationPredicate = Objects.requireNonNull(builder.validationPredicate, "validationPredicate must not be null");
        editable = builder.editable;
        includedInTemplate = builder.includedInTemplate;
        setBySleeper = builder.setBySleeper;
    }

    static Builder builder() {
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
    public String getDescription() {
        return description;
    }

    @Override
    public PropertyGroup getPropertyGroup() {
        return propertyGroup;
    }

    @Override
    public String computeValue(String value, InstanceProperties instanceProperties, TableProperties tableProperties) {
        return computeValue.computeValue(value, instanceProperties, tableProperties);
    }

    @Override
    public String getDefaultValue() {
        return defaultValue;
    }

    @Override
    public SleeperProperty getDefaultProperty() {
        return defaultProperty;
    }

    @Override
    public Predicate<String> getValidationPredicate() {
        return validationPredicate;
    }

    @Override
    public boolean isRunCdkDeployWhenChanged() {
        return false;
    }

    @Override
    public boolean isEditable() {
        return editable && !setBySleeper;
    }

    @Override
    public boolean isUserDefined() {
        return !setBySleeper;
    }

    @Override
    public boolean isIncludedInTemplate() {
        return includedInTemplate;
    }

    public String toString() {
        return propertyName;
    }

    static final class Builder {
        private String propertyName;
        private String description;
        private PropertyGroup propertyGroup;
        private TablePropertyComputeValue computeValue;
        private String defaultValue;
        private SleeperProperty defaultProperty;
        private Predicate<String> validationPredicate = s -> true;
        private Consumer<TableProperty> addToIndex;
        private boolean editable = true;
        private boolean includedInTemplate = true;
        private boolean setBySleeper;

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

        public Builder propertyGroup(PropertyGroup propertyGroup) {
            this.propertyGroup = propertyGroup;
            return this;
        }

        public Builder computeValue(TablePropertyComputeValue computeValue) {
            if (this.computeValue != null) {
                throw new IllegalArgumentException("Set value computation twice for property " + propertyName);
            }
            this.computeValue = computeValue;
            return this;
        }

        public Builder getDefaultValue(BiFunction<InstanceProperties, TableProperties, String> getDefault) {
            return computeValue(TablePropertyComputeValue.applyDefaultValue(getDefault));
        }

        public Builder defaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
            return computeValue(TablePropertyComputeValue.fixedDefault(defaultValue));
        }

        public Builder defaultProperty(SleeperProperty defaultProperty) {
            return defaultPropertyWithBehaviour(defaultProperty, TablePropertyComputeValue::defaultProperty);
        }

        public <T extends SleeperProperty> Builder defaultPropertyWithBehaviour(T defaultProperty, Function<T, TablePropertyComputeValue> behaviour) {
            this.defaultProperty = defaultProperty;
            this.defaultValue = defaultProperty.getDefaultValue();
            return computeValue(behaviour.apply(defaultProperty))
                    .validationPredicate(defaultProperty.getValidationPredicate());
        }

        public Builder validationPredicate(Predicate<String> validationPredicate) {
            this.validationPredicate = validationPredicate;
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

        public Builder setBySleeper(boolean setBySleeper) {
            this.setBySleeper = setBySleeper;
            return this;
        }

        public Builder addToIndex(Consumer<TableProperty> addToIndex) {
            this.addToIndex = addToIndex;
            return this;
        }

        // We want an exception to be thrown if addToIndex is null
        @SuppressFBWarnings("UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR")
        public TableProperty build() {
            TableProperty property = new TablePropertyImpl(this);
            addToIndex.accept(property);
            return property;
        }
    }
}
