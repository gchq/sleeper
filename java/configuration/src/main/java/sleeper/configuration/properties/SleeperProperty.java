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
package sleeper.configuration.properties;

import java.util.Locale;
import java.util.function.Predicate;

/**
 * Defines a property used in Sleeper configuration. Used to generate documentation and to validate values and process
 * changes for a single Sleeper property.
 */
public interface SleeperProperty {

    /**
     * Retrieves the name of the property, used to set the value in a properties file.
     *
     * @return the property name
     */
    String getPropertyName();

    /**
     * Retrieves the default value of the property. May be null if the property has no default value. This is usually
     * the value the property will have if it is not set, but some properties have other behaviour to compute thier
     * value, e.g. to retrieve the value from another property.
     *
     * @return the default value
     */
    String getDefaultValue();

    /**
     * Retrieves a description of the property. Used in documentation and when displaying the value in a human-readable
     * way.
     *
     * @return the description
     */
    String getDescription();

    /**
     * Retrieves the property's group. Used to gather related properties together for display.
     *
     * @return the group
     */
    PropertyGroup getPropertyGroup();

    default boolean isSetByCdk() {
        return false;
    }

    default boolean isEditable() {
        return !isSetByCdk();
    }

    default boolean isUserDefined() {
        return !isSetByCdk();
    }

    default Predicate<String> getValidationPredicate() {
        return s -> true;
    }

    default void validate(String value, SleeperPropertiesValidationReporter reporter) {
        if (!getValidationPredicate().test(value)) {
            reporter.invalidProperty(this, value);
        }
    }

    default String toEnvironmentVariable() {
        return getPropertyName().toUpperCase(Locale.ROOT).replace('.', '_');
    }

    /**
     * Whether or not the system must be redeployed to apply a change to the property.
     *
     * @return True if the property can only be applied by running the CDK, and not just by saving it to S3
     */
    boolean isRunCdkDeployWhenChanged();

    default boolean isIncludedInTemplate() {
        return false;
    }

    default boolean isIncludedInBasicTemplate() {
        return false;
    }

    default boolean isIgnoreEmptyValue() {
        return true;
    }

}
