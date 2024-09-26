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
package sleeper.core.properties;

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
     * the value the property will have if it is not set, but some properties have other behaviour to compute their
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

    /**
     * Checks whether the property is set automatically at deploy time by the CDK. The CDK will clear and overwrite all
     * properties that are set by the CDK, on every deployment.
     *
     * @return true if set by the CDK, false if set before CDK deployment
     */
    default boolean isSetByCdk() {
        return false;
    }

    /**
     * Checks whether the property is editable. For example, changing the instance ID would mean referring to a
     * completely different instance of Sleeper, so it doesn't make sense to edit it.
     *
     * @return true if the property is editable
     */
    default boolean isEditable() {
        return !isSetByCdk();
    }

    /**
     * Checks whether the property may be set by the user. If false, it may be set by the CDK, or by some other
     * automated process.
     *
     * @return true if the property may be set by the user
     */
    default boolean isUserDefined() {
        return !isSetByCdk();
    }

    /**
     * Retrieves a predicate to check whether a value of this property is valid. Called any time a value is validated.
     * Used to inherit the validation predicate for a property that defaults to the value of another property.
     *
     * @return the predicate
     */
    default Predicate<String> getValidationPredicate() {
        return s -> true;
    }

    /**
     * Builds an environment variable name that is equivalent to the name of this property. Used when the configuration
     * bucket is set in an environment variable.
     *
     * @return the environment variable name
     */
    default String toEnvironmentVariable() {
        return getPropertyName().toUpperCase(Locale.ROOT).replace('.', '_');
    }

    /**
     * Whether or not the system must be redeployed to apply a change to the property.
     *
     * @return True if the property can only be applied by running the CDK, and not just by saving it to S3
     */
    boolean isRunCdkDeployWhenChanged();

    /**
     * Checks whether this property should be included in a template to fill in property values. Used to generate the
     * template. This should only be false if we expect the property not to be set in the properties file. For example,
     * a Sleeper table schema is usually set in a separate file.
     *
     * @return true if the property should be included in a full template
     */
    default boolean isIncludedInTemplate() {
        return false;
    }

    /**
     * Checks whether this property should be included in the short version of a template to fill in property values.
     * Used to generate the template. This should only be true for properties that usually need to be deliberately set
     * for every instance.
     *
     * @return true if the property should be included in a basic template
     */
    default boolean isIncludedInBasicTemplate() {
        return false;
    }

    /**
     * Checks whether an empty value of the property should be ignored. For most properties, if someone sets it to an
     * empty string, we can assume they intended not to set the property at all, and we can consider this equivalent.
     * There are some properties that have meaning when they are set to an empty string. For example the optional stacks
     * is a list of comma-separated values, and we want to be able to set it to an empty list to deploy none of the
     * optional stacks.
     *
     * @return true if an empty string is equivalent to no value set for this property
     */
    default boolean isIgnoreEmptyValue() {
        return true;
    }

}
