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
 * Interface to be implemented by all property enums. Allows SleeperProperties to set generically. Depending on
 * implementation the class may want to provide a means of validating the provided property.
 */
public interface SleeperProperty {
    String getPropertyName();

    String getDefaultValue();

    String getDescription();

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

    default Predicate<String> validationPredicate() {
        return s -> true;
    }

    default void validate(String value, SleeperPropertiesValidationReporter reporter) {
        if (!validationPredicate().test(value)) {
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
