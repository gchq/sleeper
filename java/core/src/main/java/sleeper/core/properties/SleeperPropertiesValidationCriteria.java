/*
 * Copyright 2022-2026 Crown Copyright
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

import java.util.List;
import java.util.function.Predicate;

/**
 * A validation criteria for Sleeper configuration properties that compares multiple properties.
 *
 * @param <T> the type of property definitions
 * @param <P> the type of property values
 */
public interface SleeperPropertiesValidationCriteria<T extends SleeperProperty> extends Predicate<SleeperProperties<T>> {

    /**
     * Reports which properties are validated by this criteria.
     *
     * @return the property definitions
     */
    List<T> getPropertiesValidated();

    /**
     * Creates a validation criteria based on the properties validated.
     *
     * @param  properties the properties validated
     * @param  predicate  the predicate
     * @return            the validation criteria
     */
    public static <T extends SleeperProperty> SleeperPropertiesValidationCriteria<T> forProperties(List<T> properties, Predicate<SleeperProperties<T>> predicate) {
        return new SleeperPropertiesValidationCriteria<T>() {
            @Override
            public boolean test(SleeperProperties<T> properties) {
                return predicate.test(properties);
            }

            @Override
            public List<T> getPropertiesValidated() {
                return properties;
            }
        };
    }

}
