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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A listener to be notified of invalid Sleeper configuration property values. Gathers invalid values to throw as an
 * exception.
 */
public class SleeperPropertiesValidationReporter {

    private final Map<SleeperProperty, String> invalidValues = new LinkedHashMap<>();

    /**
     * Records that the given property value is invalid.
     *
     * @param property the property
     * @param value    the value
     */
    public void invalidProperty(SleeperProperty property, String value) {
        invalidValues.put(property, value);
    }

    /**
     * Throws an exception if any property values were invalid.
     *
     * @throws SleeperPropertiesInvalidException if any values were reported as invalid
     */
    public void throwIfFailed() throws SleeperPropertiesInvalidException {
        if (!invalidValues.isEmpty()) {
            throw new SleeperPropertiesInvalidException(invalidValues);
        }
    }

}
