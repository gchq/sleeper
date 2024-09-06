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

import java.util.Map;

public class SleeperPropertiesInvalidException extends IllegalArgumentException {

    private final transient Map<SleeperProperty, String> invalidValues;

    public SleeperPropertiesInvalidException(Map<SleeperProperty, String> invalidValues) {
        super(buildMessage(invalidValues));
        this.invalidValues = invalidValues;
    }

    private static String buildMessage(Map<SleeperProperty, String> invalidValues) {
        String message = buildFailureMessage(firstFailure(invalidValues));
        int failures = invalidValues.size();
        if (failures > 1) {
            message += " Failure 1 of " + failures + ".";
        }
        return message;
    }

    private static Map.Entry<SleeperProperty, String> firstFailure(Map<SleeperProperty, String> invalidValues) {
        return invalidValues.entrySet().stream().findFirst().orElseThrow();
    }

    private static String buildFailureMessage(Map.Entry<SleeperProperty, String> failure) {
        SleeperProperty property = failure.getKey();
        String value = failure.getValue();
        return "Property " + property.getPropertyName() + " was invalid. It was " + quoteIfSet(value) + ".";
    }

    private static String quoteIfSet(String value) {
        if (value != null) {
            return "\"" + value + "\"";
        } else {
            return "unset";
        }
    }

    public Map<SleeperProperty, String> getInvalidValues() {
        return invalidValues;
    }
}
