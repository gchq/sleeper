/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.environment.cdk.config;

public class IntParameter {
    private final String key;
    private final int defaultValue;

    private IntParameter(String key, int defaultValue) {
        this.key = key;
        this.defaultValue = defaultValue;
    }

    int get(AppContext context) {
        return OptionalStringParameter.getOptionalString(context, key)
                .map(value -> parse(key, value))
                .orElse(defaultValue);
    }

    public StringValue value(int value) {
        return value("" + value);
    }

    public StringValue value(String value) {
        return new StringValue(key, value);
    }

    static IntParameter keyAndDefault(String key, int defaultValue) {
        return new IntParameter(key, defaultValue);
    }

    private static int parse(String key, String value) {
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            throw new IllegalArgumentException(key + "must be an integer", e);
        }
    }

}
