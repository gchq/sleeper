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

public class StringParameter {

    private final String key;
    private final String defaultValue;

    private StringParameter(String key, String defaultValue) {
        this.key = key;
        this.defaultValue = defaultValue;
    }

    String get(AppContext context) {
        return getStringOrDefault(context, key, defaultValue);
    }

    public StringValue value(String value) {
        return new StringValue(key, value);
    }

    static StringParameter keyAndDefault(String key, String defaultValue) {
        return new StringParameter(key, defaultValue);
    }

    static String getStringOrDefault(AppContext context, String key, String defaultValue) {
        Object object = context.get(key);
        if (object instanceof String) {
            if ("".equals(object)) {
                throw new IllegalArgumentException(key + " must not be an empty string");
            }
            return (String) object;
        } else {
            return defaultValue;
        }
    }
}
