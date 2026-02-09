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

import java.util.List;

public class StringListParameter {

    private final String key;

    private StringListParameter(String key) {
        this.key = key;
    }

    List<String> get(AppContext context) {
        return readList(context.get(key));
    }

    public StringValue value(String... values) {
        return new StringValue(key, String.join(",", values));
    }

    static StringListParameter key(String key) {
        return new StringListParameter(key);
    }

    private List<String> readList(Object value) {
        if (value == null) {
            return List.of();
        } else if (value instanceof String) {
            return readList((String) value);
        } else {
            throw new IllegalArgumentException(key + " must be a comma-separated string");
        }
    }

    private static List<String> readList(String value) {
        if (value.length() < 1) {
            return List.of();
        } else {
            return List.of(value.split(","));
        }
    }

}
