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
package sleeper.environment.cdk.config;

import java.util.LinkedHashMap;
import java.util.Map;

public class MapParameter {

    private final String key;

    private MapParameter(String key) {
        this.key = key;
    }

    static MapParameter key(String key) {
        return new MapParameter(key);
    }

    public StringValue value(String... entries) {
        return new StringValue(key, String.join(",", entries));
    }

    Map<String, String> get(AppContext context) {
        return readMap(context.get(key));
    }

    private Map<String, String> readMap(Object value) {
        if (value == null) {
            return Map.of();
        } else if (value instanceof String) {
            return readMap((String) value);
        } else {
            throw new IllegalArgumentException(key + " must be a comma-separated list of key,value pairs");
        }
    }

    private Map<String, String> readMap(String value) {
        Map<String, String> map = new LinkedHashMap<>();
        if (value.isEmpty()) {
            return map;
        }
        String[] entries = value.split(",", -1);
        if (entries.length % 2 != 0) {
            throw new IllegalArgumentException(
                    key + " must have a value for every key (even number of comma-separated entries), got: " + value);
        }
        for (int i = 0; i < entries.length; i += 2) {
            String tagKey = entries[i];
            String tagValue = entries[i + 1];
            if (tagKey.isEmpty() || tagValue.isEmpty()) {
                throw new IllegalArgumentException(key + " must not contain an empty key or value, got: " + value);
            }
            map.put(tagKey, tagValue);
        }
        return map;
    }

}
