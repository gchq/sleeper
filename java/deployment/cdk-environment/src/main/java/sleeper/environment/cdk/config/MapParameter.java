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
            throw new IllegalArgumentException(key + " must be a comma-separated list of key=value pairs");
        }
    }

    private Map<String, String> readMap(String value) {
        Map<String, String> map = new LinkedHashMap<>();
        if (value.isEmpty()) {
            return map;
        }
        for (String entry : value.split(",")) {
            int index = entry.indexOf('=');
            if (index < 0) {
                throw new IllegalArgumentException(key + " entry must be in the form key=value, got: " + entry);
            }
            map.put(entry.substring(0, index), entry.substring(index + 1));
        }
        return map;
    }

}
