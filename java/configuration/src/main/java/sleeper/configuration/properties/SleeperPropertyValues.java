/*
 * Copyright 2022-2023 Crown Copyright
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

import com.google.common.collect.Lists;

import sleeper.configuration.Utils;
import sleeper.configuration.properties.instance.SleeperProperty;

import java.util.List;

public interface SleeperPropertyValues<T extends SleeperProperty> {

    String get(T property);

    default boolean getBoolean(T property) {
        return Boolean.parseBoolean(get(property));
    }

    default int getInt(T property) {
        return Integer.parseInt(get(property));
    }

    default long getLong(T property) {
        return Long.parseLong(get(property));
    }

    default double getDouble(T property) {
        return Double.parseDouble(get(property));
    }

    default long getBytes(T property) {
        return Utils.readBytes(get(property));
    }

    default List<String> getList(T property) {
        return SleeperPropertyValues.readList(get(property));
    }

    static List<String> readList(String value) {
        if (value == null) {
            return List.of();
        } else {
            return Lists.newArrayList(value.split(","));
        }
    }

}
