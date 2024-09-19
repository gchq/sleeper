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

import com.google.common.collect.Lists;
import org.apache.commons.lang3.EnumUtils;

import sleeper.configuration.Utils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

@FunctionalInterface
public interface SleeperPropertyValues<T extends SleeperProperty> {

    String get(T property);

    default boolean getBoolean(T property) {
        return Boolean.parseBoolean(get(property));
    }

    default Integer getInt(T property) {
        String val = get(property);
        if (val != null) {
            return Integer.parseInt(val);
        } else {
            return null;
        }
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

    default <E extends Enum<E>> List<E> getEnumList(T property, Class<E> enumClass) {
        return streamEnumList(property, enumClass).collect(toUnmodifiableList());
    }

    default <E extends Enum<E>> Stream<E> streamEnumList(T property, Class<E> enumClass) {
        return streamEnumList(property, get(property), enumClass);
    }

    default <E extends Enum<E>> E getEnumValue(T property, Class<E> enumClass) {
        String value = get(property);
        return Optional.ofNullable(value)
                .map(mode -> EnumUtils.getEnumIgnoreCase(enumClass, mode))
                .orElseThrow(() -> new IllegalArgumentException("Unrecognised value for " + property + ": " + value));
    }

    static List<String> readList(String value) {
        if (value == null || value.length() < 1) {
            return List.of();
        } else {
            return Lists.newArrayList(value.split(","));
        }
    }

    static <E extends Enum<E>> Stream<E> streamEnumList(SleeperProperty property, String value, Class<E> enumClass) {
        return readList(value).stream()
                .map(item -> Optional.ofNullable(EnumUtils.getEnumIgnoreCase(enumClass, item))
                        .orElseThrow(() -> new IllegalArgumentException("Unrecognised value for " + property + ": " + item)));
    }
}
