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

package sleeper.core.properties;

import org.apache.commons.lang3.EnumUtils;

import sleeper.core.properties.validation.SleeperPropertyValueUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * Provides access to values of Sleeper configuration properties.
 *
 * @param <T> the type of properties whose values are held
 */
@FunctionalInterface
public interface SleeperPropertyValues<T extends SleeperProperty> {

    /**
     * Retrieves the value of a property. Please call the getter relevant to the type of the property, see other methods
     * on this class.
     *
     * @param  property the property
     * @return          the value of the property
     */
    String get(T property);

    /**
     * Retrieves the value of a boolean property. Please call the getter relevant to the type of the property, see other
     * methods on this class.
     *
     * @param  property the property
     * @return          the value of the property
     */
    default boolean getBoolean(T property) {
        return Boolean.parseBoolean(get(property));
    }

    /**
     * Retrieves the value of an integer property. Please call the getter relevant to the type of the property, see
     * other methods on this class.
     *
     * @param  property the property
     * @return          the value of the property
     */
    default Integer getInt(T property) {
        String val = get(property);
        if (val != null) {
            return Integer.parseInt(val);
        } else {
            return null;
        }
    }

    /**
     * Retrieves the value of a nullable integer property. Please call the getter relevant to the type of the property,
     * see
     * other methods on this class.
     *
     * @param  property the property
     * @return          the value of the property
     */
    default Integer getIntOrNull(T property) {
        String val = get(property);
        if (val != null) {
            return Integer.parseInt(val);
        } else {
            return null;
        }
    }

    /**
     * Retrieves the value of a long integer property. Please call the getter relevant to the type of the property, see
     * other methods on this class.
     *
     * @param  property the property
     * @return          the value of the property
     */
    default long getLong(T property) {
        return Long.parseLong(get(property));
    }

    /**
     * Retrieves the value of a nullable long integer property. Please call the getter relevant to the type of the
     * property, see other methods on this class.
     *
     * @param  property the property
     * @return          the value of the property
     */
    default Long getLongOrNull(T property) {
        String val = get(property);
        if (val != null) {
            return Long.parseLong(val);
        } else {
            return null;
        }
    }

    /**
     * Retrieves the value of a double precision floating point property. Please call the getter relevant to the type of
     * the property, see other methods on this class.
     *
     * @param  property the property
     * @return          the value of the property
     */
    default double getDouble(T property) {
        return Double.parseDouble(get(property));
    }

    /**
     * Retrieves the value of a property for a number of bytes. Please call the getter relevant to the type of the
     * property, see other methods on this class.
     *
     * @param  property the property
     * @return          the value of the property
     */
    default long getBytes(T property) {
        return SleeperPropertyValueUtils.readBytes(get(property));
    }

    /**
     * Retrieves the value of a property for a list of strings. Please call the getter relevant to the type of the
     * property, see other methods on this class.
     *
     * @param  property the property
     * @return          the value of the property
     */
    default List<String> getList(T property) {
        return SleeperPropertyValueUtils.readList(get(property));
    }

    /**
     * Retrieves the value of a property for a list of an enum type. Please call the getter relevant to the
     * type of the property, see other methods on this class.
     *
     * @param  property  the property
     * @param  enumClass the enum type
     * @return           the value of the property
     */
    default <E extends Enum<E>> List<E> getEnumList(T property, Class<E> enumClass) {
        return streamEnumList(property, enumClass).collect(toUnmodifiableList());
    }

    /**
     * Streams the values of a property for a list of an enum type. Please call the getter relevant to the
     * type of the property, see other methods on this class.
     *
     * @param  property  the property
     * @param  enumClass the enum type
     * @return           the values of the property
     */
    default <E extends Enum<E>> Stream<E> streamEnumList(T property, Class<E> enumClass) {
        return SleeperPropertyValueUtils.streamEnumList(property, get(property), enumClass);
    }

    /**
     * Retrieves the value of a property of an enum type. Please call the getter relevant to the type of the property,
     * see other methods on this class.
     *
     * @param  property  the property
     * @param  enumClass the enum type
     * @return           the value of the property
     */
    default <E extends Enum<E>> E getEnumValue(T property, Class<E> enumClass) {
        String value = get(property);
        return Optional.ofNullable(value)
                .map(mode -> EnumUtils.getEnumIgnoreCase(enumClass, mode))
                .orElseThrow(() -> new IllegalArgumentException("Unrecognised value for " + property + ": " + value));
    }
}
