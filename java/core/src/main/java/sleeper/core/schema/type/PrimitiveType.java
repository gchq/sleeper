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
package sleeper.core.schema.type;

import com.facebook.collections.ByteArray;

import java.util.Comparator;

/**
 * A marker interface used to identify types that are primitives. These can be used for row and sort keys.
 */
public class PrimitiveType implements Type {

    private static final Comparator<Comparable> COMPARATOR = Comparator.nullsLast(Comparator.naturalOrder());

    protected PrimitiveType() {
    }

    /**
     * Converts a value of this type to a comparable object.
     *
     * @param  value the value
     * @return       the comparable object
     */
    public Comparable toComparable(Object value) {
        if (value == null) {
            return null;
        } else if (this instanceof ByteArrayType) {
            return ByteArray.wrap((byte[]) value);
        } else {
            return (Comparable) value;
        }
    }

    /**
     * Returns a comparator that can compare values of primitive types after they have been converted to a comparable.
     *
     * @return the comparator
     */
    public Comparator<Comparable> comparator() {
        return COMPARATOR;
    }
}
