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
package sleeper.core.schema.type;

import com.facebook.collections.ByteArray;

/**
 * A primitive type that represents a byte array.
 */
public class ByteArrayType implements PrimitiveType {

    @Override
    public Comparable toComparable(Object value) {
        if (value == null) {
            return null;
        } else {
            return ByteArray.wrap((byte[]) value);
        }
    }

    @Override
    public int hashCode() {
        return 4;
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        if (obj instanceof ByteArrayType) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "ByteArrayType{}";
    }
}
