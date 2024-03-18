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
package sleeper.core.record;

import com.facebook.collections.ByteArray;

import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;

import java.util.Comparator;

/**
 * A {@link Comparator} for a 1-dimensional key.
 */
public final class SingleKeyComparator implements Comparator<Object> {
    private final PrimitiveType type;

    public static SingleKeyComparator from(PrimitiveType type) {
        if (!(type instanceof IntType)
                && !(type instanceof LongType)
                && !(type instanceof StringType)
                && !(type instanceof ByteArrayType)) {
            throw new IllegalArgumentException("type must be one of IntType, LongType, StringType, ByteArrayType");
        }
        return new SingleKeyComparator(type);
    }

    public SingleKeyComparator(PrimitiveType type) {
        this.type = type;
    }

    @Override
    public int compare(Object key1, Object key2) {
        if (type instanceof IntType) {
            return Integer.compare((int) key1, (int) key2);
        }
        if (type instanceof LongType) {
            return Long.compare((long) key1, (long) key2);
        }
        if (type instanceof StringType) {
            return ((String) key1).compareTo((String) key2);
        }
        if (type instanceof ByteArrayType) {
            ByteArray ba1 = ByteArray.wrap((byte[]) key1);
            ByteArray ba2 = ByteArray.wrap((byte[]) key2);
            return ba1.compareTo(ba2);
        }
        throw new RuntimeException("Unknown type " + type);
    }
}
