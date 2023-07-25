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

package sleeper.systemtest.datageneration;

import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;

public interface GenerateRangeByField {
    Object rowKey(long number);

    default Object sortKey(long number) {
        return rowKey(number);
    }

    default Object value(long number) {
        return rowKey(number);
    }

    static GenerateRangeByField forType(Type type) {
        if (type instanceof IntType) {
            return num -> (int) num;
        }
        if (type instanceof LongType) {
            return num -> num;
        }
        if (type instanceof StringType) {
            return num -> "record-" + num;
        }
        if (type instanceof ByteArrayType) {
            return num -> new byte[]{(byte) num};
        }
        throw new IllegalArgumentException("Unknown type " + type);
    }
}
