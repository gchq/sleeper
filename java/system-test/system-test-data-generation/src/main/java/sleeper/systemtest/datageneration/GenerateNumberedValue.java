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

import org.apache.commons.lang.StringUtils;

import sleeper.core.schema.Field;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;

import java.nio.ByteBuffer;

public interface GenerateNumberedValue {
    Object generateValue(long number);

    static GenerateNumberedValue forField(KeyType keyType, Field field) {
        Type fieldType = field.getType();
        if (fieldType instanceof IntType) {
            return num -> (int) num;
        }
        if (fieldType instanceof LongType) {
            return num -> num;
        }
        if (fieldType instanceof StringType) {
            switch (keyType) {
                case ROW:
                default:
                    return stringFromPrefixAndPadToSize("row-", 19);
                case SORT:
                    return stringFromPrefixAndPadToSize("sort-", 19);
                case VALUE:
                    return stringFromPrefixAndPadToSize("Value ", 19);
            }
        }
        if (fieldType instanceof ByteArrayType) {
            return num -> {
                ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
                buf.putLong(num);
                return buf.array();
            };
        }
        throw new IllegalArgumentException("Unknown type " + fieldType);
    }

    static GenerateNumberedValue stringFromPrefixAndPadToSize(String prefix, int size) {
        return num -> prefix + StringUtils.leftPad(num + "", size, "0");
    }
}
