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

public interface GenerateRangeValue {
    Object generateValue(long number);

    static GenerateRangeValue forType(KeyType keyType, Type fieldType) {
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
                    return num -> "row-" + num;
                case SORT:
                    return num -> "sort-" + num;
                case VALUE:
                    return num -> "Value " + num;
            }
        }
        if (fieldType instanceof ByteArrayType) {
            return num -> new byte[]{(byte) num};
        }
        throw new IllegalArgumentException("Unknown type " + fieldType);
    }
}
