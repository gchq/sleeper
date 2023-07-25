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

import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class GenerateRangeRecords {
    private GenerateRangeRecords() {
    }

    public static List<Record> recordsForRange(Schema schema, LongStream longStream) {
        return longStream
                .mapToObj(i -> new Record(Map.of(
                        "key", valueForKeyType(schema.getField("key").orElseThrow().getType(), i))))
                .collect(Collectors.toUnmodifiableList());
    }

    public static Object valueForKeyType(Type type, long num) {
        if (type instanceof IntType) {
            return (int) num;
        }
        if (type instanceof LongType) {
            return num;
        }
        if (type instanceof StringType) {
            return "record-" + num;
        }
        if (type instanceof ByteArrayType) {
            return new byte[]{(byte) num};
        }
        throw new IllegalArgumentException("Unknown type " + type);
    }
}
