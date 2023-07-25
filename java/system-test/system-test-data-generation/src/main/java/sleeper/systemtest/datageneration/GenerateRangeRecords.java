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
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class GenerateRangeRecords {
    private GenerateRangeRecords() {
    }

    public static List<Record> recordsForRange(Schema schema, LongStream longStream) {
        return longStream
                .mapToObj(i -> new Record(mapForNumber(schema, i)))
                .collect(Collectors.toUnmodifiableList());
    }

    private static Map<String, Object> mapForNumber(Schema schema, long num) {
        return Stream.of(
                        entriesForFieldType(num, KeyType.ROW, schema.getRowKeyFields()),
                        entriesForFieldType(num, KeyType.SORT, schema.getSortKeyFields()),
                        entriesForFieldType(num, KeyType.VALUE, schema.getValueFields()))
                .flatMap(s -> s)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Stream<Map.Entry<String, Object>> entriesForFieldType(
            long num, KeyType keyType, List<Field> fields) {
        return fields.stream()
                .map(field -> entryForField(num, keyType, field));
    }

    private static Map.Entry<String, Object> entryForField(
            long num, KeyType keyType, Field field) {
        return Map.entry(field.getName(),
                GenerateRangeValue.forType(keyType, field.getType()).generateValue(num));
    }
}
