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

public class GenerateNumberedRecords {
    private GenerateNumberedRecords() {
    }

    public static Stream<Record> from(Schema schema, LongStream numbers) {
        return numbers.mapToObj(number -> numberedRecord(schema, number));
    }

    public static Record numberedRecord(Schema schema, long number) {
        return new Record(mapForNumber(schema, number));
    }

    private static Map<String, Object> mapForNumber(Schema schema, long number) {
        return Stream.of(
                        entriesForFieldType(number, KeyType.ROW, schema.getRowKeyFields()),
                        entriesForFieldType(number, KeyType.SORT, schema.getSortKeyFields()),
                        entriesForFieldType(number, KeyType.VALUE, schema.getValueFields()))
                .flatMap(entryStream -> entryStream)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Stream<Map.Entry<String, Object>> entriesForFieldType(
            long number, KeyType keyType, List<Field> fields) {
        return fields.stream()
                .map(field -> entryForField(number, keyType, field));
    }

    private static Map.Entry<String, Object> entryForField(
            long number, KeyType keyType, Field field) {
        return Map.entry(field.getName(),
                GenerateNumberedValue.forField(keyType, field).generateValue(number));
    }
}
