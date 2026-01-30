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

package sleeper.systemtest.dsl.sourcedata;

import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class GenerateNumberedRows {
    private final Configuration configuration;
    private final Schema schema;

    private GenerateNumberedRows(Configuration configuration, Schema schema) {
        this.configuration = configuration;
        this.schema = schema;
    }

    public static GenerateNumberedRows from(Schema schema, GenerateNumberedValueOverrides overrides) {
        return new GenerateNumberedRows(configureOverrides(overrides), schema);
    }

    public static GenerateNumberedRows from(Schema schema) {
        return new GenerateNumberedRows(GenerateNumberedValue::forField, schema);
    }

    private static Configuration configureOverrides(GenerateNumberedValueOverrides overrides) {
        return (keyType, field) -> overrides.getGenerator(keyType, field)
                .orElseGet(() -> GenerateNumberedValue.forField(keyType, field));
    }

    public Stream<Row> streamFrom(LongStream numbers) {
        return numbers.mapToObj(this::row);
    }

    public Iterator<Row> iteratorFrom(LongStream numbers) {
        return streamFrom(numbers).iterator();
    }

    public Iterable<Row> iterableFrom(LongStream numbers) {
        return () -> iteratorFrom(numbers);
    }

    public Iterable<Row> iterableFrom(Supplier<LongStream> numbers) {
        return () -> iteratorFrom(numbers.get());
    }

    public Iterable<Row> iterableOverRange(long startInclusive, long endExclusive) {
        return iterableFrom(() -> LongStream.range(startInclusive, endExclusive));
    }

    public Iterable<Row> iterableOverRangeClosed(long startInclusive, long endInclusive) {
        return iterableFrom(() -> LongStream.rangeClosed(startInclusive, endInclusive));
    }

    public Iterable<Row> iterableOver(long... numbers) {
        return iterableFrom(() -> LongStream.of(numbers));
    }

    public Iterable<Row> iterableOver(Collection<Long> numbers) {
        return iterableFrom(() -> numbers.stream().mapToLong(n -> n));
    }

    public Row row(long number) {
        return new Row(mapForNumber(number));
    }

    private Map<String, Object> mapForNumber(long number) {
        return Stream.of(
                entriesForFieldType(number, KeyType.ROW, schema.getRowKeyFields()),
                entriesForFieldType(number, KeyType.SORT, schema.getSortKeyFields()),
                entriesForFieldType(number, KeyType.VALUE, schema.getValueFields()))
                .flatMap(entryStream -> entryStream)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Stream<Map.Entry<String, Object>> entriesForFieldType(
            long number, KeyType keyType, List<Field> fields) {
        return fields.stream()
                .map(field -> entryForField(number, keyType, field));
    }

    private Map.Entry<String, Object> entryForField(
            long number, KeyType keyType, Field field) {
        return Map.entry(field.getName(),
                configuration.getGenerator(keyType, field).generateValue(number));
    }

    private interface Configuration {
        GenerateNumberedValue getGenerator(KeyType keyType, Field field);
    }
}
