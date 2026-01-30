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

import org.junit.jupiter.api.Test;

import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.applyFormat;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideKeyAndFieldType;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrides;
import static sleeper.systemtest.dsl.sourcedata.KeyType.ROW;
import static sleeper.systemtest.dsl.sourcedata.KeyType.SORT;
import static sleeper.systemtest.dsl.sourcedata.KeyType.VALUE;

public class GenerateNumberedRowsTest {
    @Test
    void shouldGenerateTwoRowsWithStringType() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("rowkey", new StringType()))
                .sortKeyFields(new Field("sortkey", new StringType()))
                .valueFields(new Field("value", new StringType()))
                .build();

        // When/Then
        assertThat(GenerateNumberedRows.from(schema).iterableFrom(LongStream.of(1, Long.MAX_VALUE)))
                .containsExactly(
                        new Row(Map.of(
                                "rowkey", "row-0000000000000000001",
                                "sortkey", "sort-0000000000000000001",
                                "value", "Value 0000000000000000001")),
                        new Row(Map.of(
                                "rowkey", "row-9223372036854775807",
                                "sortkey", "sort-9223372036854775807",
                                "value", "Value 9223372036854775807")));
    }

    @Test
    void shouldGenerateTwoRowsWithIntType() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("rowkey", new IntType()))
                .sortKeyFields(new Field("sortkey", new IntType()))
                .valueFields(new Field("value", new IntType()))
                .build();

        // When/Then
        assertThat(GenerateNumberedRows.from(schema).iterableFrom(LongStream.rangeClosed(1, 2)))
                .containsExactly(
                        new Row(Map.of(
                                "rowkey", 1,
                                "sortkey", 1,
                                "value", 1)),
                        new Row(Map.of(
                                "rowkey", 2,
                                "sortkey", 2,
                                "value", 2)));
    }

    @Test
    void shouldGenerateTwoRowsWithLongType() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("rowkey", new LongType()))
                .sortKeyFields(new Field("sortkey", new LongType()))
                .valueFields(new Field("value", new LongType()))
                .build();

        // When/Then
        assertThat(GenerateNumberedRows.from(schema).iterableFrom(LongStream.rangeClosed(1, 2)))
                .containsExactly(
                        new Row(Map.of(
                                "rowkey", 1L,
                                "sortkey", 1L,
                                "value", 1L)),
                        new Row(Map.of(
                                "rowkey", 2L,
                                "sortkey", 2L,
                                "value", 2L)));
    }

    @Test
    void shouldGenerateTwoRowsWithByteArrayType() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("rowkey", new ByteArrayType()))
                .sortKeyFields(new Field("sortkey", new ByteArrayType()))
                .valueFields(new Field("value", new ByteArrayType()))
                .build();

        // When/Then
        assertThat(GenerateNumberedRows.from(schema).iterableFrom(LongStream.rangeClosed(1, 2)))
                .containsExactly(
                        new Row(Map.of(
                                "rowkey", new byte[]{0, 0, 0, 0, 0, 0, 0, 1},
                                "sortkey", new byte[]{0, 0, 0, 0, 0, 0, 0, 1},
                                "value", new byte[]{0, 0, 0, 0, 0, 0, 0, 1})),
                        new Row(Map.of(
                                "rowkey", new byte[]{0, 0, 0, 0, 0, 0, 0, 2},
                                "sortkey", new byte[]{0, 0, 0, 0, 0, 0, 0, 2},
                                "value", new byte[]{0, 0, 0, 0, 0, 0, 0, 2})));
    }

    @Test
    void shouldOverrideStringGeneratorForOneFieldType() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("rowkey", new StringType()))
                .sortKeyFields(new Field("sortkey", new StringType()))
                .valueFields(new Field("value", new StringType()))
                .build();
        GenerateNumberedValueOverrides overrides = overrideKeyAndFieldType(ROW, StringType.class, numberStringAndZeroPadTo(3));

        // When/Then
        assertThat(GenerateNumberedRows.from(schema, overrides).iterableFrom(LongStream.of(1, 999)))
                .containsExactly(
                        new Row(Map.of(
                                "rowkey", "001",
                                "sortkey", "sort-0000000000000000001",
                                "value", "Value 0000000000000000001")),
                        new Row(Map.of(
                                "rowkey", "999",
                                "sortkey", "sort-0000000000000000999",
                                "value", "Value 0000000000000000999")));
    }

    @Test
    void shouldOverrideStringGeneratorForAllFieldTypes() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("rowkey", new StringType()))
                .sortKeyFields(new Field("sortkey", new StringType()))
                .valueFields(new Field("value", new StringType()))
                .build();
        GenerateNumberedValueOverrides overrides = overrides(
                overrideKeyAndFieldType(ROW, StringType.class,
                        numberStringAndZeroPadTo(3).then(applyFormat("customrow-%s"))),
                overrideKeyAndFieldType(SORT, StringType.class,
                        numberStringAndZeroPadTo(3).then(addPrefix("customsort-"))),
                overrideKeyAndFieldType(VALUE, StringType.class,
                        numberStringAndZeroPadTo(3).then(addPrefix("Custom value "))));

        // When/Then
        assertThat(GenerateNumberedRows.from(schema, overrides).iterableFrom(LongStream.of(1, 999)))
                .containsExactly(
                        new Row(Map.of(
                                "rowkey", "customrow-001",
                                "sortkey", "customsort-001",
                                "value", "Custom value 001")),
                        new Row(Map.of(
                                "rowkey", "customrow-999",
                                "sortkey", "customsort-999",
                                "value", "Custom value 999")));
    }

    @Test
    void shouldOverrideStringGeneratorForFieldByName() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("rowkey", new StringType()))
                .sortKeyFields(new Field("sortkey", new StringType()))
                .valueFields(new Field("value", new StringType()))
                .build();
        GenerateNumberedValueOverrides overrides = overrides(
                overrideField("rowkey",
                        numberStringAndZeroPadTo(5).then(addPrefix("rowkey-"))),
                overrideField("sortkey",
                        numberStringAndZeroPadTo(5).then(addPrefix("sortkey-"))),
                overrideField("value",
                        numberStringAndZeroPadTo(5).then(addPrefix("A value "))));

        // When/Then
        assertThat(GenerateNumberedRows.from(schema, overrides).iterableFrom(LongStream.of(1, 12345)))
                .containsExactly(
                        new Row(Map.of(
                                "rowkey", "rowkey-00001",
                                "sortkey", "sortkey-00001",
                                "value", "A value 00001")),
                        new Row(Map.of(
                                "rowkey", "rowkey-12345",
                                "sortkey", "sortkey-12345",
                                "value", "A value 12345")));
    }

    @Test
    void shouldIterateThroughRowsTwiceWithSameIterable() {
        // Given
        Schema schema = createSchemaWithKey("key", new IntType());
        Supplier<LongStream> numbers = () -> LongStream.rangeClosed(1, 3);

        // When
        Iterable<Row> iterable = GenerateNumberedRows.from(schema).iterableFrom(numbers);

        // Then
        List<Row> expected = List.of(
                new Row(Map.of("key", 1)),
                new Row(Map.of("key", 2)),
                new Row(Map.of("key", 3)));
        assertThat(iterable).containsExactlyElementsOf(expected);
        assertThat(iterable).containsExactlyElementsOf(expected);
    }
}
