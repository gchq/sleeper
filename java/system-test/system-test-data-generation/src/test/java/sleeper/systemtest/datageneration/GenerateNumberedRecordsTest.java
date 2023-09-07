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

import org.junit.jupiter.api.Test;

import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.datageneration.GenerateNumberedRecords.PartialConfiguration.overrideField;
import static sleeper.systemtest.datageneration.GenerateNumberedRecords.PartialConfiguration.overrideKeyAndFieldType;
import static sleeper.systemtest.datageneration.GenerateNumberedRecords.PartialConfiguration.overrides;
import static sleeper.systemtest.datageneration.GenerateNumberedValue.stringFromPrefixAndPadToSize;
import static sleeper.systemtest.datageneration.KeyType.ROW;
import static sleeper.systemtest.datageneration.KeyType.SORT;
import static sleeper.systemtest.datageneration.KeyType.VALUE;

public class GenerateNumberedRecordsTest {
    @Test
    void shouldGenerateTwoRecordsWithStringType() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("rowkey", new StringType()))
                .sortKeyFields(new Field("sortkey", new StringType()))
                .valueFields(new Field("value", new StringType()))
                .build();

        // When/Then
        assertThat(GenerateNumberedRecords.from(schema, LongStream.of(1, Long.MAX_VALUE)))
                .containsExactly(
                        new Record(Map.of(
                                "rowkey", "row-0000000000000000001",
                                "sortkey", "sort-0000000000000000001",
                                "value", "Value 0000000000000000001")),
                        new Record(Map.of(
                                "rowkey", "row-9223372036854775807",
                                "sortkey", "sort-9223372036854775807",
                                "value", "Value 9223372036854775807")));
    }

    @Test
    void shouldGenerateTwoRecordsWithIntType() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("rowkey", new IntType()))
                .sortKeyFields(new Field("sortkey", new IntType()))
                .valueFields(new Field("value", new IntType()))
                .build();

        // When/Then
        assertThat(GenerateNumberedRecords.from(schema, LongStream.rangeClosed(1, 2)))
                .containsExactly(
                        new Record(Map.of(
                                "rowkey", 1,
                                "sortkey", 1,
                                "value", 1)),
                        new Record(Map.of(
                                "rowkey", 2,
                                "sortkey", 2,
                                "value", 2)));
    }

    @Test
    void shouldGenerateTwoRecordsWithLongType() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("rowkey", new LongType()))
                .sortKeyFields(new Field("sortkey", new LongType()))
                .valueFields(new Field("value", new LongType()))
                .build();

        // When/Then
        assertThat(GenerateNumberedRecords.from(schema, LongStream.rangeClosed(1, 2)))
                .containsExactly(
                        new Record(Map.of(
                                "rowkey", 1L,
                                "sortkey", 1L,
                                "value", 1L)),
                        new Record(Map.of(
                                "rowkey", 2L,
                                "sortkey", 2L,
                                "value", 2L)));
    }

    @Test
    void shouldGenerateTwoRecordsWithByteArrayType() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("rowkey", new ByteArrayType()))
                .sortKeyFields(new Field("sortkey", new ByteArrayType()))
                .valueFields(new Field("value", new ByteArrayType()))
                .build();

        // When/Then
        assertThat(GenerateNumberedRecords.from(schema, LongStream.rangeClosed(1, 2)))
                .containsExactly(
                        new Record(Map.of(
                                "rowkey", new byte[]{0, 0, 0, 0, 0, 0, 0, 1},
                                "sortkey", new byte[]{0, 0, 0, 0, 0, 0, 0, 1},
                                "value", new byte[]{0, 0, 0, 0, 0, 0, 0, 1})),
                        new Record(Map.of(
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
        GenerateNumberedRecords generator = new GenerateNumberedRecords(schema,
                overrideKeyAndFieldType(ROW, StringType.class,
                        stringFromPrefixAndPadToSize("customrow-", 3)));

        // When/Then
        assertThat(generator.generate(LongStream.of(1, 999)))
                .containsExactly(
                        new Record(Map.of(
                                "rowkey", "customrow-001",
                                "sortkey", "sort-0000000000000000001",
                                "value", "Value 0000000000000000001")),
                        new Record(Map.of(
                                "rowkey", "customrow-999",
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
        GenerateNumberedRecords generator = new GenerateNumberedRecords(schema, overrides(
                overrideKeyAndFieldType(ROW, StringType.class,
                        stringFromPrefixAndPadToSize("customrow-", 3)),
                overrideKeyAndFieldType(SORT, StringType.class,
                        stringFromPrefixAndPadToSize("customsort-", 3)),
                overrideKeyAndFieldType(VALUE, StringType.class,
                        stringFromPrefixAndPadToSize("Custom value ", 3))));

        // When/Then
        assertThat(generator.generate(LongStream.of(1, 999)))
                .containsExactly(
                        new Record(Map.of(
                                "rowkey", "customrow-001",
                                "sortkey", "customsort-001",
                                "value", "Custom value 001")),
                        new Record(Map.of(
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
        GenerateNumberedRecords generator = new GenerateNumberedRecords(schema, overrides(
                overrideField("rowkey",
                        stringFromPrefixAndPadToSize("rowkey-", 5)),
                overrideField("sortkey",
                        stringFromPrefixAndPadToSize("sortkey-", 5)),
                overrideField("value",
                        stringFromPrefixAndPadToSize("A value ", 5))));

        // When/Then
        assertThat(generator.generate(LongStream.of(1, 12345)))
                .containsExactly(
                        new Record(Map.of(
                                "rowkey", "rowkey-00001",
                                "sortkey", "sortkey-00001",
                                "value", "A value 00001")),
                        new Record(Map.of(
                                "rowkey", "rowkey-12345",
                                "sortkey", "sortkey-12345",
                                "value", "A value 12345")));
    }
}
