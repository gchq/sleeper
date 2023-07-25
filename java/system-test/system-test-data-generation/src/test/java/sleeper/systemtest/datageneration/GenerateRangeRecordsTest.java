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
import static sleeper.systemtest.datageneration.GenerateRangeRecords.recordsForRange;

public class GenerateRangeRecordsTest {
    @Test
    void shouldGenerateTwoRecordsWithStringType() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("rowkey", new StringType()))
                .sortKeyFields(new Field("sortkey", new StringType()))
                .valueFields(new Field("value", new StringType()))
                .build();

        // When/Then
        assertThat(recordsForRange(schema, LongStream.of(1, Long.MAX_VALUE)))
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
        assertThat(recordsForRange(schema, LongStream.rangeClosed(1, 2)))
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
        assertThat(recordsForRange(schema, LongStream.rangeClosed(1, 2)))
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
        assertThat(recordsForRange(schema, LongStream.rangeClosed(1, 2)))
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
}
