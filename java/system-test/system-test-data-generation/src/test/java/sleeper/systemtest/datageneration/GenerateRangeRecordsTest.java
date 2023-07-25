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
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .build();

        assertThat(recordsForRange(schema, LongStream.rangeClosed(1, 2)))
                .containsExactly(
                        new Record(Map.of("key", "row-1")),
                        new Record(Map.of("key", "row-2")));
    }

    @Test
    void shouldGenerateTwoRecordsWithIntType() {
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .build();

        assertThat(recordsForRange(schema, LongStream.rangeClosed(1, 2)))
                .containsExactly(
                        new Record(Map.of("key", 1)),
                        new Record(Map.of("key", 2)));
    }

    @Test
    void shouldGenerateTwoRecordsWithLongType() {
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new LongType()))
                .build();

        assertThat(recordsForRange(schema, LongStream.rangeClosed(1, 2)))
                .containsExactly(
                        new Record(Map.of("key", 1L)),
                        new Record(Map.of("key", 2L)));
    }

    @Test
    void shouldGenerateTwoRecordsWithByteArrayType() {
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new ByteArrayType()))
                .build();

        assertThat(recordsForRange(schema, LongStream.rangeClosed(1, 2)))
                .containsExactly(
                        new Record(Map.of("key", new byte[]{1})),
                        new Record(Map.of("key", new byte[]{2})));
    }

    @Test
    void shouldGenerateTwoRecordsWithAllFieldTypes() {
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("rowKey", new StringType()))
                .sortKeyFields(new Field("sortKey", new StringType()))
                .valueFields(new Field("value", new StringType()))
                .build();

        assertThat(recordsForRange(schema, LongStream.rangeClosed(1, 2)))
                .containsExactly(
                        new Record(Map.of(
                                "rowKey", "row-1",
                                "sortKey", "sort-1",
                                "value", "Value 1")),
                        new Record(Map.of(
                                "rowKey", "row-2",
                                "sortKey", "sort-2",
                                "value", "Value 2")));
    }
}
