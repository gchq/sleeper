/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.query.core.model;

import org.junit.jupiter.api.Test;

import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryTest {

    @Test
    public void testEqualsAndHashcode() {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range1 = rangeFactory.createRange(field, 1L, true, 10L, true);
        Range range2 = rangeFactory.createRange(field, 1L, true, 10L, true);
        Range range3 = rangeFactory.createRange(field, 1L, true, 100L, true);
        Region region1 = new Region(range1);
        Region region2 = new Region(range2);
        Region region3 = new Region(range3);
        String tableName = UUID.randomUUID().toString();
        Query query1 = Query.builder()
                .tableName(tableName)
                .queryId("A")
                .regions(List.of(region1))
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeIteratorClassName("iteratorClassName")
                        .queryTimeIteratorConfig("iteratorConfig")
                        .build())
                .build();
        Query query2 = Query.builder()
                .tableName(tableName)
                .queryId("A")
                .regions(List.of(region2))
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeIteratorClassName("iteratorClassName")
                        .queryTimeIteratorConfig("iteratorConfig")
                        .build())
                .build();
        Query query3 = Query.builder()
                .tableName(tableName)
                .queryId("B")
                .regions(List.of(region2))
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeIteratorClassName("iteratorClassName")
                        .queryTimeIteratorConfig("iteratorConfig")
                        .build())
                .build();
        Query query4 = Query.builder()
                .tableName(tableName)
                .queryId("A")
                .regions(List.of(region3))
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeIteratorClassName("iteratorClassName")
                        .queryTimeIteratorConfig("iteratorConfig")
                        .build())
                .build();

        // When
        boolean test1 = query1.equals(query2);
        boolean test2 = query1.equals(query3);
        boolean test3 = query1.equals(query4);
        int hashCode1 = query1.hashCode();
        int hashCode2 = query2.hashCode();
        int hashCode3 = query3.hashCode();
        int hashCode4 = query4.hashCode();

        // Then
        assertThat(test1).isTrue();
        assertThat(test2).isFalse();
        assertThat(test3).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
        assertThat(hashCode4).isNotEqualTo(hashCode1);
    }

    @Test
    public void testEqualsAndHashcodeWithNullIterator() {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range = rangeFactory.createRange(field, 1L, true, 10L, true);
        Region region = new Region(range);
        String tableName = UUID.randomUUID().toString();
        Query query1 = Query.builder()
                .tableName(tableName)
                .queryId("A")
                .regions(List.of(region))
                .build();
        Query query2 = Query.builder()
                .tableName(tableName)
                .queryId("A")
                .regions(List.of(region))
                .build();
        Query query3 = Query.builder()
                .tableName(tableName)
                .queryId("B")
                .regions(List.of(region))
                .build();

        // When
        boolean test1 = query1.equals(query2);
        boolean test2 = query1.equals(query3);
        int hashCode1 = query1.hashCode();
        int hashCode2 = query2.hashCode();
        int hashCode3 = query3.hashCode();

        // Then
        assertThat(test1).isTrue();
        assertThat(test2).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
    }

    @Test
    public void testEqualsAndHashcodeWithMultipleRanges() {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range1 = rangeFactory.createRange(field, 1L, true, 10L, true);
        Range range2 = rangeFactory.createRange(field, 100L, true, 200L, true);
        Region region1 = new Region(range1);
        Region region2 = new Region(range2);
        String tableName = UUID.randomUUID().toString();
        Query query1 = Query.builder()
                .tableName(tableName)
                .queryId("A")
                .regions(List.of(region1, region2))
                .build();
        Query query2 = Query.builder()
                .tableName(tableName)
                .queryId("A")
                .regions(List.of(region1, region2))
                .build();
        Query query3 = Query.builder()
                .tableName(tableName)
                .queryId("A")
                .regions(List.of(region1))
                .build();

        // When
        boolean test1 = query1.equals(query2);
        boolean test2 = query1.equals(query3);
        int hashCode1 = query1.hashCode();
        int hashCode2 = query2.hashCode();
        int hashCode3 = query3.hashCode();

        // Then
        assertThat(test1).isTrue();
        assertThat(test2).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
    }

    @Test
    public void testEqualsAndHashcodeWithByteArray() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range1 = rangeFactory.createRange(field, new byte[]{10}, true, new byte[]{20}, true);
        Range range2 = rangeFactory.createRange(field, new byte[]{11}, true, new byte[]{20}, true);
        Region region1 = new Region(range1);
        Region region2 = new Region(range2);
        String tableName = UUID.randomUUID().toString();
        Query query1 = Query.builder()
                .tableName(tableName)
                .queryId("A")
                .regions(List.of(region1))
                .build();
        Query query2 = Query.builder()
                .tableName(tableName)
                .queryId("A")
                .regions(List.of(region1))
                .build();
        Query query3 = Query.builder()
                .tableName(tableName)
                .queryId("A")
                .regions(List.of(region2))
                .build();

        // When
        boolean test1 = query1.equals(query2);
        boolean test2 = query1.equals(query3);
        int hashCode1 = query1.hashCode();
        int hashCode2 = query2.hashCode();
        int hashCode3 = query3.hashCode();

        // Then
        assertThat(test1).isTrue();
        assertThat(test2).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
    }
}
