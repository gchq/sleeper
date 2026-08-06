/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.athena.record;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import org.apache.arrow.vector.types.Types;
import org.junit.jupiter.api.Test;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DataFusionSqlFactoryTest {

    private static final Schema SCHEMA = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .valueFields(
                    new Field("value1", new StringType()),
                    new Field("value2", new IntType()),
                    new Field("value3", new LongType()))
            .build();
    private final DataFusionSqlFactory factory = new DataFusionSqlFactory(SCHEMA);

    @Test
    void shouldReturnNullWhenNoConstraints() {
        // Given
        Map<String, ValueSet> constraints = Map.of();

        // When
        String sql = factory.toSql(constraints);

        // Then
        assertThat(sql).isNull();
    }

    @Test
    void shouldReturnNullWhenOnlyRowKeyConstrained() {
        // Given
        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("key", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.VARCHAR.getType(), true, false).add("a").build());

        // When
        String sql = factory.toSql(constraints);

        // Then
        assertThat(sql).isNull();
    }

    @Test
    void shouldReturnCorrectSqlForSingleValueEquality() {
        // Given
        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("value2", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.INT.getType(), true, false).add(5).build());

        // When
        String sql = factory.toSql(constraints);

        // Then
        assertThat(sql).isEqualTo("SELECT * FROM query_results WHERE \"value2\" = 5;");
    }

    @Test
    void shouldReturnCorrectSqlForList() {
        // Given
        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("value1", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.VARCHAR.getType(), true, false).add("a").add("b").build());

        // When
        String sql = factory.toSql(constraints);

        // Then
        assertThat(sql).isEqualTo("SELECT * FROM query_results WHERE \"value1\" IN ('a', 'b');");
    }

    @Test
    void shouldReturnCorrectSqlForListOfNots() {
        // Given
        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("value2", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.INT.getType(), false, false).add(1).add(2).build());

        // When
        String sql = factory.toSql(constraints);

        // Then
        assertThat(sql).isEqualTo("SELECT * FROM query_results WHERE \"value2\" NOT IN (1, 2);");
    }

    @Test
    void shouldReturnCorrectSqlForBoundedRange() {
        // Given
        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("value3", SortedRangeSet.of(Range.range(new BlockAllocatorImpl(),
                Types.MinorType.BIGINT.getType(), 4L, true, 9L, false)));

        // When
        String sql = factory.toSql(constraints);

        // Then
        assertThat(sql).isEqualTo("SELECT * FROM query_results WHERE (\"value3\" >= 4 AND \"value3\" < 9);");
    }

    @Test
    void shouldReturnCorrectSqlForRangeWithNoMax() {
        // Given
        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("value2", SortedRangeSet.of(Range.greaterThan(new BlockAllocatorImpl(),
                Types.MinorType.INT.getType(), 4)));

        // When
        String sql = factory.toSql(constraints);

        // Then
        assertThat(sql).isEqualTo("SELECT * FROM query_results WHERE \"value2\" > 4;");
    }

    @Test
    void shouldReturnCorrectSqlForDisjointRanges() {
        // Given
        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("value2", SortedRangeSet.of(
                Range.range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(), 1, true, 3, false),
                Range.range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(), 7, true, 9, false)));

        // When
        String sql = factory.toSql(constraints);

        // Then
        assertThat(sql).isEqualTo("SELECT * FROM query_results WHERE ((\"value2\" >= 1 AND \"value2\" < 3) "
                + "OR (\"value2\" >= 7 AND \"value2\" < 9));");
    }

    @Test
    void shouldReturnCorrectSqlForAndConditionsAcrossValueFields() {
        // Given
        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("value3", SortedRangeSet.of(Range.greaterThan(new BlockAllocatorImpl(),
                Types.MinorType.BIGINT.getType(), 10L)));
        constraints.put("value1", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.VARCHAR.getType(), true, false).add("x").build());

        // When
        String sql = factory.toSql(constraints);

        // Then
        assertThat(sql).isEqualTo("SELECT * FROM query_results WHERE \"value1\" = 'x' AND \"value3\" > 10;");
    }

    @Test
    void shouldReturnCorrectSqlWhenNullsAllowed() {
        // Given
        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("value1", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.VARCHAR.getType(), true, true).add("a").build());

        // When
        String sql = factory.toSql(constraints);

        // Then
        assertThat(sql).isEqualTo("SELECT * FROM query_results WHERE (\"value1\" = 'a' OR \"value1\" IS NULL);");
    }

    @Test
    void shouldReturnSqlWithEscapedSingleQuotesInStringLiterals() {
        // Given
        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("value1", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.VARCHAR.getType(), true, false).add("O'P").build());

        // When
        String sql = factory.toSql(constraints);

        // Then
        assertThat(sql).isEqualTo("SELECT * FROM query_results WHERE \"value1\" = 'O''P';");
    }
}
