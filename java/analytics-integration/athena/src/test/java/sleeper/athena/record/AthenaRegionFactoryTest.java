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

import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AthenaRegionFactoryTest {

    private static final Schema SCHEMA = Schema.builder()
            .rowKeyFields(
                    new Field("key1", new StringType()),
                    new Field("key2", new IntType()),
                    new Field("key3", new LongType()))
            .valueFields(new Field("value", new StringType()))
            .build();
    private final AthenaRegionFactory factory = new AthenaRegionFactory(SCHEMA);
    private final Region partitionRegion = factory.partitionRegion(
            List.of("a", 0, 0L), List.of("d", 100, 100L));

    @Test
    void shouldBuildPartitionRegionFromDecodedBounds() {
        // When
        Region region = factory.partitionRegion(List.of("a", 0, 0L), List.of("d", 100, 100L));

        // Then
        assertThat(region.getRange("key1").getMin()).isEqualTo("a");
        assertThat(region.getRange("key1").isMinInclusive()).isTrue();
        assertThat(region.getRange("key1").getMax()).isEqualTo("d");
        assertThat(region.getRange("key1").isMaxInclusive()).isFalse();
        assertThat(region.getRange("key2").getMin()).isEqualTo(0);
        assertThat(region.getRange("key2").isMinInclusive()).isTrue();
        assertThat(region.getRange("key2").getMax()).isEqualTo(100);
        assertThat(region.getRange("key2").isMaxInclusive()).isFalse();
        assertThat(region.getRange("key3").getMin()).isEqualTo(0L);
        assertThat(region.getRange("key3").isMinInclusive()).isTrue();
        assertThat(region.getRange("key3").getMax()).isEqualTo(100L);
        assertThat(region.getRange("key3").isMaxInclusive()).isFalse();
    }

    @Test
    void shouldBuildPartitionRegionWithUnboundedMaxWhenMaxIsNull() {
        // When
        Region region = factory.partitionRegion(List.of("a", 0, 0L), java.util.Arrays.asList("d", 100, null));

        // Then
        assertThat(region.getRange("key1").getMin()).isEqualTo("a");
        assertThat(region.getRange("key1").isMinInclusive()).isTrue();
        assertThat(region.getRange("key1").getMax()).isEqualTo("d");
        assertThat(region.getRange("key1").isMaxInclusive()).isFalse();
        assertThat(region.getRange("key2").getMin()).isEqualTo(0);
        assertThat(region.getRange("key2").isMinInclusive()).isTrue();
        assertThat(region.getRange("key2").getMax()).isEqualTo(100);
        assertThat(region.getRange("key2").isMaxInclusive()).isFalse();
        assertThat(region.getRange("key3").getMin()).isEqualTo(0L);
        assertThat(region.getRange("key3").isMinInclusive()).isTrue();
        assertThat(region.getRange("key3").getMax()).isNull();
        assertThat(region.getRange("key3").isMaxInclusive()).isFalse();
    }

    @Test
    void shouldReturnPartitionRegionWhenNoRowKeyConstraints() {
        // When
        List<Region> regions = factory.queryRegions(Map.of(), partitionRegion);

        // Then
        assertThat(regions).containsExactly(partitionRegion);
    }

    @Test
    void shouldTranslateRowKeyRangeConstraintToRegion() {
        // Given
        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("key2", SortedRangeSet.of(Range.range(new BlockAllocatorImpl(),
                Types.MinorType.INT.getType(), 1, true, 3, false)));

        // When
        List<Region> regions = factory.queryRegions(constraints, partitionRegion);

        // Then
        assertThat(regions).hasSize(1);
        Region region = regions.get(0);
        assertThat(region.getRange("key2").getMin()).isEqualTo(1);
        assertThat(region.getRange("key2").isMinInclusive()).isTrue();
        assertThat(region.getRange("key2").getMax()).isEqualTo(3);
        assertThat(region.getRange("key2").isMaxInclusive()).isFalse();
        // Unconstrained dimensions cover all values (open upper bound)
        assertThat(region.getRange("key1").getMin()).isEqualTo("");
        assertThat(region.getRange("key1").isMinInclusive()).isTrue();
        assertThat(region.getRange("key1").getMax()).isNull();
        assertThat(region.getRange("key3").getMin()).isEqualTo(Long.MIN_VALUE);
        assertThat(region.getRange("key3").isMinInclusive()).isTrue();
        assertThat(region.getRange("key3").getMax()).isNull();
    }

    @Test
    void shouldTranslateRowKeyRangeWithNoMaxConstraint() {
        // Given
        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("key2", SortedRangeSet.of(Range.greaterThanOrEqual(new BlockAllocatorImpl(),
                Types.MinorType.INT.getType(), 15)));

        // When
        Region region = factory.queryRegions(constraints, partitionRegion).get(0);

        // Then
        assertThat(region.getRange("key2").getMin()).isEqualTo(15);
        assertThat(region.getRange("key2").isMinInclusive()).isTrue();
        assertThat(region.getRange("key2").getMax()).isNull();
        assertThat(region.getRange("key1").getMin()).isEqualTo("");
        assertThat(region.getRange("key1").isMinInclusive()).isTrue();
        assertThat(region.getRange("key1").getMax()).isNull();
        assertThat(region.getRange("key3").getMin()).isEqualTo(Long.MIN_VALUE);
        assertThat(region.getRange("key3").isMinInclusive()).isTrue();
        assertThat(region.getRange("key3").getMax()).isNull();
        assertThat(region.getRange("key3").isMaxInclusive()).isFalse();
    }

    @Test
    void shouldTranslateSingleValueEqualityToExactRange() {
        // Given
        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("key1", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.VARCHAR.getType(), true, false).add("foo").build());

        // When
        Region region = factory.queryRegions(constraints, partitionRegion).get(0);

        // Then
        assertThat(region.getRange("key1").getMin()).isEqualTo("foo");
        assertThat(region.getRange("key1").isMinInclusive()).isTrue();
        assertThat(region.getRange("key1").getMax()).isEqualTo("foo");
        assertThat(region.getRange("key1").isMaxInclusive()).isTrue();
    }

    @Test
    void shouldFallBackToPartitionRegionForMultiValueEqualityOnNonFirstRowKey() {
        // Given
        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("key2", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.INT.getType(), true, false).add(1).add(5).build());

        // When
        List<Region> regions = factory.queryRegions(constraints, partitionRegion);

        // Then
        assertThat(regions).containsExactly(partitionRegion);
    }

    @Test
    void shouldExpandFirstRowKeyInListIntoOneRegionPerValue() {
        // Given
        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("key1", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.VARCHAR.getType(), true, false).add("b").add("c").build());

        // When
        List<Region> regions = factory.queryRegions(constraints, partitionRegion);

        // Then one region per value, each an exact range on key1 with the other dimensions unconstrained
        assertThat(regions).hasSize(2);
        assertThat(regions).allSatisfy(region -> {
            assertThat(region.getRange("key1").getMin()).isEqualTo(region.getRange("key1").getMax());
            assertThat(region.getRange("key1").isMinInclusive()).isTrue();
            assertThat(region.getRange("key1").isMaxInclusive()).isTrue();
            assertThat(region.getRange("key2").getMax()).isNull();
            assertThat(region.getRange("key3").getMax()).isNull();
        });
        assertThat(regions).extracting(region -> region.getRange("key1").getMin())
                .containsExactlyInAnyOrder("b", "c");
    }

    @Test
    void shouldExpandFirstRowKeyDisjointRangesIntoOneRegionPerRange() {
        // Given
        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("key1", SortedRangeSet.of(
                Range.range(new BlockAllocatorImpl(), Types.MinorType.VARCHAR.getType(), "a1", true, "a9", false),
                Range.range(new BlockAllocatorImpl(), Types.MinorType.VARCHAR.getType(), "c1", true, "c4", false)));

        // When
        List<Region> regions = factory.queryRegions(constraints, partitionRegion);

        // Then one region per disjoint range
        assertThat(regions).hasSize(2);
        assertThat(regions).extracting(region -> region.getRange("key1").getMin())
                .containsExactlyInAnyOrder("a1", "c1");
        assertThat(regions).extracting(region -> region.getRange("key1").getMax())
                .containsExactlyInAnyOrder("a9", "c4");
    }

    @Test
    void shouldConstrainOtherRowKeysWithinEachExpandedFirstKeyRegion() {
        // Given an IN list on the first row key and a range on a later row key
        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("key1", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.VARCHAR.getType(), true, false).add("b").add("c").build());
        constraints.put("key2", SortedRangeSet.of(Range.range(new BlockAllocatorImpl(),
                Types.MinorType.INT.getType(), 1, true, 3, false)));

        // When
        List<Region> regions = factory.queryRegions(constraints, partitionRegion);

        // Then
        assertThat(regions).hasSize(2);
        assertThat(regions).allSatisfy(region -> {
            assertThat(region.getRange("key2").getMin()).isEqualTo(1);
            assertThat(region.getRange("key2").getMax()).isEqualTo(3);
        });
    }

    @Test
    void shouldIgnoreValueFieldConstraints() {
        // Given
        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("value", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.VARCHAR.getType(), true, false).add("x").build());

        // When
        List<Region> regions = factory.queryRegions(constraints, partitionRegion);

        // Then row-key regions are unaffected
        assertThat(regions).containsExactly(partitionRegion);
    }
}
