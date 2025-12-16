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
package sleeper.foreign;

import jnr.ffi.Runtime;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class FFISleeperRegionTest {
    jnr.ffi.Runtime runtime = Runtime.getSystemRuntime();

    @Test
    void shouldThrowOnRegionValidateMaxsIncorrectLength() {
        // Given
        FFISleeperRegion region = new FFISleeperRegion(runtime);
        region.mins.populate(new Integer[]{1, 2, 3, 4, 5}, false);
        region.maxs.populate(new Integer[]{1, 2, 3, 4}, false);
        region.mins_inclusive.populate(new Boolean[]{false, false, false, false, false}, false);
        region.maxs_inclusive.populate(new Boolean[]{false, false, false, false, false}, false);
        region.dimension_indexes.populate(new Integer[]{1, 2, 3, 4, 5}, false);

        // When / Then
        assertThatIllegalStateException()
                .isThrownBy(() -> region.validate())
                .withMessage("region maxs has length 4 but there are 5 row keys in region");
    }

    @Test
    void shouldThrowOnRegionValidateMinsInclusiveIncorrectLength() {
        // Given
        FFISleeperRegion region = new FFISleeperRegion(runtime);
        region.mins.populate(new Integer[]{1, 2, 3, 4, 5}, false);
        region.maxs.populate(new Integer[]{1, 2, 3, 4, 5}, false);
        region.mins_inclusive.populate(new Boolean[]{false, false, false, false}, false);
        region.maxs_inclusive.populate(new Boolean[]{false, false, false, false, false}, false);

        // When / Then
        assertThatIllegalStateException()
                .isThrownBy(() -> region.validate())
                .withMessage("region mins inclusive has length 4 but there are 5 row keys in region");
    }

    @Test
    void shouldThrowOnRegionValidateMaxsInclusiveIncorrectLength() {
        // Given
        FFISleeperRegion region = new FFISleeperRegion(runtime);
        region.mins.populate(new Integer[]{1, 2, 3, 4, 5}, false);
        region.maxs.populate(new Integer[]{1, 2, 3, 4, 5}, false);
        region.mins_inclusive.populate(new Boolean[]{false, false, false, false, false}, false);
        region.maxs_inclusive.populate(new Boolean[]{false, false, false, false}, false);
        region.dimension_indexes.populate(new Integer[]{1, 2, 3, 4, 5}, false);

        // When / Then
        assertThatIllegalStateException()
                .isThrownBy(() -> region.validate())
                .withMessage("region maxs inclusive has length 4 but there are 5 row keys in region");
    }

    @Test
    void shouldThrowOnRegionValidateDimensionIndexesIncorrectLength() {
        // Given
        FFISleeperRegion region = new FFISleeperRegion(runtime);
        region.mins.populate(new Integer[]{1, 2, 3, 4, 5}, false);
        region.maxs.populate(new Integer[]{1, 2, 3, 4, 5}, false);
        region.mins_inclusive.populate(new Boolean[]{false, false, false, false, false}, false);
        region.maxs_inclusive.populate(new Boolean[]{false, false, false, false, false}, false);
        region.dimension_indexes.populate(new Integer[]{1, 2, 3, 4}, false);

        // When / Then
        assertThatIllegalStateException()
                .isThrownBy(() -> region.validate())
                .withMessage("region dimension indexes has length 4 but there are 5 row keys in region");
    }

    @Test
    void shouldValidate() {
        // Given
        FFISleeperRegion region = new FFISleeperRegion(runtime);
        region.mins.populate(new Integer[]{1, 2, 3, 4, 5}, false);
        region.maxs.populate(new Integer[]{1, 2, 3, 4, 5}, false);
        region.mins_inclusive.populate(new Boolean[]{false, false, false, false, false}, false);
        region.maxs_inclusive.populate(new Boolean[]{false, false, false, false, false}, false);
        region.dimension_indexes.populate(new Integer[]{1, 2, 3, 4, 5}, false);

        // When / Then
        assertThatCode(() -> region.validate())
                .doesNotThrowAnyException();
    }

    @Nested
    @DisplayName("Map to and from a Sleeper region")
    class MapToFromSleeperRegion {

        @Test
        void shouldMapOneLongKey() {
            // Given
            Schema schema = createSchemaWithKey("key", new LongType());
            Region region = new Region(new RangeFactory(schema).createRange("key", 0L, true, 100L, false));

            // When
            FFISleeperRegion ffiRegion = FFISleeperRegion.from(region, schema, runtime);
            Region found = ffiRegion.toSleeperRegion(schema);

            // Then
            assertThat(found).isEqualTo(region);
        }

        @Test
        void shouldMapOneIntKey() {
            // Given
            Schema schema = createSchemaWithKey("key", new IntType());
            Region region = new Region(new RangeFactory(schema).createRange("key", 0, true, 100, false));

            // When
            FFISleeperRegion ffiRegion = FFISleeperRegion.from(region, schema, runtime);
            Region found = ffiRegion.toSleeperRegion(schema);

            // Then
            assertThat(found).isEqualTo(region);
        }

        @Test
        void shouldMapOneStringKey() {
            // Given
            Schema schema = createSchemaWithKey("key", new StringType());
            Region region = new Region(new RangeFactory(schema).createRange("key", "a", true, "z", false));

            // When
            FFISleeperRegion ffiRegion = FFISleeperRegion.from(region, schema, runtime);
            Region found = ffiRegion.toSleeperRegion(schema);

            // Then
            assertThat(found).isEqualTo(region);
        }

        @Test
        void shouldMapOneByteArrayKey() {
            // Given
            Schema schema = createSchemaWithKey("key", new ByteArrayType());
            Region region = new Region(new RangeFactory(schema).createRange("key", new byte[]{1, 2}, true, new byte[]{3, 4}, false));

            // When
            FFISleeperRegion ffiRegion = FFISleeperRegion.from(region, schema, runtime);
            Region found = ffiRegion.toSleeperRegion(schema);

            // Then
            assertThat(found).isEqualTo(region);
        }

        @Test
        void shouldMapMinAndMaxInclusive() {
            // Given
            Schema schema = createSchemaWithKey("key", new IntType());
            Region region = new Region(new RangeFactory(schema).createRange("key", 0, true, 100, true));

            // When
            FFISleeperRegion ffiRegion = FFISleeperRegion.from(region, schema, runtime);
            Region found = ffiRegion.toSleeperRegion(schema);

            // Then
            assertThat(found).isEqualTo(region);
        }

        @Test
        void shouldMapMinAndMaxExclusive() {
            // Given
            Schema schema = createSchemaWithKey("key", new IntType());
            Region region = new Region(new RangeFactory(schema).createRange("key", 0, false, 100, false));

            // When
            FFISleeperRegion ffiRegion = FFISleeperRegion.from(region, schema, runtime);
            Region found = ffiRegion.toSleeperRegion(schema);

            // Then
            assertThat(found).isEqualTo(region);
        }

        @Test
        void shouldMapNullMax() {
            // Given
            Schema schema = createSchemaWithKey("key", new StringType());
            Region region = new Region(new RangeFactory(schema).createRange("key", "", true, null, false));

            // When
            FFISleeperRegion ffiRegion = FFISleeperRegion.from(region, schema, runtime);
            Region found = ffiRegion.toSleeperRegion(schema);

            // Then
            assertThat(found).isEqualTo(region);
        }

        @Test
        void shouldMapMultidimensionalKeyWithAllValues() {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(
                            new Field("key1", new IntType()),
                            new Field("key2", new IntType()))
                    .build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Region region = new Region(List.of(
                    rangeFactory.createRange("key1", 1, true, 10, false),
                    rangeFactory.createRange("key2", 20, false, 50, true)));

            // When
            FFISleeperRegion ffiRegion = FFISleeperRegion.from(region, schema, runtime);
            Region found = ffiRegion.toSleeperRegion(schema);

            // Then
            assertThat(found).isEqualTo(region);
        }

        @Test
        void shouldMapMultidimensionalKeyWithOnlyMiddleValue() {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(
                            new Field("key1", new IntType()),
                            new Field("key2", new IntType()),
                            new Field("key3", new IntType()))
                    .build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Region region = new Region(List.of(
                    rangeFactory.createRange("key2", 1, true, 10, false)));

            // When
            FFISleeperRegion ffiRegion = FFISleeperRegion.from(region, schema, runtime);
            Region found = ffiRegion.toSleeperRegion(schema);

            // Then
            assertThat(found).isEqualTo(region);
        }
    }
}
