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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

public class FFISleeperRegionTest {

    @Test
    void shouldThrowOnRegionValidateMaxsIncorrectLength() {
        // Given
        jnr.ffi.Runtime runtime = Runtime.getSystemRuntime();
        FFISleeperRegion region = new FFISleeperRegion(runtime);
        Integer[] minBounds = new Integer[]{1, 2, 3, 4, 5};
        Integer[] maxBounds = new Integer[]{1, 2, 3, 4};
        region.mins.populate(minBounds, false);
        region.maxs.populate(maxBounds, false);
        Boolean[] minbBoolBounds = new Boolean[]{false, false, false, false, false};
        Boolean[] maxBoolBounds = new Boolean[]{false, false, false, false, false};
        region.mins_inclusive.populate(minbBoolBounds, false);
        region.maxs_inclusive.populate(maxBoolBounds, false);
        Integer[] dimensions = new Integer[]{0, 1, 2, 3, 4};
        region.region_dimensions.populate(dimensions, false);

        // Then
        assertThatIllegalStateException()
                .isThrownBy(() -> region.validate())
                .withMessage("region maxs has length 4 but there are 5 row key columns in region");
    }

    @Test
    void shouldThrowOnRegionValidateMinsInclusiveIncorrectLength() {
        // Given
        jnr.ffi.Runtime runtime = Runtime.getSystemRuntime();
        FFISleeperRegion region = new FFISleeperRegion(runtime);
        Integer[] minBounds = new Integer[]{1, 2, 3, 4, 5};
        Integer[] maxBounds = new Integer[]{1, 2, 3, 4, 5};
        region.mins.populate(minBounds, false);
        region.maxs.populate(maxBounds, false);
        Boolean[] minbBoolBounds = new Boolean[]{false, false, false, false};
        Boolean[] maxBoolBounds = new Boolean[]{false, false, false, false, false};
        region.mins_inclusive.populate(minbBoolBounds, false);
        region.maxs_inclusive.populate(maxBoolBounds, false);
        Integer[] dimensions = new Integer[]{0, 1, 2, 3, 4};
        region.region_dimensions.populate(dimensions, false);

        // Then
        assertThatIllegalStateException()
                .isThrownBy(() -> region.validate())
                .withMessage("region mins inclusive has length 4 but there are 5 row key columns in region");
    }

    @Test
    void shouldThrowOnRegionValidateMaxsInclusiveIncorrectLength() {
        // Given
        jnr.ffi.Runtime runtime = Runtime.getSystemRuntime();
        FFISleeperRegion region = new FFISleeperRegion(runtime);
        Integer[] minBounds = new Integer[]{1, 2, 3, 4, 5};
        Integer[] maxBounds = new Integer[]{1, 2, 3, 4, 5};
        region.mins.populate(minBounds, false);
        region.maxs.populate(maxBounds, false);
        Boolean[] minbBoolBounds = new Boolean[]{false, false, false, false, false};
        Boolean[] maxBoolBounds = new Boolean[]{false, false, false, false};
        region.mins_inclusive.populate(minbBoolBounds, false);
        region.maxs_inclusive.populate(maxBoolBounds, false);
        Integer[] dimensions = new Integer[]{0, 1, 2, 3, 4};
        region.region_dimensions.populate(dimensions, false);

        // Then
        assertThatIllegalStateException()
                .isThrownBy(() -> region.validate())
                .withMessage("region maxs inclusive has length 4 but there are 5 row key columns in region");
    }

    @Test
    void shouldThrowOnRegionValidateDimensionsIncorrectLength() {
        // Given
        jnr.ffi.Runtime runtime = Runtime.getSystemRuntime();
        FFISleeperRegion region = new FFISleeperRegion(runtime);
        Integer[] minBounds = new Integer[]{1, 2, 3, 4, 5};
        Integer[] maxBounds = new Integer[]{1, 2, 3, 4, 5};
        region.mins.populate(minBounds, false);
        region.maxs.populate(maxBounds, false);
        Boolean[] minbBoolBounds = new Boolean[]{false, false, false, false, false};
        Boolean[] maxBoolBounds = new Boolean[]{false, false, false, false, false};
        region.mins_inclusive.populate(minbBoolBounds, false);
        region.maxs_inclusive.populate(maxBoolBounds, false);
        Integer[] dimensions = new Integer[]{0, 1, 2, 3};
        region.region_dimensions.populate(dimensions, false);

        // Then
        assertThatIllegalStateException()
                .isThrownBy(() -> region.validate())
                .withMessage("region dimensions has length 4 but there are 5 row key columns in region");
    }

    @Test
    void shouldRegionValidateCorrectly() {
        // Given
        jnr.ffi.Runtime runtime = Runtime.getSystemRuntime();
        FFISleeperRegion region = new FFISleeperRegion(runtime);
        Integer[] minBounds = new Integer[]{1, 2, 3, 4, 5};
        Integer[] maxBounds = new Integer[]{1, 2, 3, 4, 5};
        region.mins.populate(minBounds, false);
        region.maxs.populate(maxBounds, false);
        Boolean[] minbBoolBounds = new Boolean[]{false, false, false, false, false};
        Boolean[] maxBoolBounds = new Boolean[]{false, false, false, false, false};
        region.mins_inclusive.populate(minbBoolBounds, false);
        region.maxs_inclusive.populate(maxBoolBounds, false);
        Integer[] dimensions = new Integer[]{0, 1, 2, 3, 4};
        region.region_dimensions.populate(dimensions, false);

        // Then
        region.validate();
    }
}
