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
package sleeper.query.datafusion;

import jnr.ffi.Runtime;
import org.junit.jupiter.api.Test;

import sleeper.foreign.FFIBytes;
import sleeper.foreign.FFISleeperRegion;

import static org.assertj.core.api.Assertions.assertThat;

class FFILeafPartitionQueryConfigTest {

    @Test
    void shouldConstructWithoutExceptions() {
        // Given
        Runtime runtime = Runtime.getSystemRuntime();

        // When
        FFILeafPartitionQueryConfig config = new FFILeafPartitionQueryConfig(runtime);

        // Then
        assertThat(config.common).isNotNull();
        assertThat(config.query_region_len.get()).isEqualTo(0);
        assertThat(config.query_regions).isNotNull();
        assertThat(config.requested_value_fields_set.get()).isFalse();
        assertThat(config.requested_value_fields_len.get()).isEqualTo(0);
        assertThat(config.requested_value_fields).isNotNull();
        assertThat(config.explain_plans.get()).isFalse();
    }

    @Test
    void shouldSetQueryRegionsWhenValidRegionsGiven() {
        // Given
        Runtime runtime = Runtime.getSystemRuntime();
        FFILeafPartitionQueryConfig config = new FFILeafPartitionQueryConfig(runtime);
        FFISleeperRegion region1 = new FFISleeperRegion(runtime);
        FFISleeperRegion region2 = new FFISleeperRegion(runtime);

        // When
        config.setQueryRegions(new FFISleeperRegion[]{region1, region2});

        // Then
        assertThat(config.query_region_len.get()).isEqualTo(2);
    }

    @Test
    void shouldSetRequestedValueFieldsWhenValidFieldssGiven() {
        // Given
        Runtime runtime = Runtime.getSystemRuntime();
        FFILeafPartitionQueryConfig config = new FFILeafPartitionQueryConfig(runtime);
        FFIBytes field1 = new FFIBytes(runtime, new byte[]{65, 66, 67});
        FFIBytes field2 = new FFIBytes(runtime, new byte[]{68, 69, 70});

        // When
        config.setRequestedValueFields(new FFIBytes[]{field1, field2});

        // Then
        assertThat(config.requested_value_fields_len.get()).isEqualTo(2);
    }

}
