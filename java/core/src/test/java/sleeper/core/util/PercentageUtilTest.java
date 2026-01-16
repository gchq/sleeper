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
package sleeper.core.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PercentageUtilTest {

    @Test
    void shouldGetPercentageOfLongValueRoundingUp() {
        // When / Then
        // 60% of 6 is 3.6, rounding up to 4
        assertThat(PercentageUtil.getCeilPercent(6L, 60))
                .isEqualTo(4L);
    }

    @Test
    void shouldGetExactPercentageOfLongValueWhenRoundingUp() {
        // When / Then
        assertThat(PercentageUtil.getCeilPercent(6L, 50))
                .isEqualTo(3L);
    }

    @Test
    void shouldGetZeroPercentOfLongValueWhenRoundingUp() {
        // When / Then
        assertThat(PercentageUtil.getCeilPercent(6L, 0))
                .isEqualTo(0L);
    }

    @Test
    void shouldGet100PercentOfLongValueWhenRoundingUp() {
        // When / Then
        assertThat(PercentageUtil.getCeilPercent(6L, 100))
                .isEqualTo(6L);
    }

    @Test
    void shouldGetOver100PercentOfLongValueWhenRoundingUp() {
        // When / Then
        assertThat(PercentageUtil.getCeilPercent(6L, 150))
                .isEqualTo(9L);
    }

}
