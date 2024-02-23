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

package sleeper.core.util;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SplitIntoBatchesTest {

    @Test
    void shouldSplitListIntoOneFullBatchAndOnePartialBatchLeftOver() {
        assertThat(SplitIntoBatches.splitListIntoBatchesOf(2, List.of("A", "B", "C")))
                .containsExactly(List.of("A", "B"), List.of("C"));
    }

    @Test
    void shouldSplitListIntoTwoFullBatches() {
        assertThat(SplitIntoBatches.splitListIntoBatchesOf(2, List.of("A", "B", "C", "D")))
                .containsExactly(List.of("A", "B"), List.of("C", "D"));
    }

    @Test
    void shouldSplitListIntoOneFullBatch() {
        assertThat(SplitIntoBatches.splitListIntoBatchesOf(3, List.of("A", "B", "C")))
                .containsExactly(List.of("A", "B", "C"));
    }

    @Test
    void shouldSplitListIntoOnePartialBatch() {
        assertThat(SplitIntoBatches.splitListIntoBatchesOf(3, List.of("A", "B")))
                .containsExactly(List.of("A", "B"));
    }

    @Test
    void shouldSplitEmptyListToNoBatches() {
        assertThat(SplitIntoBatches.splitListIntoBatchesOf(3, List.of()))
                .isEmpty();
    }

    @Test
    void shouldFailWithBatchSizeLowerThanOne() {
        assertThatThrownBy(() -> SplitIntoBatches.splitListIntoBatchesOf(0, List.of("A", "B")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
