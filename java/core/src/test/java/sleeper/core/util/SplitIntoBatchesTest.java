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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SplitIntoBatchesTest {

    @Nested
    @DisplayName("Split a list into batches")
    class SplitAList {

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

    @Nested
    @DisplayName("Split a stream into batches reusing a list")
    class SplitAStreamReusingList {

        @Test
        void shouldSplitIntoOneFullBatchAndOnePartialBatchLeftOver() {
            assertThat(splitToBatchesOf(2, Stream.of("A", "B", "C")))
                    .containsExactly(List.of("A", "B"), List.of("C"));
        }

        @Test
        void shouldReuseSameListForEachBatch() {
            List<List<String>> batches = new ArrayList<>();
            SplitIntoBatches.reusingListOfSize(2, Stream.of("A", "B", "C"),
                    batch -> batches.add(batch));
            assertThat(batches)
                    .containsExactly(List.of("C"), List.of("C"));
        }

        @Test
        void shouldSplitIntoTwoFullBatches() {
            assertThat(splitToBatchesOf(2, Stream.of("A", "B", "C", "D")))
                    .containsExactly(List.of("A", "B"), List.of("C", "D"));
        }

        @Test
        void shouldSplitIntoOneFullBatch() {
            assertThat(splitToBatchesOf(3, Stream.of("A", "B", "C")))
                    .containsExactly(List.of("A", "B", "C"));
        }

        @Test
        void shouldSplitIntoOnePartialBatch() {
            assertThat(splitToBatchesOf(3, Stream.of("A", "B")))
                    .containsExactly(List.of("A", "B"));
        }

        @Test
        void shouldSplitEmptyStreamToNoBatches() {
            assertThat(splitToBatchesOf(3, Stream.of()))
                    .isEmpty();
        }

        @Test
        void shouldFailWithBatchSizeLowerThanOne() {
            Consumer<List<String>> ignoreBatch = batch -> {
            };
            assertThatThrownBy(() -> SplitIntoBatches.reusingListOfSize(0, Stream.of("A", "B"), ignoreBatch))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        private List<List<String>> splitToBatchesOf(int batchSize, Stream<String> stream) {
            List<List<String>> batches = new ArrayList<>();
            SplitIntoBatches.reusingListOfSize(batchSize, stream,
                    batch -> batches.add(new ArrayList<>(batch)));
            return batches;
        }
    }

    @Nested
    @DisplayName("Split a stream into batches partitioning to multiple lists")
    class SplitAStreamPartitioningToLists {

        @Test
        void shouldSplitIntoTwoFullBatches() {
            assertThat(SplitIntoBatches.toListsOfSize(2, Stream.of("A", "B", "C", "D")))
                    .containsExactly(List.of("A", "B"), List.of("C", "D"));
        }

        @Test
        void shouldSplitIntoOneFullBatchAndOnePartialBatchLeftOver() {
            assertThat(SplitIntoBatches.toListsOfSize(2, Stream.of("A", "B", "C")))
                    .containsExactly(List.of("A", "B"), List.of("C"));
        }

        @Test
        void shouldSplitIntoOneFullBatch() {
            assertThat(SplitIntoBatches.toListsOfSize(3, Stream.of("A", "B", "C")))
                    .containsExactly(List.of("A", "B", "C"));
        }

        @Test
        void shouldSplitIntoOnePartialBatch() {
            assertThat(SplitIntoBatches.toListsOfSize(3, Stream.of("A", "B")))
                    .containsExactly(List.of("A", "B"));
        }

        @Test
        void shouldSplitEmptyStreamToNoBatches() {
            assertThat(SplitIntoBatches.toListsOfSize(3, Stream.of()))
                    .isEmpty();
        }

        @Test
        void shouldFailWithBatchSizeLowerThanOne() {
            assertThatThrownBy(() -> SplitIntoBatches.toListsOfSize(0, Stream.of("A", "B")))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void shouldFailWithParallelStream() {
            assertThatThrownBy(() -> SplitIntoBatches.toListsOfSize(0, Stream.of("A", "B").parallel()))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
