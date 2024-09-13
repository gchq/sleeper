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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingInt;
import static java.util.stream.Collectors.toUnmodifiableList;
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
    @DisplayName("Process streamed batches in parallel")
    class ParallelBatches {

        @Test
        void shouldSplitIntoTwoFullBatches() throws Exception {
            assertThat(consumeParallelBatchesOf(2, Stream.of("A", "B", "C", "D")))
                    .containsExactlyInAnyOrder("A", "B", "C", "D");
        }

        @Test
        void shouldSplitIntoOneFullBatchAndOnePartialBatchLeftOver() throws Exception {
            assertThat(consumeParallelBatchesOf(2, Stream.of("A", "B", "C")))
                    .containsExactlyInAnyOrder("A", "B", "C");
        }

        @Test
        void shouldSplitIntoOneFullBatch() throws Exception {
            assertThat(consumeParallelBatchesOf(3, Stream.of("A", "B", "C")))
                    .containsExactlyInAnyOrder("A", "B", "C");
        }

        @Test
        void shouldSplitIntoOnePartialBatch() throws Exception {
            assertThat(consumeParallelBatchesOf(3, Stream.of("A", "B")))
                    .containsExactlyInAnyOrder("A", "B");
        }

        @Test
        void shouldSplitEmptyStreamToNoBatches() throws Exception {
            assertThat(consumeParallelBatchesOf(3, Stream.of()))
                    .isEmpty();
        }

        @Test
        void shouldTakeConsistentFullBatchesOverManyValues() throws Exception {
            List<Integer> input = IntStream.range(1, 1000)
                    .mapToObj(i -> i).collect(toUnmodifiableList());
            List<List<Integer>> output = trackParallelBatchesOf(10, input.stream());
            assertThat(flattenBatches(output)).containsExactlyInAnyOrderElementsOf(input);
            // TODO decide how to handle this
            // assertThat(countBatchesBySize(output))
            //         .isEqualTo(Map.of(2, 6, 1, 1));
        }

        @Test
        void shouldFailWithBatchSizeLowerThanOne() {
            assertThatThrownBy(() -> SplitIntoBatches.streamBatchesOf(0, Stream.of("A", "B")))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        private Collection<String> consumeParallelBatchesOf(int batchSize, Stream<String> stream) throws Exception {
            return trackParallelBatchesOf(batchSize, stream).stream()
                    .flatMap(List::stream).collect(toUnmodifiableList());
        }

        private <T> List<T> flattenBatches(List<List<T>> output) throws Exception {
            return output.stream().flatMap(List::stream).collect(toUnmodifiableList());
        }

        private <T> Map<Integer, Integer> countBatchesBySize(List<List<T>> output) throws Exception {
            return output.stream()
                    .collect(groupingBy(batch -> batch.size(), summingInt(batch -> 1)));
        }

        private <T> List<List<T>> trackParallelBatchesOf(int batchSize, Stream<T> stream) throws Exception {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            List<List<T>> output = new ArrayList<>();
            SplitIntoBatches.streamBatchesOf(batchSize, stream).parallel().forEach(batch -> {
                executor.submit(() -> output.add(batch));
            });
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.SECONDS);
            return output;
        }
    }
}
