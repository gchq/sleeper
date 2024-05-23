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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A utility class to help split a collection of items into batches.
 */
public class SplitIntoBatches {
    private SplitIntoBatches() {
    }

    /**
     * Splits a list of items into batches of a given size.
     *
     * @param  <T>       the item type
     * @param  batchSize the maximum size of sub-list to return
     * @param  list      the list to split
     * @return           an iterable of sub-lists backed by the source list
     */
    public static <T> Iterable<List<T>> splitListIntoBatchesOf(int batchSize, List<T> list) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("Batch size must be at least 1, found " + batchSize);
        }
        return () -> streamBatchesOf(batchSize, list).iterator();
    }

    private static <T> Stream<List<T>> streamBatchesOf(int batchSize, List<T> items) {
        return IntStream.iterate(0, i -> i < items.size(), i -> i + batchSize)
                .mapToObj(i -> items.subList(i, Math.min(i + batchSize, items.size())));
    }

    /**
     * Performs an operation on batches of items, reusing the same list to store the current batch.
     *
     * @param <T>       the item type
     * @param batchSize the number of items to process in a batch
     * @param items     a stream of items to process in batches
     * @param operation an operation to perform on a batch of items
     */
    public static <T> void reusingListOfSize(int batchSize, Stream<T> items, Consumer<List<T>> operation) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("Batch size must be at least 1, found " + batchSize);
        }
        List<T> batch = new ArrayList<>(batchSize);
        items.forEach(item -> {
            if (batch.size() >= batchSize) {
                operation.accept(batch);
                batch.clear();
            }
            batch.add(item);
        });
        if (!batch.isEmpty()) {
            operation.accept(batch);
        }
    }
}
