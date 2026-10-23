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

package sleeper.core.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

    /**
     * Splits a list of items into batches of a given size.
     *
     * @param  <T>       the item type
     * @param  batchSize the maximum size of sub-list to return
     * @param  list      the list to split
     * @return           a stream of sub-lists backed by the source list
     */
    public static <T> Stream<List<T>> streamBatchesOf(int batchSize, List<T> list) {
        return IntStream.iterate(0, i -> i < list.size(), i -> i + batchSize)
                .mapToObj(i -> list.subList(i, Math.min(i + batchSize, list.size())));
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

    /**
     * Splits a stream of items into batches of a given size.
     *
     * @param  <T>       the item type
     * @param  batchSize the number of items to fill a batch
     * @param  items     a stream of items to split into batches
     * @return           a stream of batches
     */
    public static <T> Stream<List<T>> streamBatchesOf(int batchSize, Stream<T> items) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("Batch size must be at least 1, found " + batchSize);
        }
        BatchSpliterator<T> spliterator = new BatchSpliterator<>(batchSize, items.spliterator());
        return StreamSupport.stream(spliterator, false);
    }

    /**
     * Partitions an iterator into batches.
     *
     * @param <T> the item type
     */
    private static class BatchSpliterator<T> implements Spliterator<List<T>> {
        /**
         * The fewest batches each piece of a split must still be able to fill. Every piece ends
         * with one partial batch, so this limits how much of the output can be partial batches:
         * at 32, around one batch in 32 at worst. Raising it wastes fewer batches, but stops
         * smaller inputs from being split across processors.
         */
        private static final int MIN_BATCHES_PER_SPLIT = 32;

        private final int batchSize;
        private final Spliterator<T> source;

        private BatchSpliterator(int batchSize, Spliterator<T> source) {
            this.batchSize = batchSize;
            this.source = source;
        }

        @Override
        public boolean tryAdvance(Consumer<? super List<T>> action) {
            return source.tryAdvance(firstItem -> {
                List<T> batch = new ArrayList<>(batchSize);
                batch.add(firstItem);
                for (int i = 1; i < batchSize; i++) {
                    if (!source.tryAdvance(batch::add)) {
                        break;
                    }
                }
                action.accept(batch);
            });
        }

        @Override
        public Spliterator<List<T>> trySplit() {
            // Splitting divides the remaining items in two, and each piece goes on to produce
            // its own trailing partial batch. Splitting too far therefore fills the output with
            // undersized batches. Require enough items that both pieces stay above the minimum,
            // rather than just the piece being split, or a split just above the minimum leaves
            // two pieces well below it.
            long minItemsPerPiece = (long) batchSize * MIN_BATCHES_PER_SPLIT;
            if (source.estimateSize() < 2 * minItemsPerPiece) {
                return null;
            }
            Spliterator<T> split = source.trySplit();
            if (split == null) {
                return null;
            } else {
                return new BatchSpliterator<>(batchSize, split);
            }
        }

        @Override
        public long estimateSize() {
            long sourceEstimate = source.estimateSize();
            long lastBatchSize = sourceEstimate % batchSize;
            return sourceEstimate / batchSize + (lastBatchSize == 0 ? 0 : 1);
        }

        @Override
        public int characteristics() {
            return source.characteristics();
        }

    }
}
