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
package sleeper.splitter.core.extend;

import sleeper.core.partition.Partition;
import sleeper.splitter.core.split.SplitPartitionResult;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;

/**
 * Tracks which partition should be split next. Checks how many rows were included in the sketches for each partition,
 * and keeps an ordering to always split the partition with the most rows included in the sketch.
 */
class SplitPriorityTracker {

    private final PriorityQueue<PartitionPriority> queue = new PriorityQueue<>(
            Comparator.comparing(PartitionPriority::rowsSketched).reversed()
                    .thenComparing(PartitionPriority::order));
    private final PartitionSketchIndex sketchIndex;

    SplitPriorityTracker(List<Partition> leafPartitions, PartitionSketchIndex sketchIndex) {
        this.sketchIndex = sketchIndex;
        leafPartitions.forEach(this::add);
    }

    Optional<Partition> nextPartition() {
        return Optional.ofNullable(queue.poll())
                .map(PartitionPriority::partition);
    }

    void recordSplit(SplitPartitionResult result) {
        add(result.getLeftChild());
        add(result.getRightChild());
    }

    private void add(Partition partition) {
        queue.add(new PartitionPriority(partition, sketchIndex.getNumberOfRecordsSketched(partition), queue.size()));
    }

    private record PartitionPriority(Partition partition, long rowsSketched, int order) {
    }

}
