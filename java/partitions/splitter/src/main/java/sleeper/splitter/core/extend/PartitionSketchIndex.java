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
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.sketches.Sketches;
import sleeper.splitter.core.sketches.SketchesForSplitting;
import sleeper.splitter.core.sketches.WrappedSketchForSplitting;
import sleeper.splitter.core.sketches.WrappedSketchesForSplitting;
import sleeper.splitter.core.split.SplitPartitionResult;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Tracks sketches for each partition. Adds derived sketches to the index as partitions are split.
 */
class PartitionSketchIndex {

    private final Schema schema;
    private final Map<String, WrappedSketchesForSplitting> partitionIdToSketches;

    private PartitionSketchIndex(Schema schema, Map<String, WrappedSketchesForSplitting> partitionIdToSketches) {
        this.schema = schema;
        this.partitionIdToSketches = partitionIdToSketches;
    }

    static PartitionSketchIndex from(Schema schema, Map<String, Sketches> partitionIdToSketches) {
        return new PartitionSketchIndex(schema,
                partitionIdToSketches.entrySet().stream()
                        .collect(Collectors.toMap(Entry::getKey, entry -> WrappedSketchesForSplitting.from(schema, entry.getValue()))));
    }

    SketchesForSplitting getSketches(Partition partition) {
        return partitionIdToSketches.get(partition.getId());
    }

    long getNumberOfRecordsSketched(Partition partition) {
        return getSketches(partition).getNumberOfRecordsSketched(schema);
    }

    void recordSplit(SplitPartitionResult result) {
        int dimension = result.getParentPartition().getDimension();
        Field field = schema.getRowKeyFields().get(dimension);
        WrappedSketchesForSplitting parentSketches = partitionIdToSketches.get(result.getParentPartition().getId());
        partitionIdToSketches.put(result.getLeftChild().getId(), parentSketches.splitOnField(field, WrappedSketchForSplitting::splitLeft));
        partitionIdToSketches.put(result.getRightChild().getId(), parentSketches.splitOnField(field, WrappedSketchForSplitting::splitRight));
    }

}
