/*
 * Copyright 2022 Crown Copyright
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
package sleeper.compaction.job;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Schema;
import sleeper.statestore.FileInfoFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static sleeper.compaction.job.CompactionJobTestUtils.createInstanceProperties;
import static sleeper.compaction.job.CompactionJobTestUtils.createSchema;
import static sleeper.compaction.job.CompactionJobTestUtils.createTableProperties;

public class CompactionJobTestDataHelper {

    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final Schema schema = createSchema();
    private final TableProperties tableProperties = createTableProperties(schema, instanceProperties);
    private final CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);

    public Partition singlePartition() {
        return new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct().get(0);
    }

    public CompactionJob singleFileCompaction(Partition partition) {
        FileInfoFactory fileFactory = new FileInfoFactory(schema, Collections.singletonList(partition));
        return jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile(100L, "a", "z")),
                partition.getId());
    }

    public CompactionJob singleFileSplittingCompaction(String rootPartitionId, String leftPartitionId, String rightPartitionId) {
        List<Partition> partitions = new PartitionsBuilder(schema)
                .leavesWithSplits(Arrays.asList(leftPartitionId, rightPartitionId), Collections.singletonList("p"))
                .parentJoining(rootPartitionId, leftPartitionId, rightPartitionId)
                .buildList();
        FileInfoFactory fileFactory = new FileInfoFactory(schema, partitions);
        return jobFactory.createSplittingCompactionJob(
                Collections.singletonList(fileFactory.rootFile(100L, "a", "z")),
                rootPartitionId, leftPartitionId, rightPartitionId, "p", 0);
    }


}
