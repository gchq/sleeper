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

import org.junit.Test;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.statestore.FileInfoFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobTestUtils.createInstanceProperties;
import static sleeper.compaction.job.CompactionJobTestUtils.createSchema;
import static sleeper.compaction.job.CompactionJobTestUtils.createTableProperties;
import static sleeper.compaction.job.CompactionJobTestUtils.singlePartition;

public class CompactionJobStatusTest {

    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final Schema schema = createSchema();
    private final TableProperties tableProperties = createTableProperties(schema, instanceProperties);
    private final CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);

    @Test
    public void shouldBuildCompactionJobStartedFromJob() {
        Partition partition = singlePartition(schema);
        FileInfoFactory fileFactory = new FileInfoFactory(schema, Collections.singletonList(partition));
        CompactionJob job = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile(100L, "a", "z")),
                partition.getId());
        Instant updateTime = Instant.parse("2022-09-22T13:33:12.001Z");

        assertThat(CompactionJobStatus.created(job, updateTime)).isEqualTo(
                CompactionJobStatus.builder().jobId(job.getId())
                        .createdStatus(created -> created
                                .updateTime(updateTime)
                                .partitionId(partition.getId())
                                .inputFilesCount(1))
                        .build());
    }

    @Test
    public void shouldBuildSplittingCompactionJobStartedFromJob() {
        List<Partition> partitions = new PartitionsBuilder(schema)
                .leavesWithSplits(Arrays.asList("A", "B"), Collections.singletonList("p"))
                .parentJoining("C", "A", "B")
                .buildList();
        FileInfoFactory fileFactory = new FileInfoFactory(schema, partitions);
        CompactionJob job = jobFactory.createSplittingCompactionJob(
                Collections.singletonList(fileFactory.rootFile(100L, "a", "z")),
                "C", "A", "B", "p", 0);
        Instant updateTime = Instant.parse("2022-09-22T13:33:12.001Z");

        assertThat(CompactionJobStatus.created(job, updateTime)).isEqualTo(
                CompactionJobStatus.builder().jobId(job.getId())
                        .createdStatus(created -> created
                                .updateTime(updateTime)
                                .partitionId("C")
                                .inputFilesCount(1)
                                .childPartitionIds(Arrays.asList("A", "B")))
                        .build());
    }
}
