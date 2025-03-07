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
package sleeper.systemtest.dsl.compaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;

import java.util.List;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingLong;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.IN_MEMORY_MAIN;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;

@InMemoryDslTest
public class CompactionFakeCommitTest {
    PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA).singlePartition("root").buildTree();
    FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) throws Exception {
        sleeper.connectToInstance(IN_MEMORY_MAIN);
        sleeper.partitioning().setPartitions(partitions);
    }

    @Test
    void shouldFakeCompactionCommits(SleeperSystemTest sleeper) throws Exception {
        // Given
        StreamFakeCompactions compactions = StreamFakeCompactions.builder()
                .numCompactions(100)
                .generateInputFiles(i -> List.of(fileFactory.rootFile("input-" + i + ".parquet", 100)))
                .generateJobId(i -> "job-" + i)
                .generateOutputFile(i -> fileFactory.rootFile("output-" + i + ".parquet", 100))
                .build();
        sleeper.stateStore().fakeCommits().setupStateStore(store -> {
            compactions.streamAddFiles().forEach(update(store)::addTransaction);
            compactions.streamAssignJobIds().forEach(update(store)::addTransaction);
        });

        // When
        sleeper.compaction()
                .sendFakeCommits(compactions)
                .waitForJobs();

        // Then
        assertThat(sleeper.tableFiles().recordsByFilename())
                .isEqualTo(compactions.streamOutputFiles()
                        .collect(groupingBy(FileReference::getFilename,
                                summingLong(FileReference::getNumberOfRecords))));
    }

}
