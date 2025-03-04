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
package sleeper.systemtest.drivers.compaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.systemtest.drivers.testutil.LocalStackDslTest;
import sleeper.systemtest.dsl.SleeperSystemTest;

import java.util.List;
import java.util.stream.IntStream;

import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.systemtest.drivers.testutil.LocalStackTestInstance.LOCALSTACK_MAIN;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;

@LocalStackDslTest
public class CompactionFakeCommitIT {
    PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA).singlePartition("root").buildTree();
    FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) throws Exception {
        sleeper.connectToInstance(LOCALSTACK_MAIN);
        sleeper.partitioning().setPartitions(partitions);
    }

    @Test
    void shouldFakeCompactionCommits(SleeperSystemTest sleeper) throws Exception {
        // Given
        int numCompactions = 10;
        List<FileReference> fakeInputs = IntStream.rangeClosed(1, numCompactions)
                .mapToObj(i -> fileFactory.rootFile("input-" + i + ".parquet", 100))
                .toList();
        List<String> fakeJobIds = IntStream.rangeClosed(1, numCompactions)
                .mapToObj(i -> "job-" + i)
                .toList();
        List<FileReference> fakeOutputs = IntStream.rangeClosed(1, numCompactions)
                .mapToObj(i -> fileFactory.rootFile("output-" + i + ".parquet", 100))
                .toList();
        sleeper.stateStore().fakeCommits().setupStateStore(store -> {
            update(store).addFiles(fakeInputs);
            update(store).assignSingleInputFileToJobs(fakeInputs, fakeJobIds);
        });

        // When
        sleeper.compaction()
                .sendFakeCommitsWithSingleFiles(fakeInputs, fakeJobIds, fakeOutputs);

        // Then
        // TODO check commit queue
    }
}
