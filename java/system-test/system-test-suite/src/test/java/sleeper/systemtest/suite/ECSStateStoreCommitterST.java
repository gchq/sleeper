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
package sleeper.systemtest.suite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.statestore.StateStoreCommitMessage;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.ECS_STATESTORE;

@SystemTest
public class ECSStateStoreCommitterST {

    @BeforeEach
    void setUp(SleeperDsl sleeper) {
        sleeper.connectToInstanceAddOfflineTable(ECS_STATESTORE);
    }

    @Test
    void shouldAddManyFiles(SleeperDsl sleeper) throws Exception {
        // Given
        PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA).singlePartition("root").buildTree();
        sleeper.partitioning().setPartitions(partitions);

        // When
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
        sleeper.stateStore().fakeCommits()
                .sendBatched(IntStream.rangeClosed(1, 1000)
                        .mapToObj(i -> fileFactory.rootFile("file-" + i + ".parquet", i))
                        .map(StateStoreCommitMessage::addFile))
                .waitForCommitLogs();

        // Then
        assertThat(sleeper.tableFiles().rowsByFilename()).isEqualTo(
                LongStream.rangeClosed(1, 1000).mapToObj(i -> i)
                        .collect(toMap(i -> "file-" + i + ".parquet", i -> i)));
    }
}
