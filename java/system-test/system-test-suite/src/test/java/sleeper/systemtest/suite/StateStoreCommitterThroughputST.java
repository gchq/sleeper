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
package sleeper.systemtest.suite;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.statestore.StateStoreCommitMessage;
import sleeper.systemtest.suite.testutil.Slow;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.time.Duration;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.DEFAULT_SCHEMA;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.COMMITTER_THROUGHPUT;

@SystemTest
@Slow
@Execution(SAME_THREAD)
public class StateStoreCommitterThroughputST {

    @Test
    void shouldMeetExpectedThroughputWhenCommittingFilesWithNoJobOnOneTable(SleeperSystemTest sleeper) throws Exception {
        // Given
        sleeper.connectToInstance(COMMITTER_THROUGHPUT);
        PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA).singlePartition("root").buildTree();
        sleeper.partitioning().setPartitions(partitions);

        // When
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
        sleeper.stateStore().fakeCommits()
                .sendBatched(IntStream.rangeClosed(1, 1000)
                        .mapToObj(i -> fileFactory.rootFile("file-" + i + ".parquet", i))
                        .map(StateStoreCommitMessage::addFile))
                .waitForCommitLogs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(20), Duration.ofMinutes(3)));

        // Then
        assertThat(sleeper.tableFiles().references()).hasSize(1000);
        assertThat(sleeper.stateStore().commitsPerSecondForTable())
                .isBetween(70.0, 110.0);
    }

    @Test
    void shouldMeetExpectedThroughputWhenCommittingFilesWithIngestJobOnOneTable(SleeperSystemTest sleeper) throws Exception {
        // Given
        sleeper.connectToInstance(COMMITTER_THROUGHPUT);
        PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA).singlePartition("root").buildTree();
        sleeper.partitioning().setPartitions(partitions);

        // When
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
        sleeper.stateStore().fakeCommits()
                .sendBatched(IntStream.rangeClosed(1, 1000)
                        .mapToObj(i -> fileFactory.rootFile("file-" + i + ".parquet", i))
                        .map(StateStoreCommitMessage::addFileWithJob))
                .waitForCommitLogs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(20), Duration.ofMinutes(3)));

        // Then
        assertThat(sleeper.tableFiles().references()).hasSize(1000);
        assertThat(sleeper.stateStore().commitsPerSecondForTable())
                .isBetween(30.0, 50.0);
    }

    @Test
    void shouldMeetExpectedThroughputWhenCommittingFilesWithNoJobOnMultipleTables(SleeperSystemTest sleeper) throws Exception {
        // Given
        sleeper.connectToInstanceNoTables(COMMITTER_THROUGHPUT);
        sleeper.tables().createMany(10, DEFAULT_SCHEMA);
        PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA).singlePartition("root").buildTree();
        sleeper.tables().forEach(() -> sleeper.partitioning().setPartitions(partitions));

        // When
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
        sleeper.stateStore().fakeCommits()
                .pauseReceivingCommitMessages()
                .sendBatchedForEachTable(IntStream.rangeClosed(1, 1000)
                        .mapToObj(i -> fileFactory.rootFile("file-" + i + ".parquet", i))
                        .map(StateStoreCommitMessage::addFile))
                .resumeReceivingCommitMessages().waitForCommitLogs(
                        PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(20), Duration.ofMinutes(3)));

        // Then
        assertThat(sleeper.tableFiles().referencesByTable())
                .hasSize(10)
                .allSatisfy((table, files) -> assertThat(files).hasSize(1000));
        assertThat(sleeper.stateStore().commitsPerSecondByTable())
                .hasSize(10)
                .allSatisfy((table, commitsPerSecond) -> assertThat(commitsPerSecond)
                        .isBetween(20.0, 110.0));
    }

}
