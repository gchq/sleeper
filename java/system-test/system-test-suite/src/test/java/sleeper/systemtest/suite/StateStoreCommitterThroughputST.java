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
import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FileReferenceTestData.withJobId;
import static sleeper.core.statestore.FilesReportTestHelper.activeAndReadyForGCFiles;
import static sleeper.core.statestore.FilesReportTestHelper.activeFiles;
import static sleeper.core.statestore.ReplaceFileReferencesRequest.replaceJobFileReferences;
import static sleeper.core.testutils.printers.FileReferencePrinter.printFiles;
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
                        .mapToObj(i -> fileFactory.rootFile(filename(i), i))
                        .map(StateStoreCommitMessage::addFile))
                .waitForCommitLogs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(20), Duration.ofMinutes(3)));

        // Then
        assertThat(sleeper.tableFiles().references()).hasSize(1000);
        assertThat(sleeper.stateStore().commitsPerSecondForTable())
                .satisfies(expectedCommitsPerSecondForTransactionLogOnly());
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
                        .mapToObj(i -> fileFactory.rootFile(filename(i), i))
                        .map(StateStoreCommitMessage::addFileWithJob))
                .waitForCommitLogs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(20), Duration.ofMinutes(3)));

        // Then
        assertThat(sleeper.tableFiles().references()).hasSize(1000);
        assertThat(sleeper.stateStore().commitsPerSecondForTable())
                .satisfies(expectedCommitsPerSecondForTransactionLogAndStatusStore());
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
                .sendBatchedForEachTable(IntStream.rangeClosed(1, 1000)
                        .mapToObj(i -> fileFactory.rootFile(filename(i), i))
                        .map(StateStoreCommitMessage::addFile))
                .waitForCommitLogs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(20), Duration.ofMinutes(3)));

        // Then
        assertThat(sleeper.tableFiles().referencesByTable())
                .hasSize(10)
                .allSatisfy((table, files) -> assertThat(files).hasSize(1000));
        assertThat(sleeper.stateStore().commitsPerSecondByTable())
                .hasSize(10)
                .allSatisfy((table, commitsPerSecond) -> assertThat(commitsPerSecond)
                        .satisfies(expectedCommitsPerSecondForTransactionLogAcrossTables()));
    }

    @Test
    void shouldMeetExpectedThroughputWhenCommittingCompactionJobIdAssignment(SleeperSystemTest sleeper) throws Exception {
        // Given
        sleeper.connectToInstance(COMMITTER_THROUGHPUT);
        PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA).singlePartition("root").buildTree();
        sleeper.partitioning().setPartitions(partitions);
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
        sleeper.stateStore().fakeCommits().setupStateStore(store -> {
            store.addFiles(
                    IntStream.rangeClosed(1, 1000)
                            .mapToObj(i -> fileFactory.rootFile(filename(i), i))
                            .collect(toUnmodifiableList()));
        });

        // When
        sleeper.stateStore().fakeCommits()
                .sendBatched(IntStream.rangeClosed(1, 1000)
                        .mapToObj(i -> factory -> factory.assignJobOnPartitionToFiles(jobId(i), "root", List.of(filename(i)))))
                .waitForCommitLogs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(20), Duration.ofMinutes(3)));

        // Then
        assertThat(printFiles(partitions, sleeper.tableFiles().all()))
                .isEqualTo(printFiles(partitions, activeFiles(
                        IntStream.rangeClosed(1, 1000)
                                .mapToObj(i -> withJobId(jobId(i), fileFactory.rootFile(filename(i), i)))
                                .collect(toUnmodifiableList()))));
        assertThat(sleeper.stateStore().commitsPerSecondForTable())
                .satisfies(expectedCommitsPerSecondForTransactionLogAndStatusStore());
    }

    @Test
    void shouldMeetExpectedThroughputWhenCommittingCompaction(SleeperSystemTest sleeper) throws Exception {
        // Given
        sleeper.connectToInstance(COMMITTER_THROUGHPUT);
        PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA).singlePartition("root").buildTree();
        sleeper.partitioning().setPartitions(partitions);
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
        sleeper.stateStore().fakeCommits().setupStateStore(store -> {
            store.addFiles(IntStream.rangeClosed(1, 1000).mapToObj(i -> i)
                    .flatMap(i -> Stream.of(
                            fileFactory.rootFile(filename(i), i),
                            fileFactory.rootFile(filename(i + 1000), i)))
                    .collect(toUnmodifiableList()));
            store.assignJobIds(IntStream.rangeClosed(1, 1000)
                    .mapToObj(i -> assignJobOnPartitionToFiles(
                            jobId(i), "root", List.of(filename(i), filename(i + 1000))))
                    .collect(toUnmodifiableList()));
        });

        // When
        sleeper.stateStore().fakeCommits()
                .sendBatched(IntStream.rangeClosed(1, 1000)
                        .mapToObj(i -> factory -> factory.commitCompactionForPartitionOnTaskInRun(
                                jobId(i), "root", List.of(filename(i), filename(i + 1000)),
                                "test-task", jobRunId(i), summary(startTime(i), Duration.ofMinutes(1), i * 2, i * 2))))
                .waitForCommitLogs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(20), Duration.ofMinutes(3)));

        // Then
        assertThat(printFiles(partitions, sleeper.tableFiles().all()))
                .isEqualTo(printFiles(partitions, activeAndReadyForGCFiles(
                        IntStream.rangeClosed(1, 1000)
                                .mapToObj(i -> fileFactory.rootFile(filename(i), i * 2))
                                .collect(toUnmodifiableList()),
                        IntStream.rangeClosed(1, 1000).mapToObj(i -> i)
                                .flatMap(i -> Stream.of(filename(i), filename(i + 1000)))
                                .collect(toUnmodifiableList()))));
        assertThat(sleeper.stateStore().commitsPerSecondForTable())
                .satisfies(expectedCommitsPerSecondForTransactionLogAndStatusStore());
    }

    @Test
    void shouldMeetExpectedThroughputWhenCommittingDeletedFiles(SleeperSystemTest sleeper) throws Exception {
        // Given
        sleeper.connectToInstance(COMMITTER_THROUGHPUT);
        PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA).singlePartition("root").buildTree();
        sleeper.partitioning().setPartitions(partitions);
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
        sleeper.stateStore().fakeCommits().setupStateStore(store -> {
            store.addFiles(IntStream.rangeClosed(1, 1000).mapToObj(i -> i)
                    .flatMap(i -> Stream.of(
                            fileFactory.rootFile(filename(i), i),
                            fileFactory.rootFile(filename(i + 1000), i)))
                    .collect(toUnmodifiableList()));
            store.assignJobIds(IntStream.rangeClosed(1, 1000)
                    .mapToObj(i -> assignJobOnPartitionToFiles(
                            jobId(i), "root", List.of(filename(i), filename(i + 1000))))
                    .collect(toUnmodifiableList()));
            store.atomicallyReplaceFileReferencesWithNewOnes(IntStream.rangeClosed(1, 1000)
                    .mapToObj(i -> replaceJobFileReferences(
                            jobId(i), "root", List.of(filename(i), filename(i + 1000)),
                            fileFactory.rootFile(filename(i + 2000), i * 2)))
                    .collect(toUnmodifiableList()));
        });

        // When
        sleeper.stateStore().fakeCommits()
                .sendBatched(IntStream.rangeClosed(1, 1000)
                        .mapToObj(i -> factory -> factory.filesDeleted(List.of(filename(i), filename(i + 1000)))))
                .waitForCommitLogs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(20), Duration.ofMinutes(3)));

        // Then
        assertThat(printFiles(partitions, sleeper.tableFiles().all()))
                .isEqualTo(printFiles(partitions, activeFiles(
                        IntStream.rangeClosed(1, 1000)
                                .mapToObj(i -> fileFactory.rootFile(filename(i + 2000), i * 2))
                                .collect(toUnmodifiableList()))));
        assertThat(sleeper.stateStore().commitsPerSecondForTable())
                .satisfies(expectedCommitsPerSecondForTransactionLogOnly());
    }

    @Test
    void shouldMeetExpectedThroughputWhenPerformingManyOperationsOnMultipleTables(SleeperSystemTest sleeper) throws Exception {
        // Given
        sleeper.connectToInstanceNoTables(COMMITTER_THROUGHPUT);
        sleeper.tables().createMany(10, DEFAULT_SCHEMA);
        PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA).singlePartition("root").buildTree();
        sleeper.tables().forEach(() -> sleeper.partitioning().setPartitions(partitions));

        // When
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
        sleeper.stateStore().fakeCommits()
                .sendBatchedInOrderForEachTable(IntStream.rangeClosed(1, 1000).mapToObj(i -> i)
                        .flatMap(i -> Stream.of(
                                factory -> factory.addFilesWithJob(List.of(
                                        fileFactory.rootFile(filename(i), i),
                                        fileFactory.rootFile(filename(i + 1000), i))),
                                factory -> factory.assignJobOnPartitionToFiles(jobId(i), "root",
                                        List.of(filename(i), filename(i + 1000))),
                                factory -> factory.commitCompactionForPartitionOnTaskInRun(
                                        jobId(i), "root", List.of(filename(i), filename(i + 1000)),
                                        "test-task", jobRunId(i), summary(startTime(i), Duration.ofMinutes(1), i * 2, i * 2)),
                                factory -> factory.filesDeleted(List.of(filename(i), filename(i + 1000))))))
                .waitForCommitLogs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(20), Duration.ofMinutes(3)));

        // Then
        assertThat(sleeper.tableFiles().referencesByTable())
                .hasSize(10)
                .allSatisfy((table, files) -> assertThat(files).hasSize(1000));
        assertThat(sleeper.stateStore().commitsPerSecondByTable())
                .hasSize(10)
                .allSatisfy((table, commitsPerSecond) -> assertThat(commitsPerSecond)
                        .satisfies(expectedCommitsPerSecondForTransactionLogAcrossTables()));
    }

    private String filename(int i) {
        return "file-" + i + ".parquet";
    }

    private String jobId(int i) {
        return "job-" + i;
    }

    private String jobRunId(int i) {
        return "job-run-" + i;
    }

    private Instant startTime(int i) {
        return Instant.parse("2024-09-11T13:56:00Z").plus(Duration.ofSeconds(i));
    }

    private static Consumer<Double> expectedCommitsPerSecondForTransactionLogOnly() {
        return commitsPerSecond -> assertThat(commitsPerSecond)
                .isBetween(90.0, 200.0);
    }

    private static Consumer<Double> expectedCommitsPerSecondForTransactionLogAndStatusStore() {
        return commitsPerSecond -> assertThat(commitsPerSecond)
                .isBetween(35.0, 80.0);
    }

    private static Consumer<Double> expectedCommitsPerSecondForTransactionLogAcrossTables() {
        return commitsPerSecond -> assertThat(commitsPerSecond)
                .isBetween(20.0, 200.0);
    }

}
