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
package sleeper.systemtest.dsl.statestore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.transactionlog.transaction.impl.AssignJobIdsTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;
import sleeper.core.util.LoggedDuration;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingLong;
import static java.util.stream.Collectors.toUnmodifiableList;

public class SystemTestStateStoreFakeCommits {
    public static final Logger LOGGER = LoggerFactory.getLogger(SystemTestStateStoreFakeCommits.class);

    private final SystemTestInstanceContext instance;
    private final StateStoreCommitterDriver driver;
    private final PollWithRetriesDriver pollDriver;
    private final WaitForStateStoreCommitLogs waiter;
    private final Map<String, Integer> waitForNumCommitsByTableId = new ConcurrentHashMap<>();
    private final Instant getRunsAfterTime;

    public SystemTestStateStoreFakeCommits(
            SystemTestContext context,
            StateStoreCommitterDriver driver,
            StateStoreCommitterLogsDriver logsDriver,
            PollWithRetriesDriver pollDriver) {
        this.driver = driver;
        this.pollDriver = pollDriver;
        instance = context.instance();
        waiter = new WaitForStateStoreCommitLogs(logsDriver);
        getRunsAfterTime = context.reporting().getRecordingStartTime();
    }

    public SystemTestStateStoreFakeCommits setupStateStore(StateStoreSetup setup) {
        setup.setup(instance.getStateStore());
        return this;
    }

    public SystemTestStateStoreFakeCommits sendBatched(Stream<StateStoreCommitMessage.Commit> commits) {
        sendParallelBatches(forCurrentTable(commits));
        return this;
    }

    public SystemTestStateStoreFakeCommits sendBatchedForEachTable(Stream<StateStoreCommitMessage.Commit> commits) {
        sendParallelBatches(forEachTable(commits));
        return this;
    }

    public SystemTestStateStoreFakeCommits sendBatchedInOrderForEachTable(Stream<StateStoreCommitMessage.Commit> commits) {
        sendSequentialBatches(forEachTable(commits));
        return this;
    }

    public SystemTestStateStoreFakeCommits send(StateStoreCommitMessage.Commit commit) {
        sendSequentialBatches(forCurrentTable(Stream.of(commit)));
        return this;
    }

    public SystemTestStateStoreFakeCommits waitForCommitLogs() throws InterruptedException {
        waiter.waitForCommitLogs(
                pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(20), Duration.ofMinutes(5)),
                waitForNumCommitsByTableId, getRunsAfterTime);
        return this;
    }

    public SystemTestStateStoreFakeCommits pauseReceivingCommitMessages() {
        driver.pauseReceivingMessages();
        return this;
    }

    public SystemTestStateStoreFakeCommits resumeReceivingCommitMessages() {
        driver.resumeReceivingMessages();
        return this;
    }

    public void compactAllFilesToOnePerPartition() {
        CompactionJobFactory jobFactory = new CompactionJobFactory(instance.getInstanceProperties(), instance.getTableProperties());
        setupStateStore(stateStore -> {
            List<FileReference> allReferences = stateStore.getFileReferences();
            Map<String, List<FileReference>> partitionIdToReferences = allReferences
                    .stream().collect(groupingBy(FileReference::getPartitionId));
            Map<String, Long> partitionIdToRecords = allReferences
                    .stream().collect(groupingBy(FileReference::getPartitionId, summingLong(FileReference::getNumberOfRecords)));
            List<CompactionJob> jobs = partitionIdToReferences.entrySet().stream()
                    .map(entry -> jobFactory.createCompactionJob(entry.getValue(), entry.getKey()))
                    .toList();
            new AssignJobIdsTransaction(jobs.stream()
                    .map(CompactionJob::createAssignJobIdRequest)
                    .toList())
                    .synchronousCommit(stateStore);
            new ReplaceFileReferencesTransaction(jobs.stream()
                    .map(job -> job.replaceFileReferencesRequestBuilder(
                            partitionIdToRecords.get(job.getPartitionId()))
                            .build())
                    .toList())
                    .synchronousCommit(stateStore);
        });
    }

    private void sendParallelBatches(Stream<StateStoreCommitRequest> messages) {
        Instant startTime = Instant.now();
        driver.sendCommitMessagesInParallelBatches(countCommits(messages));
        LOGGER.info("Sent commit messages in parallel batches in {}, total commits by table ID: {}",
                LoggedDuration.withShortOutput(startTime, Instant.now()), waitForNumCommitsByTableId);
    }

    private void sendSequentialBatches(Stream<StateStoreCommitRequest> messages) {
        Instant startTime = Instant.now();
        driver.sendCommitMessagesInSequentialBatches(countCommits(messages));
        LOGGER.info("Sent commit messages in sequential batches in {}, total commits by table ID: {}",
                LoggedDuration.withShortOutput(startTime, Instant.now()), waitForNumCommitsByTableId);
    }

    private Stream<StateStoreCommitRequest> forCurrentTable(Stream<StateStoreCommitMessage.Commit> commits) {
        StateStoreCommitMessageFactory factory = new StateStoreCommitMessageFactory(
                instance.getInstanceProperties(), instance.getTableProperties());
        return commits.map(commit -> commit.createMessage(factory));
    }

    private Stream<StateStoreCommitRequest> forEachTable(Stream<StateStoreCommitMessage.Commit> commits) {
        InstanceProperties instanceProperties = instance.getInstanceProperties();
        List<StateStoreCommitMessageFactory> factories = instance.streamTableProperties()
                .map(tableProperties -> new StateStoreCommitMessageFactory(instanceProperties, tableProperties))
                .collect(toUnmodifiableList());
        return commits.flatMap(commit -> factories.stream().map(factory -> commit.createMessage(factory)));
    }

    private Stream<StateStoreCommitRequest> countCommits(Stream<StateStoreCommitRequest> messages) {
        return messages
                .peek(message -> waitForNumCommitsByTableId.compute(
                        message.getTableId(),
                        (id, count) -> count == null ? 1 : count + 1));
    }

    @FunctionalInterface
    public interface StateStoreSetup {

        void setup(StateStore stateStore);
    }
}
