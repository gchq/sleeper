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
package sleeper.compaction.core.task;

import org.junit.jupiter.api.BeforeEach;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.compaction.core.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.core.job.commit.CompactionJobCommitterOrSendToLambda;
import sleeper.compaction.core.task.CompactionTask.MessageHandle;
import sleeper.compaction.core.task.CompactionTask.MessageReceiver;
import sleeper.compaction.core.testutils.InMemoryCompactionJobStatusStore;
import sleeper.compaction.core.testutils.InMemoryCompactionTaskStatusStore;
import sleeper.compaction.core.testutils.StateStoreWaitForFilesTestHelper;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.core.util.ThreadSleep;
import sleeper.core.util.ThreadSleepTestHelper;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_COMMIT_ASYNC;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreTestHelper.inMemoryStateStoreWithSinglePartition;

public class CompactionTaskTestBase {
    protected static final String DEFAULT_TABLE_ID = "test-table-id";
    protected static final String DEFAULT_TABLE_NAME = "test-table-name";
    protected static final String DEFAULT_TASK_ID = "test-task-id";
    protected static final Instant DEFAULT_CREATED_TIME = Instant.parse("2024-03-04T10:50:00Z");

    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final Schema schema = schemaWithKey("key");
    private final List<TableProperties> tables = new ArrayList<>();
    private final Map<String, StateStore> stateStoreByTableId = new HashMap<>();
    protected final TableProperties tableProperties = createTable(DEFAULT_TABLE_ID, DEFAULT_TABLE_NAME);
    protected final StateStore stateStore = stateStore(tableProperties);
    protected final FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
    protected final Queue<CompactionJob> jobsOnQueue = new LinkedList<>();
    protected final List<CompactionJob> consumedJobs = new ArrayList<>();
    protected final List<CompactionJob> jobsReturnedToQueue = new ArrayList<>();
    protected final InMemoryCompactionJobStatusStore jobStore = new InMemoryCompactionJobStatusStore();
    protected final CompactionTaskStatusStore taskStore = new InMemoryCompactionTaskStatusStore();
    protected final List<Duration> sleeps = new ArrayList<>();
    protected final List<CompactionJobCommitRequest> commitRequestsOnQueue = new ArrayList<>();
    protected final List<Duration> foundWaitsForFileAssignment = new ArrayList<>();
    private ThreadSleep waiterForFileAssignment = ThreadSleepTestHelper.recordWaits(foundWaitsForFileAssignment);

    @BeforeEach
    void setUpBase() {
        instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 0);
        instanceProperties.setNumber(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES, 10);
    }

    protected TableProperties createTable(String tableId, String tableName) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, tableId);
        tableProperties.set(TABLE_NAME, tableName);
        tables.add(tableProperties);
        stateStoreByTableId.put(tableId, inMemoryStateStoreWithSinglePartition(schema));
        return tableProperties;
    }

    protected StateStore stateStore(TableProperties table) {
        return stateStoreByTableId.get(table.get(TABLE_ID));
    }

    protected void runTask(CompactionRunner compactor) throws Exception {
        runTask(compactor, timePassesAMinuteAtATime());
    }

    protected void runTask(CompactionRunner compactor, Supplier<Instant> timeSupplier) throws Exception {
        runTask(pollQueue(), noWaitForFileAssignment(), compactor, timeSupplier, DEFAULT_TASK_ID, jobRunIdsInSequence());
    }

    protected void runTask(String taskId, CompactionRunner compactor, Supplier<Instant> timeSupplier) throws Exception {
        runTask(pollQueue(), noWaitForFileAssignment(), compactor, timeSupplier, taskId, jobRunIdsInSequence());
    }

    protected void runTask(String taskId, CompactionRunner compactor, Supplier<String> jobRunIdSupplier, Supplier<Instant> timeSupplier) throws Exception {
        runTask(pollQueue(), noWaitForFileAssignment(), compactor, timeSupplier, taskId, jobRunIdSupplier);
    }

    protected void runTaskCheckingFiles(StateStoreWaitForFiles fileAssignmentCheck, CompactionRunner compactor) throws Exception {
        runTaskCheckingFiles(fileAssignmentCheck, compactor, timePassesAMinuteAtATime());
    }

    protected void runTaskCheckingFiles(StateStoreWaitForFiles fileAssignmentCheck, CompactionRunner compactor, Supplier<Instant> timeSupplier) throws Exception {
        runTask(pollQueue(), fileAssignmentCheck, compactor, timeSupplier, DEFAULT_TASK_ID, jobRunIdsInSequence());
    }

    protected void runTask(
            MessageReceiver messageReceiver,
            CompactionRunner compactor,
            Supplier<Instant> timeSupplier) throws Exception {
        runTask(messageReceiver, noWaitForFileAssignment(), compactor, timeSupplier, DEFAULT_TASK_ID, jobRunIdsInSequence());
    }

    private void runTask(
            MessageReceiver messageReceiver,
            StateStoreWaitForFiles fileAssignmentCheck,
            CompactionRunner compactor,
            Supplier<Instant> timeSupplier,
            String taskId, Supplier<String> jobRunIdSupplier) throws Exception {
        CompactionJobCommitterOrSendToLambda committer = new CompactionJobCommitterOrSendToLambda(
                tablePropertiesProvider(), stateStoreProvider(),
                jobStore, commitRequestsOnQueue::add, timeSupplier);
        CompactionRunnerFactory selector = (job, properties) -> compactor;
        new CompactionTask(instanceProperties, tablePropertiesProvider(), PropertiesReloader.neverReload(),
                stateStoreProvider(), messageReceiver, fileAssignmentCheck,
                committer, jobStore, taskStore, selector, taskId, jobRunIdSupplier, timeSupplier, sleeps::add)
                .run();
    }

    private StateStoreWaitForFiles noWaitForFileAssignment() {
        return waitForFileAssignment().withAttempts(1);
    }

    protected StateStoreWaitForFilesTestHelper waitForFileAssignment() {
        return waitForFileAssignment(timePassesAMinuteAtATime());
    }

    protected StateStoreWaitForFilesTestHelper waitForFileAssignment(Supplier<Instant> timeSupplier) {
        return new StateStoreWaitForFilesTestHelper(tablePropertiesProvider(), stateStoreProvider(), jobStore, waiterForFileAssignment, timeSupplier);
    }

    private TablePropertiesProvider tablePropertiesProvider() {
        return new FixedTablePropertiesProvider(tables);
    }

    private StateStoreProvider stateStoreProvider() {
        return FixedStateStoreProvider.byTableId(stateStoreByTableId);
    }

    private Supplier<String> jobRunIdsInSequence() {
        AtomicInteger runNumber = new AtomicInteger();
        return () -> "test-job-run-" + runNumber.incrementAndGet();
    }

    protected CompactionJob createJobOnQueue(String jobId) throws Exception {
        return createJobOnQueue(jobId, tableProperties, stateStore);
    }

    protected CompactionJob createJobOnQueue(String jobId, TableProperties tableProperties, StateStore stateStore) throws Exception {
        CompactionJob job = createJob(jobId, tableProperties, stateStore);
        jobsOnQueue.add(job);
        return job;
    }

    protected CompactionJob createJob(String jobId) throws Exception {
        return createJob(jobId, tableProperties, stateStore);
    }

    protected CompactionJob createJobOnQueueNotAssignedToFiles(String jobId) throws Exception {
        CompactionJob job = createJobNotInStateStore(jobId, tableProperties);
        addInputFiles(job, stateStore);
        jobsOnQueue.add(job);
        return job;
    }

    protected CompactionJob createJob(String jobId, TableProperties tableProperties, StateStore stateStore) throws Exception {
        CompactionJob job = createJobNotInStateStore(jobId, tableProperties);
        addInputFiles(job, stateStore);
        assignFilesToJob(job, stateStore);
        return job;
    }

    protected CompactionJob createJobNotInStateStore(String jobId, TableProperties tableProperties) throws Exception {
        CompactionJob job = CompactionJob.builder()
                .tableId(tableProperties.get(TABLE_ID))
                .jobId(jobId)
                .partitionId("root")
                .inputFiles(List.of(UUID.randomUUID().toString()))
                .outputFile(UUID.randomUUID().toString()).build();
        jobStore.jobCreated(job, DEFAULT_CREATED_TIME);
        return job;
    }

    private void addInputFiles(CompactionJob job, StateStore stateStore) throws Exception {
        for (String inputFile : job.getInputFiles()) {
            stateStore.addFile(factory.rootFile(inputFile, 123L));
        }
    }

    protected void assignFilesToJob(CompactionJob job, StateStore stateStore) throws Exception {
        stateStore.assignJobIds(List.of(job.createAssignJobIdRequest()));
    }

    protected void send(CompactionJob job) {
        jobsOnQueue.add(job);
    }

    protected void actionAfterWaitForFileAssignment(ThreadSleepTestHelper.WaitAction action) throws Exception {
        waiterForFileAssignment = ThreadSleepTestHelper.withActionAfterWait(waiterForFileAssignment, action);
    }

    private MessageReceiver pollQueue() {
        return () -> {
            CompactionJob job = jobsOnQueue.poll();
            if (job != null) {
                return Optional.of(new FakeMessageHandle(job));
            } else {
                return Optional.empty();
            }
        };
    }

    protected MessageReceiver pollQueue(MessageReceiver... actions) {
        Iterator<MessageReceiver> getAction = List.of(actions).iterator();
        return () -> {
            if (getAction.hasNext()) {
                return getAction.next().receiveMessage();
            } else {
                throw new IllegalStateException("Unexpected queue poll");
            }
        };
    }

    protected MessageReceiver receiveJob() {
        return () -> {
            if (jobsOnQueue.isEmpty()) {
                throw new IllegalStateException("Expected job on queue");
            }
            return Optional.of(new FakeMessageHandle(jobsOnQueue.poll()));
        };
    }

    protected MessageReceiver receiveNoJob() {
        return () -> {
            if (!jobsOnQueue.isEmpty()) {
                throw new IllegalStateException("Expected no jobs on queue");
            }
            return Optional.empty();
        };
    }

    protected MessageReceiver receiveNoJobAnd(Runnable action) {
        return () -> {
            if (!jobsOnQueue.isEmpty()) {
                throw new IllegalStateException("Expected no jobs on queue");
            }
            action.run();
            return Optional.empty();
        };
    }

    protected CompactionRunner jobsSucceed(int numJobs) {
        return processJobs(Stream.generate(() -> jobSucceeds())
                .limit(numJobs)
                .toArray(ProcessJob[]::new));
    }

    protected ProcessJob jobSucceeds(RecordsProcessed summary) {
        return new ProcessJob(summary);
    }

    protected ProcessJob jobSucceeds() {
        return new ProcessJob(10L);
    }

    protected ProcessJob jobFails() {
        return new ProcessJob(new RuntimeException("Something failed"));
    }

    protected ProcessJob jobFails(RuntimeException failure) {
        return new ProcessJob(failure);
    }

    protected CompactionRunner processNoJobs() {
        return processJobs();
    }

    protected CompactionRunner processJobs(ProcessJob... actions) {
        Iterator<ProcessJob> getAction = List.of(actions).iterator();
        return (job, table, partition) -> {
            if (getAction.hasNext()) {
                return getAction.next().run(job);
            } else {
                throw new IllegalStateException("Unexpected job: " + job);
            }
        };
    }

    protected void setAsyncCommit(boolean enabled, TableProperties... tableProperties) {
        for (TableProperties table : tableProperties) {
            table.set(COMPACTION_JOB_COMMIT_ASYNC, "" + enabled);
        }
    }

    private Supplier<Instant> timePassesAMinuteAtATime() {
        return timePassesAMinuteAtATimeFrom(Instant.parse("2024-09-04T09:50:00Z"));
    }

    protected Supplier<Instant> timePassesAMinuteAtATimeFrom(Instant firstTime) {
        return Stream.iterate(firstTime,
                time -> time.plus(Duration.ofMinutes(1)))
                .iterator()::next;
    }

    protected class ProcessJob {
        private final RuntimeException failure;
        private final RecordsProcessed recordsProcessed;
        private ProcessJobAction action = () -> {
        };

        ProcessJob(RuntimeException failure) {
            this.failure = failure;
            this.recordsProcessed = null;
        }

        ProcessJob(long records) {
            this(new RecordsProcessed(records, records));
        }

        ProcessJob(RecordsProcessed summary) {
            this.failure = null;
            this.recordsProcessed = summary;
        }

        public ProcessJob withAction(ProcessJobAction action) {
            this.action = action;
            return this;
        }

        private RecordsProcessed run(CompactionJob job) {
            try {
                action.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (failure != null) {
                throw failure;
            } else {
                return recordsProcessed;
            }
        }
    }

    public interface ProcessJobAction {
        void run() throws Exception;
    }

    protected class FakeMessageHandle implements MessageHandle {
        private final CompactionJob job;

        FakeMessageHandle(CompactionJob job) {
            this.job = job;
        }

        public CompactionJob getJob() {
            return job;
        }

        public void close() {
        }

        public void deleteFromQueue() {
            consumedJobs.add(job);
        }

        public void returnToQueue() {
            jobsReturnedToQueue.add(job);
        }
    }
}
