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

package sleeper.systemtest.dsl.testutil.drivers;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.TableIndex;
import sleeper.ingest.core.IngestResult;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.IngestJobHandler;
import sleeper.ingest.core.job.IngestJobMessageHandler;
import sleeper.ingest.core.job.status.InMemoryIngestJobStatusStore;
import sleeper.ingest.core.job.status.IngestJobStatusStore;
import sleeper.ingest.core.task.InMemoryIngestTaskStatusStore;
import sleeper.ingest.core.task.IngestTask;
import sleeper.ingest.core.task.IngestTaskStatusStore;
import sleeper.ingest.runner.impl.commit.AddFilesToStateStore;
import sleeper.query.runner.recordretrieval.InMemoryDataStore;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.ingest.IngestByQueueDriver;
import sleeper.systemtest.dsl.ingest.InvokeIngestTasks;
import sleeper.systemtest.dsl.ingest.InvokeIngestTasksDriver;
import sleeper.systemtest.dsl.instance.SleeperTablesDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;
import sleeper.systemtest.dsl.util.WaitForJobs;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;

public class InMemoryIngestByQueue {
    private final Queue<IngestJob> jobsQueue = new LinkedList<>();
    private final List<IngestTask> runningTasks = new ArrayList<>();
    private final IngestTaskStatusStore taskStore = new InMemoryIngestTaskStatusStore();
    private final IngestJobStatusStore jobStore = new InMemoryIngestJobStatusStore();
    private final InMemoryDataStore sourceFiles;
    private final InMemoryDataStore data;
    private final InMemorySketchesStore sketches;
    private final Supplier<Instant> timeSupplier = () -> Instant.now().plus(Duration.ofSeconds(1));

    public InMemoryIngestByQueue(InMemoryDataStore sourceFiles, InMemoryDataStore data, InMemorySketchesStore sketches) {
        this.sourceFiles = sourceFiles;
        this.data = data;
        this.sketches = sketches;
    }

    public IngestByQueueDriver byQueueDriver() {
        return (queueUrl, tableName, files) -> {
            IngestJob job = IngestJob.builder()
                    .id(UUID.randomUUID().toString())
                    .tableName(tableName)
                    .files(files)
                    .build();
            send(job);
            return job.getId();
        };
    }

    public synchronized void send(IngestJob job) {
        jobsQueue.add(job);
    }

    public InvokeIngestTasksDriver tasksDriver(SystemTestContext context) {
        return () -> new InvokeIngestTasks(() -> {
            IngestTask task = newTask(context, UUID.randomUUID().toString());
            task.start();
            runningTasks.add(task);
            task.handleOneMessage();
        }, jobStore);
    }

    private IngestTask newTask(SystemTestContext context, String taskId) {
        return new IngestTask(() -> UUID.randomUUID().toString(), timeSupplier,
                messageReceiver(context), ingester(context, taskId), jobStore, taskStore, taskId);
    }

    private IngestTask.MessageReceiver messageReceiver(SystemTestContext context) {
        IngestJobMessageHandler<IngestJob> messageHandler = messageHandler(context);
        return () -> {
            if (jobsQueue.isEmpty()) {
                return Optional.empty();
            }
            return messageHandler.validate(jobsQueue.poll(), "in-memory message")
                    .map(FakeMessageHandle::new);
        };
    }

    private IngestJobHandler ingester(SystemTestContext context, String taskId) {
        return (job, jobRunId) -> ingest(job, taskId, jobRunId, context);
    }

    public WaitForJobs waitForIngest(SystemTestContext context, PollWithRetriesDriver pollDriver) {
        return WaitForJobs.forIngest(context.instance(), properties -> {
            finishJobsOn(randomRunningTask());
            finishTasks();
            return jobStore;
        }, properties -> taskStore, pollDriver);
    }

    public WaitForJobs waitForBulkImport(SystemTestContext context, PollWithRetriesDriver pollDriver) {
        return WaitForJobs.forBulkImport(context.instance(), properties -> {
            IngestTask task = newTask(context, "bulk-import-task");
            finishJobsOn(fixedTask(task));
            return jobStore;
        }, pollDriver);
    }

    public IngestJobStatusStore jobStore() {
        return jobStore;
    }

    public IngestTaskStatusStore taskStore() {
        return taskStore;
    }

    private void finishJobsOn(Supplier<IngestTask> taskSupplier) {
        while (taskSupplier.get().handleOneMessage()) {
        }
    }

    private Supplier<IngestTask> randomRunningTask() {
        Random random = new Random();
        return () -> runningTasks.get(random.nextInt(runningTasks.size()));
    }

    private Supplier<IngestTask> fixedTask(IngestTask task) {
        return () -> task;
    }

    private void finishTasks() {
        runningTasks.forEach(IngestTask::finish);
        runningTasks.clear();
    }

    private IngestResult ingest(IngestJob job, String taskId, String jobRunId, SystemTestContext context) {
        InstanceProperties instanceProperties = context.instance().getInstanceProperties();
        TableProperties tableProperties = context.instance().getTablePropertiesProvider().getById(job.getTableId());
        StateStore stateStore = context.instance().getStateStore(tableProperties);
        AddFilesToStateStore addFilesToStateStore = AddFilesToStateStore.synchronous(stateStore, jobStore,
                updateBuilder -> updateBuilder.job(job).taskId(taskId).jobRunId(jobRunId).writtenTime(timeSupplier.get()));
        Iterator<Record> iterator = sourceFiles.streamRecords(filesWithFs(instanceProperties, job)).iterator();
        return new InMemoryDirectIngestDriver(context.instance(), data, sketches)
                .ingest(instanceProperties, tableProperties, stateStore, addFilesToStateStore, iterator);
    }

    private List<String> filesWithFs(InstanceProperties instanceProperties, IngestJob job) {
        String fs = instanceProperties.get(FILE_SYSTEM);
        return job.getFiles().stream()
                .map(file -> fs + file)
                .collect(toUnmodifiableList());
    }

    private IngestJobMessageHandler<IngestJob> messageHandler(SystemTestContext context) {
        SystemTestInstanceContext instance = context.instance();
        SleeperTablesDriver tablesDriver = instance.adminDrivers().tables(context.parameters());
        TableIndex tableIndex = tablesDriver.tableIndex(instance.getInstanceProperties());
        return IngestJobMessageHandler.forIngestJob()
                .tableIndex(tableIndex)
                .ingestJobStatusStore(jobStore)
                .expandDirectories(files -> files)
                .build();
    }

    private static class FakeMessageHandle implements IngestTask.MessageHandle {

        private final IngestJob job;

        FakeMessageHandle(IngestJob job) {
            this.job = job;
        }

        @Override
        public IngestJob getJob() {
            return job;
        }

        @Override
        public void completed(RecordsProcessedSummary summary) {
        }

        @Override
        public void failed() {
        }

        @Override
        public void close() {
        }
    }
}
