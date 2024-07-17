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

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.ingest.IngestResult;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.status.InMemoryIngestJobStatusStore;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.task.InMemoryIngestTaskStatusStore;
import sleeper.ingest.task.IngestTaskFinishedStatus;
import sleeper.ingest.task.IngestTaskStatus;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.query.runner.recordretrieval.InMemoryDataStore;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.ingest.IngestByQueueDriver;
import sleeper.systemtest.dsl.ingest.InvokeIngestTasksDriver;
import sleeper.systemtest.dsl.util.WaitForJobs;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.ingest.job.status.IngestJobFinishedEvent.ingestJobFinished;
import static sleeper.ingest.job.status.IngestJobStartedEvent.ingestJobStarted;

public class InMemoryIngestByQueue {
    private final List<IngestJob> queuedJobs = new ArrayList<>();
    private final List<IngestTaskStatus> runningTasks = new ArrayList<>();
    private final IngestTaskStatusStore taskStore = new InMemoryIngestTaskStatusStore();
    private final IngestJobStatusStore jobStore = new InMemoryIngestJobStatusStore();
    private final InMemoryDataStore sourceFiles;
    private final InMemoryDataStore data;
    private final InMemorySketchesStore sketches;

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
        queuedJobs.add(job);
    }

    public InvokeIngestTasksDriver tasksDriver() {
        return (expectedTasks, poll) -> {
            for (int i = 0; i < expectedTasks; i++) {
                IngestTaskStatus task = IngestTaskStatus.builder()
                        .taskId(UUID.randomUUID().toString())
                        .startTime(Instant.now())
                        .build();
                taskStore.taskStarted(task);
                runningTasks.add(task);
            }
        };
    }

    public WaitForJobs waitForIngest(SystemTestContext context) {
        return WaitForJobs.forIngest(context.instance(), properties -> {
            String taskId = runningTasks.stream().map(IngestTaskStatus::getTaskId)
                    .findFirst().orElseThrow();
            finishJobs(context, taskId);
            finishTasks();
            return jobStore;
        }, properties -> taskStore);
    }

    public WaitForJobs waitForBulkImport(SystemTestContext context) {
        return WaitForJobs.forBulkImport(context.instance(), properties -> {
            finishJobs(context, "bulk-import-task");
            return jobStore;
        });
    }

    public IngestJobStatusStore jobStore() {
        return jobStore;
    }

    public IngestTaskStatusStore taskStore() {
        return taskStore;
    }

    private void finishJobs(SystemTestContext context, String taskId) {
        for (IngestJob queuedJob : queuedJobs) {
            IngestJob job = addTableId(queuedJob, context);
            ingest(job, context, taskId);
        }
        queuedJobs.clear();
    }

    private void finishTasks() {
        for (IngestTaskStatus task : runningTasks) {
            taskStore.taskFinished(IngestTaskStatus.builder()
                    .taskId(task.getTaskId())
                    .startTime(task.getStartTime())
                    .finished(task.getStartTime().plus(Duration.ofMinutes(2)), IngestTaskFinishedStatus.builder())
                    .build());
        }
        runningTasks.clear();
    }

    private IngestJob addTableId(IngestJob job, SystemTestContext context) {
        if (job.getTableId() != null) {
            return job;
        } else {
            TablePropertiesProvider provider = context.instance().getTablePropertiesProvider();
            return job.toBuilder().tableId(provider.getByName(job.getTableName()).get(TABLE_ID)).build();
        }
    }

    private void ingest(IngestJob job, SystemTestContext context, String taskId) {
        Instant startTime = Instant.now();
        String fs = context.instance().getInstanceProperties().get(FILE_SYSTEM);
        TableProperties tableProperties = context.instance().getTablePropertiesProvider().getById(job.getTableId());
        List<String> filesWithFs = job.getFiles().stream()
                .map(file -> fs + file)
                .collect(toUnmodifiableList());
        IngestResult result = new InMemoryDirectIngestDriver(context.instance(), data, sketches)
                .ingest(tableProperties, sourceFiles.streamRecords(filesWithFs).iterator());
        Instant finishTime = startTime.plus(Duration.ofMinutes(1));

        jobStore.jobStarted(ingestJobStarted(job, startTime).taskId(taskId).build());
        jobStore.jobFinished(ingestJobFinished(job,
                new RecordsProcessedSummary(
                        result.asRecordsProcessed(),
                        startTime, finishTime))
                .taskId(taskId).fileReferencesAddedByJob(result.getFileReferenceList()).build());
    }
}
