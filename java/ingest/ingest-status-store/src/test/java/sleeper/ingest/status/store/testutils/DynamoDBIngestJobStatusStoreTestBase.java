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
package sleeper.ingest.status.store.testutils;

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.table.TableIdGenerator;
import sleeper.core.table.TableStatus;
import sleeper.core.table.TableStatusTestHelper;
import sleeper.dynamodb.test.DynamoDBTestBase;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.IngestJobTestData;
import sleeper.ingest.core.job.status.IngestJobAddedFilesEvent;
import sleeper.ingest.core.job.status.IngestJobAddedFilesStatus;
import sleeper.ingest.core.job.status.IngestJobFailedEvent;
import sleeper.ingest.core.job.status.IngestJobFinishedEvent;
import sleeper.ingest.core.job.status.IngestJobFinishedStatus;
import sleeper.ingest.core.job.status.IngestJobStartedEvent;
import sleeper.ingest.core.job.status.IngestJobStatus;
import sleeper.ingest.core.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.DynamoDBIngestJobStatusStore;
import sleeper.ingest.status.store.job.DynamoDBIngestJobStatusStoreCreator;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.IngestProperty.INGEST_JOB_STATUS_TTL_IN_SECONDS;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.record.process.status.ProcessStatusUpdateTestHelper.defaultUpdateTime;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.filesWithReferences;
import static sleeper.ingest.core.job.status.IngestJobFailedEvent.ingestJobFailed;
import static sleeper.ingest.core.job.status.IngestJobFinishedEvent.ingestJobFinished;
import static sleeper.ingest.core.job.status.IngestJobStartedEvent.ingestJobStarted;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.failedIngestJob;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.finishedIngestJob;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.finishedIngestRun;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.ingestStartedStatus;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.jobStatus;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.startedIngestJob;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.startedIngestRun;
import static sleeper.ingest.status.store.testutils.IngestStatusStoreTestUtils.createInstanceProperties;
import static sleeper.ingest.status.store.testutils.IngestStatusStoreTestUtils.createSchema;
import static sleeper.ingest.status.store.testutils.IngestStatusStoreTestUtils.createTableProperties;

public class DynamoDBIngestJobStatusStoreTestBase extends DynamoDBTestBase {

    protected static final RecursiveComparisonConfiguration IGNORE_UPDATE_TIMES = RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("expiryDate")
            .withIgnoredFieldsMatchingRegexes("jobRun.+updateTime").build();
    protected static final RecursiveComparisonConfiguration IGNORE_EXPIRY_DATE = RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("expiryDate").build();
    public static final String DEFAULT_TASK_ID = "task-id";
    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final String jobStatusTableName = DynamoDBIngestJobStatusStore.jobUpdatesTableName(instanceProperties.get(ID));
    protected final Schema schema = createSchema();
    private final TableProperties tableProperties = createTableProperties(schema, instanceProperties);

    protected final String tableName = tableProperties.get(TABLE_NAME);
    protected final TableStatus table = tableProperties.getStatus();
    protected final String tableId = tableProperties.get(TABLE_ID);
    protected final IngestJobStatusStore store = IngestJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);

    @BeforeEach
    public void setUp() {
        DynamoDBIngestJobStatusStoreCreator.create(instanceProperties, dynamoDBClient);
    }

    @AfterEach
    public void tearDown() {
        dynamoDBClient.deleteTable(jobStatusTableName);
    }

    protected IngestJobStatusStore storeWithTimeToLiveAndUpdateTimes(Duration timeToLive, Instant... updateTimes) {
        instanceProperties.set(INGEST_JOB_STATUS_TTL_IN_SECONDS, "" + timeToLive.getSeconds());
        return storeWithUpdateTimes(updateTimes);
    }

    protected IngestJobStatusStore storeWithUpdateTimes(Instant... updateTimes) {
        return IngestJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties,
                Arrays.stream(updateTimes).iterator()::next);
    }

    protected static RecordsProcessedSummary defaultSummary(Instant startTime, Instant finishTime) {
        return new RecordsProcessedSummary(
                new RecordsProcessed(200L, 100L),
                startTime, finishTime);
    }

    protected static IngestJobStartedEvent defaultJobStartedEvent(IngestJob job, Instant startedTime) {
        return ingestJobStarted(job, startedTime).taskId(DEFAULT_TASK_ID).build();
    }

    protected static IngestJobAddedFilesEvent defaultJobAddedFilesEvent(IngestJob job, List<FileReference> files, Instant writtenTime) {
        return job.addedFilesEventBuilder(writtenTime)
                .files(filesWithReferences(files))
                .taskId(DEFAULT_TASK_ID)
                .build();
    }

    protected static IngestJobFinishedEvent defaultJobFinishedEvent(
            IngestJob job, Instant startedTime, Instant finishedTime) {
        return defaultJobFinishedEvent(job, defaultSummary(startedTime, finishedTime));
    }

    protected static IngestJobFinishedEvent defaultJobFinishedEvent(IngestJob job, RecordsProcessedSummary summary) {
        return ingestJobFinished(job, summary).taskId(DEFAULT_TASK_ID).numFilesWrittenByJob(2).build();
    }

    protected static IngestJobFinishedEvent defaultJobFinishedButUncommittedEvent(
            IngestJob job, Instant startedTime, Instant finishedTime, int numFilesAdded) {
        return ingestJobFinished(job, defaultSummary(startedTime, finishedTime))
                .committedBySeparateFileUpdates(true)
                .numFilesWrittenByJob(numFilesAdded)
                .taskId(DEFAULT_TASK_ID)
                .build();
    }

    protected static IngestJobFailedEvent defaultJobFailedEvent(
            IngestJob job, Instant startedTime, Instant finishedTime, List<String> failureReasons) {
        return defaultJobFailedEvent(job, new ProcessRunTime(startedTime, finishedTime), failureReasons);
    }

    protected static IngestJobFailedEvent defaultJobFailedEvent(
            IngestJob job, ProcessRunTime runTime, List<String> failureReasons) {
        return ingestJobFailed(job, runTime)
                .failureReasons(failureReasons).taskId(DEFAULT_TASK_ID).build();
    }

    protected static IngestJobStatus defaultJobStartedStatus(IngestJob job, Instant startedTime) {
        return startedIngestJob(job, DEFAULT_TASK_ID, startedTime);
    }

    protected static IngestJobStatus defaultJobAddedFilesStatus(IngestJob job, Instant startedTime, Instant writtenTime, int fileCount) {
        return jobStatus(job, ProcessRun.builder()
                .taskId(DEFAULT_TASK_ID)
                .startedStatus(ingestStartedStatus(job, startedTime))
                .statusUpdate(IngestJobAddedFilesStatus.builder()
                        .writtenTime(writtenTime)
                        .updateTime(defaultUpdateTime(writtenTime))
                        .fileCount(fileCount)
                        .build())
                .build());
    }

    protected static IngestJobStatus defaultJobFinishedStatus(IngestJob job, Instant startedTime, Instant finishedTime) {
        return defaultJobFinishedStatus(job, defaultSummary(startedTime, finishedTime));
    }

    protected static IngestJobStatus defaultJobFinishedStatus(IngestJob job, RecordsProcessedSummary summary) {
        return finishedIngestJob(job, DEFAULT_TASK_ID, summary, 2);
    }

    protected static IngestJobStatus defaultJobFinishedButUncommittedStatus(IngestJob job, Instant startedTime, Instant finishedTime, int numFiles) {
        return jobStatus(job, ProcessRun.builder()
                .taskId(DEFAULT_TASK_ID)
                .startedStatus(ingestStartedStatus(job, startedTime))
                .finishedStatus(IngestJobFinishedStatus.updateTimeAndSummary(defaultUpdateTime(finishedTime), defaultSummary(startedTime, finishedTime))
                        .committedBySeparateFileUpdates(true)
                        .numFilesWrittenByJob(numFiles)
                        .build())
                .build());
    }

    protected static IngestJobStatus defaultJobFinishedAndCommittedStatus(IngestJob job, Instant startedTime, Instant writtenTime, Instant finishedTime, int numFiles) {
        return jobStatus(job, ProcessRun.builder()
                .taskId(DEFAULT_TASK_ID)
                .startedStatus(ingestStartedStatus(job, startedTime))
                .finishedStatus(IngestJobFinishedStatus.updateTimeAndSummary(defaultUpdateTime(finishedTime), defaultSummary(startedTime, finishedTime))
                        .committedBySeparateFileUpdates(true)
                        .numFilesWrittenByJob(numFiles)
                        .build())
                .statusUpdate(IngestJobAddedFilesStatus.builder()
                        .writtenTime(writtenTime)
                        .updateTime(defaultUpdateTime(writtenTime))
                        .fileCount(numFiles)
                        .build())
                .build());
    }

    protected static IngestJobStatus defaultJobFailedStatus(IngestJob job, Instant startedTime, Instant finishedTime, List<String> failureReasons) {
        return defaultJobFailedStatus(job, new ProcessRunTime(startedTime, finishedTime), failureReasons);
    }

    protected static IngestJobStatus defaultJobFailedStatus(IngestJob job, ProcessRunTime runTime, List<String> failureReasons) {
        return failedIngestJob(job, DEFAULT_TASK_ID, runTime, failureReasons);
    }

    protected static ProcessRun defaultJobStartedRun(IngestJob job, Instant startedTime) {
        return startedIngestRun(job, DEFAULT_TASK_ID, startedTime);
    }

    protected static ProcessRun defaultJobFinishedRun(IngestJob job, Instant startedTime, Instant finishedTime) {
        return defaultJobFinishedRun(job, defaultSummary(startedTime, finishedTime));
    }

    protected static ProcessRun defaultJobFinishedRun(IngestJob job, RecordsProcessedSummary summary) {
        return finishedIngestRun(job, DEFAULT_TASK_ID, summary, 2);
    }

    protected IngestJobStatus getJobStatus(String jobId) {
        return getJobStatus(store, jobId);
    }

    protected IngestJobStatus getJobStatus(IngestJobStatusStore store, String jobId) {
        return store.getJob(jobId)
                .orElseThrow(() -> new IllegalStateException("Job not found: " + jobId));
    }

    protected List<IngestJobStatus> getAllJobStatuses() {
        return store.getAllJobs(tableId);
    }

    protected IngestJob jobWithFiles(String... filenames) {
        return IngestJobTestData.createJobWithTableAndFiles(UUID.randomUUID().toString(), table, filenames);
    }

    protected IngestJob jobWithTableAndFiles(String tableName, String... filenames) {
        TableStatus table = TableStatusTestHelper.uniqueIdAndName(new TableIdGenerator().generateString(), tableName);
        return IngestJobTestData.createJobWithTableAndFiles(UUID.randomUUID().toString(), table, filenames);
    }
}
