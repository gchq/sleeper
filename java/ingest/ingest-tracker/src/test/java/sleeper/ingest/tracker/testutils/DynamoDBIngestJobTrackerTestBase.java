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
package sleeper.ingest.tracker.testutils;

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.table.TableIdGenerator;
import sleeper.core.table.TableStatus;
import sleeper.core.table.TableStatusTestHelper;
import sleeper.core.tracker.ingest.job.IngestJobStatus;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.job.query.IngestJobAddedFilesStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobFinishedStatus;
import sleeper.core.tracker.ingest.job.update.IngestJobAddedFilesEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobFailedEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobFinishedEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobStartedEvent;
import sleeper.core.tracker.job.run.JobRun;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.JobRunTime;
import sleeper.core.tracker.job.run.RecordsProcessed;
import sleeper.dynamodb.test.DynamoDBTestBase;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.IngestJobTestData;
import sleeper.ingest.tracker.job.DynamoDBIngestJobTracker;
import sleeper.ingest.tracker.job.DynamoDBIngestJobTrackerCreator;
import sleeper.ingest.tracker.job.IngestJobTrackerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.IngestProperty.INGEST_JOB_STATUS_TTL_IN_SECONDS;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.filesWithReferences;
import static sleeper.core.tracker.job.run.JobRunTestData.jobRunOnTask;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.defaultUpdateTime;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.failedIngestJob;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.finishedIngestJob;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.finishedIngestRun;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.ingestJobStatus;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.ingestStartedStatus;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.startedIngestJob;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.startedIngestRun;
import static sleeper.ingest.tracker.testutils.IngestTrackerTestUtils.createInstanceProperties;
import static sleeper.ingest.tracker.testutils.IngestTrackerTestUtils.createSchema;
import static sleeper.ingest.tracker.testutils.IngestTrackerTestUtils.createTableProperties;

public class DynamoDBIngestJobTrackerTestBase extends DynamoDBTestBase {

    protected static final RecursiveComparisonConfiguration IGNORE_UPDATE_TIMES = RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("expiryDate")
            .withIgnoredFieldsMatchingRegexes("jobRun.+updateTime").build();
    protected static final RecursiveComparisonConfiguration IGNORE_EXPIRY_DATE = RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("expiryDate").build();
    public static final String DEFAULT_TASK_ID = "task-id";
    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final String jobStatusTableName = DynamoDBIngestJobTracker.jobUpdatesTableName(instanceProperties.get(ID));
    protected final Schema schema = createSchema();
    private final TableProperties tableProperties = createTableProperties(schema, instanceProperties);

    protected final String tableName = tableProperties.get(TABLE_NAME);
    protected final TableStatus table = tableProperties.getStatus();
    protected final String tableId = tableProperties.get(TABLE_ID);
    protected final IngestJobTracker tracker = IngestJobTrackerFactory.getTracker(dynamoDBClient, instanceProperties);

    @BeforeEach
    public void setUp() {
        DynamoDBIngestJobTrackerCreator.create(instanceProperties, dynamoDBClient);
    }

    @AfterEach
    public void tearDown() {
        dynamoDBClient.deleteTable(jobStatusTableName);
    }

    protected IngestJobTracker trackerWithTimeToLiveAndUpdateTimes(Duration timeToLive, Instant... updateTimes) {
        instanceProperties.set(INGEST_JOB_STATUS_TTL_IN_SECONDS, "" + timeToLive.getSeconds());
        return trackerWithUpdateTimes(updateTimes);
    }

    protected IngestJobTracker trackerWithUpdateTimes(Instant... updateTimes) {
        return IngestJobTrackerFactory.getTracker(dynamoDBClient, instanceProperties,
                Arrays.stream(updateTimes).iterator()::next);
    }

    protected static JobRunSummary defaultSummary(Instant startTime, Instant finishTime) {
        return new JobRunSummary(
                defaultRecordsProcessed(),
                startTime, finishTime);
    }

    protected static RecordsProcessed defaultRecordsProcessed() {
        return new RecordsProcessed(200, 200);
    }

    protected static IngestJobStartedEvent defaultJobStartedEvent(IngestJob job, Instant startedTime) {
        return job.startedEventBuilder(startedTime).taskId(DEFAULT_TASK_ID).build();
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

    protected static IngestJobFinishedEvent defaultJobFinishedEvent(IngestJob job, JobRunSummary summary) {
        return job.finishedEventBuilder(summary).taskId(DEFAULT_TASK_ID).numFilesWrittenByJob(2).build();
    }

    protected static IngestJobFinishedEvent defaultJobFinishedButUncommittedEvent(
            IngestJob job, Instant startedTime, Instant finishedTime, int numFilesAdded) {
        return job.finishedEventBuilder(defaultSummary(startedTime, finishedTime))
                .committedBySeparateFileUpdates(true)
                .numFilesWrittenByJob(numFilesAdded)
                .taskId(DEFAULT_TASK_ID)
                .build();
    }

    protected static IngestJobFailedEvent defaultJobFailedEvent(
            IngestJob job, Instant failureTime, List<String> failureReasons) {
        return job.failedEventBuilder(failureTime)
                .failureReasons(failureReasons).taskId(DEFAULT_TASK_ID).build();
    }

    protected static IngestJobStatus defaultJobStartedStatus(IngestJob job, Instant startedTime) {
        return startedIngestJob(job, DEFAULT_TASK_ID, startedTime);
    }

    protected static IngestJobStatus defaultJobAddedFilesStatus(IngestJob job, Instant startedTime, Instant writtenTime, int fileCount) {
        return ingestJobStatus(job, jobRunOnTask(DEFAULT_TASK_ID,
                ingestStartedStatus(job, startedTime),
                IngestJobAddedFilesStatus.builder()
                        .writtenTime(writtenTime)
                        .updateTime(defaultUpdateTime(writtenTime))
                        .fileCount(fileCount)
                        .build()));
    }

    protected static IngestJobStatus defaultJobFinishedStatus(IngestJob job, Instant startedTime, Instant finishedTime) {
        return defaultJobFinishedStatus(job, defaultSummary(startedTime, finishedTime));
    }

    protected static IngestJobStatus defaultJobFinishedStatus(IngestJob job, JobRunSummary summary) {
        return finishedIngestJob(job, DEFAULT_TASK_ID, summary, 2);
    }

    protected static IngestJobStatus defaultJobFinishedButUncommittedStatus(IngestJob job, Instant startedTime, Instant finishedTime, int numFiles) {
        return ingestJobStatus(job, jobRunOnTask(DEFAULT_TASK_ID,
                ingestStartedStatus(job, startedTime),
                IngestJobFinishedStatus.builder()
                        .updateTime(defaultUpdateTime(finishedTime))
                        .finishTime(finishedTime)
                        .recordsProcessed(defaultRecordsProcessed())
                        .committedBySeparateFileUpdates(true)
                        .numFilesWrittenByJob(numFiles)
                        .build()));
    }

    protected static IngestJobStatus defaultJobFinishedAndCommittedStatus(IngestJob job, Instant startedTime, Instant writtenTime, Instant finishedTime, int numFiles) {
        return ingestJobStatus(job, jobRunOnTask(DEFAULT_TASK_ID,
                ingestStartedStatus(job, startedTime),
                IngestJobFinishedStatus.builder()
                        .updateTime(defaultUpdateTime(finishedTime))
                        .finishTime(finishedTime)
                        .recordsProcessed(defaultRecordsProcessed())
                        .committedBySeparateFileUpdates(true)
                        .numFilesWrittenByJob(numFiles)
                        .build(),
                IngestJobAddedFilesStatus.builder()
                        .writtenTime(writtenTime)
                        .updateTime(defaultUpdateTime(writtenTime))
                        .fileCount(numFiles)
                        .build()));
    }

    protected static IngestJobStatus defaultJobFailedStatus(IngestJob job, Instant startedTime, Instant finishedTime, List<String> failureReasons) {
        return defaultJobFailedStatus(job, new JobRunTime(startedTime, finishedTime), failureReasons);
    }

    protected static IngestJobStatus defaultJobFailedStatus(IngestJob job, JobRunTime runTime, List<String> failureReasons) {
        return failedIngestJob(job, DEFAULT_TASK_ID, runTime, failureReasons);
    }

    protected static JobRun defaultJobStartedRun(IngestJob job, Instant startedTime) {
        return startedIngestRun(job, DEFAULT_TASK_ID, startedTime);
    }

    protected static JobRun defaultJobFinishedRun(IngestJob job, Instant startedTime, Instant finishedTime) {
        return defaultJobFinishedRun(job, defaultSummary(startedTime, finishedTime));
    }

    protected static JobRun defaultJobFinishedRun(IngestJob job, JobRunSummary summary) {
        return finishedIngestRun(job, DEFAULT_TASK_ID, summary, 2);
    }

    protected IngestJobStatus getJobStatus(String jobId) {
        return getJobStatus(tracker, jobId);
    }

    protected IngestJobStatus getJobStatus(IngestJobTracker tracker, String jobId) {
        return tracker.getJob(jobId)
                .orElseThrow(() -> new IllegalStateException("Job not found: " + jobId));
    }

    protected List<IngestJobStatus> getAllJobStatuses() {
        return tracker.getAllJobs(tableId);
    }

    protected IngestJob jobWithFiles(String... filenames) {
        return IngestJobTestData.createJobWithTableAndFiles(UUID.randomUUID().toString(), table, filenames);
    }

    protected IngestJob jobWithTableAndFiles(String tableName, String... filenames) {
        TableStatus table = TableStatusTestHelper.uniqueIdAndName(new TableIdGenerator().generateString(), tableName);
        return IngestJobTestData.createJobWithTableAndFiles(UUID.randomUUID().toString(), table, filenames);
    }
}
