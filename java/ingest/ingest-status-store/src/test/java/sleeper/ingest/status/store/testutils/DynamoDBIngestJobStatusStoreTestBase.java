/*
 * Copyright 2022-2023 Crown Copyright
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

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.schema.Schema;
import sleeper.dynamodb.tools.DynamoDBTestBase;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobTestData;
import sleeper.ingest.job.status.IngestJobFinishedEvent;
import sleeper.ingest.job.status.IngestJobStartedEvent;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.DynamoDBIngestJobStatusStore;
import sleeper.ingest.status.store.job.DynamoDBIngestJobStatusStoreCreator;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_JOB_STATUS_TTL_IN_SECONDS;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.ingest.job.status.IngestJobFinishedEvent.ingestJobFinished;
import static sleeper.ingest.job.status.IngestJobStartedEvent.ingestJobStarted;
import static sleeper.ingest.job.status.IngestJobStatusTestData.finishedIngestJob;
import static sleeper.ingest.job.status.IngestJobStatusTestData.finishedIngestRun;
import static sleeper.ingest.job.status.IngestJobStatusTestData.startedIngestJob;
import static sleeper.ingest.job.status.IngestJobStatusTestData.startedIngestRun;
import static sleeper.ingest.status.store.testutils.IngestStatusStoreTestUtils.createInstanceProperties;
import static sleeper.ingest.status.store.testutils.IngestStatusStoreTestUtils.createSchema;
import static sleeper.ingest.status.store.testutils.IngestStatusStoreTestUtils.createTableProperties;

public class DynamoDBIngestJobStatusStoreTestBase extends DynamoDBTestBase {

    protected static final RecursiveComparisonConfiguration IGNORE_UPDATE_TIMES = RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("expiryDate")
            .withIgnoredFieldsMatchingRegexes("jobRun.+updateTime").build();
    public static final String DEFAULT_TASK_ID = "task-id";
    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final String jobStatusTableName = DynamoDBIngestJobStatusStore.jobStatusTableName(instanceProperties.get(ID));
    private final Schema schema = createSchema();
    private final TableProperties tableProperties = createTableProperties(schema, instanceProperties);

    protected final String tableName = tableProperties.get(TABLE_NAME);
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
        return ingestJobStarted(DEFAULT_TASK_ID, job, startedTime);
    }

    protected static IngestJobFinishedEvent defaultJobFinishedEvent(
            IngestJob job, Instant startedTime, Instant finishedTime) {
        return defaultJobFinishedEvent(job, defaultSummary(startedTime, finishedTime));
    }

    protected static IngestJobFinishedEvent defaultJobFinishedEvent(IngestJob job, RecordsProcessedSummary summary) {
        return ingestJobFinished(DEFAULT_TASK_ID, job, summary);
    }

    protected static IngestJobStatus defaultJobStartedStatus(IngestJob job, Instant startedTime) {
        return startedIngestJob(job, DEFAULT_TASK_ID, startedTime);
    }

    protected static IngestJobStatus defaultJobFinishedStatus(IngestJob job, Instant startedTime, Instant finishedTime) {
        return defaultJobFinishedStatus(job, defaultSummary(startedTime, finishedTime));
    }

    protected static IngestJobStatus defaultJobFinishedStatus(IngestJob job, RecordsProcessedSummary summary) {
        return finishedIngestJob(job, DEFAULT_TASK_ID, summary);
    }

    protected static ProcessRun defaultJobStartedRun(IngestJob job, Instant startedTime) {
        return startedIngestRun(job, DEFAULT_TASK_ID, startedTime);
    }

    protected static ProcessRun defaultJobFinishedRun(IngestJob job, Instant startedTime, Instant finishedTime) {
        return defaultJobFinishedRun(job, defaultSummary(startedTime, finishedTime));
    }

    protected static ProcessRun defaultJobFinishedRun(IngestJob job, RecordsProcessedSummary summary) {
        return finishedIngestRun(job, DEFAULT_TASK_ID, summary);
    }

    protected IngestJobStatus getJobStatus(String jobId) {
        return getJobStatus(store, jobId);
    }

    protected IngestJobStatus getJobStatus(IngestJobStatusStore store, String jobId) {
        return store.getJob(jobId)
                .orElseThrow(() -> new IllegalStateException("Job not found: " + jobId));
    }

    protected List<IngestJobStatus> getAllJobStatuses() {
        return store.getAllJobs(tableName);
    }

    protected IngestJob jobWithFiles(String... filenames) {
        return jobWithTableAndFiles(tableName, filenames);
    }

    protected IngestJob jobWithTableAndFiles(String tableName, String... filenames) {
        return IngestJobTestData.createJobWithTableAndFiles(UUID.randomUUID().toString(), tableName, filenames);
    }
}
