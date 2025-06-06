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
package sleeper.compaction.tracker.testutils;

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.tracker.job.CompactionJobTrackerFactory;
import sleeper.compaction.tracker.job.DynamoDBCompactionJobTracker;
import sleeper.compaction.tracker.job.DynamoDBCompactionJobTrackerCreator;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.JobRunTime;
import sleeper.core.tracker.job.run.RecordsProcessed;
import sleeper.localstack.test.LocalStackTestBase;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static sleeper.compaction.core.job.CompactionJobStatusFromJobTestData.compactionJobCreated;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_STATUS_TTL_IN_SECONDS;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionCommittedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionFinishedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionStartedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.failedCompactionRun;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.startedCompactionRun;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.uncommittedCompactionRun;
import static sleeper.core.tracker.job.run.JobRunTestData.jobRunOnTask;

public class DynamoDBCompactionJobTrackerTestBase extends LocalStackTestBase {

    protected static final RecursiveComparisonConfiguration IGNORE_UPDATE_TIMES = RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("createUpdateTime", "expiryDate")
            .withIgnoredFieldsMatchingRegexes("jobRun.+updateTime").build();
    protected static final RecursiveComparisonConfiguration IGNORE_EXPIRY_TIME = RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("expiryDate").build();
    public static final String DEFAULT_TASK_ID = "task-id";
    public static final String DEFAULT_TASK_ID_2 = "task-id-2";
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final String jobStatusTableName = DynamoDBCompactionJobTracker.jobLookupTableName(instanceProperties.get(ID));
    private final Schema schema = createSchemaWithKey("key", new StringType());
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);

    protected final String tableId = tableProperties.get(TABLE_ID);
    protected final CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
    protected final CompactionJobTracker tracker = CompactionJobTrackerFactory.getTracker(dynamoClientV2, instanceProperties);

    @BeforeEach
    public void setUp() {
        DynamoDBCompactionJobTrackerCreator.create(instanceProperties, dynamoClientV2);
    }

    @AfterEach
    public void tearDown() {
        dynamoClientV2.deleteTable(DeleteTableRequest.builder().tableName(jobStatusTableName).build());
    }

    protected CompactionJobTracker trackerWithTimeToLiveAndUpdateTimes(Duration timeToLive, Instant... updateTimes) {
        instanceProperties.set(COMPACTION_JOB_STATUS_TTL_IN_SECONDS, "" + timeToLive.getSeconds());
        return trackerWithUpdateTimes(updateTimes);
    }

    protected CompactionJobTracker trackerWithUpdateTime(Instant updateTime) {
        return trackerWithUpdateTimes(updateTime);
    }

    protected CompactionJobTracker trackerWithUpdateTimes(Instant... updateTimes) {
        return new DynamoDBCompactionJobTracker(dynamoClientV2, instanceProperties,
                true, Arrays.stream(updateTimes).iterator()::next);
    }

    protected Partition singlePartition() {
        return new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct().get(0);
    }

    protected FileReferenceFactory fileFactory(Partition singlePartition) {
        return fileFactory(Collections.singletonList(singlePartition));
    }

    protected FileReferenceFactory fileFactoryWithPartitions(Consumer<PartitionsBuilder> partitionConfig) {
        PartitionsBuilder builder = new PartitionsBuilder(schema);
        partitionConfig.accept(builder);
        return fileFactory(builder.buildList());
    }

    private FileReferenceFactory fileFactory(List<Partition> partitions) {
        return FileReferenceFactory.from(partitions);
    }

    protected CompactionJobFactory jobFactoryForOtherTable() {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        return new CompactionJobFactory(instanceProperties, tableProperties);
    }

    protected void storeJobsCreated(CompactionJob... jobs) {
        for (CompactionJob job : jobs) {
            storeJobCreated(job);
        }
    }

    protected void storeJobCreated(CompactionJob job) {
        tracker.jobCreated(job.createCreatedEvent());
    }

    protected void storeJobCreated(CompactionJobTracker store, CompactionJob job) {
        store.jobCreated(job.createCreatedEvent());
    }

    protected void storeJobCreatedAtTime(Instant updateTime, CompactionJob job) {
        trackerWithUpdateTime(updateTime).jobCreated(job.createCreatedEvent());
    }

    protected static Instant ignoredUpdateTime() {
        return Instant.now();
    }

    protected static Instant defaultStartTime() {
        return Instant.parse("2022-09-23T10:51:00.001Z");
    }

    protected static Instant defaultCommitTime() {
        return Instant.parse("2022-09-23T10:53:00.001Z");
    }

    protected static JobRunSummary defaultSummary() {
        return new JobRunSummary(
                new RecordsProcessed(200L, 100L),
                defaultRunTime());
    }

    protected static JobRunTime defaultRunTime() {
        return new JobRunTime(
                defaultStartTime(), defaultFinishTime());
    }

    protected static Instant defaultFinishTime() {
        return Instant.parse("2022-09-23T10:52:00.001Z");
    }

    protected static CompactionJobStatus startedStatusWithDefaults(CompactionJob job) {
        return compactionJobCreated(job, ignoredUpdateTime(),
                startedCompactionRun(DEFAULT_TASK_ID, defaultStartTime()));
    }

    protected static CompactionJobStatus finishedUncommittedStatusWithDefaults(CompactionJob job) {
        return compactionJobCreated(job, ignoredUpdateTime(),
                uncommittedCompactionRun(DEFAULT_TASK_ID, defaultSummary()));
    }

    protected static CompactionJobStatus finishedThenCommittedStatusWithDefaults(CompactionJob job) {
        return finishedThenCommittedStatusWithDefaults(job, defaultSummary());
    }

    protected static CompactionJobStatus finishedThenCommittedStatusWithDefaults(CompactionJob job, JobRunSummary summary) {
        return compactionJobCreated(job, ignoredUpdateTime(),
                jobRunOnTask(DEFAULT_TASK_ID,
                        compactionStartedStatus(summary.getStartTime()),
                        compactionFinishedStatus(summary),
                        compactionCommittedStatus(defaultCommitTime())));
    }

    protected static CompactionJobStatus failedStatusWithDefaults(CompactionJob job, List<String> failureReasons) {
        return failedStatusWithDefaults(job, defaultRunTime(), failureReasons);
    }

    protected static CompactionJobStatus failedStatusWithDefaults(CompactionJob job, JobRunTime runTime, List<String> failureReasons) {
        return compactionJobCreated(job, ignoredUpdateTime(),
                failedCompactionRun(DEFAULT_TASK_ID, runTime, failureReasons));
    }

    protected CompactionJobStatus getJobStatus(String jobId) {
        return getJobStatus(tracker, jobId);
    }

    protected CompactionJobStatus getJobStatus(CompactionJobTracker store, String jobId) {
        return store.getJob(jobId).orElseThrow(() -> new IllegalStateException("Job not found: " + jobId));
    }

    protected List<CompactionJobStatus> getAllJobStatuses() {
        return tracker.getAllJobs(tableId);
    }
}
