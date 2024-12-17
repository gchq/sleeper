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
package sleeper.compaction.core.testutils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobTestDataHelper;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessRun;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.compactionCommittedStatus;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.compactionCreatedStatus;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.compactionFailedStatus;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.compactionFinishedStatus;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.compactionStartedStatus;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.jobStatusFrom;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;
import static sleeper.core.record.process.status.ProcessStatusUpdateTestHelper.defaultUpdateTime;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.forJob;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.forJobOnTask;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.records;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

class InMemoryCompactionJobStatusStoreTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
    private final String tableId = tableProperties.get(TABLE_ID);
    private final CompactionJobTestDataHelper dataHelper = CompactionJobTestDataHelper.forTable(instanceProperties, tableProperties);
    private final InMemoryCompactionJobStatusStore store = new InMemoryCompactionJobStatusStore();

    @Nested
    @DisplayName("Store status updates")
    class StoreStatusUpdates {

        @Test
        void shouldStoreInputFilesAssigned() {
            // Given
            Instant createdTime = Instant.parse("2023-03-29T12:27:42Z");
            CompactionJob job = addCreatedJob(createdTime);

            // When / Then
            assertThat(store.streamAllJobs(tableId))
                    .containsExactly(jobCreated(job, createdTime));
        }

        @Test
        void shouldStoreStartedJob() {
            // Given
            Instant createdTime = Instant.parse("2023-03-29T12:27:42Z");
            Instant startedTime = Instant.parse("2023-03-29T12:27:43Z");
            String taskId = "test-task";
            CompactionJob job = addStartedJob(createdTime, startedTime, taskId);

            // When / Then
            assertThat(store.streamAllJobs(tableId))
                    .containsExactly(jobStatusFrom(records().fromUpdates(
                            forJob(job.getId(), compactionCreatedStatus(job, createdTime)),
                            forJobOnTask(job.getId(), taskId, compactionStartedStatus(startedTime)))));
        }

        @Test
        void shouldStoreUncommittedJob() {
            // Given
            Instant createdTime = Instant.parse("2023-03-29T12:27:42Z");
            Instant startedTime = Instant.parse("2023-03-29T12:27:43Z");
            Instant finishedTime = Instant.parse("2023-03-29T12:27:44Z");
            String taskId = "test-task";
            CompactionJob job = addFinishedJobUncommitted(createdTime,
                    summary(startedTime, finishedTime, 100, 100), taskId);

            // When / Then
            assertThat(store.streamAllJobs(tableId))
                    .containsExactly(jobStatusFrom(records().fromUpdates(
                            forJob(job.getId(), compactionCreatedStatus(job, createdTime)),
                            forJobOnTask(job.getId(), taskId,
                                    compactionStartedStatus(startedTime),
                                    compactionFinishedStatus(summary(startedTime, finishedTime, 100, 100))))));
        }

        @Test
        void shouldStoreFinishedJob() {
            // Given
            Instant createdTime = Instant.parse("2023-03-29T12:27:42Z");
            Instant startedTime = Instant.parse("2023-03-29T12:27:43Z");
            Instant finishedTime = Instant.parse("2023-03-29T12:27:44Z");
            Instant committedTime = Instant.parse("2023-03-29T12:27:45Z");
            String taskId = "test-task";
            CompactionJob job = addFinishedJobCommitted(createdTime,
                    summary(startedTime, finishedTime, 100, 100), committedTime, taskId);

            // When / Then
            assertThat(store.streamAllJobs(tableId))
                    .containsExactly(jobStatusFrom(records().fromUpdates(
                            forJob(job.getId(), compactionCreatedStatus(job, createdTime)),
                            forJobOnTask(job.getId(), taskId,
                                    compactionStartedStatus(startedTime),
                                    compactionFinishedStatus(summary(startedTime, finishedTime, 100, 100)),
                                    compactionCommittedStatus(committedTime)))));
        }

        @Test
        void shouldStoreFailedJob() {
            // Given
            Instant createdTime = Instant.parse("2023-03-29T12:27:42Z");
            Instant startedTime = Instant.parse("2023-03-29T12:27:43Z");
            Instant finishedTime = Instant.parse("2023-03-29T12:27:44Z");
            String taskId = "test-task";
            List<String> failureReasons = List.of("Something went wrong");
            CompactionJob job = addFailedJob(createdTime,
                    new ProcessRunTime(startedTime, finishedTime), taskId, failureReasons);

            // When / Then
            assertThat(store.streamAllJobs(tableId))
                    .containsExactly(jobStatusFrom(records().fromUpdates(
                            forJob(job.getId(), compactionCreatedStatus(job, createdTime)),
                            forJobOnTask(job.getId(), taskId,
                                    compactionStartedStatus(startedTime),
                                    compactionFailedStatus(new ProcessRunTime(startedTime, finishedTime), failureReasons)))));
        }

        @Test
        void shouldUseDefaultUpdateTimeIfUpdateTimeNotFixed() {
            // Given
            Instant createdTime = Instant.parse("2023-03-29T12:27:42Z");
            Instant startedTime = Instant.parse("2023-03-29T12:27:43Z");
            Instant finishTime = Instant.parse("2023-03-29T12:28:43Z");
            String taskId = "test-task";

            RecordsProcessedSummary summary = summary(startedTime, finishTime, 100L, 100L);
            CompactionJob job = dataHelper.singleFileCompaction();
            store.jobCreated(job.createCreatedEvent(), createdTime);
            store.jobStarted(job.startedEventBuilder(startedTime).taskId(taskId).build());
            store.jobFinished(job.finishedEventBuilder(summary).taskId(taskId).build());

            // When / Then
            assertThat(store.streamAllJobs(tableId))
                    .containsExactly(jobStatusFrom(records().fromUpdates(
                            forJob(job.getId(), compactionCreatedStatus(job, createdTime)),
                            forJobOnTask(job.getId(), taskId,
                                    compactionStartedStatus(startedTime),
                                    compactionFinishedStatus(summary)))));
        }
    }

    @Nested
    @DisplayName("Get job by ID")
    class GetJobById {
        @Test
        void shouldGetJobById() {
            // Given
            Instant storeTime = Instant.parse("2023-03-29T12:27:42Z");
            CompactionJob job = addCreatedJob(storeTime);

            // When / Then
            assertThat(store.getJob(job.getId()))
                    .contains(jobCreated(job, storeTime));
        }

        @Test
        void shouldFailToFindJobWhenIdDoesNotMatch() {
            // Given
            addCreatedJob(Instant.parse("2023-03-29T12:27:42Z"));

            // When / Then
            assertThat(store.getJob("not-a-job")).isEmpty();
        }

        @Test
        void shouldFailToFindJobWhenNonePresent() {
            assertThat(store.getJob("not-a-job")).isEmpty();
        }
    }

    @Nested
    @DisplayName("Get all jobs")
    class GetAllJobs {

        @Test
        void shouldGetMultipleJobs() {
            // Given
            Instant time1 = Instant.parse("2023-03-29T12:27:42Z");
            CompactionJob job1 = addCreatedJob(time1);
            Instant time2 = Instant.parse("2023-03-29T12:27:43Z");
            CompactionJob job2 = addCreatedJob(time2);
            Instant time3 = Instant.parse("2023-03-29T12:27:44Z");
            CompactionJob job3 = addCreatedJob(time3);

            // When / Then
            assertThat(store.getAllJobs(tableId))
                    .containsExactly(
                            jobCreated(job3, time3),
                            jobCreated(job2, time2),
                            jobCreated(job1, time1));
        }

        @Test
        void shouldGetNoJobs() {
            assertThat(store.getAllJobs("no-jobs-table"))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("Get unfinished jobs")
    class GetUnfinishedJobs {
        @Test
        void shouldGetUnfinishedJobs() {
            // Given
            Instant createdTime1 = Instant.parse("2023-03-29T12:27:42Z");
            Instant startedTime1 = Instant.parse("2023-03-29T12:27:43Z");
            String taskId1 = "test-task-1";
            CompactionJob job1 = addStartedJob(createdTime1, startedTime1, taskId1);

            Instant createdTime2 = Instant.parse("2023-03-29T13:27:42Z");
            Instant startedTime2 = Instant.parse("2023-03-29T13:27:43Z");
            Instant finishedTime2 = Instant.parse("2023-03-29T13:27:44Z");
            Instant committedTime2 = Instant.parse("2023-03-29T13:27:45Z");
            String taskId2 = "test-task-2";
            addFinishedJobCommitted(createdTime2, summary(startedTime2, finishedTime2, 100, 100), committedTime2, taskId2);

            // When / Then
            assertThat(store.getUnfinishedJobs(tableId))
                    .containsExactly(
                            jobStatusFrom(records().fromUpdates(
                                    forJob(job1.getId(), compactionCreatedStatus(job1, createdTime1)),
                                    forJobOnTask(job1.getId(), taskId1, compactionStartedStatus(startedTime1)))));
        }

        @Test
        void shouldGetNoJobsWhenNoneUnfinished() {
            // Given
            addFinishedJobCommitted(Instant.parse("2023-03-29T15:10:12Z"),
                    summary(Instant.parse("2023-03-29T15:11:12Z"),
                            Instant.parse("2023-03-29T15:12:12Z"),
                            100, 100),
                    Instant.parse("2023-03-29T15:13:12Z"),
                    "test-task");

            // When / Then
            assertThat(store.getUnfinishedJobs(tableId)).isEmpty();
        }

        @Test
        void shouldGetNoJobsWhenNonePresent() {
            assertThat(store.getUnfinishedJobs(tableId)).isEmpty();
        }
    }

    @Nested
    @DisplayName("Get jobs by task ID")
    class GetJobsByTaskId {
        @Test
        void shouldGetJobsByTaskId() {
            // Given
            Instant createdTime1 = Instant.parse("2023-03-29T12:27:42Z");
            Instant startedTime1 = Instant.parse("2023-03-29T12:27:43Z");
            String taskId1 = "test-task-1";
            CompactionJob job1 = addStartedJob(createdTime1, startedTime1, taskId1);

            Instant createdTime2 = Instant.parse("2023-03-29T13:27:42Z");
            Instant startedTime2 = Instant.parse("2023-03-29T13:27:43Z");
            String taskId2 = "test-task-2";
            addStartedJob(createdTime2, startedTime2, taskId2);

            // When / Then
            assertThat(store.getJobsByTaskId(tableId, taskId1))
                    .containsExactly(
                            jobStatusFrom(records().fromUpdates(
                                    forJob(job1.getId(), compactionCreatedStatus(job1, createdTime1)),
                                    forJobOnTask(job1.getId(), taskId1, compactionStartedStatus(startedTime1)))));
        }

        @Test
        void shouldGetNoJobsWhenNoneForGivenTask() {
            // Given
            addFinishedJobUncommitted(Instant.parse("2023-03-29T15:10:12Z"),
                    summary(Instant.parse("2023-03-29T15:11:12Z"),
                            Instant.parse("2023-03-29T15:12:12Z"),
                            100, 100),
                    "test-task");

            // When / Then
            assertThat(store.getJobsByTaskId(tableId, "other-task")).isEmpty();
        }

        @Test
        void shouldGetNoJobsWhenNonePresent() {
            assertThat(store.getJobsByTaskId(tableId, "some-task")).isEmpty();
        }
    }

    @Nested
    @DisplayName("Get jobs in time period")
    class GetJobsInTimePeriod {
        @Test
        void shouldGetJobsInTimePeriod() {
            // Given
            Instant createdTime1 = Instant.parse("2023-03-29T12:27:42Z");
            Instant startedTime1 = Instant.parse("2023-03-29T12:27:43Z");
            String taskId1 = "test-task-1";
            CompactionJob job1 = addStartedJob(createdTime1, startedTime1, taskId1);

            Instant createdTime2 = Instant.parse("2023-03-29T13:27:42Z");
            Instant startedTime2 = Instant.parse("2023-03-29T13:27:43Z");
            String taskId2 = "test-task-2";
            addStartedJob(createdTime2, startedTime2, taskId2);

            // When / Then
            assertThat(store.getJobsInTimePeriod(tableId,
                    Instant.parse("2023-03-29T12:00:00Z"),
                    Instant.parse("2023-03-29T13:00:00Z")))
                    .containsExactly(
                            jobStatusFrom(records().fromUpdates(
                                    forJob(job1.getId(), compactionCreatedStatus(job1, createdTime1)),
                                    forJobOnTask(job1.getId(), taskId1, compactionStartedStatus(startedTime1)))));
        }

        @Test
        void shouldGetJobInTimePeriodWhenFirstRunIsInPeriod() {
            // Given
            Instant createdTime = Instant.parse("2024-07-01T10:40:00Z");
            Instant startedTime1 = Instant.parse("2024-07-01T10:41:00Z");
            Instant finishedTime1 = Instant.parse("2024-07-01T10:43:00Z");
            Instant committedTime1 = Instant.parse("2024-07-01T10:44:10Z");
            RecordsProcessedSummary summary1 = summary(startedTime1, finishedTime1, 100L, 100L);
            Instant startedTime2 = Instant.parse("2024-07-01T10:41:10Z");
            Instant finishedTime2 = Instant.parse("2024-07-01T10:42:10Z");
            Instant committedTime2 = Instant.parse("2024-07-01T10:42:20Z");
            RecordsProcessedSummary summary2 = summary(startedTime2, finishedTime2, 100L, 100L);
            String taskId1 = "test-task-1";
            String taskId2 = "test-task-2";
            CompactionJob job = dataHelper.singleFileCompaction();
            store.jobCreated(job.createCreatedEvent(), createdTime);
            store.jobStarted(job.startedEventBuilder(startedTime1).taskId(taskId1).build());
            store.jobStarted(job.startedEventBuilder(startedTime2).taskId(taskId2).build());
            store.jobFinished(job.finishedEventBuilder(summary2).taskId(taskId2).build());
            store.jobCommitted(job.committedEventBuilder(committedTime2).taskId(taskId2).build());
            store.jobFinished(job.finishedEventBuilder(summary1).taskId(taskId1).build());
            store.jobCommitted(job.committedEventBuilder(committedTime1).taskId(taskId1).build());

            // When / Then
            assertThat(store.getJobsInTimePeriod(tableId,
                    Instant.parse("2024-07-01T10:42:30Z"),
                    Instant.parse("2024-07-01T11:00:00Z")))
                    .containsExactly(
                            jobStatusFrom(records().fromUpdates(
                                    forJob(job.getId(), compactionCreatedStatus(job, createdTime)),
                                    forJobOnTask(job.getId(), taskId1,
                                            compactionStartedStatus(startedTime1),
                                            compactionFinishedStatus(summary1),
                                            compactionCommittedStatus(committedTime1)),
                                    forJobOnTask(job.getId(), taskId2,
                                            compactionStartedStatus(startedTime2),
                                            compactionFinishedStatus(summary2),
                                            compactionCommittedStatus(committedTime2)))));
        }

        @Test
        void shouldGetNoJobsWhenNoneInGivenPeriod() {
            // Given
            addFinishedJobUncommitted(Instant.parse("2023-03-29T15:10:12Z"),
                    summary(Instant.parse("2023-03-29T15:11:12Z"),
                            Instant.parse("2023-03-29T15:12:12Z"),
                            100, 100),
                    "test-task");

            // When / Then
            assertThat(store.getJobsInTimePeriod(tableId,
                    Instant.parse("2023-03-29T14:00:00Z"),
                    Instant.parse("2023-03-29T15:00:00Z"))).isEmpty();
        }

        @Test
        void shouldGetNoJobsWhenNonePresent() {
            assertThat(store.getJobsInTimePeriod(tableId,
                    Instant.parse("2023-03-29T14:00:00Z"),
                    Instant.parse("2023-03-29T15:00:00Z"))).isEmpty();
        }
    }

    @Nested
    @DisplayName("Track asynchronous commit")
    class TrackAsynchronousCommit {

        @Test
        void shouldTrackJobFinishedInTaskButNotYetCommitted() {
            // Given
            Instant createdTime = Instant.parse("2023-03-29T12:27:42Z");
            Instant startedTime = Instant.parse("2023-03-29T12:27:43Z");
            RecordsProcessedSummary summary = summary(
                    startedTime, Duration.ofMinutes(1), 100, 100);
            String taskId = "test-task";
            CompactionJob job = addFinishedJobUncommitted(createdTime, summary, taskId);

            // When / Then
            assertThat(store.streamAllJobs(tableId))
                    .containsExactly(jobStatusFrom(records().fromUpdates(
                            forJob(job.getId(), compactionCreatedStatus(job, createdTime)),
                            forJobOnTask(job.getId(), taskId,
                                    compactionStartedStatus(startedTime),
                                    compactionFinishedStatus(summary)))));
        }

        @Test
        void shouldTrackJobFinishedAndCommitted() {
            // Given
            Instant createdTime = Instant.parse("2023-03-29T12:27:42Z");
            Instant startedTime = Instant.parse("2023-03-29T12:27:43Z");
            Instant committedTime = Instant.parse("2023-03-29T12:30:00Z");
            RecordsProcessedSummary summary = summary(
                    startedTime, Duration.ofMinutes(1), 100, 100);
            String taskId = "test-task";
            CompactionJob job = addFinishedJobCommitted(createdTime, summary, committedTime, taskId);

            // When / Then
            assertThat(store.streamAllJobs(tableId))
                    .containsExactly(jobStatusFrom(records().fromUpdates(
                            forJob(job.getId(), compactionCreatedStatus(job, createdTime)),
                            forJobOnTask(job.getId(), taskId,
                                    compactionStartedStatus(startedTime),
                                    compactionFinishedStatus(summary),
                                    compactionCommittedStatus(committedTime)))));
        }

        @Test
        void shouldTrackJobStartedTwiceThenFinishedTwiceFromSameTask() {
            // Given
            Instant createdTime = Instant.parse("2023-03-29T12:27:00Z");
            Instant startedTime1 = Instant.parse("2023-03-29T12:27:10Z");
            RecordsProcessedSummary summary1 = summary(
                    startedTime1, Duration.ofMinutes(1), 100, 100);
            Instant committedTime1 = Instant.parse("2023-03-29T12:28:11Z");
            Instant startedTime2 = Instant.parse("2023-03-29T12:28:15Z");
            RecordsProcessedSummary summary2 = summary(
                    startedTime2, Duration.ofMinutes(1), 100, 100);
            String taskId = "test-task";
            String runId1 = "test-run-1";
            String runId2 = "test-run-2";

            // When
            CompactionJob job = dataHelper.singleFileCompaction();
            store.jobCreated(job.createCreatedEvent(), createdTime);
            store.jobStarted(job.startedEventBuilder(startedTime1).taskId(taskId).jobRunId(runId1).build());
            store.jobFinished(job.finishedEventBuilder(summary1).taskId(taskId).jobRunId(runId1).build());
            store.jobCommitted(job.committedEventBuilder(committedTime1).taskId(taskId).jobRunId(runId1).build());
            store.jobStarted(job.startedEventBuilder(startedTime2).taskId(taskId).jobRunId(runId2).build());
            store.jobFailed(job.failedEventBuilder(summary2.getRunTime()).taskId(taskId).jobRunId(runId2)
                    .failure(new RuntimeException("Could not commit same compaction twice")).build());

            // Then
            assertThat(store.streamAllJobs(tableId))
                    .containsExactly(jobCreated(job, createdTime,
                            ProcessRun.builder().taskId(taskId)
                                    .startedStatus(compactionStartedStatus(startedTime2))
                                    .finishedStatus(compactionFailedStatus(summary2.getRunTime(), List.of("Could not commit same compaction twice")))
                                    .build(),
                            ProcessRun.builder().taskId(taskId)
                                    .startedStatus(compactionStartedStatus(startedTime1))
                                    .finishedStatus(compactionFinishedStatus(summary1))
                                    .statusUpdate(compactionCommittedStatus(committedTime1))
                                    .build()));
        }
    }

    private CompactionJob addCreatedJob(Instant createdTime) {
        CompactionJob job = dataHelper.singleFileCompaction();
        store.fixUpdateTime(createdTime);
        store.jobCreated(job.createCreatedEvent());
        return job;
    }

    private CompactionJob addStartedJob(Instant createdTime, Instant startedTime, String taskId) {
        CompactionJob job = addCreatedJob(createdTime);
        store.fixUpdateTime(defaultUpdateTime(startedTime));
        store.jobStarted(job.startedEventBuilder(startedTime).taskId(taskId).build());
        return job;
    }

    private CompactionJob addFinishedJobUncommitted(Instant createdTime, RecordsProcessedSummary summary, String taskId) {
        CompactionJob job = addStartedJob(createdTime, summary.getStartTime(), taskId);
        store.fixUpdateTime(defaultUpdateTime(summary.getFinishTime()));
        store.jobFinished(job.finishedEventBuilder(summary).taskId(taskId).build());
        return job;
    }

    private CompactionJob addFinishedJobCommitted(Instant createdTime, RecordsProcessedSummary summary, Instant committedTime, String taskId) {
        CompactionJob job = addFinishedJobUncommitted(createdTime, summary, taskId);
        store.fixUpdateTime(defaultUpdateTime(committedTime));
        store.jobCommitted(job.committedEventBuilder(committedTime).taskId(taskId).build());
        return job;
    }

    private CompactionJob addFailedJob(Instant createdTime, ProcessRunTime runTime, String taskId, List<String> failureReasons) {
        CompactionJob job = addStartedJob(createdTime, runTime.getStartTime(), taskId);
        store.fixUpdateTime(defaultUpdateTime(runTime.getFinishTime()));
        store.jobFailed(job.failedEventBuilder(runTime).failureReasons(failureReasons).taskId(taskId).build());
        return job;
    }
}
