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
package sleeper.core.tracker.compaction.job;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.tracker.compaction.job.query.CompactionJobCreatedStatus;
import sleeper.core.tracker.compaction.job.update.CompactionJobCreatedEvent;
import sleeper.core.tracker.job.run.JobRun;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.JobRunTime;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.tracker.compaction.job.CompactionJobEventTestData.compactionCommittedEventBuilder;
import static sleeper.core.tracker.compaction.job.CompactionJobEventTestData.compactionFailedEventBuilder;
import static sleeper.core.tracker.compaction.job.CompactionJobEventTestData.compactionFinishedEventBuilder;
import static sleeper.core.tracker.compaction.job.CompactionJobEventTestData.compactionStartedEventBuilder;
import static sleeper.core.tracker.compaction.job.CompactionJobEventTestData.defaultCompactionJobCreatedEventForTable;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionCommittedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionFailedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionFinishedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionJobCreated;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionStartedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.jobStatusFrom;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.defaultUpdateTime;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.forJob;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.forJobOnTask;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.records;

class InMemoryCompactionJobTrackerTest {

    private final String tableId = "some-table";
    private final InMemoryCompactionJobTracker tracker = new InMemoryCompactionJobTracker();

    @Nested
    @DisplayName("Store status updates")
    class StoreStatusUpdates {

        @Test
        void shouldStoreInputFilesAssigned() {
            // Given
            Instant createdTime = Instant.parse("2023-03-29T12:27:42Z");
            CompactionJobCreatedEvent job = addCreatedJob(createdTime);

            // When / Then
            assertThat(tracker.streamAllJobs(tableId))
                    .containsExactly(compactionJobCreated(job, createdTime));
        }

        @Test
        void shouldStoreStartedJob() {
            // Given
            Instant createdTime = Instant.parse("2023-03-29T12:27:42Z");
            Instant startedTime = Instant.parse("2023-03-29T12:27:43Z");
            String taskId = "test-task";
            CompactionJobCreatedEvent job = addStartedJob(createdTime, startedTime, taskId);

            // When / Then
            assertThat(tracker.streamAllJobs(tableId))
                    .containsExactly(jobStatusFrom(records().fromUpdates(
                            forJob(job.getJobId(), CompactionJobCreatedStatus.from(job, createdTime)),
                            forJobOnTask(job.getJobId(), taskId, compactionStartedStatus(startedTime)))));
        }

        @Test
        void shouldStoreUncommittedJob() {
            // Given
            Instant createdTime = Instant.parse("2023-03-29T12:27:42Z");
            Instant startedTime = Instant.parse("2023-03-29T12:27:43Z");
            Instant finishedTime = Instant.parse("2023-03-29T12:27:44Z");
            String taskId = "test-task";
            CompactionJobCreatedEvent job = addFinishedJobUncommitted(createdTime,
                    summary(startedTime, finishedTime, 100, 100), taskId);

            // When / Then
            assertThat(tracker.streamAllJobs(tableId))
                    .containsExactly(jobStatusFrom(records().fromUpdates(
                            forJob(job.getJobId(), CompactionJobCreatedStatus.from(job, createdTime)),
                            forJobOnTask(job.getJobId(), taskId,
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
            CompactionJobCreatedEvent job = addFinishedJobCommitted(createdTime,
                    summary(startedTime, finishedTime, 100, 100), committedTime, taskId);

            // When / Then
            assertThat(tracker.streamAllJobs(tableId))
                    .containsExactly(jobStatusFrom(records().fromUpdates(
                            forJob(job.getJobId(), CompactionJobCreatedStatus.from(job, createdTime)),
                            forJobOnTask(job.getJobId(), taskId,
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
            CompactionJobCreatedEvent job = addFailedJob(createdTime,
                    new JobRunTime(startedTime, finishedTime), taskId, failureReasons);

            // When / Then
            assertThat(tracker.streamAllJobs(tableId))
                    .containsExactly(jobStatusFrom(records().fromUpdates(
                            forJob(job.getJobId(), CompactionJobCreatedStatus.from(job, createdTime)),
                            forJobOnTask(job.getJobId(), taskId,
                                    compactionStartedStatus(startedTime),
                                    compactionFailedStatus(new JobRunTime(startedTime, finishedTime), failureReasons)))));
        }

        @Test
        void shouldUseDefaultUpdateTimeIfUpdateTimeNotFixed() {
            // Given
            Instant createdTime = Instant.parse("2023-03-29T12:27:42Z");
            Instant startedTime = Instant.parse("2023-03-29T12:27:43Z");
            Instant finishTime = Instant.parse("2023-03-29T12:28:43Z");
            String taskId = "test-task";

            JobRunSummary summary = summary(startedTime, finishTime, 100L, 100L);
            CompactionJobCreatedEvent job = defaultJob();
            tracker.jobCreated(job, createdTime);
            tracker.jobStarted(compactionStartedEventBuilder(job, startedTime).taskId(taskId).build());
            tracker.jobFinished(compactionFinishedEventBuilder(job, summary).taskId(taskId).build());

            // When / Then
            assertThat(tracker.streamAllJobs(tableId))
                    .containsExactly(jobStatusFrom(records().fromUpdates(
                            forJob(job.getJobId(), CompactionJobCreatedStatus.from(job, createdTime)),
                            forJobOnTask(job.getJobId(), taskId,
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
            CompactionJobCreatedEvent job = addCreatedJob(storeTime);

            // When / Then
            assertThat(tracker.getJob(job.getJobId()))
                    .contains(compactionJobCreated(job, storeTime));
        }

        @Test
        void shouldFailToFindJobWhenIdDoesNotMatch() {
            // Given
            addCreatedJob(Instant.parse("2023-03-29T12:27:42Z"));

            // When / Then
            assertThat(tracker.getJob("not-a-job")).isEmpty();
        }

        @Test
        void shouldFailToFindJobWhenNonePresent() {
            assertThat(tracker.getJob("not-a-job")).isEmpty();
        }
    }

    @Nested
    @DisplayName("Get all jobs")
    class GetAllJobs {

        @Test
        void shouldGetMultipleJobs() {
            // Given
            Instant time1 = Instant.parse("2023-03-29T12:27:42Z");
            CompactionJobCreatedEvent job1 = addCreatedJob(time1);
            Instant time2 = Instant.parse("2023-03-29T12:27:43Z");
            CompactionJobCreatedEvent job2 = addCreatedJob(time2);
            Instant time3 = Instant.parse("2023-03-29T12:27:44Z");
            CompactionJobCreatedEvent job3 = addCreatedJob(time3);

            // When / Then
            assertThat(tracker.getAllJobs(tableId))
                    .containsExactly(
                            compactionJobCreated(job3, time3),
                            compactionJobCreated(job2, time2),
                            compactionJobCreated(job1, time1));
        }

        @Test
        void shouldGetNoJobs() {
            assertThat(tracker.getAllJobs("no-jobs-table"))
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
            CompactionJobCreatedEvent job1 = addStartedJob(createdTime1, startedTime1, taskId1);

            Instant createdTime2 = Instant.parse("2023-03-29T13:27:42Z");
            Instant startedTime2 = Instant.parse("2023-03-29T13:27:43Z");
            Instant finishedTime2 = Instant.parse("2023-03-29T13:27:44Z");
            Instant committedTime2 = Instant.parse("2023-03-29T13:27:45Z");
            String taskId2 = "test-task-2";
            addFinishedJobCommitted(createdTime2, summary(startedTime2, finishedTime2, 100, 100), committedTime2, taskId2);

            // When / Then
            assertThat(tracker.getUnfinishedJobs(tableId))
                    .containsExactly(
                            jobStatusFrom(records().fromUpdates(
                                    forJob(job1.getJobId(), CompactionJobCreatedStatus.from(job1, createdTime1)),
                                    forJobOnTask(job1.getJobId(), taskId1, compactionStartedStatus(startedTime1)))));
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
            assertThat(tracker.getUnfinishedJobs(tableId)).isEmpty();
        }

        @Test
        void shouldGetNoJobsWhenNonePresent() {
            assertThat(tracker.getUnfinishedJobs(tableId)).isEmpty();
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
            CompactionJobCreatedEvent job1 = addStartedJob(createdTime1, startedTime1, taskId1);

            Instant createdTime2 = Instant.parse("2023-03-29T13:27:42Z");
            Instant startedTime2 = Instant.parse("2023-03-29T13:27:43Z");
            String taskId2 = "test-task-2";
            addStartedJob(createdTime2, startedTime2, taskId2);

            // When / Then
            assertThat(tracker.getJobsByTaskId(tableId, taskId1))
                    .containsExactly(
                            jobStatusFrom(records().fromUpdates(
                                    forJob(job1.getJobId(), CompactionJobCreatedStatus.from(job1, createdTime1)),
                                    forJobOnTask(job1.getJobId(), taskId1, compactionStartedStatus(startedTime1)))));
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
            assertThat(tracker.getJobsByTaskId(tableId, "other-task")).isEmpty();
        }

        @Test
        void shouldGetNoJobsWhenNonePresent() {
            assertThat(tracker.getJobsByTaskId(tableId, "some-task")).isEmpty();
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
            CompactionJobCreatedEvent job1 = addStartedJob(createdTime1, startedTime1, taskId1);

            Instant createdTime2 = Instant.parse("2023-03-29T13:27:42Z");
            Instant startedTime2 = Instant.parse("2023-03-29T13:27:43Z");
            String taskId2 = "test-task-2";
            addStartedJob(createdTime2, startedTime2, taskId2);

            // When / Then
            assertThat(tracker.getJobsInTimePeriod(tableId,
                    Instant.parse("2023-03-29T12:00:00Z"),
                    Instant.parse("2023-03-29T13:00:00Z")))
                    .containsExactly(
                            jobStatusFrom(records().fromUpdates(
                                    forJob(job1.getJobId(), CompactionJobCreatedStatus.from(job1, createdTime1)),
                                    forJobOnTask(job1.getJobId(), taskId1, compactionStartedStatus(startedTime1)))));
        }

        @Test
        void shouldGetJobInTimePeriodWhenFirstRunIsInPeriod() {
            // Given
            Instant createdTime = Instant.parse("2024-07-01T10:40:00Z");
            Instant startedTime1 = Instant.parse("2024-07-01T10:41:00Z");
            Instant finishedTime1 = Instant.parse("2024-07-01T10:43:00Z");
            Instant committedTime1 = Instant.parse("2024-07-01T10:44:10Z");
            JobRunSummary summary1 = summary(startedTime1, finishedTime1, 100L, 100L);
            Instant startedTime2 = Instant.parse("2024-07-01T10:41:10Z");
            Instant finishedTime2 = Instant.parse("2024-07-01T10:42:10Z");
            Instant committedTime2 = Instant.parse("2024-07-01T10:42:20Z");
            JobRunSummary summary2 = summary(startedTime2, finishedTime2, 100L, 100L);
            String taskId1 = "test-task-1";
            String taskId2 = "test-task-2";
            CompactionJobCreatedEvent job = defaultJob();
            tracker.jobCreated(job, createdTime);
            tracker.jobStarted(compactionStartedEventBuilder(job, startedTime1).taskId(taskId1).build());
            tracker.jobStarted(compactionStartedEventBuilder(job, startedTime2).taskId(taskId2).build());
            tracker.jobFinished(compactionFinishedEventBuilder(job, summary2).taskId(taskId2).build());
            tracker.jobCommitted(compactionCommittedEventBuilder(job, committedTime2).taskId(taskId2).build());
            tracker.jobFinished(compactionFinishedEventBuilder(job, summary1).taskId(taskId1).build());
            tracker.jobCommitted(compactionCommittedEventBuilder(job, committedTime1).taskId(taskId1).build());

            // When / Then
            assertThat(tracker.getJobsInTimePeriod(tableId,
                    Instant.parse("2024-07-01T10:42:30Z"),
                    Instant.parse("2024-07-01T11:00:00Z")))
                    .containsExactly(
                            jobStatusFrom(records().fromUpdates(
                                    forJob(job.getJobId(), CompactionJobCreatedStatus.from(job, createdTime)),
                                    forJobOnTask(job.getJobId(), taskId1,
                                            compactionStartedStatus(startedTime1),
                                            compactionFinishedStatus(summary1),
                                            compactionCommittedStatus(committedTime1)),
                                    forJobOnTask(job.getJobId(), taskId2,
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
            assertThat(tracker.getJobsInTimePeriod(tableId,
                    Instant.parse("2023-03-29T14:00:00Z"),
                    Instant.parse("2023-03-29T15:00:00Z"))).isEmpty();
        }

        @Test
        void shouldGetNoJobsWhenNonePresent() {
            assertThat(tracker.getJobsInTimePeriod(tableId,
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
            JobRunSummary summary = summary(
                    startedTime, Duration.ofMinutes(1), 100, 100);
            String taskId = "test-task";
            CompactionJobCreatedEvent job = addFinishedJobUncommitted(createdTime, summary, taskId);

            // When / Then
            assertThat(tracker.streamAllJobs(tableId))
                    .containsExactly(jobStatusFrom(records().fromUpdates(
                            forJob(job.getJobId(), CompactionJobCreatedStatus.from(job, createdTime)),
                            forJobOnTask(job.getJobId(), taskId,
                                    compactionStartedStatus(startedTime),
                                    compactionFinishedStatus(summary)))));
        }

        @Test
        void shouldTrackJobFinishedAndCommitted() {
            // Given
            Instant createdTime = Instant.parse("2023-03-29T12:27:42Z");
            Instant startedTime = Instant.parse("2023-03-29T12:27:43Z");
            Instant committedTime = Instant.parse("2023-03-29T12:30:00Z");
            JobRunSummary summary = summary(
                    startedTime, Duration.ofMinutes(1), 100, 100);
            String taskId = "test-task";
            CompactionJobCreatedEvent job = addFinishedJobCommitted(createdTime, summary, committedTime, taskId);

            // When / Then
            assertThat(tracker.streamAllJobs(tableId))
                    .containsExactly(jobStatusFrom(records().fromUpdates(
                            forJob(job.getJobId(), CompactionJobCreatedStatus.from(job, createdTime)),
                            forJobOnTask(job.getJobId(), taskId,
                                    compactionStartedStatus(startedTime),
                                    compactionFinishedStatus(summary),
                                    compactionCommittedStatus(committedTime)))));
        }

        @Test
        void shouldTrackJobStartedTwiceThenFinishedTwiceFromSameTask() {
            // Given
            Instant createdTime = Instant.parse("2023-03-29T12:27:00Z");
            Instant startedTime1 = Instant.parse("2023-03-29T12:27:10Z");
            JobRunSummary summary1 = summary(
                    startedTime1, Duration.ofMinutes(1), 100, 100);
            Instant committedTime1 = Instant.parse("2023-03-29T12:28:11Z");
            Instant startedTime2 = Instant.parse("2023-03-29T12:28:15Z");
            JobRunSummary summary2 = summary(
                    startedTime2, Duration.ofMinutes(1), 100, 100);
            String taskId = "test-task";
            String runId1 = "test-run-1";
            String runId2 = "test-run-2";

            // When
            CompactionJobCreatedEvent job = defaultJob();
            tracker.jobCreated(job, createdTime);
            tracker.jobStarted(compactionStartedEventBuilder(job, startedTime1).taskId(taskId).jobRunId(runId1).build());
            tracker.jobFinished(compactionFinishedEventBuilder(job, summary1).taskId(taskId).jobRunId(runId1).build());
            tracker.jobCommitted(compactionCommittedEventBuilder(job, committedTime1).taskId(taskId).jobRunId(runId1).build());
            tracker.jobStarted(compactionStartedEventBuilder(job, startedTime2).taskId(taskId).jobRunId(runId2).build());
            tracker.jobFailed(compactionFailedEventBuilder(job, summary2.getRunTime()).taskId(taskId).jobRunId(runId2)
                    .failure(new RuntimeException("Could not commit same compaction twice")).build());

            // Then
            assertThat(tracker.streamAllJobs(tableId))
                    .containsExactly(compactionJobCreated(job, createdTime,
                            JobRun.builder().taskId(taskId)
                                    .startedStatus(compactionStartedStatus(startedTime2))
                                    .finishedStatus(compactionFailedStatus(summary2.getRunTime(), List.of("Could not commit same compaction twice")))
                                    .build(),
                            JobRun.builder().taskId(taskId)
                                    .startedStatus(compactionStartedStatus(startedTime1))
                                    .finishedStatus(compactionFinishedStatus(summary1))
                                    .statusUpdate(compactionCommittedStatus(committedTime1))
                                    .build()));
        }
    }

    private CompactionJobCreatedEvent addCreatedJob(Instant createdTime) {
        CompactionJobCreatedEvent job = defaultJob();
        tracker.fixUpdateTime(createdTime);
        tracker.jobCreated(job);
        return job;
    }

    private CompactionJobCreatedEvent defaultJob() {
        return defaultCompactionJobCreatedEventForTable(tableId);
    }

    private CompactionJobCreatedEvent addStartedJob(Instant createdTime, Instant startedTime, String taskId) {
        CompactionJobCreatedEvent job = addCreatedJob(createdTime);
        tracker.fixUpdateTime(defaultUpdateTime(startedTime));
        tracker.jobStarted(compactionStartedEventBuilder(job, startedTime).taskId(taskId).build());
        return job;
    }

    private CompactionJobCreatedEvent addFinishedJobUncommitted(Instant createdTime, JobRunSummary summary, String taskId) {
        CompactionJobCreatedEvent job = addStartedJob(createdTime, summary.getStartTime(), taskId);
        tracker.fixUpdateTime(defaultUpdateTime(summary.getFinishTime()));
        tracker.jobFinished(compactionFinishedEventBuilder(job, summary).taskId(taskId).build());
        return job;
    }

    private CompactionJobCreatedEvent addFinishedJobCommitted(Instant createdTime, JobRunSummary summary, Instant committedTime, String taskId) {
        CompactionJobCreatedEvent job = addFinishedJobUncommitted(createdTime, summary, taskId);
        tracker.fixUpdateTime(defaultUpdateTime(committedTime));
        tracker.jobCommitted(compactionCommittedEventBuilder(job, committedTime).taskId(taskId).build());
        return job;
    }

    private CompactionJobCreatedEvent addFailedJob(Instant createdTime, JobRunTime runTime, String taskId, List<String> failureReasons) {
        CompactionJobCreatedEvent job = addStartedJob(createdTime, runTime.getStartTime(), taskId);
        tracker.fixUpdateTime(defaultUpdateTime(runTime.getFinishTime()));
        tracker.jobFailed(compactionFailedEventBuilder(job, runTime).failureReasons(failureReasons).taskId(taskId).build());
        return job;
    }
}
