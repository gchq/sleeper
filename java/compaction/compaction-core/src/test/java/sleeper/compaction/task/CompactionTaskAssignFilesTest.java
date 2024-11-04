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
package sleeper.compaction.task;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobStatusTestData.failedCompactionRun;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.job.CompactionJobStatusTestData.uncommittedCompactionRun;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_WAIT_FOR_INPUT_FILE_ASSIGNMENT;
import static sleeper.core.properties.table.TableProperty.STATESTORE_ASYNC_COMMITS_ENABLED;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;

public class CompactionTaskAssignFilesTest extends CompactionTaskTestBase {

    @BeforeEach
    void setUp() {
        instanceProperties.set(COMPACTION_TASK_WAIT_FOR_INPUT_FILE_ASSIGNMENT, "true");
    }

    @Test
    void shouldRetryOnceWaitingForFilesToBeAssignedToJob() throws Exception {
        // Given
        CompactionJob job = createJobOnQueueNotAssignedToFiles("job1");
        actionAfterWaitForFileAssignment(() -> {
            assignFilesToJob(job, stateStore);
        });

        // When
        runTaskCheckingFiles(
                waitForFileAssignment().withAttempts(2),
                processJobs(jobSucceeds()));

        // Then
        assertThat(consumedJobs).containsExactly(job);
        assertThat(jobsReturnedToQueue).isEmpty();
        assertThat(jobsOnQueue).isEmpty();
        assertThat(foundWaitsForFileAssignment).hasSize(1);
    }

    @Test
    void shouldFailJobWhenTimingOutWaitingForFilesToBeAssignedToJob() throws Exception {
        // Given
        CompactionJob job = createJobOnQueueNotAssignedToFiles("job1");

        // When
        runTaskCheckingFiles(
                waitForFileAssignment().withAttempts(1),
                processNoJobs());

        // Then
        assertThat(consumedJobs).isEmpty();
        assertThat(jobsReturnedToQueue).containsExactly(job);
        assertThat(jobsOnQueue).isEmpty();
        assertThat(foundWaitsForFileAssignment).isEmpty();
    }

    @Test
    void shouldNotUpdateStatusStoreWhenTimingOutWaitingForFilesToBeAssignedToJob() throws Exception {
        // Given
        Instant waitForFilesTime = Instant.parse("2024-02-22T13:50:01Z");
        Instant failTime = Instant.parse("2024-02-22T13:50:03Z");
        Queue<Instant> times = new LinkedList<>(List.of(waitForFilesTime, failTime));
        CompactionJob job = createJobOnQueueNotAssignedToFiles("job1");

        // When
        runTaskCheckingFiles(
                waitForFileAssignment(times::poll).withAttempts(1),
                processNoJobs());

        // Then
        assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                jobCreated(job, DEFAULT_CREATED_TIME,
                        failedCompactionRun(DEFAULT_TASK_ID, new ProcessRunTime(waitForFilesTime, failTime), List.of(
                                "Too many retries waiting for input files to be assigned to job in state store"))));
    }

    @Test
    void shouldFailWhenFileDeletedBeforeJob() throws Exception {
        // Given
        Instant waitForFilesTime = Instant.parse("2024-02-22T13:50:01Z");
        Instant failTime = Instant.parse("2024-02-22T13:50:03Z");
        Queue<Instant> times = new LinkedList<>(List.of(waitForFilesTime, failTime));
        CompactionJob job = createJob("test-job");
        send(job);
        stateStore.clearFileData();

        // When
        runTaskCheckingFiles(
                waitForFileAssignment(times::poll).withAttempts(1),
                processJobs(jobSucceeds()));

        // Then
        assertThat(stateStore.getFileReferences()).isEmpty();
        assertThat(jobsReturnedToQueue).containsExactly(job);
        assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                jobCreated(job, DEFAULT_CREATED_TIME,
                        failedCompactionRun(DEFAULT_TASK_ID, new ProcessRunTime(waitForFilesTime, failTime), List.of(
                                "File reference not found in partition root, filename " + job.getInputFiles().get(0)))));
    }

    @Test
    void shouldFailAtEndWhenFileAssignmentCheckDisabledWithDirectCommit() throws Exception {
        // Given
        instanceProperties.set(COMPACTION_TASK_WAIT_FOR_INPUT_FILE_ASSIGNMENT, "false");
        tableProperties.set(STATESTORE_ASYNC_COMMITS_ENABLED, "false");
        CompactionJob job = createJob("test-job");
        send(job);
        stateStore.clearFileData();

        // When
        runTaskCheckingFiles(
                waitForFileAssignment(timePassesAMinuteAtATimeFrom(Instant.parse("2024-10-28T11:45:00Z"))).withAttempts(10),
                processJobs(jobSucceeds()),
                timePassesAMinuteAtATimeFrom(Instant.parse("2024-10-28T11:50:00Z")));

        // Then
        assertThat(jobsReturnedToQueue).containsExactly(job);
        assertThat(jobsOnQueue).isEmpty();
        assertThat(foundWaitsForFileAssignment).isEmpty();
        assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                jobCreated(job, DEFAULT_CREATED_TIME,
                        failedCompactionRun(DEFAULT_TASK_ID,
                                Instant.parse("2024-10-28T11:51:00Z"),
                                Instant.parse("2024-10-28T11:52:00Z"),
                                Instant.parse("2024-10-28T11:53:00Z"),
                                List.of("1 replace file reference requests failed to update the state store",
                                        "File not found: " + job.getInputFiles().get(0)))));
    }

    @Test
    void shouldSendInvalidCommitToQueueWhenFileAssignmentCheckDisabledWithAsyncCommit() throws Exception {
        // Given
        instanceProperties.set(COMPACTION_TASK_WAIT_FOR_INPUT_FILE_ASSIGNMENT, "false");
        tableProperties.set(STATESTORE_ASYNC_COMMITS_ENABLED, "true");
        CompactionJob job = createJob("test-job");
        send(job);
        stateStore.clearFileData();

        // When
        runTaskCheckingFiles(
                waitForFileAssignment(timePassesAMinuteAtATimeFrom(Instant.parse("2024-10-28T11:45:00Z"))).withAttempts(10),
                processJobs(jobSucceeds(new RecordsProcessed(10L, 5L))),
                timePassesAMinuteAtATimeFrom(Instant.parse("2024-10-28T11:50:00Z")));

        // Then
        RecordsProcessedSummary expectedSummary = summary(
                Instant.parse("2024-10-28T11:51:00Z"), Duration.ofMinutes(1), 10, 5);
        assertThat(jobsReturnedToQueue).isEmpty();
        assertThat(jobsOnQueue).isEmpty();
        assertThat(foundWaitsForFileAssignment).isEmpty();
        assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                jobCreated(job, DEFAULT_CREATED_TIME,
                        uncommittedCompactionRun(DEFAULT_TASK_ID, expectedSummary)));
        assertThat(commitRequestsOnQueue).containsExactly(
                new CompactionJobCommitRequest(job, DEFAULT_TASK_ID, "test-job-run-1", expectedSummary));
    }
}
