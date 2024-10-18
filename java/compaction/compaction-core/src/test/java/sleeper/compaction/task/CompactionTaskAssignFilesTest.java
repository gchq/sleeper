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

import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.core.record.process.ProcessRunTime;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobStatusTestData.failedCompactionRun;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;

public class CompactionTaskAssignFilesTest extends CompactionTaskTestBase {

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
        assertThat(successfulJobs).containsExactly(job);
        assertThat(failedJobs).isEmpty();
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
        assertThat(successfulJobs).isEmpty();
        assertThat(failedJobs).containsExactly(job);
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
        assertThat(failedJobs).containsExactly(job);
        assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                jobCreated(job, DEFAULT_CREATED_TIME,
                        failedCompactionRun(DEFAULT_TASK_ID, new ProcessRunTime(waitForFilesTime, failTime), List.of(
                                "File reference not found in partition root, filename " + job.getInputFiles().get(0)))));
    }
}
