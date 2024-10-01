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

import static org.assertj.core.api.Assertions.assertThat;
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
                waitForFileAssignmentWithAttempts(2),
                jobsSucceed(1));

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
                waitForFileAssignmentWithAttempts(1),
                jobsSucceed(1));

        // Then
        assertThat(successfulJobs).isEmpty();
        assertThat(failedJobs).containsExactly(job);
        assertThat(jobsOnQueue).isEmpty();
        assertThat(foundWaitsForFileAssignment).isEmpty();
    }

    @Test
    void shouldNotUpdateStatusStoreWhenTimingOutWaitingForFilesToBeAssignedToJob() throws Exception {
        // Given
        CompactionJob job = createJobOnQueueNotAssignedToFiles("job1");

        // When
        runTaskCheckingFiles(
                waitForFileAssignmentWithAttempts(1),
                jobsSucceed(1));

        // Then
        assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                jobCreated(job, DEFAULT_CREATED_TIME));
    }
}
