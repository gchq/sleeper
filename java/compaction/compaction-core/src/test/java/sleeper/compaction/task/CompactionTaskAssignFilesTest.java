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
import sleeper.compaction.task.CompactionTask.WaitForFileAssignment;
import sleeper.core.util.PollWithRetries;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionTaskAssignFilesTest extends CompactionTaskTestBase {
    private final List<CompactionJob> assignmentCheckRetries = new ArrayList<>();

    @Test
    void shouldRetryOnceWaitingForFilesToBeAssignedToJob() throws Exception {
        // Given
        CompactionJob job = createJobOnQueue("job1");

        // When
        runTaskCheckingFiles(
                waitForFilesAssignment(false, true),
                jobsSucceed(1));

        // Then
        assertThat(successfulJobs).containsExactly(job);
        assertThat(failedJobs).isEmpty();
        assertThat(jobsOnQueue).isEmpty();
        assertThat(assignmentCheckRetries).containsExactly(job);
    }

    @Test
    void shouldFailJobWhenTimingOutWaitingForFilesToBeAssignedToJob() throws Exception {
        // Given
        CompactionJob job = createJobOnQueue("job1");

        // When
        runTaskCheckingFiles(
                waitForFilesAssignment(false),
                jobsSucceed(1));

        // Then
        assertThat(successfulJobs).isEmpty();
        assertThat(failedJobs).containsExactly(job);
        assertThat(jobsOnQueue).isEmpty();
        assertThat(assignmentCheckRetries).containsExactly(job);
    }

    private WaitForFileAssignment waitForFilesAssignment(Boolean... checkResults) {
        Iterator<Boolean> checks = List.of(checkResults).iterator();
        return job -> {
            PollWithRetries.immediateRetries(10).pollUntil("files assigned to job", () -> {
                if (checks.hasNext()) {
                    boolean check = checks.next();
                    if (!check) {
                        assignmentCheckRetries.add(job);
                    }
                    return check;
                } else {
                    return false;
                }
            });
        };
    }
}
