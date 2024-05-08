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
package sleeper.compaction.job.execution;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_JOB_COMPLETION_ASYNC;

public class CompactionTaskCommitTest extends CompactionTaskTestBase {
    @Nested
    @DisplayName("Send commits to queue")
    class SendCommitsToQueue {

        @BeforeEach
        public void setup() {
            tableProperties.set(COMPACTION_JOB_COMPLETION_ASYNC, "true");
        }

        @Test
        void shouldSendJobCommitRequestToQueue() throws Exception {
            // Given
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"),   // Start
                    Instant.parse("2024-02-22T13:50:01Z"),   // Job started
                    Instant.parse("2024-02-22T13:50:02Z"),   // Job completed
                    Instant.parse("2024-02-22T13:50:05Z"))); // Finish
            CompactionJob job1 = createJobOnQueue("job1");
            RecordsProcessed job1Summary = new RecordsProcessed(10L, 5L);

            // When
            runTask(processJobs(jobSucceeds(job1Summary)), times::poll);

            // Then
            assertThat(successfulJobs).containsExactly(job1);
            assertThat(failedJobs).isEmpty();
            assertThat(jobsOnQueue).isEmpty();
            assertThat(commitRequestsOnQueue).containsExactly(
                    commitRequestFor(job1,
                            new RecordsProcessedSummary(job1Summary,
                                    Instant.parse("2024-02-22T13:50:01Z"),
                                    Instant.parse("2024-02-22T13:50:02Z"))));
        }

        private CompactionJobCommitRequest commitRequestFor(CompactionJob job, RecordsProcessedSummary summary) {
            return new CompactionJobCommitRequest(job, DEFAULT_TASK_ID, summary);
        }
    }
}
