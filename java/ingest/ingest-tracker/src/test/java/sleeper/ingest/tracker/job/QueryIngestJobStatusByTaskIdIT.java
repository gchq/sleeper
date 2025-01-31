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
package sleeper.ingest.tracker.job;

import org.junit.jupiter.api.Test;

import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.tracker.testutils.DynamoDBIngestJobTrackerTestBase;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.ingestJobStatus;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.startedIngestJob;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.startedIngestRun;

public class QueryIngestJobStatusByTaskIdIT extends DynamoDBIngestJobTrackerTestBase {

    @Test
    public void shouldReturnIngestJobsByTaskId() {
        // Given
        String searchingTaskId = "test-task";
        IngestJob job1 = jobWithFiles("file1");
        IngestJob job2 = jobWithFiles("file2");
        Instant startedTime1 = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant startedTime2 = Instant.parse("2022-12-14T13:52:12.001Z");

        // When
        tracker.jobStarted(job1.startedEventBuilder(startedTime1).taskId(searchingTaskId).build());
        tracker.jobStarted(job2.startedEventBuilder(startedTime2).taskId("another-task").build());

        // Then
        assertThat(tracker.getJobsByTaskId(tableId, searchingTaskId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(startedIngestJob(job1, searchingTaskId, startedTime1));
    }

    @Test
    public void shouldReturnIngestJobByTaskIdInOneRun() {
        // Given
        String taskId1 = "task-id-1";
        String searchingTaskId = "test-task";
        String taskId3 = "task-id-3";
        IngestJob job = jobWithFiles("file");
        Instant startedTime1 = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant startedTime2 = Instant.parse("2022-12-14T13:52:12.001Z");
        Instant startedTime3 = Instant.parse("2022-12-14T13:53:12.001Z");

        // When
        tracker.jobStarted(job.startedEventBuilder(startedTime1).taskId(taskId1).build());
        tracker.jobStarted(job.startedEventBuilder(startedTime2).taskId(searchingTaskId).build());
        tracker.jobStarted(job.startedEventBuilder(startedTime3).taskId(taskId3).build());

        // Then
        assertThat(tracker.getJobsByTaskId(tableId, searchingTaskId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(ingestJobStatus(job,
                        startedIngestRun(job, taskId3, startedTime3),
                        startedIngestRun(job, searchingTaskId, startedTime2),
                        startedIngestRun(job, taskId1, startedTime1)));
    }

    @Test
    public void shouldReturnNoIngestJobsByTaskId() {
        // When / Then
        assertThat(tracker.getJobsByTaskId(tableId, "not-present")).isNullOrEmpty();
    }
}
