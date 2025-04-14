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
package sleeper.core.tracker.ingest.task;

import org.junit.jupiter.api.Test;

import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.RecordsProcessed;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class IngestTaskStatusesBuilderTest {

    @Test
    public void shouldCombineStatusUpdatesIntoTaskStatus() {
        // Given
        String taskId = "test-task";
        Instant startTime = Instant.parse("2022-10-12T15:45:00.001Z");
        IngestTaskFinishedStatus finishedStatus = IngestTaskFinishedStatus.builder()
                .addJobSummary(new JobRunSummary(
                        new RecordsProcessed(300L, 200L),
                        Instant.parse("2022-10-12T15:45:01.001Z"),
                        Instant.parse("2022-10-12T15:46:01.001Z")))
                .finish(Instant.parse("2022-10-12T15:46:02.001Z"))
                .build();
        Instant expiryDate = Instant.parse("2022-11-12T15:45:00.001Z");

        // When
        List<IngestTaskStatus> statuses = new IngestTaskStatusesBuilder()
                .taskStarted(taskId, startTime, expiryDate)
                .taskFinished(taskId, finishedStatus)
                .build();

        // Then
        assertThat(statuses)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(IngestTaskStatus.builder()
                        .taskId(taskId)
                        .startTime(startTime)
                        .finishedStatus(finishedStatus)
                        .expiryDate(expiryDate)
                        .build());
    }

    @Test
    public void shouldOrderTasksByStartTimeMostRecentFirst() {
        // Given
        String taskId1 = "test-task-1";
        String taskId2 = "test-task-2";
        String taskId3 = "test-task-3";
        Instant startTime1 = Instant.parse("2022-10-12T15:45:00.001Z");
        Instant startTime2 = Instant.parse("2022-10-12T15:46:00.001Z");
        Instant startTime3 = Instant.parse("2022-10-12T15:47:00.001Z");
        Instant expiryDate1 = Instant.parse("2022-11-12T15:45:00.001Z");
        Instant expiryDate2 = Instant.parse("2022-11-12T15:46:00.001Z");
        Instant expiryDate3 = Instant.parse("2022-11-12T15:47:00.001Z");

        // When
        List<IngestTaskStatus> statuses = new IngestTaskStatusesBuilder()
                .taskStarted(taskId3, startTime3, expiryDate3)
                .taskStarted(taskId1, startTime1, expiryDate1)
                .taskStarted(taskId2, startTime2, expiryDate2)
                .build();

        // Then
        assertThat(statuses)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(
                        IngestTaskStatus.builder().taskId(taskId3).startTime(startTime3).expiryDate(expiryDate3).build(),
                        IngestTaskStatus.builder().taskId(taskId2).startTime(startTime2).expiryDate(expiryDate2).build(),
                        IngestTaskStatus.builder().taskId(taskId1).startTime(startTime1).expiryDate(expiryDate1).build());
    }

    @Test
    public void shouldIgnoreStatusIfStartedUpdateIsOmittedAsItMayHaveExpired() {
        // Given
        String taskId = "test-task";
        IngestTaskFinishedStatus finishedStatus = IngestTaskFinishedStatus.builder()
                .addJobSummary(new JobRunSummary(
                        new RecordsProcessed(300L, 200L),
                        Instant.parse("2022-10-12T15:45:01.001Z"),
                        Instant.parse("2022-10-12T15:46:01.001Z")))
                .finish(Instant.parse("2022-10-12T15:46:02.001Z"))
                .build();

        // When
        List<IngestTaskStatus> statuses = new IngestTaskStatusesBuilder()
                .taskFinished(taskId, finishedStatus)
                .build();

        // Then
        assertThat(statuses).isEmpty();
    }
}
