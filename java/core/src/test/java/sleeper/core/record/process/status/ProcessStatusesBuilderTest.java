/*
 * Copyright 2022 Crown Copyright
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

package sleeper.core.record.process.status;

import org.junit.Test;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcessStatusesBuilderTest {
    private static final String TEST_JOB_ID_1 = "test-job-1";
    private static final String TEST_JOB_ID_2 = "test-job-2";
    private static final String TEST_TASK_ID = "test-task";
    private static final Instant TEST_EXPIRY_DATE = Instant.parse("2022-09-25T09:23:30.012Z");

    @Test
    public void shouldBuildProcessRunListFromIndividualUpdateRecordsWithLatestRunFirst() {
        // Given
        ProcessStartedStatus started1 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-23T09:23:30.012Z"),
                Instant.parse("2022-09-23T09:23:30.001Z"));
        ProcessFinishedStatus finished1 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-23T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(200L, 100L),
                        Instant.parse("2022-09-23T09:23:30.001Z"),
                        Instant.parse("2022-09-23T09:24:00.001Z")));
        ProcessStartedStatus started2 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessFinishedStatus finished2 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-24T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-24T09:23:30.001Z"),
                        Instant.parse("2022-09-24T09:24:00.001Z")));

        ProcessStatusesBuilder processStatusesBuilder = new ProcessStatusesBuilder();
        List<ProcessStatusUpdate> updateList = Arrays.asList(started1, finished1, started2, finished2);

        // When
        updateList.stream()
                .map(update -> new ProcessStatusUpdateRecord(TEST_JOB_ID_1, TEST_EXPIRY_DATE, update, TEST_TASK_ID))
                .forEach(processStatusesBuilder::processUpdate);

        // Then
        assertThat(processStatusesBuilder.buildRunList(TEST_JOB_ID_1))
                .containsExactly(
                        ProcessRun.builder()
                                .taskId(TEST_TASK_ID)
                                .startedStatus(started2)
                                .finishedStatus(finished2)
                                .build(),
                        ProcessRun.builder()
                                .taskId(TEST_TASK_ID)
                                .startedStatus(started1)
                                .finishedStatus(finished1)
                                .build());
    }

    @Test
    public void shouldBuildProcessRunListFromIndividualUpdateRecordsForSpecificJob() {
        // Given
        ProcessStartedStatus started1 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-23T09:23:30.012Z"),
                Instant.parse("2022-09-23T09:23:30.001Z"));
        ProcessFinishedStatus finished1 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-23T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(200L, 100L),
                        Instant.parse("2022-09-23T09:23:30.001Z"),
                        Instant.parse("2022-09-23T09:24:00.001Z")));
        ProcessStartedStatus started2 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessFinishedStatus finished2 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-24T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-24T09:23:30.001Z"),
                        Instant.parse("2022-09-24T09:24:00.001Z")));

        ProcessStatusesBuilder processStatusesBuilder = new ProcessStatusesBuilder();
        List<ProcessStatusUpdate> updateListJob1 = Arrays.asList(started1, finished1);
        List<ProcessStatusUpdate> updateListJob2 = Arrays.asList(started2, finished2);

        // When
        updateListJob1.stream()
                .map(update -> new ProcessStatusUpdateRecord(TEST_JOB_ID_1, TEST_EXPIRY_DATE, update, TEST_TASK_ID))
                .forEach(processStatusesBuilder::processUpdate);
        updateListJob2.stream()
                .map(update -> new ProcessStatusUpdateRecord(TEST_JOB_ID_2, TEST_EXPIRY_DATE, update, TEST_TASK_ID))
                .forEach(processStatusesBuilder::processUpdate);

        // Then
        assertThat(processStatusesBuilder.buildRunList(TEST_JOB_ID_2))
                .containsExactly(
                        ProcessRun.builder()
                                .taskId(TEST_TASK_ID)
                                .startedStatus(started2)
                                .finishedStatus(finished2)
                                .build());
    }
}
