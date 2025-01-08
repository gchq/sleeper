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
package sleeper.core.tracker.ingest.task;

import org.junit.jupiter.api.Test;

import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Instant;
import java.time.Period;

import static org.assertj.core.api.Assertions.assertThat;

public class IngestTaskStatusInPeriodTest {
    private static final Instant EPOCH_START = Instant.ofEpochMilli(0);
    private static final Instant FAR_FUTURE = EPOCH_START.plus(Period.ofDays(999999999));

    @Test
    public void shouldBeInPeriodWithStartTimeOnly() {
        // Given
        IngestTaskStatus task = taskWithStartTime(
                Instant.parse("2022-10-06T11:19:00.001Z"));

        // When / Then
        assertThat(task.isInPeriod(EPOCH_START, FAR_FUTURE)).isTrue();
    }

    @Test
    public void shouldNotBeInPeriodWithStartTimeOnlyWhenEndIsStartTime() {
        // Given
        Instant startTime = Instant.parse("2022-10-06T11:19:00.001Z");
        IngestTaskStatus task = taskWithStartTime(startTime);

        // When / Then
        assertThat(task.isInPeriod(EPOCH_START, startTime)).isFalse();
    }

    @Test
    public void shouldBeInPeriodWhenUnfinishedTaskStartsAtWindowStartTime() {
        // Given
        Instant startTime = Instant.parse("2022-10-06T11:19:00.001Z");
        IngestTaskStatus task = taskWithStartTime(startTime);

        // When / Then
        assertThat(task.isInPeriod(startTime, FAR_FUTURE)).isTrue();
    }

    @Test
    public void shouldBeInPeriodWithStartAndFinishTime() {
        // Given
        IngestTaskStatus task = taskWithStartAndFinishTime(
                Instant.parse("2022-10-06T11:19:00.001Z"),
                Instant.parse("2022-10-06T11:19:30.001Z"));

        // When / Then
        assertThat(task.isInPeriod(EPOCH_START, FAR_FUTURE)).isTrue();
    }

    @Test
    public void shouldNotBeInPeriodWhenEndIsStartTime() {
        // Given
        Instant startTime = Instant.parse("2022-10-06T11:19:00.001Z");
        Instant finishTime = Instant.parse("2022-10-06T11:19:31.001Z");
        IngestTaskStatus task = taskWithStartAndFinishTime(startTime, finishTime);

        // When / Then
        assertThat(task.isInPeriod(EPOCH_START, startTime)).isFalse();
    }

    @Test
    public void shouldBeInPeriodWhenEndIsFinishTime() {
        // Given
        Instant startTime = Instant.parse("2022-10-06T11:19:00.001Z");
        Instant finishTime = Instant.parse("2022-10-06T11:19:31.001Z");
        IngestTaskStatus task = taskWithStartAndFinishTime(startTime, finishTime);

        // When / Then
        assertThat(task.isInPeriod(EPOCH_START, finishTime)).isTrue();
    }

    @Test
    public void shouldNotBeInPeriodWhenStartIsFinishTime() {
        // Given
        Instant startTime = Instant.parse("2022-10-06T11:19:00.001Z");
        Instant finishTime = Instant.parse("2022-10-06T11:19:31.001Z");
        IngestTaskStatus task = taskWithStartAndFinishTime(startTime, finishTime);

        // When / Then
        assertThat(task.isInPeriod(finishTime, FAR_FUTURE)).isFalse();
    }

    @Test
    public void shouldBeInPeriodWhenStartIsStartTime() {
        // Given
        Instant startTime = Instant.parse("2022-10-06T11:19:00.001Z");
        Instant finishTime = Instant.parse("2022-10-06T11:19:31.001Z");
        IngestTaskStatus task = taskWithStartAndFinishTime(startTime, finishTime);

        // When / Then
        assertThat(task.isInPeriod(startTime, FAR_FUTURE)).isTrue();
    }

    @Test
    public void shouldBeInPeriodWhenStartedBeforePeriodAndStillRunning() {
        // Given
        Instant startTime = Instant.parse("2022-10-06T11:00:00.001Z");
        Instant periodStart = Instant.parse("2022-10-06T11:10:00.000Z");
        Instant periodEnd = Instant.parse("2022-10-06T12:00:00.000Z");

        // When / Then
        assertThat(taskWithStartTime(startTime).isInPeriod(periodStart, periodEnd)).isTrue();
    }

    @Test
    public void shouldNotBeInPeriodWhenStartedAfterPeriodAndStillRunning() {
        // Given
        Instant periodStart = Instant.parse("2022-10-06T11:00:00.000Z");
        Instant periodEnd = Instant.parse("2022-10-06T12:00:00.000Z");
        Instant startTime = Instant.parse("2022-10-06T12:10:00.001Z");

        // When / Then
        assertThat(taskWithStartTime(startTime).isInPeriod(periodStart, periodEnd)).isFalse();
    }

    private static IngestTaskStatus taskWithStartTime(Instant startTime) {
        return taskBuilder(startTime).build();
    }

    private static IngestTaskStatus taskWithStartAndFinishTime(Instant startTime, Instant finishTime) {
        return taskBuilder(startTime)
                .finished(finishTime, IngestTaskFinishedStatus.builder()
                        .addJobSummary(new RecordsProcessedSummary(
                                new RecordsProcessed(200, 100),
                                startTime, finishTime)))
                .build();
    }

    private static IngestTaskStatus.Builder taskBuilder(Instant startTime) {
        return IngestTaskStatus.builder().taskId("test-task-id").startTime(startTime);
    }
}
