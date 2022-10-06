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
package sleeper.compaction.job;

import org.junit.Test;
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobFinishedStatus;
import sleeper.compaction.job.status.CompactionJobStartedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobTestDataHelper.DEFAULT_TASK_ID;

public class CompactionJobStatusInPeriodTest {

    private final CompactionJob job = new CompactionJobTestDataHelper().singleFileCompaction();
    private final CompactionJobSummary summary = new CompactionJobSummary(
            new CompactionJobRecordsProcessed(200L, 100L),
            Instant.parse("2022-09-23T11:00:00.000Z"), Instant.parse("2022-09-23T11:30:00.000Z"));

    @Test
    public void shouldBeInPeriodWithCreatedUpdateTime() {
        // Given
        Instant startTime = Instant.parse("2022-09-23T11:44:00.000Z");
        Instant updateTime = Instant.parse("2022-09-23T11:44:01.000Z");
        Instant endTime = Instant.parse("2022-09-23T11:44:02.000Z");
        CompactionJobStatus status = CompactionJobStatus.created(job, updateTime);

        // When / Then
        assertThat(status.isInPeriod(startTime, endTime)).isTrue();
    }

    @Test
    public void shouldBeBeforePeriodWithCreatedUpdateTime() {
        // Given
        Instant updateTime = Instant.parse("2022-09-23T11:44:00.000Z");
        Instant startTime = Instant.parse("2022-09-23T11:44:01.000Z");
        Instant endTime = Instant.parse("2022-09-23T11:44:02.000Z");
        CompactionJobStatus status = CompactionJobStatus.created(job, updateTime);

        // When / Then
        assertThat(status.isInPeriod(startTime, endTime)).isFalse();
    }

    @Test
    public void shouldBeAfterPeriodWithCreatedUpdateTime() {
        // Given
        Instant startTime = Instant.parse("2022-09-23T11:44:00.000Z");
        Instant endTime = Instant.parse("2022-09-23T11:44:01.000Z");
        Instant updateTime = Instant.parse("2022-09-23T11:44:02.000Z");
        CompactionJobStatus status = CompactionJobStatus.created(job, updateTime);

        // When / Then
        assertThat(status.isInPeriod(startTime, endTime)).isFalse();
    }

    @Test
    public void shouldOverlapPeriodWithStartedUpdateTime() {
        // Given
        Instant beforeTime = Instant.parse("2022-09-23T11:43:00.000Z");
        Instant startTime = Instant.parse("2022-09-23T11:44:00.000Z");
        Instant updateTime = Instant.parse("2022-09-23T11:44:01.000Z");
        Instant endTime = Instant.parse("2022-09-23T11:44:02.000Z");
        CompactionJobStatus status = CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, beforeTime))
                .startedStatus(CompactionJobStartedStatus.updateAndStartTimeWithTaskId(updateTime, beforeTime, DEFAULT_TASK_ID))
                .build();

        // When / Then
        assertThat(status.isInPeriod(startTime, endTime)).isTrue();
    }

    @Test
    public void shouldIncludePeriodWithStartedUpdateTime() {
        // Given
        Instant beforeTime = Instant.parse("2022-09-23T11:43:00.000Z");
        Instant startTime = Instant.parse("2022-09-23T11:44:00.000Z");
        Instant endTime = Instant.parse("2022-09-23T11:44:01.000Z");
        Instant updateTime = Instant.parse("2022-09-23T11:44:02.000Z");
        CompactionJobStatus status = CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, beforeTime))
                .startedStatus(CompactionJobStartedStatus.updateAndStartTimeWithTaskId(updateTime, beforeTime, DEFAULT_TASK_ID))
                .build();

        // When / Then
        assertThat(status.isInPeriod(startTime, endTime)).isTrue();
    }

    @Test
    public void shouldBeBeforePeriodWithStartedUpdateTime() {
        // Given
        Instant beforeTime = Instant.parse("2022-09-23T11:43:00.000Z");
        Instant updateTime = Instant.parse("2022-09-23T11:44:00.000Z");
        Instant startTime = Instant.parse("2022-09-23T11:44:01.000Z");
        Instant endTime = Instant.parse("2022-09-23T11:44:02.000Z");
        CompactionJobStatus status = CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, beforeTime))
                .startedStatus(CompactionJobStartedStatus.updateAndStartTimeWithTaskId(updateTime, beforeTime, DEFAULT_TASK_ID))
                .build();

        // When / Then
        assertThat(status.isInPeriod(startTime, endTime)).isFalse();
    }

    @Test
    public void shouldBeAfterPeriodWithStartedUpdateTime() {
        // Given
        Instant startTime = Instant.parse("2022-09-23T11:44:00.000Z");
        Instant endTime = Instant.parse("2022-09-23T11:44:01.000Z");
        Instant afterTime = Instant.parse("2022-09-23T11:44:02.000Z");
        Instant updateTime = Instant.parse("2022-09-23T11:44:03.000Z");
        CompactionJobStatus status = CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, afterTime))
                .startedStatus(CompactionJobStartedStatus.updateAndStartTimeWithTaskId(updateTime, afterTime, DEFAULT_TASK_ID))
                .build();

        // When / Then
        assertThat(status.isInPeriod(startTime, endTime)).isFalse();
    }

    @Test
    public void shouldOverlapPeriodWithFinishedUpdateTime() {
        // Given
        Instant beforeTime = Instant.parse("2022-09-23T11:43:00.000Z");
        Instant startTime = Instant.parse("2022-09-23T11:44:00.000Z");
        Instant updateTime = Instant.parse("2022-09-23T11:44:01.000Z");
        Instant endTime = Instant.parse("2022-09-23T11:44:02.000Z");
        CompactionJobStatus status = CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, beforeTime))
                .startedStatus(CompactionJobStartedStatus.updateAndStartTimeWithTaskId(beforeTime, beforeTime, DEFAULT_TASK_ID))
                .finishedStatus(CompactionJobFinishedStatus.updateTimeAndSummaryWithTaskId(updateTime, summary, DEFAULT_TASK_ID))
                .build();

        // When / Then
        assertThat(status.isInPeriod(startTime, endTime)).isTrue();
    }

    @Test
    public void shouldIncludePeriodWithFinishedUpdateTime() {
        // Given
        Instant beforeTime = Instant.parse("2022-09-23T11:43:00.000Z");
        Instant startTime = Instant.parse("2022-09-23T11:44:00.000Z");
        Instant endTime = Instant.parse("2022-09-23T11:44:01.000Z");
        Instant updateTime = Instant.parse("2022-09-23T11:44:02.000Z");
        CompactionJobStatus status = CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, beforeTime))
                .startedStatus(CompactionJobStartedStatus.updateAndStartTimeWithTaskId(beforeTime, beforeTime, DEFAULT_TASK_ID))
                .finishedStatus(CompactionJobFinishedStatus.updateTimeAndSummaryWithTaskId(updateTime, summary, DEFAULT_TASK_ID))
                .build();

        // When / Then
        assertThat(status.isInPeriod(startTime, endTime)).isTrue();
    }

    @Test
    public void shouldBeBeforePeriodWithFinishedUpdateTime() {
        // Given
        Instant beforeTime = Instant.parse("2022-09-23T11:43:00.000Z");
        Instant updateTime = Instant.parse("2022-09-23T11:44:00.000Z");
        Instant startTime = Instant.parse("2022-09-23T11:44:01.000Z");
        Instant endTime = Instant.parse("2022-09-23T11:44:02.000Z");
        CompactionJobStatus status = CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, beforeTime))
                .startedStatus(CompactionJobStartedStatus.updateAndStartTimeWithTaskId(beforeTime, beforeTime, DEFAULT_TASK_ID))
                .finishedStatus(CompactionJobFinishedStatus.updateTimeAndSummaryWithTaskId(updateTime, summary, DEFAULT_TASK_ID))
                .build();

        // When / Then
        assertThat(status.isInPeriod(startTime, endTime)).isFalse();
    }

    @Test
    public void shouldBeAfterPeriodWithFinishedUpdateTime() {
        // Given
        Instant startTime = Instant.parse("2022-09-23T11:44:00.000Z");
        Instant endTime = Instant.parse("2022-09-23T11:44:01.000Z");
        Instant afterTime = Instant.parse("2022-09-23T11:44:02.000Z");
        Instant updateTime = Instant.parse("2022-09-23T11:44:03.000Z");
        CompactionJobStatus status = CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, afterTime))
                .startedStatus(CompactionJobStartedStatus.updateAndStartTimeWithTaskId(afterTime, afterTime, DEFAULT_TASK_ID))
                .finishedStatus(CompactionJobFinishedStatus.updateTimeAndSummaryWithTaskId(updateTime, summary, DEFAULT_TASK_ID))
                .build();

        // When / Then
        assertThat(status.isInPeriod(startTime, endTime)).isFalse();
    }
}
