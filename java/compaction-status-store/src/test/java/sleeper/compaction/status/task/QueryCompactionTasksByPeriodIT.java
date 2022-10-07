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
package sleeper.compaction.status.task;

import org.junit.Test;
import sleeper.compaction.job.CompactionJobRecordsProcessed;
import sleeper.compaction.job.CompactionJobSummary;
import sleeper.compaction.status.testutils.DynamoDBCompactionTaskStatusStoreTestBase;
import sleeper.compaction.task.CompactionTaskFinishedStatus;
import sleeper.compaction.task.CompactionTaskStatus;

import java.time.Instant;
import java.time.Period;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryCompactionTasksByPeriodIT extends DynamoDBCompactionTaskStatusStoreTestBase {

    @Test
    public void shouldReportCompactionTaskFinished() {
        // Given
        CompactionTaskStatus task = taskWithStartAndFinishTime(
                Instant.parse("2022-10-06T11:19:00.001Z"),
                Instant.parse("2022-10-06T11:19:31.001Z"));

        // When
        store.taskStarted(task);
        store.taskFinished(task);

        // Then
        Instant epochStart = Instant.ofEpochMilli(0);
        Instant farFuture = epochStart.plus(Period.ofDays(999999999));
        assertThat(store.getTasksInTimePeriod(epochStart, farFuture))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_EXPIRY_DATE)
                .containsExactly(task);
    }

    @Test
    public void shouldExcludeCompactionTaskOutsidePeriod() {
        // Given
        CompactionTaskStatus task = taskWithStartAndFinishTime(
                Instant.parse("2022-10-06T11:19:00.001Z"),
                Instant.parse("2022-10-06T11:19:31.001Z"));

        // When
        store.taskStarted(task);
        store.taskFinished(task);

        // Then
        Instant epochStart = Instant.ofEpochMilli(0);
        Instant earlyEpoch = epochStart.plus(Period.ofDays(1));
        assertThat(store.getTasksInTimePeriod(epochStart, earlyEpoch)).isEmpty();
    }

    @Test
    public void shouldSortByStartTimeMostRecentFirst() {
        // Given
        CompactionTaskStatus task1 = taskWithStartAndFinishTime(
                Instant.parse("2022-10-06T11:19:00.001Z"),
                Instant.parse("2022-10-06T11:19:31.001Z"));
        CompactionTaskStatus task2 = taskWithStartAndFinishTime(
                Instant.parse("2022-10-06T11:19:10.001Z"),
                Instant.parse("2022-10-06T11:19:29.001Z"));

        // When
        store.taskStarted(task1);
        store.taskFinished(task1);
        store.taskStarted(task2);
        store.taskFinished(task2);

        // Then
        assertThat(store.getTasksInTimePeriod(
                Instant.parse("2022-10-06T11:19:00.000Z"),
                Instant.parse("2022-10-06T11:20:00.000Z")))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_EXPIRY_DATE)
                .containsExactly(task2, task1);
    }

    private CompactionTaskStatus taskWithStartAndFinishTime(Instant startTime, Instant finishTime) {
        return CompactionTaskStatus.started(startTime)
                .finished(CompactionTaskFinishedStatus.builder()
                        .addJobSummary(new CompactionJobSummary(
                                new CompactionJobRecordsProcessed(200, 100),
                                startTime, finishTime
                        )), finishTime)
                .build();
    }
}
