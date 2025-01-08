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
package sleeper.compaction.status.store.task;

import org.junit.jupiter.api.Test;

import sleeper.compaction.status.store.testutils.DynamoDBCompactionTaskTrackerTestBase;
import sleeper.core.tracker.compaction.task.CompactionTaskStatus;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryCompactionTasksByPeriodIT extends DynamoDBCompactionTaskTrackerTestBase {

    @Test
    public void shouldReportCompactionTaskFinished() {
        // Given
        CompactionTaskStatus task = taskWithStartAndFinishTime(
                Instant.parse("2022-10-06T11:19:00.001Z"),
                Instant.parse("2022-10-06T11:19:31.001Z"));
        Instant periodStart = Instant.parse("2022-10-06T11:00:00.000Z");
        Instant periodEnd = Instant.parse("2022-10-06T12:00:00.000Z");

        // When
        tracker.taskStarted(task);
        tracker.taskFinished(task);

        // Then
        assertThat(tracker.getTasksInTimePeriod(periodStart, periodEnd))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_EXPIRY_DATE)
                .containsExactly(task);
    }

    @Test
    public void shouldExcludeCompactionTaskOutsidePeriod() {
        // Given
        CompactionTaskStatus task = taskWithStartAndFinishTime(
                Instant.parse("2022-10-06T11:19:00.001Z"),
                Instant.parse("2022-10-06T11:19:31.001Z"));
        Instant periodStart = Instant.parse("2022-10-06T10:00:00.000Z");
        Instant periodEnd = Instant.parse("2022-10-06T11:00:00.000Z");

        // When
        tracker.taskStarted(task);
        tracker.taskFinished(task);

        // Then
        assertThat(tracker.getTasksInTimePeriod(periodStart, periodEnd)).isEmpty();
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
        Instant periodStart = Instant.parse("2022-10-06T11:00:00.000Z");
        Instant periodEnd = Instant.parse("2022-10-06T12:00:00.000Z");

        // When
        tracker.taskStarted(task1);
        tracker.taskFinished(task1);
        tracker.taskStarted(task2);
        tracker.taskFinished(task2);

        // Then
        assertThat(tracker.getTasksInTimePeriod(periodStart, periodEnd))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_EXPIRY_DATE)
                .containsExactly(task2, task1);
    }
}
