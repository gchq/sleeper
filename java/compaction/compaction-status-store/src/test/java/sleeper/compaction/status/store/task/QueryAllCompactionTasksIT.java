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

import sleeper.compaction.status.store.testutils.DynamoDBCompactionTaskStatusStoreTestBase;
import sleeper.core.tracker.compaction.task.CompactionTaskStatus;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryAllCompactionTasksIT extends DynamoDBCompactionTaskStatusStoreTestBase {

    @Test
    public void shouldReportMultipleCompactionTasksSortedMostRecentFirst() {
        // Given
        CompactionTaskStatus task1 = taskWithStartTime(
                Instant.parse("2022-10-06T11:18:00.001Z"));
        CompactionTaskStatus task2 = taskWithStartAndFinishTime(
                Instant.parse("2022-10-06T11:19:00.001Z"),
                Instant.parse("2022-10-06T11:21:00.001Z"));
        CompactionTaskStatus task3 = taskWithStartTime(
                Instant.parse("2022-10-06T11:20:00.001Z"));
        CompactionTaskStatus task4 = taskWithStartAndFinishTime(
                Instant.parse("2022-10-06T11:21:00.001Z"),
                Instant.parse("2022-10-06T11:22:00.001Z"));

        // When
        tracker.taskStarted(task1);
        tracker.taskStarted(task2);
        tracker.taskStarted(task3);
        tracker.taskFinished(task2);
        tracker.taskStarted(task4);
        tracker.taskFinished(task4);

        // Then
        assertThat(tracker.getAllTasks())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_EXPIRY_DATE)
                .containsExactly(task4, task3, task2, task1);
    }

    @Test
    public void shouldReportNoCompactionTasks() {
        // When / Then
        assertThat(tracker.getAllTasks()).isEmpty();
    }
}
