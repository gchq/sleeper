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
package sleeper.ingest.status.store.task;

import org.junit.jupiter.api.Test;

import sleeper.ingest.core.task.IngestTaskStatus;
import sleeper.ingest.status.store.testutils.DynamoDBIngestTaskStatusStoreTestBase;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryAllIngestTasksIT extends DynamoDBIngestTaskStatusStoreTestBase {

    @Test
    public void shouldReportMultipleIngestTasksSortedMostRecentFirst() {
        // Given
        IngestTaskStatus task1 = taskWithStartTime(
                Instant.parse("2022-10-06T11:18:00.001Z"));
        IngestTaskStatus task2 = taskWithStartAndFinishTime(
                Instant.parse("2022-10-06T11:19:00.001Z"),
                Instant.parse("2022-10-06T11:21:00.001Z"));
        IngestTaskStatus task3 = taskWithStartTime(
                Instant.parse("2022-10-06T11:20:00.001Z"));
        IngestTaskStatus task4 = taskWithStartAndFinishTime(
                Instant.parse("2022-10-06T11:21:00.001Z"),
                Instant.parse("2022-10-06T11:22:00.001Z"));

        // When
        store.taskStarted(task1);
        store.taskStarted(task2);
        store.taskStarted(task3);
        store.taskFinished(task2);
        store.taskStarted(task4);
        store.taskFinished(task4);

        // Then
        assertThat(store.getAllTasks())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_EXPIRY_DATE)
                .containsExactly(task4, task3, task2, task1);
    }

    @Test
    public void shouldReportNoIngestTasks() {
        // When / Then
        assertThat(store.getAllTasks()).isEmpty();
    }
}
