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

public class QueryIngestTasksInProgressIT extends DynamoDBIngestTaskStatusStoreTestBase {

    @Test
    public void shouldIncludeUnfinishedTask() {
        // Given
        IngestTaskStatus task = startedTaskWithDefaults();

        // When
        store.taskStarted(task);

        // Then
        assertThat(store.getTasksInProgress())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_EXPIRY_DATE)
                .containsExactly(task);
    }

    @Test
    public void shouldExcludeFinishedTask() {
        // Given
        IngestTaskStatus task = finishedTaskWithDefaults();

        // When
        store.taskStarted(task);
        store.taskFinished(task);

        // Then
        assertThat(store.getTasksInProgress()).isEmpty();
    }

    @Test
    public void shouldSortByStartTimeMostRecentFirst() {
        // Given
        IngestTaskStatus task1 = startedTaskWithDefaultsBuilder()
                .startTime(Instant.parse("2022-10-06T11:19:00.001Z")).build();
        IngestTaskStatus task2 = startedTaskWithDefaultsBuilder()
                .startTime(Instant.parse("2022-10-06T11:19:10.001Z")).build();

        // When
        store.taskStarted(task1);
        store.taskStarted(task2);

        // Then
        assertThat(store.getTasksInProgress())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_EXPIRY_DATE)
                .containsExactly(task2, task1);
    }
}
