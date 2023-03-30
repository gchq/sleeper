/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.compaction.testutils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.task.CompactionTaskStatus;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.compaction.task.CompactionTaskStatusTestData.finishedStatusWithDefaultSummary;
import static sleeper.compaction.task.CompactionTaskStatusTestData.finishedStatusWithDefaults;
import static sleeper.compaction.task.CompactionTaskStatusTestData.startedStatusBuilder;
import static sleeper.compaction.task.CompactionTaskStatusTestData.startedStatusBuilderWithDefaults;

public class CompactionTaskStatusStoreInMemoryTest {
    private final CompactionTaskStatusStoreInMemory store = new CompactionTaskStatusStoreInMemory();

    @Nested
    @DisplayName("Store status updates")
    class StoreStatusUpdates {
        @Test
        void shouldStoreStartedTask() {
            // Given
            CompactionTaskStatus started = startedStatusBuilderWithDefaults().build();

            // When
            store.taskStarted(started);

            // Then
            assertThat(store.getAllTasks()).containsExactly(started);
        }

        @Test
        void shouldStoreFinishedTask() {
            // Given
            CompactionTaskStatus started = startedStatusBuilderWithDefaults().build();
            CompactionTaskStatus finished = finishedStatusWithDefaults();

            // When
            store.taskStarted(started);
            store.taskFinished(finished);

            // Then
            assertThat(store.getAllTasks()).containsExactly(finished);
        }

        @Test
        public void shouldRefuseSameTaskStartedMultipleTimes() {
            // Given
            CompactionTaskStatus started = startedStatusBuilderWithDefaults().build();

            // When
            store.taskStarted(started);

            // Then
            assertThatThrownBy(() -> store.taskStarted(started))
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        public void shouldRefuseTaskFinishedButNotStarted() {
            // Given
            CompactionTaskStatus finished = finishedStatusWithDefaults();

            // When/Then
            assertThatThrownBy(() -> store.taskFinished(finished))
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        public void shouldRefuseFinishedTaskReportedAsStarted() {
            // Given
            CompactionTaskStatus finished = finishedStatusWithDefaults();

            // When/Then
            assertThatThrownBy(() -> store.taskStarted(finished))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    @DisplayName("Get all tasks")
    class GetAllTasks {
        @Test
        void shouldGetMultipleTasks() {
            // Given
            CompactionTaskStatus started1 = startedStatusBuilder(Instant.parse("2023-03-30T11:44:00Z"))
                    .taskId("test-task-1").build();
            CompactionTaskStatus finished1 = finishedStatusWithDefaultSummary("test-task-1",
                    Instant.parse("2023-03-30T11:44:00Z"), Instant.parse("2023-03-30T12:00:00Z"));
            CompactionTaskStatus started2 = startedStatusBuilder(Instant.parse("2023-03-30T11:45:00Z"))
                    .taskId("test-task-2").build();
            CompactionTaskStatus finished2 = finishedStatusWithDefaultSummary("test-task-2",
                    Instant.parse("2023-03-30T12:44:00Z"), Instant.parse("2023-03-30T13:00:00Z"));

            // When
            store.taskStarted(started1);
            store.taskFinished(finished1);
            store.taskStarted(started2);
            store.taskFinished(finished2);

            // Then
            assertThat(store.getAllTasks())
                    .containsExactly(finished2, finished1);
        }

        @Test
        void shouldGetNoTasks() {
            // When/Then
            assertThat(store.getAllTasks()).isEmpty();
        }
    }
}
