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

package sleeper.core.tracker.compaction.task;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.tracker.compaction.task.CompactionTaskStatusTestData.finishedStatusWithDefaultSummary;
import static sleeper.core.tracker.compaction.task.CompactionTaskStatusTestData.finishedStatusWithDefaults;
import static sleeper.core.tracker.compaction.task.CompactionTaskStatusTestData.startedStatusBuilder;
import static sleeper.core.tracker.compaction.task.CompactionTaskStatusTestData.startedStatusBuilderWithDefaults;

public class InMemoryCompactionTaskStatusStoreTest {
    private final InMemoryCompactionTaskStatusStore store = new InMemoryCompactionTaskStatusStore();

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
        void shouldRefuseSameTaskStartedMultipleTimes() {
            // Given
            CompactionTaskStatus started = startedStatusBuilderWithDefaults().build();

            // When
            store.taskStarted(started);

            // Then
            assertThatThrownBy(() -> store.taskStarted(started))
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        void shouldRefuseTaskFinishedButNotStarted() {
            // Given
            CompactionTaskStatus finished = finishedStatusWithDefaults();

            // When/Then
            assertThatThrownBy(() -> store.taskFinished(finished))
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        void shouldRefuseFinishedTaskReportedAsStarted() {
            // Given
            CompactionTaskStatus finished = finishedStatusWithDefaults();

            // When/Then
            assertThatThrownBy(() -> store.taskStarted(finished))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    @DisplayName("Get task by ID")
    class GetTaskById {

        @Test
        void shouldGetTaskById() {
            // Given
            CompactionTaskStatus started = startedStatusBuilderWithDefaults()
                    .taskId("some-test-task-id").build();

            // When
            store.taskStarted(started);

            // Then
            assertThat(store.getTask("some-test-task-id")).isEqualTo(started);
        }

        @Test
        void shouldGetNoTaskById() {
            // Given
            CompactionTaskStatus started = startedStatusBuilderWithDefaults()
                    .taskId("some-test-task-id").build();

            // When
            store.taskStarted(started);

            // Then
            assertThat(store.getTask("other-test-task-id")).isNull();
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
            CompactionTaskStatus started2 = startedStatusBuilder(Instant.parse("2023-03-30T12:44:00Z"))
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

    @DisplayName("Get unfinished tasks")
    @Nested
    class GetUnfinishedTasks {
        @Test
        void shouldGetUnfinishedTask() {
            // Given
            CompactionTaskStatus started = startedStatusBuilderWithDefaults().build();

            // When
            store.taskStarted(started);

            // Then
            assertThat(store.getTasksInProgress())
                    .containsExactly(started);
        }

        @Test
        void shouldGetMultipleTasks() {
            // Given
            CompactionTaskStatus started1 = startedStatusBuilderWithDefaults()
                    .taskId("test-task-1").build();
            CompactionTaskStatus started2 = startedStatusBuilderWithDefaults()
                    .taskId("test-task-2").build();

            // When
            store.taskStarted(started1);
            store.taskStarted(started2);

            // Then
            assertThat(store.getTasksInProgress())
                    .containsExactly(started2, started1);
        }

        @Test
        void shouldGetNoTasksWhenTasksAreFinished() {
            // Given
            CompactionTaskStatus started1 = startedStatusBuilder(Instant.parse("2023-03-30T11:44:00Z"))
                    .taskId("test-task-1").build();
            CompactionTaskStatus finished1 = finishedStatusWithDefaultSummary("test-task-1",
                    Instant.parse("2023-03-30T11:44:00Z"), Instant.parse("2023-03-30T12:00:00Z"));

            // When
            store.taskStarted(started1);
            store.taskFinished(finished1);

            // Then
            assertThat(store.getTasksInProgress()).isEmpty();
        }

        @Test
        void shouldGetNoTasksWhenNoneInStore() {
            // When/Then
            assertThat(store.getTasksInProgress()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Get tasks in time period")
    class GetTasksInTimePeriod {

        @Test
        void shouldGetTaskInPeriod() {
            // Given
            CompactionTaskStatus started1 = startedStatusBuilder(Instant.parse("2023-03-30T11:44:00Z"))
                    .taskId("test-task-1").build();
            CompactionTaskStatus finished1 = finishedStatusWithDefaultSummary("test-task-1",
                    Instant.parse("2023-03-30T11:44:00Z"), Instant.parse("2023-03-30T12:00:00Z"));
            CompactionTaskStatus started2 = startedStatusBuilder(Instant.parse("2023-03-30T12:44:00Z"))
                    .taskId("test-task-2").build();
            CompactionTaskStatus finished2 = finishedStatusWithDefaultSummary("test-task-2",
                    Instant.parse("2023-03-30T12:44:00Z"), Instant.parse("2023-03-30T13:00:00Z"));

            // When
            store.taskStarted(started1);
            store.taskFinished(finished1);
            store.taskStarted(started2);
            store.taskFinished(finished2);

            // Then
            assertThat(store.getTasksInTimePeriod(
                    Instant.parse("2023-03-30T12:30:00Z"),
                    Instant.parse("2023-03-30T13:30:00Z")))
                    .containsExactly(finished2);
        }

        @Test
        void shouldGetMultipleTasks() {
            // Given
            CompactionTaskStatus started1 = startedStatusBuilder(Instant.parse("2023-03-30T11:44:00Z"))
                    .taskId("test-task-1").build();
            CompactionTaskStatus finished1 = finishedStatusWithDefaultSummary("test-task-1",
                    Instant.parse("2023-03-30T11:44:00Z"), Instant.parse("2023-03-30T12:00:00Z"));
            CompactionTaskStatus started2 = startedStatusBuilder(Instant.parse("2023-03-30T12:44:00Z"))
                    .taskId("test-task-2").build();
            CompactionTaskStatus finished2 = finishedStatusWithDefaultSummary("test-task-2",
                    Instant.parse("2023-03-30T12:44:00Z"), Instant.parse("2023-03-30T13:00:00Z"));

            // When
            store.taskStarted(started1);
            store.taskFinished(finished1);
            store.taskStarted(started2);
            store.taskFinished(finished2);

            // Then
            assertThat(store.getTasksInTimePeriod(
                    Instant.parse("2023-03-30T11:30:00Z"),
                    Instant.parse("2023-03-30T13:30:00Z")))
                    .containsExactly(finished2, finished1);
        }

        @Test
        void shouldGetNoTasksInPeriod() {
            // Given
            CompactionTaskStatus started = startedStatusBuilder(
                    Instant.parse("2023-03-30T13:44:00Z"))
                    .taskId("test-task").build();

            // When
            store.taskStarted(started);

            // Then
            assertThat(store.getTasksInTimePeriod(
                    Instant.parse("2023-03-30T12:30:00Z"),
                    Instant.parse("2023-03-30T13:30:00Z")))
                    .isEmpty();
        }
    }
}
