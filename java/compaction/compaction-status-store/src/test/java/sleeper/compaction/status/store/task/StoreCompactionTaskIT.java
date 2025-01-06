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

import static org.assertj.core.api.Assertions.assertThat;

public class StoreCompactionTaskIT extends DynamoDBCompactionTaskStatusStoreTestBase {
    @Test
    public void shouldReportCompactionTaskStarted() {
        // Given
        CompactionTaskStatus taskStatus = startedTaskWithDefaults();

        // When
        tracker.taskStarted(taskStatus);

        // Then
        assertThat(tracker.getTask(taskStatus.getTaskId()))
                .usingRecursiveComparison(IGNORE_EXPIRY_DATE)
                .isEqualTo(taskStatus);
    }

    @Test
    public void shouldReportCompactionTaskFinished() {
        // Given
        CompactionTaskStatus taskStatus = finishedTaskWithDefaults();

        // When
        tracker.taskStarted(taskStatus);
        tracker.taskFinished(taskStatus);

        // Then
        assertThat(tracker.getTask(taskStatus.getTaskId()))
                .usingRecursiveComparison(IGNORE_EXPIRY_DATE)
                .isEqualTo(taskStatus);
    }

    @Test
    public void shouldReportCompactionTaskFinishedWithDurationInSecondsNotAWholeNumber() {
        // Given
        CompactionTaskStatus taskStatus = finishedTaskWithDefaultsAndDurationInSecondsNotAWholeNumber();

        // When
        tracker.taskStarted(taskStatus);
        tracker.taskFinished(taskStatus);

        // Then
        assertThat(tracker.getTask(taskStatus.getTaskId()))
                .usingRecursiveComparison(IGNORE_EXPIRY_DATE)
                .isEqualTo(taskStatus);
    }

    @Test
    public void shouldReportNoCompactionTaskExistsInStore() {
        // Given
        CompactionTaskStatus taskStatus = startedTaskWithDefaults();

        // When/Then
        assertThat(tracker.getTask(taskStatus.getTaskId()))
                .usingRecursiveComparison(IGNORE_EXPIRY_DATE)
                .isNull();
    }

}
