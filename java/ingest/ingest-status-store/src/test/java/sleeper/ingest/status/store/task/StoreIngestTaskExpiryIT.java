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
import sleeper.ingest.core.task.IngestTaskStatusStore;
import sleeper.ingest.status.store.testutils.DynamoDBIngestTaskStatusStoreTestBase;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoField;

import static org.assertj.core.api.Assertions.assertThat;

public class StoreIngestTaskExpiryIT extends DynamoDBIngestTaskStatusStoreTestBase {

    @Test
    public void shouldUpdateExpiryDateForCompactionTaskStatusStarted() {
        // Given
        IngestTaskStatus taskStatus = startedTaskWithDefaults();
        Duration timeToLive = Duration.ofDays(7);
        IngestTaskStatusStore store = storeWithTimeToLiveAndUpdateTimes(timeToLive, defaultTaskStartTime());

        // When
        store.taskStarted(taskStatus);

        // Then
        assertThat(store.getTask(taskStatus.getTaskId()).getExpiryDate())
                .isEqualTo(timePlusDurationAsExpiry(defaultTaskStartTime(), timeToLive));
    }

    @Test
    public void shouldUpdateExpiryDateForCompactionTaskStatusFinished() {
        // Given
        IngestTaskStatus taskStatus = finishedTaskWithDefaults();
        Duration timeToLive = Duration.ofDays(7);
        IngestTaskStatusStore store = storeWithTimeToLiveAndUpdateTimes(timeToLive,
                defaultTaskStartTime(), defaultTaskFinishTime());

        // When
        store.taskStarted(taskStatus);
        store.taskFinished(taskStatus);

        // Then
        assertThat(store.getTask(taskStatus.getTaskId()).getExpiryDate())
                .isEqualTo(timePlusDurationAsExpiry(defaultTaskStartTime(), timeToLive));
    }

    @Test
    public void shouldUpdateDifferentExpiryDateForCompactionTaskStatusStarted() {
        // Given
        IngestTaskStatus taskStatus = startedTaskWithDefaults();
        Duration timeToLive = Duration.ofDays(1);
        IngestTaskStatusStore store = storeWithTimeToLiveAndUpdateTimes(timeToLive, defaultTaskStartTime());

        // When
        store.taskStarted(taskStatus);

        // Then
        assertThat(store.getTask(taskStatus.getTaskId()).getExpiryDate())
                .isEqualTo(timePlusDurationAsExpiry(defaultTaskStartTime(), timeToLive));
    }

    private static Instant timePlusDurationAsExpiry(Instant time, Duration timeToLive) {
        return time.plus(timeToLive).with(ChronoField.MILLI_OF_SECOND, 0);
    }
}
