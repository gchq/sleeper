/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.compaction.trackerv2.task;

import org.junit.jupiter.api.Test;

import sleeper.compaction.trackerv2.testutils.DynamoDBCompactionTaskTrackerTestBase;
import sleeper.core.tracker.compaction.task.CompactionTaskStatus;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoField;

import static org.assertj.core.api.Assertions.assertThat;

public class StoreCompactionTaskExpiryIT extends DynamoDBCompactionTaskTrackerTestBase {

    @Test
    public void shouldUpdateExpiryDateForCompactionTaskStatusStarted() {
        // Given
        CompactionTaskStatus taskStatus = startedTaskWithDefaults();
        Duration timeToLive = Duration.ofDays(7);
        CompactionTaskTracker tracker = trackerWithTimeToLiveAndUpdateTimes(timeToLive, defaultTaskStartTime());

        // When
        tracker.taskStarted(taskStatus);

        // Then
        assertThat(tracker.getTask(taskStatus.getTaskId()).getExpiryDate())
                .isEqualTo(timePlusDurationAsExpiry(defaultTaskStartTime(), timeToLive));
    }

    @Test
    public void shouldUpdateExpiryDateForCompactionTaskStatusFinished() {
        // Given
        CompactionTaskStatus taskStatus = finishedTaskWithDefaults();
        Duration timeToLive = Duration.ofDays(7);
        CompactionTaskTracker tracker = trackerWithTimeToLiveAndUpdateTimes(timeToLive,
                defaultTaskStartTime(), defaultTaskFinishTime());

        // When
        tracker.taskStarted(taskStatus);
        tracker.taskFinished(taskStatus);

        // Then
        assertThat(tracker.getTask(taskStatus.getTaskId()).getExpiryDate())
                .isEqualTo(timePlusDurationAsExpiry(defaultTaskStartTime(), timeToLive));
    }

    @Test
    public void shouldUpdateDifferentExpiryDateForCompactionTaskStatusStarted() {
        // Given
        CompactionTaskStatus taskStatus = startedTaskWithDefaults();
        Duration timeToLive = Duration.ofDays(1);
        CompactionTaskTracker tracker = trackerWithTimeToLiveAndUpdateTimes(timeToLive, defaultTaskStartTime());

        // When
        tracker.taskStarted(taskStatus);

        // Then
        assertThat(tracker.getTask(taskStatus.getTaskId()).getExpiryDate())
                .isEqualTo(timePlusDurationAsExpiry(defaultTaskStartTime(), timeToLive));
    }

    private static Instant timePlusDurationAsExpiry(Instant time, Duration timeToLive) {
        return time.plus(timeToLive).with(ChronoField.MILLI_OF_SECOND, 0);
    }
}
