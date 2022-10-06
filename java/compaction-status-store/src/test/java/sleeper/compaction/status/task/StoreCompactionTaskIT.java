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
import sleeper.compaction.task.CompactionTaskStatus;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class StoreCompactionTaskIT extends DynamoDBCompactionTaskStatusStoreTestBase {
    @Test
    public void shouldReportCompactionTaskStarted() {
        // Given
        CompactionTaskStatus taskStatus = startedTaskWithDefaults();

        // When
        store.taskStarted(taskStatus);

        // Then
        assertThat(store.getTask(taskStatus.getTaskId()))
                .usingRecursiveComparison(IGNORE_UPDATE_TIMES)
                .isEqualTo(taskStatus);
    }

    @Test
    public void shouldReportCompactionTaskFinished() {
        // Given
        CompactionTaskStatus taskStatus = finishedTaskWithDefaults(createJobSummary());

        // When
        store.taskStarted(taskStatus);
        store.taskFinished(taskStatus);

        // Then
        assertThat(store.getTask(taskStatus.getTaskId()))
                .usingRecursiveComparison(IGNORE_UPDATE_TIMES)
                .isEqualTo(taskStatus);
    }

    @Test
    public void shouldReportNoCompactionTaskExistsInStore() {
        // Given
        CompactionTaskStatus taskStatus = startedTaskWithDefaults();

        // When/Then
        assertThat(store.getTask(taskStatus.getTaskId()))
                .usingRecursiveComparison(IGNORE_UPDATE_TIMES)
                .isNull();
    }

    private List<CompactionJobSummary> createJobSummary() {
        Instant jobStartedUpdateTime = Instant.parse("2022-09-22T14:00:04.000Z");
        Instant jobFinishTime = Instant.parse("2022-09-22T14:00:14.000Z");

        CompactionJobSummary summary = new CompactionJobSummary(
                new CompactionJobRecordsProcessed(4800L, 2400L), jobStartedUpdateTime, jobFinishTime);

        return Collections.singletonList(summary);
    }
}
