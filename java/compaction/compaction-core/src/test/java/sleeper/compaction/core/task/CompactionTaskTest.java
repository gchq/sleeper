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

package sleeper.compaction.core.task;

import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.process.ProcessRunTime;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.compactionJobCreated;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.failedCompactionRun;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;

public class CompactionTaskTest extends CompactionTaskTestBase {

    @Test
    void shouldRunJobFromQueueThenTerminate() throws Exception {
        // Given
        CompactionJob job = createJobOnQueue("job1");

        // When
        runTask(jobsSucceed(1));

        // Then
        assertThat(consumedJobs).containsExactly(job);
        assertThat(jobsReturnedToQueue).isEmpty();
        assertThat(jobsOnQueue).isEmpty();
    }

    @Test
    void shouldFailJobFromQueueThenTerminate() throws Exception {
        // Given
        CompactionJob job = createJobOnQueue("job1");

        // When
        runTask(processJobs(jobFails()));

        // Then
        assertThat(consumedJobs).isEmpty();
        assertThat(jobsReturnedToQueue).containsExactly(job);
        assertThat(jobsOnQueue).isEmpty();
    }

    @Test
    void shouldProcessTwoJobsFromQueueThenTerminate() throws Exception {
        // Given
        CompactionJob job1 = createJobOnQueue("job1");
        CompactionJob job2 = createJobOnQueue("job2");

        // When
        runTask(processJobs(jobSucceeds(), jobFails()));

        // Then
        assertThat(consumedJobs).containsExactly(job1);
        assertThat(jobsReturnedToQueue).containsExactly(job2);
        assertThat(jobsOnQueue).isEmpty();
    }

    @Test
    void shouldDiscardJobsForNonExistentTable() throws Exception {
        // Given
        TableProperties table = createTestTableProperties(instanceProperties, schema);
        CompactionJob job1 = createJobNotInStateStore("job1", table);
        CompactionJob job2 = createJobNotInStateStore("job2", table);
        jobsOnQueue.add(job1);
        jobsOnQueue.add(job2);
        Instant startTime1 = Instant.parse("2024-02-22T13:50:01Z");
        Instant finishTime1 = Instant.parse("2024-02-22T13:50:02Z");
        Instant startTime2 = Instant.parse("2024-02-22T13:50:03Z");
        Instant finishTime2 = Instant.parse("2024-02-22T13:50:04Z");
        Queue<Instant> times = new LinkedList<>(List.of(
                Instant.parse("2024-02-22T13:50:00Z"),   // Task start
                startTime1, finishTime1, startTime2, finishTime2,
                Instant.parse("2024-02-22T13:50:07Z"))); // Task finish

        // When
        runTask(processNoJobs(), times::poll);

        // Then
        assertThat(consumedJobs).containsExactly(job1, job2);
        assertThat(jobsReturnedToQueue).isEmpty();
        assertThat(jobsOnQueue).isEmpty();
        assertThat(jobStore.getAllJobs(table.get(TABLE_ID))).containsExactlyInAnyOrder(
                compactionJobCreated(job1, DEFAULT_CREATED_TIME,
                        failedCompactionRun(DEFAULT_TASK_ID, new ProcessRunTime(startTime1, finishTime1), List.of(
                                "Table not found with ID \"" + table.get(TABLE_ID) + "\""))),
                compactionJobCreated(job2, DEFAULT_CREATED_TIME,
                        failedCompactionRun(DEFAULT_TASK_ID, new ProcessRunTime(startTime2, finishTime2), List.of(
                                "Table not found with ID \"" + table.get(TABLE_ID) + "\""))));
    }
}
