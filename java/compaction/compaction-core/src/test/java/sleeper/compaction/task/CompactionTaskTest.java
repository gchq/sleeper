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

package sleeper.compaction.task;

import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.core.properties.table.TableProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;

public class CompactionTaskTest extends CompactionTaskTestBase {

    @Test
    void shouldRunJobFromQueueThenTerminate() throws Exception {
        // Given
        CompactionJob job = createJobOnQueue("job1");

        // When
        runTask(jobsSucceed(1));

        // Then
        assertThat(successfulJobs).containsExactly(job);
        assertThat(failedJobs).isEmpty();
        assertThat(jobsOnQueue).isEmpty();
    }

    @Test
    void shouldFailJobFromQueueThenTerminate() throws Exception {
        // Given
        CompactionJob job = createJobOnQueue("job1");

        // When
        runTask(processJobs(jobFails()));

        // Then
        assertThat(successfulJobs).isEmpty();
        assertThat(failedJobs).containsExactly(job);
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
        assertThat(successfulJobs).containsExactly(job1);
        assertThat(failedJobs).containsExactly(job2);
        assertThat(jobsOnQueue).isEmpty();
    }

    @Test
    void shouldDiscardJobsForNonExistentTable() throws Exception {
        // Given
        TableProperties table = createTestTableProperties(instanceProperties, schema);
        jobsOnQueue.add(createJobNotInStateStore("job1", table));
        jobsOnQueue.add(createJobNotInStateStore("job2", table));

        // When
        runTask(processNoJobs());

        // Then
        assertThat(successfulJobs).isEmpty();
        assertThat(failedJobs).isEmpty();
        assertThat(jobsOnQueue).isEmpty();
    }
}
