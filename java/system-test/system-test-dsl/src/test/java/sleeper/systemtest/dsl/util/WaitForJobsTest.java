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
package sleeper.systemtest.dsl.util;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.InMemoryTableProperties;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.InMemoryCompactionJobTracker;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;
import sleeper.core.tracker.compaction.task.InMemoryCompactionTaskTracker;
import sleeper.core.tracker.ingest.job.InMemoryIngestJobTracker;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.task.InMemoryIngestTaskTracker;
import sleeper.core.tracker.ingest.task.IngestTaskTracker;
import sleeper.core.util.ThreadSleep;
import sleeper.ingest.core.job.IngestJob;
import sleeper.systemtest.dsl.util.WaitForJobs.JobTracker;
import sleeper.systemtest.dsl.util.WaitForJobs.TaskTracker;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;
import static sleeper.ingest.core.job.IngestJobTestData.createJobWithTableAndFiles;

public class WaitForJobsTest {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStoreReturningExactInstance();
    IngestJobTracker ingestJobTracker = new InMemoryIngestJobTracker();
    IngestTaskTracker ingestTaskTracker = new InMemoryIngestTaskTracker();
    CompactionJobTracker compactionJobTracker = new InMemoryCompactionJobTracker();
    CompactionTaskTracker compactionTaskTracker = new InMemoryCompactionTaskTracker();
    Instant startTime = Instant.parse("2025-02-02T00:00:00Z");
    List<Duration> foundSleeps = new ArrayList<>();
    ThreadSleep sleeper = this::recordSleep;

    @Test
    void shouldWaitForSuccessfulIngest() {
        // Given
        TableProperties table = createTable("test");
        IngestJob job = createJobWithIdAndFiles(table, "test-job", "test.parquet");
        ingestJobTracker.jobStarted(job.startedEventBuilder(startTime)
                .taskId("test-task").jobRunId("test-run").build());
        doOnSleep(() -> {
            ingestJobTracker.jobFinished(job.finishedEventBuilder(summary(startTime, Duration.ofMinutes(1), 100, 100))
                    .taskId("test-task").jobRunId("test-run").numFilesWrittenByJob(1).build());
        });

        // When
        forIngest().waitForJobs(List.of("test-job"));

        // Then
        assertThat(foundSleeps).hasSize(1);
    }

    private WaitForJobs forIngest() {
        return new WaitForJobs(() -> instanceProperties, "ingest",
                properties -> JobTracker.forIngest(tablePropertiesStore.streamAllTables().toList(), ingestJobTracker),
                properties -> TaskTracker.forIngest(ingestTaskTracker),
                pollDriver());
    }

    private WaitForJobs forCompaction() {
        return new WaitForJobs(() -> instanceProperties, "compaction",
                properties -> JobTracker.forCompaction(tablePropertiesStore.streamAllTables().toList(), compactionJobTracker),
                properties -> TaskTracker.forCompaction(compactionTaskTracker),
                pollDriver());
    }

    private TableProperties createTable(String name) {
        TableProperties properties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
        properties.set(TABLE_ID, name);
        properties.set(TABLE_NAME, name);
        tablePropertiesStore.createTable(properties);
        return properties;
    }

    private void doOnSleep(Runnable... runnables) {
        sleeper = millis -> {
            List.of(runnables).forEach(Runnable::run);
            recordSleep(millis);
        };
    }

    private IngestJob createJobWithIdAndFiles(TableProperties table, String jobId, String... filenames) {
        return createJobWithTableAndFiles(jobId, table.getStatus(), filenames);
    }

    private Instant afterNMinutes(long n) {
        return startTime.plus(n, ChronoUnit.MINUTES);
    }

    private PollWithRetriesDriver pollDriver() {
        return poll -> poll.toBuilder()
                .sleepInInterval(sleeper)
                .build();
    }

    private void recordSleep(long millis) {
        foundSleeps.add(Duration.ofMillis(millis));
    }

}
