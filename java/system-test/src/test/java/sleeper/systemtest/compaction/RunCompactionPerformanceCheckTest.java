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

package sleeper.systemtest.compaction;

import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.testutils.CompactionJobStatusStoreInMemory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.DelegatingStateStore;
import sleeper.statestore.FileInfoFactory;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.inmemory.FixedPartitionStore;
import sleeper.statestore.inmemory.InMemoryFileInfoStore;
import sleeper.systemtest.SystemTestProperties;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.record.process.RecordsProcessedSummaryTestData.summary;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.systemtest.SystemTestProperty.INGEST_MODE;
import static sleeper.systemtest.SystemTestProperty.NUMBER_OF_RECORDS_PER_WRITER;
import static sleeper.systemtest.SystemTestProperty.NUMBER_OF_WRITERS;
import static sleeper.systemtest.SystemTestProperty.SYSTEM_TEST_REPO;
import static sleeper.systemtest.compaction.RunCompactionPerformanceCheck.loadFrom;
import static sleeper.systemtest.ingest.IngestMode.DIRECT;

class RunCompactionPerformanceCheckTest {
    private static final String TEST_TABLE_NAME = "test-table";
    private final Schema schema = schemaWithKey("key", new StringType());
    private final StateStore stateStore = new DelegatingStateStore(
            new InMemoryFileInfoStore(), new FixedPartitionStore(schema));

    private final FileInfoFactory fileInfoFactory = createFileInfoFactory();
    private final CompactionJobStatusStore jobStatusStore = new CompactionJobStatusStoreInMemory();
    private final CompactionJobTestDataHelper dataHelper = CompactionJobTestDataHelper.forTable(TEST_TABLE_NAME);

    @Test
    void shouldLoadExpectedResultsCorrectlyFromInstanceProperties() throws Exception {
        // Given
        SystemTestProperties properties = validProperties();
        TableProperties tableProperties = validTableProperties(properties);

        // When
        RunCompactionPerformanceCheck runCheck = loadFrom(properties, tableProperties, stateStore, jobStatusStore);

        // Then
        assertThat(runCheck)
                .isEqualTo(RunCompactionPerformanceCheck.builder()
                        .expectedResults(CompactionPerformanceResults.builder()
                                .numOfJobs(2)
                                .numOfRecordsInRoot(50)
                                .writeRate(CompactionPerformanceResults.TARGET_RECORDS_PER_SECOND)
                                .build())
                        .results(emptyResults())
                        .build());
    }

    @Test
    void shouldLoadNumberOfJobsCorrectlyFromJobStatusStore() throws Exception {
        // Given
        SystemTestProperties properties = validProperties();
        TableProperties tableProperties = validTableProperties(properties);
        startSingleJob(Instant.parse("2023-04-14T16:57:00Z"));
        startSingleJob(Instant.parse("2023-04-14T16:59:00Z"));

        // When
        RunCompactionPerformanceCheck runCheck = loadFrom(properties, tableProperties, stateStore, jobStatusStore);

        // Then
        assertThat(runCheck)
                .extracting("results.numOfJobs")
                .isEqualTo(2);
    }

    @Test
    void shouldLoadNumberOfRecordsFromStateStore() throws Exception {
        // Given
        SystemTestProperties properties = validProperties();
        TableProperties tableProperties = validTableProperties(properties);
        stateStore.addFile(fileInfoFactory.rootFile("test1.parquet", 4, "abc", "def"));
        stateStore.addFile(fileInfoFactory.rootFile("test2.parquet", 4, "aaa", "bbb"));

        // When
        RunCompactionPerformanceCheck runCheck = loadFrom(properties, tableProperties, stateStore, jobStatusStore);

        // Then
        assertThat(runCheck)
                .extracting("results.numOfRecordsInRoot")
                .isEqualTo(8L);
    }

    @Test
    void shouldLoadWriteRateFromJobStatusStore() throws Exception {
        // Given
        SystemTestProperties properties = validProperties();
        TableProperties tableProperties = validTableProperties(properties);
        finishSingleJob(summary(Instant.parse("2023-04-14T16:57:00Z"),
                Duration.ofSeconds(10), 100L, 100L));
        finishSingleJob(summary(Instant.parse("2023-04-14T16:59:00Z"),
                Duration.ofSeconds(10), 100L, 100L));

        // When
        RunCompactionPerformanceCheck runCheck = loadFrom(properties, tableProperties, stateStore, jobStatusStore);

        // Then
        assertThat(runCheck)
                .extracting("results.writeRate")
                .isEqualTo(10.0);
    }

    private void startSingleJob(Instant startTime) {
        CompactionJob job = dataHelper.singleFileCompaction();
        jobStatusStore.jobCreated(job);
        jobStatusStore.jobStarted(job, startTime, "test-task");
    }

    private void finishSingleJob(RecordsProcessedSummary summary) {
        CompactionJob job = dataHelper.singleFileCompaction();
        jobStatusStore.jobCreated(job);
        jobStatusStore.jobStarted(job, summary.getStartTime(), "test-task");
        jobStatusStore.jobFinished(job, summary, "test-task");
    }

    private SystemTestProperties validProperties() throws IOException {
        SystemTestProperties properties = new SystemTestProperties();
        properties.set(NUMBER_OF_WRITERS, "10");
        properties.set(NUMBER_OF_RECORDS_PER_WRITER, "5");
        properties.set(INGEST_MODE, DIRECT.name());
        properties.set(SYSTEM_TEST_REPO, "test-repo");
        properties.loadFromString(createTestInstanceProperties().saveAsString());
        return properties;
    }

    private TableProperties validTableProperties(InstanceProperties properties) {
        TableProperties tableProperties = new TableProperties(properties);
        tableProperties.set(TableProperty.TABLE_NAME, TEST_TABLE_NAME);
        tableProperties.set(TableProperty.COMPACTION_FILES_BATCH_SIZE, "5");
        return tableProperties;
    }

    private FileInfoFactory createFileInfoFactory() {
        try {
            return new FileInfoFactory(schema, stateStore.getAllPartitions());
        } catch (StateStoreException e) {
            throw new IllegalStateException(e);
        }
    }

    private CompactionPerformanceResults emptyResults() {
        return CompactionPerformanceResults.builder()
                .numOfJobs(0)
                .numOfRecordsInRoot(0)
                .writeRate(Double.NaN)
                .build();
    }
}
