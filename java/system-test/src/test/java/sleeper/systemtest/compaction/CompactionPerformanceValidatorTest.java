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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.testutils.CompactionJobStatusStoreInMemory;
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

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.record.process.RecordsProcessedSummaryTestData.summary;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.systemtest.SystemTestPropertiesTestHelper.createTestSystemTestProperties;
import static sleeper.systemtest.SystemTestProperty.NUMBER_OF_RECORDS_PER_WRITER;
import static sleeper.systemtest.SystemTestProperty.NUMBER_OF_WRITERS;

class CompactionPerformanceValidatorTest {

    private static final String TEST_TABLE_NAME = "test-table";
    private final Schema schema = schemaWithKey("key", new StringType());
    private final StateStore stateStore = new DelegatingStateStore(
            new InMemoryFileInfoStore(), new FixedPartitionStore(schema));

    private final FileInfoFactory fileInfoFactory = createFileInfoFactory();
    private final CompactionJobStatusStore jobStatusStore = new CompactionJobStatusStoreInMemory();
    private final CompactionJobTestDataHelper dataHelper = CompactionJobTestDataHelper.forTable(TEST_TABLE_NAME);

    @Nested
    @DisplayName("Calculate expected results")
    class CalculateExpectedResults {
        @Test
        void shouldCalculateNumberOfJobsWhenNumberOfWritersIsSmallerThanBatchSize() throws Exception {
            // Given
            SystemTestProperties properties = createTestSystemTestProperties();
            properties.set(NUMBER_OF_WRITERS, "1");

            TableProperties tableProperties = new TableProperties(properties);
            tableProperties.set(TableProperty.COMPACTION_FILES_BATCH_SIZE, "5");

            // When
            CompactionPerformanceValidator validator = CompactionPerformanceValidator.from(properties, tableProperties);

            // Then
            assertThat(validator.getNumberOfJobsExpected())
                    .isOne();
        }

        @Test
        void shouldCalculateNumberOfJobsWhenNumberOfWritersIsLargerThanBatchSize() throws Exception {
            // Given
            SystemTestProperties properties = createTestSystemTestProperties();
            properties.set(NUMBER_OF_WRITERS, "6");

            TableProperties tableProperties = new TableProperties(properties);
            tableProperties.set(TableProperty.COMPACTION_FILES_BATCH_SIZE, "5");

            // When
            CompactionPerformanceValidator validator = CompactionPerformanceValidator.from(properties, tableProperties);

            // Then
            assertThat(validator.getNumberOfJobsExpected())
                    .isEqualTo(2);
        }

        @Test
        void shouldCalculateNumberOfRecordsExpected() throws Exception {
            // Given
            SystemTestProperties properties = createTestSystemTestProperties();
            properties.set(NUMBER_OF_WRITERS, "3");
            properties.set(NUMBER_OF_RECORDS_PER_WRITER, "10");

            TableProperties tableProperties = new TableProperties(properties);

            // When
            CompactionPerformanceValidator validator = CompactionPerformanceValidator.from(properties, tableProperties);

            // Then
            assertThat(validator.getNumberOfRecordsExpected())
                    .isEqualTo(30);
        }
    }

    @Nested
    @DisplayName("Validate actual results")
    class ValidateActualResults {
        @Test
        void shouldPassWhenSingleJobWasRunWithAllRecords() throws Exception {
            // Given
            SystemTestProperties properties = createTestSystemTestProperties();
            properties.set(NUMBER_OF_WRITERS, "1");
            properties.set(NUMBER_OF_RECORDS_PER_WRITER, "10");

            TableProperties tableProperties = new TableProperties(properties);
            tableProperties.set(TableProperty.TABLE_NAME, "test-table");
            tableProperties.set(TableProperty.COMPACTION_FILES_BATCH_SIZE, "5");

            stateStore.addFile(fileInfoFactory.rootFile(10, "aaa", "zzz"));
            reportFinishedJob(summary(Instant.parse("2023-04-17T16:15:42Z"), Duration.ofMinutes(1), 10, 10));

            // When
            CompactionPerformanceResults results = CompactionPerformanceResults.loadActual(tableProperties, stateStore, jobStatusStore);
            CompactionPerformanceValidator validator = CompactionPerformanceValidator.from(properties, tableProperties);

            // Then
            assertThatCode(() -> validator.test(results)).doesNotThrowAnyException();
        }

        @Test
        void shouldFailWhenMultipleJobsWereRunButOneJobWasExpected() throws Exception {
            // Given
            SystemTestProperties properties = createTestSystemTestProperties();
            properties.set(NUMBER_OF_WRITERS, "1");
            properties.set(NUMBER_OF_RECORDS_PER_WRITER, "10");

            TableProperties tableProperties = new TableProperties(properties);
            tableProperties.set(TableProperty.TABLE_NAME, "test-table");
            tableProperties.set(TableProperty.COMPACTION_FILES_BATCH_SIZE, "5");

            stateStore.addFile(fileInfoFactory.rootFile(10, "aaa", "zzz"));
            reportFinishedJob(summary(Instant.parse("2023-04-17T16:15:42Z"), Duration.ofMinutes(1), 5, 5));
            reportFinishedJob(summary(Instant.parse("2023-04-17T16:25:42Z"), Duration.ofMinutes(1), 5, 5));

            // When
            CompactionPerformanceResults results = CompactionPerformanceResults.loadActual(tableProperties, stateStore, jobStatusStore);
            CompactionPerformanceValidator validator = CompactionPerformanceValidator.from(properties, tableProperties);

            // Then
            assertThatThrownBy(() -> validator.test(results))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Actual number of compaction jobs 2 did not match expected value 1");
        }

        @Test
        void shouldFailWhenWhenSingleJobWasRunWithLessRecordsThanExpected() throws Exception {
            // Given
            SystemTestProperties properties = createTestSystemTestProperties();
            properties.set(NUMBER_OF_WRITERS, "1");
            properties.set(NUMBER_OF_RECORDS_PER_WRITER, "10");

            TableProperties tableProperties = new TableProperties(properties);
            tableProperties.set(TableProperty.TABLE_NAME, "test-table");
            tableProperties.set(TableProperty.COMPACTION_FILES_BATCH_SIZE, "5");

            stateStore.addFile(fileInfoFactory.rootFile(5, "aaa", "zzz"));
            reportFinishedJob(summary(Instant.parse("2023-04-17T16:15:42Z"), Duration.ofMinutes(1), 5, 5));

            // When
            CompactionPerformanceResults results = CompactionPerformanceResults.loadActual(tableProperties, stateStore, jobStatusStore);
            CompactionPerformanceValidator validator = CompactionPerformanceValidator.from(properties, tableProperties);

            // Then
            assertThatThrownBy(() -> validator.test(results))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Actual number of records 5 did not match expected value 10");
        }
    }

    private FileInfoFactory createFileInfoFactory() {
        try {
            return new FileInfoFactory(schema, stateStore.getAllPartitions());
        } catch (StateStoreException e) {
            throw new IllegalStateException(e);
        }
    }

    private void reportFinishedJob(RecordsProcessedSummary summary) {
        dataHelper.reportFinishedJob(summary, jobStatusStore);
    }

}
