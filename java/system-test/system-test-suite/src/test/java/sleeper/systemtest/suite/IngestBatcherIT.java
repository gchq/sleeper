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

package sleeper.systemtest.suite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import sleeper.core.record.Record;
import sleeper.systemtest.suite.dsl.SleeperSystemTest;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_INGEST_MODE;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_FILES;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_SIZE;
import static sleeper.configuration.properties.validation.BatchIngestMode.BULK_IMPORT_EMR;
import static sleeper.configuration.properties.validation.BatchIngestMode.STANDARD_INGEST;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@Tag("SystemTest")
public class IngestBatcherIT {

    private final SleeperSystemTest sleeper = SleeperSystemTest.getInstance();

    @BeforeEach
    void setUp() {
        sleeper.connectToInstance(MAIN);
        sleeper.ingestBatcher().clearStore();
    }

    @Test
    void shouldStandardIngestOneRecord() throws InterruptedException {
        // Given
        sleeper.updateTableProperties(tableProperties -> {
            tableProperties.set(INGEST_BATCHER_INGEST_MODE, STANDARD_INGEST.toString());
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "1");
            tableProperties.set(INGEST_BATCHER_MIN_JOB_SIZE, "1");
        });
        Record record = new Record(Map.of(
                "key", "some-id",
                "timestamp", 1234L,
                "value", "Some value"));

        // When
        sleeper.sourceFiles().create("file.parquet", record);
        sleeper.ingestBatcher().sendSourceFiles("file.parquet").invoke().waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactly(record);
    }

    @Test
    void shouldBulkImportOneRecord() throws InterruptedException {
        // Given
        sleeper.updateTableProperties(tableProperties -> {
            tableProperties.set(INGEST_BATCHER_INGEST_MODE, BULK_IMPORT_EMR.toString());
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "1");
            tableProperties.set(INGEST_BATCHER_MIN_JOB_SIZE, "1");
        });
        Record record = new Record(Map.of(
                "key", "some-id",
                "timestamp", 1234L,
                "value", "Some value"));

        // When
        sleeper.sourceFiles().create("file.parquet", record);
        sleeper.ingestBatcher().sendSourceFiles("file.parquet").invoke().waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactly(record);
    }
}
