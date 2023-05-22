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
package sleeper.ingest.batcher;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.validation.BatchIngestMode;
import sleeper.ingest.job.IngestJob;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_INGEST_MODE;

class IngestBatcherIngestModesTest extends IngestBatcherTestBase {

    @Test
    void shouldCreateJobsOnDifferentQueueForEachOfTwoTables() {
        // Given
        instanceProperties.set(INGEST_JOB_QUEUE_URL, "ingest-url");
        instanceProperties.set(BULK_IMPORT_EMR_JOB_QUEUE_URL, "bulk-import-url");
        TableProperties table1 = createTableProperties("test-table-1");
        TableProperties table2 = createTableProperties("test-table-2");
        table1.set(INGEST_BATCHER_INGEST_MODE, BatchIngestMode.STANDARD_INGEST.toString());
        table2.set(INGEST_BATCHER_INGEST_MODE, BatchIngestMode.BULK_IMPORT_EMR.toString());
        addFileToStore(builder -> builder.pathToFile("test-bucket/test-1.parquet").tableName("test-table-1"));
        addFileToStore(builder -> builder.pathToFile("test-bucket/test-2.parquet").tableName("test-table-2"));

        // When
        batchFilesWithTablesAndJobIds(List.of(table1, table2), List.of("test-job-1", "test-job-2"));

        // Then
        assertThat(queues.getMessagesByQueueUrl()).isEqualTo(Map.of(
                "ingest-url", List.of(IngestJob.builder()
                        .files("test-bucket/test-1.parquet")
                        .tableName("test-table-1")
                        .id("test-job-1")
                        .build()),
                "bulk-import-url", List.of(IngestJob.builder()
                        .files("test-bucket/test-2.parquet")
                        .tableName("test-table-2")
                        .id("test-job-2")
                        .build())));
    }
}
