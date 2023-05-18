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

import com.google.common.collect.Iterators;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.validation.BatchIngestMode;
import sleeper.ingest.job.IngestJob;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_INGEST_MODE;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_FILES;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.ingest.batcher.FileIngestRequestTestHelper.onJob;

class IngestBatcherTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
    private final IngestBatcherStateStore store = new IngestBatcherStateStoreInMemory();
    private final IngestBatcherQueuesInMemory queues = new IngestBatcherQueuesInMemory();
    private static final String DEFAULT_TABLE_NAME = "test-table";


    @BeforeEach
    void setUp() {
        instanceProperties.set(INGEST_JOB_QUEUE_URL, "test-ingest-queue-url");
        tableProperties.set(INGEST_BATCHER_INGEST_MODE, BatchIngestMode.STANDARD_INGEST.toString());
        tableProperties.set(INGEST_BATCHER_MIN_JOB_SIZE, "0");
        tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "0");
        tableProperties.set(TABLE_NAME, DEFAULT_TABLE_NAME);
    }

    private Map<String, List<Object>> queueMessages(IngestJob... jobs) {
        return Map.of("test-ingest-queue-url", List.of(jobs));
    }

    @Nested
    @DisplayName("Batch with minimum file count")
    class BatchWithMinimumFileCount {
        @Test
        void shouldBatchOneFileWhenMinimumFileCountIsOne() {
            // Given
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "1");
            FileIngestRequest request = addFileToStore("test-bucket/test.parquet");

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(store.getAllFiles()).containsExactly(
                    onJob("test-job-id", request));
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id", "test-bucket/test.parquet")));
        }

        @Test
        void shouldBatchNoFilesWhenMinimumCountIsTwoAndOneFileIsTracked() {
            // Given
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "2");
            FileIngestRequest request = addFileToStore("test-bucket/test.parquet");

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(store.getPendingFiles()).containsExactly(request);
            assertThat(queues.getMessagesByQueueUrl()).isEqualTo(Collections.emptyMap());
        }
    }

    private IngestJob jobWithFiles(String jobId, String... files) {
        return IngestJob.builder()
                .files(files)
                .tableName(DEFAULT_TABLE_NAME)
                .id(jobId)
                .build();
    }

    private FileIngestRequest addFileToStore(String pathToFile) {
        return addFileToStore(FileIngestRequest.builder()
                .pathToFile(pathToFile)
                .tableName(DEFAULT_TABLE_NAME)
                .build());
    }

    private FileIngestRequest addFileToStore(FileIngestRequest request) {
        store.addFile(request);
        return request;
    }

    private void batchFilesWithJobIds(String... jobIds) {
        IngestBatcher.builder()
                .instanceProperties(instanceProperties)
                .tablePropertiesProvider(new FixedTablePropertiesProvider(tableProperties))
                .jobIdSupplier(Iterators.forArray(jobIds)::next)
                .store(store).queueClient(queues)
                .build().batchFiles();
    }
}
