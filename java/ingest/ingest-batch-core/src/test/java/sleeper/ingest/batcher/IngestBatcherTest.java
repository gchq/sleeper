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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.ingest.job.IngestJob;

import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_FILES;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

class IngestBatcherTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));

    @Nested
    @DisplayName("Batch with minimum file count")
    class BatchWithMinimumFileCount {

        @BeforeEach
        void setUp() {
            tableProperties.set(INGEST_BATCHER_MIN_JOB_SIZE, "0");
        }

        @Test
        void shouldBatchOneFileWhenMinimumFileCountIsOne() {
            // Given
            Supplier<String> jobIdSupplier = () -> "test-job-id";
            tableProperties.set(TABLE_NAME, "test-table");
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "1");
            TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(tableProperties);
            IngestBatcher batcher = IngestBatcher.builder().tablePropertiesProvider(tablePropertiesProvider)
                    .jobIdSupplier(jobIdSupplier).build();
            List<TrackedFile> inputFiles = List.of(TrackedFile.builder()
                    .pathToFile("test-bucket/test.parquet")
                    .tableName("test-table")
                    .build());

            // When
            List<IngestJob> jobs = batcher.batchFiles(inputFiles);

            // Then
            assertThat(jobs).containsExactly(IngestJob.builder()
                    .files("test-bucket/test.parquet")
                    .tableName("test-table")
                    .id("test-job-id")
                    .build());
        }

        @Test
        void shouldBatchNoFilesWhenMinimumCountIsTwoAndOneFileIsTracked() {
            // Given
            Supplier<String> jobIdSupplier = () -> "test-job-id";
            tableProperties.set(TABLE_NAME, "test-table");
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "2");
            TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(tableProperties);
            IngestBatcher batcher = IngestBatcher.builder().tablePropertiesProvider(tablePropertiesProvider)
                    .jobIdSupplier(jobIdSupplier).build();
            List<TrackedFile> inputFiles = List.of(TrackedFile.builder()
                    .pathToFile("test-bucket/test.parquet")
                    .tableName("test-table")
                    .build());

            // When / Then
            assertThat(batcher.batchFiles(inputFiles)).isEmpty();
        }
    }
}
