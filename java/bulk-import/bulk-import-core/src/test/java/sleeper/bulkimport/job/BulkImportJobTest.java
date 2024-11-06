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

package sleeper.bulkimport.job;

import org.junit.jupiter.api.Test;

import sleeper.ingest.job.IngestJob;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BulkImportJobTest {
    @Test
    void shouldBuildIngestJobFromBulkImportJob() {
        // Given
        BulkImportJob bulkImportJob = new BulkImportJob.Builder()
                .id("test-job")
                .files(List.of("test1.parquet", "test2.parquet"))
                .tableName("test-table")
                .build();

        // When/Then
        assertThat(bulkImportJob.toIngestJob())
                .isEqualTo(IngestJob.builder()
                        .id("test-job")
                        .files(List.of("test1.parquet", "test2.parquet"))
                        .tableName("test-table").build());
    }
}
