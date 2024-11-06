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

package sleeper.bulkimport.starter;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.starter.executor.BulkImportExecutor;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatusTestHelper;
import sleeper.ingest.core.job.IngestJobMessageHandler;
import sleeper.ingest.core.job.status.InMemoryIngestJobStatusStore;
import sleeper.ingest.core.job.status.IngestJobStatusStore;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static sleeper.bulkimport.starter.BulkImportStarterLambdaTestHelper.getSqsEvent;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.jobStatus;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.rejectedRun;

public class BulkImportStarterLambdaTest {
    BulkImportExecutor executor = mock(BulkImportExecutor.class);
    TableIndex tableIndex = new InMemoryTableIndex();
    IngestJobStatusStore ingestJobStatusStore = new InMemoryIngestJobStatusStore();

    @BeforeEach
    void setUp() {
        tableIndex.create(TableStatusTestHelper.uniqueIdAndName("test-table-id", "test-table"));
    }

    @Test
    void shouldReportValidationFailureIfJsonInvalid() {
        // Given
        String json = "{";
        SQSEvent event = getSqsEvent(json);

        // When
        Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
        BulkImportStarterLambda bulkImportStarter = new BulkImportStarterLambda(executor, messageHandlerBuilder()
                .jobIdSupplier(() -> "test-job-id")
                .timeSupplier(() -> validationTime)
                .build());
        bulkImportStarter.handleRequest(event, mock(Context.class));

        // Then
        assertThat(ingestJobStatusStore.getInvalidJobs())
                .containsExactly(jobStatus("test-job-id",
                        rejectedRun("test-job-id", json, validationTime,
                                "Error parsing JSON. Reason: End of input at line 1 column 2 path $.")));
    }

    @Test
    void shouldReportModelValidationFailureIfTableNameNotProvided() {
        // Given
        String json = "{" +
                "\"files\":[]" +
                "}";
        SQSEvent event = getSqsEvent(json);

        // When
        Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
        BulkImportStarterLambda bulkImportStarter = new BulkImportStarterLambda(executor, messageHandlerBuilder()
                .jobIdSupplier(() -> "test-job-id")
                .timeSupplier(() -> validationTime)
                .build());
        bulkImportStarter.handleRequest(event, mock(Context.class));

        // Then
        assertThat(ingestJobStatusStore.getInvalidJobs())
                .containsExactly(jobStatus("test-job-id",
                        rejectedRun("test-job-id", json, validationTime,
                                "Table not found")));
    }

    @Test
    void shouldReportModelValidationFailureIfFilesNotProvided() {
        // Given
        String json = "{" +
                "\"tableName\":\"test-table\"" +
                "}";
        SQSEvent event = getSqsEvent(json);

        // When
        Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
        BulkImportStarterLambda bulkImportStarter = new BulkImportStarterLambda(executor, messageHandlerBuilder()
                .jobIdSupplier(() -> "test-job-id")
                .timeSupplier(() -> validationTime)
                .build());
        bulkImportStarter.handleRequest(event, mock(Context.class));

        // Then
        assertThat(ingestJobStatusStore.getInvalidJobs())
                .containsExactly(jobStatus("test-job-id",
                        rejectedRun("test-job-id", json, validationTime,
                                "Missing property \"files\"")));
    }

    @Test
    void shouldReportMultipleModelValidationFailures() {
        // Given
        String json = "{}";
        SQSEvent event = getSqsEvent(json);

        // When
        Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
        BulkImportStarterLambda bulkImportStarter = new BulkImportStarterLambda(executor, messageHandlerBuilder()
                .jobIdSupplier(() -> "test-job-id")
                .timeSupplier(() -> validationTime)
                .build());
        bulkImportStarter.handleRequest(event, mock(Context.class));

        // Then
        assertThat(ingestJobStatusStore.getInvalidJobs())
                .containsExactly(jobStatus("test-job-id",
                        rejectedRun("test-job-id", json, validationTime,
                                "Missing property \"files\"",
                                "Table not found")));
    }

    private IngestJobMessageHandler.Builder<BulkImportJob> messageHandlerBuilder() {
        return BulkImportStarterLambda.messageHandlerBuilder()
                .tableIndex(tableIndex)
                .ingestJobStatusStore(ingestJobStatusStore)
                .expandDirectories(files -> files);
    }
}
