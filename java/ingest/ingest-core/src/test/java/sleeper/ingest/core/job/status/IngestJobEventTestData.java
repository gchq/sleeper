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
package sleeper.ingest.core.job.status;

import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.tracker.ingest.job.update.IngestJobEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobFinishedEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobStartedEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobValidatedEvent;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * A helper for creating ingest job tracker events for tests.
 */
public class IngestJobEventTestData {

    private IngestJobEventTestData() {
    }

    public static final String DEFAULT_TABLE_ID = "test-table";

    /**
     * Creates a builder for an ingest job started event when the job was not validated as a separate event.
     *
     * @param  startTime the start time
     * @return           the builder
     */
    public static IngestJobStartedEvent.Builder ingestJobStartedEventBuilder(Instant startTime) {
        return IngestJobStartedEvent.builder()
                .jobId(UUID.randomUUID().toString())
                .tableId(DEFAULT_TABLE_ID)
                .fileCount(1)
                .startTime(startTime)
                .startOfRun(true);
    }

    /**
     * Creates a builder for an ingest job started event when the job was validated as a separate event.
     *
     * @param  startTime the start time
     * @return           the builder
     */
    public static IngestJobStartedEvent.Builder ingestJobStartedAfterValidationEventBuilder(IngestJobValidatedEvent validatedEvent, Instant startTime) {
        return IngestJobStartedEvent.builder()
                .jobId(validatedEvent.getJobId())
                .tableId(validatedEvent.getTableId())
                .fileCount(validatedEvent.getFileCount())
                .startTime(startTime)
                .startOfRun(false);
    }

    /**
     * Creates an ingest job rejected event.
     *
     * @param  validationTime the validation time
     * @param  reasons        the reasons the job was rejected
     * @param  fileCount      the number of input files for the job
     * @return                the event
     */
    public static IngestJobValidatedEvent ingestJobRejectedEvent(Instant validationTime, List<String> reasons, int fileCount) {
        return ingestJobValidatedEventBuilder(validationTime).reasons(reasons).fileCount(fileCount).build();
    }

    /**
     * Creates a builder for an ingest job accepted event.
     *
     * @param  validationTime the validation time
     * @param  fileCount      the number of input files for the job
     * @return                the builder
     */
    public static IngestJobValidatedEvent.Builder bulkImportJobAcceptedEventBuilder(Instant validationTime, int fileCount) {
        return ingestJobValidatedEventBuilder(validationTime).reasons(List.of()).fileCount(fileCount);
    }

    /**
     * Creates a builder for an ingest job validation event.
     *
     * @param  validationTime the validation time
     * @return                the builder
     */
    public static IngestJobValidatedEvent.Builder ingestJobValidatedEventBuilder(Instant validationTime) {
        return IngestJobValidatedEvent.builder()
                .jobId(UUID.randomUUID().toString())
                .tableId(DEFAULT_TABLE_ID)
                .validationTime(validationTime);
    }

    /**
     * Creates a builder for an ingest job finished event.
     *
     * @param  job     a previous event for the same job
     * @param  summary the summary
     * @return         the builder
     */
    public static IngestJobFinishedEvent.Builder ingestJobFinishedEventBuilder(IngestJobEvent job, RecordsProcessedSummary summary) {
        return IngestJobFinishedEvent.builder()
                .jobId(job.getJobId())
                .tableId(job.getTableId())
                .taskId(job.getTaskId())
                .summary(summary);
    }

}
