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
package sleeper.ingest.task;

import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Instant;
import java.util.stream.Stream;

/**
 * Test helpers for setting up ingest task statuses.
 */
public class IngestTaskStatusTestData {
    public static final Instant DEFAULT_START_TIME = Instant.parse("2022-12-07T14:56:00.001Z");
    public static final Instant DEFAULT_FINISH_TIME = Instant.parse("2022-12-07T14:57:00.001Z");

    private IngestTaskStatusTestData() {
    }

    /**
     * Creates an ingest task status that finished with no jobs.
     *
     * @return an ingest task status that finished with no jobs
     */
    public static IngestTaskStatus finishedNoJobsDefault() {
        return finishedNoJobsDefault(startedBuilderWithDefaults());
    }

    /**
     * Creates an ingest task status that finished with no jobs.
     *
     * @param  builder the task status builder to use
     * @return         an ingest task status that finished with no jobs
     */
    public static IngestTaskStatus finishedNoJobsDefault(IngestTaskStatus.Builder builder) {
        return finishedNoJobs(builder, DEFAULT_FINISH_TIME);
    }

    /**
     * Creates an ingest task status that finished with no jobs.
     *
     * @param  taskId     the task ID
     * @param  startTime  the start time
     * @param  finishTime the finish time
     * @return            an ingest task status that finished with no jobs
     */
    public static IngestTaskStatus finishedNoJobs(String taskId, Instant startTime, Instant finishTime) {
        return finishedNoJobs(IngestTaskStatus.builder().taskId(taskId).startTime(startTime), finishTime);
    }

    private static IngestTaskStatus finishedNoJobs(IngestTaskStatus.Builder builder, Instant finishTime) {
        return builder.finished(finishTime, IngestTaskFinishedStatus.builder()).build();
    }

    /**
     * Creates an ingest task status that finished with one job.
     *
     * @param  taskId         the task ID
     * @param  startTaskTime  the start time for the task
     * @param  finishTaskTime the finish time for the task
     * @param  startJobTime   the start time for the job
     * @param  finishJobTime  the finish time for the job
     * @param  recordsRead    the number of records read when running the job
     * @param  recordsWritten the number of records written when running the job
     * @return                an ingest task status that finished with one job
     */
    public static IngestTaskStatus finishedOneJob(
            String taskId, Instant startTaskTime, Instant finishTaskTime,
            Instant startJobTime, Instant finishJobTime,
            long recordsRead, long recordsWritten) {
        return IngestTaskStatus.builder().taskId(taskId).startTime(startTaskTime)
                .finished(finishTaskTime, IngestTaskFinishedStatus.builder().jobSummaries(Stream.of(
                        new RecordsProcessedSummary(
                                new RecordsProcessed(recordsRead, recordsWritten),
                                startJobTime, finishJobTime))))
                .build();
    }

    /**
     * Creates an ingest task status that finished with multiple jobs.
     *
     * @param  taskId         the task ID
     * @param  startTaskTime  the start time for the task
     * @param  finishTaskTime the finish time for the task
     * @param  summaries      a collection of record processed summaries from running jobs
     * @return                an ingest task status that finished with multiple jobs
     */
    public static IngestTaskStatus finishedMultipleJobs(
            String taskId, Instant startTaskTime, Instant finishTaskTime, RecordsProcessedSummary... summaries) {
        return finishedMultipleJobs(taskId, startTaskTime, finishTaskTime, Stream.of(summaries));
    }

    private static IngestTaskStatus finishedMultipleJobs(
            String taskId, Instant startTaskTime, Instant finishTaskTime, Stream<RecordsProcessedSummary> summaries) {
        return IngestTaskStatus.builder().taskId(taskId).startTime(startTaskTime)
                .finished(finishTaskTime, IngestTaskFinishedStatus.builder().jobSummaries(summaries))
                .build();
    }

    /**
     * Creates an ingest task status builder populated with default values.
     *
     * @return an ingest task status builder
     */
    public static IngestTaskStatus.Builder startedBuilderWithDefaults() {
        return IngestTaskStatus.builder().taskId("test-task")
                .startTime(DEFAULT_START_TIME);
    }
}
