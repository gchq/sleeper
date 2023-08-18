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
package sleeper.ingest.task;

import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;

import static sleeper.core.record.process.RecordsProcessedSummaryTestData.summary;
import static sleeper.core.statestore.FileInfoTestData.DEFAULT_NUMBER_OF_RECORDS;

public class IngestTaskStatusTestData {
    private IngestTaskStatusTestData() {
    }

    public static IngestTaskStatus finishedNoJobsDefault() {
        return finishedNoJobsDefault(startedBuilderWithDefaults());
    }


    public static IngestTaskStatus finishedNoJobsDefault(IngestTaskStatus.Builder builder) {
        return finishedNoJobs(builder, Instant.parse("2022-12-07T14:57:00.001Z"));
    }

    public static IngestTaskStatus finishedNoJobs(String taskId, Instant startTime, Instant finishTime) {
        return finishedNoJobs(IngestTaskStatus.builder().taskId(taskId).startTime(startTime), finishTime);
    }

    public static IngestTaskStatus finishedNoJobs(IngestTaskStatus.Builder builder, Instant finishTime) {
        return builder.finished(finishTime, IngestTaskFinishedStatus.builder()).build();
    }

    public static IngestTaskStatus finishedOneJobNoFiles(String taskId, Instant startTaskTime, Instant finishTaskTime,
                                                         Instant startJobTime, Instant finishJobTime) {
        return finishedOneJob(taskId, startTaskTime, finishTaskTime, startJobTime, finishJobTime, 0L, 0L);
    }

    public static IngestTaskStatus finishedOneJobOneFile(String taskId, Instant startTaskTime, Instant finishTaskTime,
                                                         Instant startJobTime, Instant finishJobTime) {
        return finishedOneJob(taskId, startTaskTime, finishTaskTime, startJobTime, finishJobTime,
                DEFAULT_NUMBER_OF_RECORDS, DEFAULT_NUMBER_OF_RECORDS);

    }

    public static IngestTaskStatus finishedOneJob(String taskId, Instant startTaskTime, Instant finishTaskTime,
                                                  Instant startJobTime, Instant finishJobTime,
                                                  long recordsRead, long recordsWritten) {
        return IngestTaskStatus.builder().taskId(taskId).startTime(startTaskTime)
                .finished(finishTaskTime, IngestTaskFinishedStatus.builder().jobSummaries(Stream.of(
                        new RecordsProcessedSummary(
                                new RecordsProcessed(recordsRead, recordsWritten),
                                startJobTime, finishJobTime))))
                .build();
    }

    public static IngestTaskStatus finishedMultipleJobs(String taskId, Instant startTaskTime, Instant finishTaskTime,
                                                        Duration duration, Instant... startJobTimes) {
        return finishedMultipleJobs(taskId, startTaskTime, finishTaskTime,
                Stream.of(startJobTimes).map(startJobTime ->
                        summary(startJobTime, duration, DEFAULT_NUMBER_OF_RECORDS, DEFAULT_NUMBER_OF_RECORDS)));
    }

    public static IngestTaskStatus finishedMultipleJobs(String taskId, Instant startTaskTime, Instant finishTaskTime,
                                                        RecordsProcessedSummary... summaries) {
        return finishedMultipleJobs(taskId, startTaskTime, finishTaskTime, Stream.of(summaries));
    }

    private static IngestTaskStatus finishedMultipleJobs(String taskId, Instant startTaskTime, Instant finishTaskTime,
                                                         Stream<RecordsProcessedSummary> summaries) {
        return IngestTaskStatus.builder().taskId(taskId).startTime(startTaskTime)
                .finished(finishTaskTime, IngestTaskFinishedStatus.builder().jobSummaries(summaries))
                .build();
    }

    public static IngestTaskStatus.Builder startedBuilderWithDefaults() {
        return IngestTaskStatus.builder().taskId("test-task")
                .startTime(Instant.parse("2022-12-07T14:56:00.001Z"));
    }
}
