/*
 * Copyright 2022 Crown Copyright
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

public class TestIngestTaskStatus {
    private TestIngestTaskStatus() {
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
        return builder.finished(IngestTaskFinishedStatus.builder(), finishTime).build();
    }

    public static IngestTaskStatus finishedOneJobDefault() {
        return finishedOneJobDefault(startedBuilderWithDefaults());
    }

    public static IngestTaskStatus finishedOneJob(String taskId, Instant startTaskTime, Instant finishTaskTime,
                                                  Instant startJobTime, Instant finishJobTime) {
        return IngestTaskStatus.builder().taskId(taskId).startTime(startTaskTime)
                .finishedStatus(IngestTaskFinishedStatus.builder().addJobSummary(
                                new RecordsProcessedSummary(
                                        new RecordsProcessed(0L, 0L),
                                        startJobTime, finishJobTime))
                        .finish(startTaskTime, finishTaskTime)
                        .build())
                .build();
    }

    public static IngestTaskStatus finishedOneJobDefault(IngestTaskStatus.Builder builder) {
        return builder.finished(IngestTaskFinishedStatus.builder()
                        .addJobSummary(new RecordsProcessedSummary(
                                new RecordsProcessed(0L, 0L),
                                Instant.parse("2022-12-07T12:37:20.123Z"),
                                Instant.parse("2022-12-07T12:37:50.123Z"))),
                Instant.parse("2022-12-07T12:38:00.123Z")).build();
    }

    public static IngestTaskStatus.Builder startedBuilderWithDefaults() {
        return IngestTaskStatus.builder().taskId("test-task")
                .startTime(Instant.parse("2022-12-07T14:56:00.001Z"));
    }
}
