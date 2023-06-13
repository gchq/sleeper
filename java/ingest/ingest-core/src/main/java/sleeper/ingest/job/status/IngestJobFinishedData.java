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

package sleeper.ingest.job.status;

import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.ingest.job.IngestJob;

import java.util.Objects;

public class IngestJobFinishedData {
    private final String runId;
    private final String taskId;
    private final IngestJob job;
    private final RecordsProcessedSummary summary;

    private IngestJobFinishedData(Builder builder) {
        runId = builder.runId;
        taskId = builder.taskId;
        job = Objects.requireNonNull(builder.job, "job must not be null");
        summary = Objects.requireNonNull(builder.summary, "summary must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static IngestJobFinishedData from(String taskId, IngestJob job, RecordsProcessedSummary summary) {
        return builder().taskId(taskId).job(job).summary(summary).build();
    }

    public String getRunId() {
        return runId;
    }

    public String getTaskId() {
        return taskId;
    }

    public IngestJob getJob() {
        return job;
    }

    public RecordsProcessedSummary getSummary() {
        return summary;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IngestJobFinishedData that = (IngestJobFinishedData) o;
        return Objects.equals(runId, that.runId) && Objects.equals(taskId, that.taskId) && Objects.equals(job, that.job) && Objects.equals(summary, that.summary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(runId, taskId, job, summary);
    }

    @Override
    public String toString() {
        return "IngestJobFinishedData{" +
                "runId='" + runId + '\'' +
                ", taskId='" + taskId + '\'' +
                ", job=" + job +
                ", summary=" + summary +
                '}';
    }

    public static final class Builder {
        private String runId;
        private String taskId;
        private IngestJob job;
        private RecordsProcessedSummary summary;

        public Builder() {
        }

        public Builder runId(String runId) {
            this.runId = runId;
            return this;
        }

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder job(IngestJob job) {
            this.job = job;
            return this;
        }

        public Builder summary(RecordsProcessedSummary summary) {
            this.summary = summary;
            return this;
        }

        public IngestJobFinishedData build() {
            return new IngestJobFinishedData(this);
        }
    }
}
