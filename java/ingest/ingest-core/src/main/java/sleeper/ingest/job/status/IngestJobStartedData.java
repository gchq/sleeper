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

import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.util.Objects;

public class IngestJobStartedData {
    private final String runId;
    private final String taskId;
    private final IngestJob job;
    private final Instant startTime;
    private final boolean startOfRun;

    private IngestJobStartedData(Builder builder) {
        runId = builder.runId;
        taskId = builder.taskId;
        job = Objects.requireNonNull(builder.job, "job must not be null");
        startTime = Objects.requireNonNull(builder.startTime, "startTime must not be null");
        startOfRun = builder.startOfRun;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static IngestJobStartedData startOfRun(String taskId, IngestJob job, Instant startTime) {
        return builder().taskId(taskId).job(job).startTime(startTime).startOfRun(true).build();
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

    public Instant getStartTime() {
        return startTime;
    }

    public boolean isStartOfRun() {
        return startOfRun;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IngestJobStartedData that = (IngestJobStartedData) o;
        return Objects.equals(runId, that.runId) && Objects.equals(taskId, that.taskId) && Objects.equals(job, that.job) && Objects.equals(startTime, that.startTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(runId, taskId, job, startTime);
    }

    @Override
    public String toString() {
        return "IngestJobStartedData{" +
                "runId='" + runId + '\'' +
                ", taskId='" + taskId + '\'' +
                ", job=" + job +
                ", startTime=" + startTime +
                '}';
    }

    public static final class Builder {
        private String runId;
        private String taskId;
        private IngestJob job;
        private Instant startTime;
        private boolean startOfRun;

        private Builder() {
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

        public Builder startTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder startOfRun(boolean startOfRun) {
            this.startOfRun = startOfRun;
            return this;
        }

        public IngestJobStartedData build() {
            return new IngestJobStartedData(this);
        }
    }
}
