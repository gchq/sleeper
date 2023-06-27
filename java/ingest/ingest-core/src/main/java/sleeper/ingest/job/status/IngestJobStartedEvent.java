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

public class IngestJobStartedEvent {
    private final IngestJob job;
    private final String jobRunId;
    private final String taskId;
    private final Instant startTime;
    private final boolean startOfRun;

    private IngestJobStartedEvent(Builder builder) {
        job = Objects.requireNonNull(builder.job, "job must not be null");
        jobRunId = builder.jobRunId;
        taskId = builder.taskId;
        startTime = Objects.requireNonNull(builder.startTime, "startTime must not be null");
        startOfRun = builder.startOfRun;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static IngestJobStartedEvent ingestJobStarted(String taskId, IngestJob job, Instant startTime) {
        return builder()
                .taskId(taskId)
                .job(job)
                .startTime(startTime)
                .startOfRun(true)
                .build();
    }

    public static IngestJobStartedEvent.Builder validatedIngestJobStarted(IngestJob job, Instant startTime) {
        return builder()
                .job(job)
                .startTime(startTime)
                .startOfRun(false);
    }

    public IngestJob getJob() {
        return job;
    }

    public String getJobRunId() {
        return jobRunId;
    }

    public String getTaskId() {
        return taskId;
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
        IngestJobStartedEvent that = (IngestJobStartedEvent) o;
        return Objects.equals(taskId, that.taskId) && Objects.equals(job, that.job) && Objects.equals(startTime, that.startTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, job, startTime);
    }

    @Override
    public String toString() {
        return "IngestJobStartedEvent{" +
                "taskId='" + taskId + '\'' +
                ", job=" + job +
                ", startTime=" + startTime +
                ", startOfRun=" + startOfRun +
                '}';
    }

    public static final class Builder {
        private IngestJob job;
        private String jobRunId;
        private String taskId;
        private Instant startTime;
        private boolean startOfRun;

        private Builder() {
        }

        public Builder job(IngestJob job) {
            this.job = job;
            return this;
        }

        public Builder jobRunId(String jobRunId) {
            this.jobRunId = jobRunId;
            return this;
        }

        public Builder taskId(String taskId) {
            this.taskId = taskId;
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

        public IngestJobStartedEvent build() {
            return new IngestJobStartedEvent(this);
        }
    }
}
