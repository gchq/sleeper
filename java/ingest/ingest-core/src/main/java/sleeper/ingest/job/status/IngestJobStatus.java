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

package sleeper.ingest.job.status;

import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessRuns;
import sleeper.core.record.process.status.ProcessStartedStatus;
import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class IngestJobStatus {
    private final String jobId;
    private final int inputFileCount;
    private final ProcessRuns jobRuns;
    private final Instant expiryDate;

    private IngestJobStatus(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        inputFileCount = builder.inputFileCount;
        jobRuns = Objects.requireNonNull(builder.jobRuns, "jobRuns must not be null");
        expiryDate = builder.expiryDate;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static IngestJobStatus started(IngestJob ingestJob, String taskId, Instant updateTime, Instant startTime) {
        return builder()
                .jobId(ingestJob.getId())
                .inputFileCount(ingestJob.getFiles().size())
                .jobRun(ProcessRun.started(taskId,
                        ProcessStartedStatus.updateAndStartTime(updateTime, startTime)))
                .build();
    }

    public String getJobId() {
        return jobId;
    }

    public int getInputFileCount() {
        return inputFileCount;
    }

    public Instant getExpiryDate() {
        return expiryDate;
    }

    public boolean isTaskIdAssigned(String taskId) {
        return jobRuns.isTaskIdAssigned(taskId);
    }

    public List<ProcessRun> getJobRuns() {
        return jobRuns.getRunList();
    }

    public boolean isFinished() {
        return jobRuns.isFinished();
    }

    public static final class Builder {
        private String jobId;
        private int inputFileCount;
        private ProcessRuns jobRuns;
        private Instant expiryDate;

        private Builder() {
        }

        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder inputFileCount(int inputFileCount) {
            this.inputFileCount = inputFileCount;
            return this;
        }

        public Builder jobRun(ProcessRun jobRun) {
            this.jobRuns = ProcessRuns.latestFirst(jobRun);
            return this;
        }

        public Builder jobRuns(ProcessRuns jobRuns) {
            this.jobRuns = jobRuns;
            return this;
        }

        public Builder expiryDate(Instant expiryDate) {
            this.expiryDate = expiryDate;
            return this;
        }

        public IngestJobStatus build() {
            return new IngestJobStatus(this);
        }
    }
}
