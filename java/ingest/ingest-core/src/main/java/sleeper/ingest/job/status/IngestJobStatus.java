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

import sleeper.core.record.process.status.JobStatusUpdates;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessRuns;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class IngestJobStatus {
    private final String jobId;
    private final int inputFilesCount;
    private final ProcessRuns jobRuns;
    private final Instant expiryDate;

    private IngestJobStatus(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        inputFilesCount = builder.inputFileCount;
        jobRuns = Objects.requireNonNull(builder.jobRuns, "jobRuns must not be null");
        expiryDate = builder.expiryDate;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static IngestJobStatus from(JobStatusUpdates statusUpdates) {
        return builder()
                .jobRuns(statusUpdates.getRuns())
                .jobId(statusUpdates.getJobId())
                .inputFileCount(lastInputFileCount(statusUpdates.getRuns()))
                .build();
    }

    private static int lastInputFileCount(ProcessRuns runs) {
        return runs.getLatestRun()
                .map(run -> (IngestJobStartedStatus) run.getStartedStatus())
                .map(IngestJobStartedStatus::getInputFileCount)
                .orElse(0);
    }

    public String getJobId() {
        return jobId;
    }

    public int getInputFilesCount() {
        return inputFilesCount;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IngestJobStatus that = (IngestJobStatus) o;
        return inputFilesCount == that.inputFilesCount
                && jobId.equals(that.jobId)
                && jobRuns.equals(that.jobRuns)
                && Objects.equals(expiryDate, that.expiryDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, inputFilesCount, jobRuns, expiryDate);
    }

    @Override
    public String toString() {
        return "IngestJobStatus{" +
                "jobId='" + jobId + '\'' +
                ", inputFilesCount=" + inputFilesCount +
                ", jobRuns=" + jobRuns +
                ", expiryDate=" + expiryDate +
                '}';
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
            this.jobRuns = ProcessRuns.latestFirst(Collections.singletonList(jobRun));
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
