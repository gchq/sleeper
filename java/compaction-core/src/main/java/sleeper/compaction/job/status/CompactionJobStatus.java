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
package sleeper.compaction.job.status;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobSummary;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class CompactionJobStatus {

    private final String jobId;
    private final CompactionJobCreatedStatus createdStatus;
    private final CompactionJobStartedStatus startedStatus;
    private final CompactionJobFinishedStatus finishedStatus;

    private CompactionJobStatus(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        createdStatus = Objects.requireNonNull(builder.createdStatus, "createdStatus must not be null");
        startedStatus = builder.startedStatus;
        finishedStatus = builder.finishedStatus;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static CompactionJobStatus created(CompactionJob job, Instant updateTime) {
        return builder()
                .jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, updateTime))
                .build();
    }

    public Instant getCreateUpdateTime() {
        return createdStatus.getUpdateTime();
    }

    public String getPartitionId() {
        return createdStatus.getPartitionId();
    }

    public int getInputFilesCount() {
        return createdStatus.getInputFilesCount();
    }

    public List<String> getChildPartitionIds() {
        return createdStatus.getChildPartitionIds();
    }

    public boolean isSplittingCompaction() {
        return !getChildPartitionIds().isEmpty();
    }

    public boolean isStarted() {
        return startedStatus != null;
    }

    public Instant getStartUpdateTime() {
        if (isStarted()) {
            return startedStatus.getUpdateTime();
        }
        return null;
    }

    public Instant getStartTime() {
        if (isStarted()) {
            return startedStatus.getStartTime();
        }
        return null;
    }

    public boolean isFinished() {
        return finishedStatus != null;
    }

    public Instant getFinishUpdateTime() {
        if (isFinished()) {
            return finishedStatus.getUpdateTime();
        }
        return null;
    }

    public Instant getFinishTime() {
        if (isFinished()) {
            return finishedStatus.getSummary().getFinishTime();
        }
        return null;
    }

    public CompactionJobSummary getFinishedSummary() {
        if (isFinished()) {
            return finishedStatus.getSummary();
        }
        return null;
    }

    public static final class Builder {
        private String jobId;
        private CompactionJobCreatedStatus createdStatus;
        private CompactionJobStartedStatus startedStatus;
        private CompactionJobFinishedStatus finishedStatus;

        private Builder() {
        }

        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder createdStatus(CompactionJobCreatedStatus createdStatus) {
            this.createdStatus = createdStatus;
            return this;
        }

        public Builder startedStatus(CompactionJobStartedStatus startedStatus) {
            this.startedStatus = startedStatus;
            return this;
        }

        public Builder finishedStatus(CompactionJobFinishedStatus finishedStatus) {
            this.finishedStatus = finishedStatus;
            return this;
        }

        public CompactionJobStatus build() {
            return new CompactionJobStatus(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactionJobStatus that = (CompactionJobStatus) o;
        return jobId.equals(that.jobId) && createdStatus.equals(that.createdStatus);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, createdStatus);
    }

    @Override
    public String toString() {
        return "CompactionJobStatus{" +
                "jobId='" + jobId + '\'' +
                ", createdStatus=" + createdStatus +
                '}';
    }
}
