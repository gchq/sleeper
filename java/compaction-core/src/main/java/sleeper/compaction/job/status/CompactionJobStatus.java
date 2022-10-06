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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class CompactionJobStatus {

    private final String jobId;
    private final CompactionJobCreatedStatus createdStatus;
    private final List<CompactionJobRun> jobRunList;
    private final Instant expiryDate;

    private CompactionJobStatus(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        createdStatus = Objects.requireNonNull(builder.createdStatus, "createdStatus must not be null");
        jobRunList = builder.jobRunList;
        expiryDate = builder.expiryDate;
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
        return !jobRunList.isEmpty();
    }

    public Instant getStartUpdateTime() {
        if (isStarted()) {
            return getLatestStartedStatus().getUpdateTime();
        }
        return null;
    }

    public Instant getStartTime() {
        if (isStarted()) {
            return getLatestStartedStatus().getStartTime();
        }
        return null;
    }

    public boolean isFinished() {
        return jobRunList.stream().anyMatch(CompactionJobRun::isFinished);
    }

    public Instant getFinishUpdateTime() {
        if (isFinished()) {
            return getLatestFinishedStatus().getUpdateTime();
        }
        return null;
    }

    public Instant getFinishTime() {
        if (isFinished()) {
            return getLatestFinishedStatus().getSummary().getFinishTime();
        }
        return null;
    }

    public CompactionJobSummary getFinishedSummary() {
        if (isFinished()) {
            return getLatestFinishedStatus().getSummary();
        }
        return null;
    }

    public Instant getExpiryDate() {
        return expiryDate;
    }

    public String getJobId() {
        return jobId;
    }

    public String getTaskId() {
        if (isStarted()) {
            return getLatestJobRun().getTaskId();
        }
        return null;
    }

    public boolean isInPeriod(Instant startTime, Instant endTime) {
        return startTime.isBefore(lastTime())
                && endTime.isAfter(firstTime());
    }

    private Instant firstTime() {
        return createdStatus.getUpdateTime();
    }

    public Instant lastTime() {
        if (isFinished()) {
            return getLatestFinishedStatus().getUpdateTime();
        } else if (isStarted()) {
            return getLatestStartedStatus().getUpdateTime();
        } else {
            return createdStatus.getUpdateTime();
        }
    }

    public CompactionJobStartedStatus getLatestStartedStatus() {
        return jobRunList.stream()
                .map(CompactionJobRun::getStartedStatus)
                .max(Comparator.comparing(CompactionJobStartedStatus::getUpdateTime))
                .orElse(null);
    }

    public CompactionJobFinishedStatus getLatestFinishedStatus() {
        return jobRunList.stream()
                .filter(CompactionJobRun::isFinished)
                .map(CompactionJobRun::getFinishedStatus)
                .max(Comparator.comparing(CompactionJobFinishedStatus::getUpdateTime))
                .orElse(null);
    }

    public CompactionJobRun getLatestJobRun() {
        return jobRunList.stream()
                .max(Comparator.comparing(CompactionJobRun::getLatestUpdateTime))
                .orElse(null);
    }


    public List<CompactionJobRun> getJobRuns() {
        return jobRunList;
    }

    public static final class Builder {
        private String jobId;
        private CompactionJobCreatedStatus createdStatus;
        private List<CompactionJobRun> jobRunList;
        private Instant expiryDate;

        private Builder() {
            jobRunList = new ArrayList<>();
        }

        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder createdStatus(CompactionJobCreatedStatus createdStatus) {
            this.createdStatus = createdStatus;
            return this;
        }

        public Builder jobRun(CompactionJobRun jobRun) {
            this.jobRunList.add(jobRun);
            return this;
        }

        public Builder jobRuns(List<CompactionJobRun> jobRunList) {
            this.jobRunList = jobRunList;
            return this;
        }

        public Builder expiryDate(Instant expiryDate) {
            this.expiryDate = expiryDate;
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
        CompactionJobStatus status = (CompactionJobStatus) o;
        return jobId.equals(status.jobId) && createdStatus.equals(status.createdStatus) && Objects.equals(jobRunList, status.jobRunList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, createdStatus, jobRunList);
    }

    @Override
    public String toString() {
        return "CompactionJobStatus{" +
                "jobId='" + jobId + '\'' +
                ", createdStatus=" + createdStatus +
                ", jobRunList=" + jobRunList +
                ", expiryDate=" + expiryDate +
                '}';
    }
}
