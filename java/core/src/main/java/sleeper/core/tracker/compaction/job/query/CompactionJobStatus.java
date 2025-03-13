/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.core.tracker.compaction.job.query;

import sleeper.core.tracker.job.run.JobRun;
import sleeper.core.tracker.job.run.JobRuns;
import sleeper.core.tracker.job.status.JobStatusUpdateRecord;
import sleeper.core.tracker.job.status.JobStatusUpdates;
import sleeper.core.tracker.job.status.TimeWindowQuery;
import sleeper.core.util.DurationStatistics;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingInt;
import static sleeper.core.tracker.compaction.job.query.CompactionJobStatusType.FAILED;
import static sleeper.core.tracker.compaction.job.query.CompactionJobStatusType.FINISHED;
import static sleeper.core.tracker.compaction.job.query.CompactionJobStatusType.IN_PROGRESS;
import static sleeper.core.tracker.compaction.job.query.CompactionJobStatusType.UNCOMMITTED;

/**
 * A report of a compaction job held in the job tracker.
 */
public class CompactionJobStatus {

    private final String jobId;
    private final String partitionId;
    private final Integer inputFilesCount;
    private final Instant createUpdateTime;
    private final JobRuns jobRuns;
    private final transient List<CompactionJobRun> runsLatestFirst;
    private final transient Map<CompactionJobStatusType, Integer> runsByStatusType;
    private final transient CompactionJobStatusType furthestRunStatusType;
    private final Instant expiryDate;

    private CompactionJobStatus(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        if (builder.createdStatus != null) {
            partitionId = builder.createdStatus.getPartitionId();
            inputFilesCount = builder.createdStatus.getInputFilesCount();
            createUpdateTime = builder.createdStatus.getUpdateTime();
        } else {
            partitionId = null;
            inputFilesCount = null;
            createUpdateTime = null;
        }
        jobRuns = builder.jobRuns;
        runsLatestFirst = jobRuns.getRunsLatestFirst().stream()
                .map(CompactionJobRun::new)
                .toList();
        runsByStatusType = runsLatestFirst.stream()
                .collect(groupingBy(CompactionJobRun::getStatusType, summingInt(run -> 1)));
        furthestRunStatusType = CompactionJobStatusType.furthestStatusTypeOfJob(runsByStatusType.keySet());
        expiryDate = builder.expiryDate;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static List<CompactionJobStatus> listFrom(Stream<JobStatusUpdateRecord> records) {
        return streamFrom(records).collect(Collectors.toList());
    }

    public static Stream<CompactionJobStatus> streamFrom(Stream<JobStatusUpdateRecord> records) {
        return JobStatusUpdates.streamFrom(records)
                .map(CompactionJobStatus::from);
    }

    private static CompactionJobStatus from(JobStatusUpdates updates) {
        return builder()
                .jobId(updates.getJobId())
                .createdStatus(updates.getFirstStatusUpdateOfType(CompactionJobCreatedStatus.class).orElse(null))
                .jobRuns(updates.getRuns())
                .expiryDate(updates.getFirstRecord().getExpiryDate())
                .build();
    }

    public static Optional<DurationStatistics> computeStatisticsOfDelayBetweenFinishAndCommit(List<CompactionJobStatus> jobs) {
        return DurationStatistics.fromIfAny(jobs.stream()
                .flatMap(CompactionJobStatus::runDelaysBetweenFinishAndCommit));
    }

    public Instant getCreateUpdateTime() {
        return createUpdateTime;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public Integer getInputFilesCount() {
        return inputFilesCount;
    }

    public boolean isStarted() {
        return !runsLatestFirst.isEmpty();
    }

    public boolean isUnstartedOrInProgress() {
        return !isStarted() || runStatusTypes().contains(IN_PROGRESS) || !runStatusTypes().contains(FINISHED);
    }

    public boolean isAnyRunInProgress() {
        return runStatusTypes().contains(IN_PROGRESS);
    }

    public boolean isAnyRunSuccessful() {
        return runStatusTypes().contains(FINISHED);
    }

    public boolean isAnyRunUnfinishedAndARunFailed() {
        return isAnyRunUnfinished() && isAnyRunFailed();
    }

    public boolean isAnyRunUnfinished() {
        return getRunsInProgress() + getRunsAwaitingCommit() > 0;
    }

    public boolean isAnyRunFailed() {
        return runStatusTypes().contains(FAILED);
    }

    public boolean isAwaitingRetry() {
        return runStatusTypes().equals(Set.of(FAILED));
    }

    public int getRunsInProgress() {
        return runsByStatusType.getOrDefault(IN_PROGRESS, 0);
    }

    public int getRunsAwaitingCommit() {
        return runsByStatusType.getOrDefault(UNCOMMITTED, 0);
    }

    public Stream<Duration> runDelaysBetweenFinishAndCommit() {
        return runsLatestFirst.stream()
                .flatMap(run -> delayBetweenFinishAndCommit(run).stream());
    }

    private Optional<Duration> delayBetweenFinishAndCommit(CompactionJobRun run) {
        return run.getCommittedStatus()
                .flatMap(committedStatus -> run.getSuccessfulFinishedStatus()
                        .map(finishedStatus -> Duration.between(
                                finishedStatus.getFinishTime(),
                                committedStatus.getCommitTime())));
    }

    private Set<CompactionJobStatusType> runStatusTypes() {
        return runsByStatusType.keySet();
    }

    public boolean isMultipleRunsAndAnySuccessful() {
        return isMultipleRuns() && isAnyRunSuccessful();
    }

    public boolean isMultipleRuns() {
        return runsLatestFirst.size() > 1;
    }

    public Instant getExpiryDate() {
        return expiryDate;
    }

    public String getJobId() {
        return jobId;
    }

    public boolean isTaskIdAssigned(String taskId) {
        return jobRuns.isTaskIdAssigned(taskId);
    }

    public boolean isInPeriod(Instant windowStartTime, Instant windowEndTime) {
        TimeWindowQuery timeWindowQuery = new TimeWindowQuery(windowStartTime, windowEndTime);
        if (isUnstartedOrInProgress()) {
            return timeWindowQuery.isUnfinishedJobInWindow(createUpdateTime);
        } else {
            return timeWindowQuery.isFinishedJobInWindow(
                    createUpdateTime, jobRuns.lastTime().orElseThrow());
        }
    }

    public List<CompactionJobRun> getRunsLatestFirst() {
        return runsLatestFirst;
    }

    public CompactionJobStatusType getFurthestStatusType() {
        return furthestRunStatusType;
    }

    public static final class Builder {
        private String jobId;
        private CompactionJobCreatedStatus createdStatus;
        private JobRuns jobRuns;
        private Instant expiryDate;

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

        public Builder singleJobRun(JobRun jobRun) {
            return jobRunsLatestFirst(Collections.singletonList(jobRun));
        }

        public Builder jobRunsLatestFirst(List<JobRun> jobRunList) {
            return jobRuns(JobRuns.latestFirst(jobRunList));
        }

        public Builder jobRuns(JobRuns jobRuns) {
            this.jobRuns = jobRuns;
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
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CompactionJobStatus)) {
            return false;
        }
        CompactionJobStatus other = (CompactionJobStatus) obj;
        return Objects.equals(jobId, other.jobId) && Objects.equals(partitionId, other.partitionId) && Objects.equals(inputFilesCount, other.inputFilesCount)
                && Objects.equals(createUpdateTime, other.createUpdateTime) && Objects.equals(jobRuns, other.jobRuns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, partitionId, inputFilesCount, createUpdateTime, jobRuns);
    }

    @Override
    public String toString() {
        return "CompactionJobStatus{jobId=" + jobId + ", partitionId=" + partitionId + ", inputFilesCount=" + inputFilesCount + ", createUpdateTime=" + createUpdateTime + ", jobRuns=" + jobRuns
                + ", expiryDate=" + expiryDate + "}";
    }
}
