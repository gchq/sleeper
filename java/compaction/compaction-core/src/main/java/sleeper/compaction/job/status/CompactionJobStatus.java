/*
 * Copyright 2022-2024 Crown Copyright
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

import sleeper.core.record.process.status.JobStatusUpdates;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessRuns;
import sleeper.core.record.process.status.ProcessStatusUpdateRecord;
import sleeper.core.record.process.status.TimeWindowQuery;
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
import static sleeper.compaction.job.status.CompactionJobStatusType.CREATED;
import static sleeper.compaction.job.status.CompactionJobStatusType.FAILED;
import static sleeper.compaction.job.status.CompactionJobStatusType.FILES_ASSIGNED;
import static sleeper.compaction.job.status.CompactionJobStatusType.FINISHED;
import static sleeper.compaction.job.status.CompactionJobStatusType.IN_PROGRESS;
import static sleeper.compaction.job.status.CompactionJobStatusType.UNCOMMITTED;

public class CompactionJobStatus {

    private final String jobId;
    private final CompactionJobCreatedStatus createdStatus;
    private final CompactionJobInputFilesAssignedStatus filesAssignedStatus;
    private final ProcessRuns jobRuns;
    private final transient Map<CompactionJobStatusType, Integer> runsByStatusType;
    private final transient CompactionJobStatusType furthestRunStatusType;
    private final Instant expiryDate;

    private CompactionJobStatus(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        createdStatus = Objects.requireNonNull(builder.createdStatus, "createdStatus must not be null");
        filesAssignedStatus = builder.filesAssignedStatus;
        jobRuns = builder.jobRuns;
        runsByStatusType = jobRuns.getRunsLatestFirst().stream()
                .collect(groupingBy(CompactionJobStatusType::statusTypeOfJobRun, summingInt(run -> 1)));
        furthestRunStatusType = CompactionJobStatusType.furthestStatusTypeOfJob(
                filesAssignedStatus == null ? CREATED : FILES_ASSIGNED,
                runsByStatusType.keySet());
        expiryDate = builder.expiryDate;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static List<CompactionJobStatus> listFrom(Stream<ProcessStatusUpdateRecord> records) {
        return streamFrom(records).collect(Collectors.toList());
    }

    public static Stream<CompactionJobStatus> streamFrom(Stream<ProcessStatusUpdateRecord> records) {
        return JobStatusUpdates.streamFrom(records)
                .map(CompactionJobStatus::from)
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    private static Optional<CompactionJobStatus> from(JobStatusUpdates updates) {
        return updates.getFirstStatusUpdateOfType(CompactionJobCreatedStatus.class)
                .map(createdStatus -> builder()
                        .jobId(updates.getJobId())
                        .createdStatus(createdStatus)
                        .filesAssignedStatus(updates.getFirstStatusUpdateOfType(CompactionJobInputFilesAssignedStatus.class).orElse(null))
                        .jobRuns(updates.getRuns())
                        .expiryDate(updates.getFirstRecord().getExpiryDate())
                        .build());
    }

    public static Optional<DurationStatistics> computeStatisticsOfDelayBetweenCreationAndFilesAssignment(List<CompactionJobStatus> jobs) {
        return DurationStatistics.fromIfAny(jobs.stream()
                .flatMap(job -> job.getDelayBetweenCreatedAndFilesAssigned().stream()));
    }

    public static Optional<DurationStatistics> computeStatisticsOfDelayBetweenFinishAndCommit(List<CompactionJobStatus> jobs) {
        return DurationStatistics.fromIfAny(jobs.stream()
                .flatMap(CompactionJobStatus::runDelaysBetweenFinishAndCommit));
    }

    public Instant getCreateUpdateTime() {
        return createdStatus.getUpdateTime();
    }

    public Optional<Instant> getInputFilesAssignedUpdateTime() {
        return Optional.ofNullable(filesAssignedStatus)
                .map(CompactionJobInputFilesAssignedStatus::getUpdateTime);
    }

    public String getPartitionId() {
        return createdStatus.getPartitionId();
    }

    public int getInputFilesCount() {
        return createdStatus.getInputFilesCount();
    }

    public boolean isStarted() {
        return jobRuns.isStarted();
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

    public Optional<Duration> getDelayBetweenCreatedAndFilesAssigned() {
        return Optional.ofNullable(filesAssignedStatus)
                .map(filesAssignedStatus -> Duration.between(
                        createdStatus.getUpdateTime(), filesAssignedStatus.getUpdateTime()));
    }

    public Stream<Duration> runDelaysBetweenFinishAndCommit() {
        return jobRuns.getRunsLatestFirst().stream()
                .flatMap(run -> delayBetweenFinishAndCommit(run).stream());
    }

    private Optional<Duration> delayBetweenFinishAndCommit(ProcessRun run) {
        return run.getLastStatusOfType(CompactionJobCommittedStatus.class)
                .flatMap(committedStatus -> run.getLastStatusOfType(CompactionJobFinishedStatus.class)
                        .map(finishedStatus -> Duration.between(
                                finishedStatus.getSummary().getFinishTime(),
                                committedStatus.getCommitTime())));
    }

    private Set<CompactionJobStatusType> runStatusTypes() {
        return runsByStatusType.keySet();
    }

    public boolean isMultipleRunsAndAnySuccessful() {
        return isMultipleRuns() && isAnyRunSuccessful();
    }

    public boolean isMultipleRuns() {
        return jobRuns.getRunsLatestFirst().size() > 1;
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
            return timeWindowQuery.isUnfinishedProcessInWindow(createdStatus.getUpdateTime());
        } else {
            return timeWindowQuery.isFinishedProcessInWindow(
                    createdStatus.getUpdateTime(), jobRuns.lastTime().orElseThrow());
        }
    }

    public List<ProcessRun> getJobRuns() {
        return jobRuns.getRunsLatestFirst();
    }

    public CompactionJobStatusType getFurthestStatusType() {
        return furthestRunStatusType;
    }

    public static final class Builder {
        private String jobId;
        private CompactionJobCreatedStatus createdStatus;
        private CompactionJobInputFilesAssignedStatus filesAssignedStatus;
        private ProcessRuns jobRuns;
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

        public Builder filesAssignedStatus(CompactionJobInputFilesAssignedStatus filesAssignedStatus) {
            this.filesAssignedStatus = filesAssignedStatus;
            return this;
        }

        public Builder singleJobRun(ProcessRun jobRun) {
            return jobRunsLatestFirst(Collections.singletonList(jobRun));
        }

        public Builder jobRunsLatestFirst(List<ProcessRun> jobRunList) {
            return jobRuns(ProcessRuns.latestFirst(jobRunList));
        }

        public Builder jobRuns(ProcessRuns jobRuns) {
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
        return Objects.equals(jobId, other.jobId) && Objects.equals(createdStatus, other.createdStatus)
                && Objects.equals(filesAssignedStatus, other.filesAssignedStatus)
                && Objects.equals(jobRuns, other.jobRuns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, createdStatus, filesAssignedStatus, jobRuns);
    }

    @Override
    public String toString() {
        return "CompactionJobStatus{jobId=" + jobId + ", createdStatus=" + createdStatus +
                ", filesAssignedStatus=" + filesAssignedStatus + ", jobRuns=" + jobRuns +
                ", expiryDate=" + expiryDate + "}";
    }
}
