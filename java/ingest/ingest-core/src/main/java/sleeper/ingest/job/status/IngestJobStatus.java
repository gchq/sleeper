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

package sleeper.ingest.job.status;

import sleeper.core.record.process.status.JobStatusUpdates;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessRuns;
import sleeper.core.record.process.status.ProcessStatusUpdateRecord;
import sleeper.core.record.process.status.TimeWindowQuery;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toSet;
import static sleeper.ingest.job.status.IngestJobStatusType.FINISHED;

public class IngestJobStatus {
    private final String jobId;
    private final ProcessRuns jobRuns;
    private final Instant expiryDate;

    private IngestJobStatus(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        jobRuns = Objects.requireNonNull(builder.jobRuns, "jobRuns must not be null");
        expiryDate = builder.expiryDate;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Stream<IngestJobStatus> streamFrom(Stream<ProcessStatusUpdateRecord> records) {
        return JobStatusUpdates.streamFrom(records)
                .map(IngestJobStatus::from)
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    private static Optional<IngestJobStatus> from(JobStatusUpdates statusUpdates) {
        ProcessRuns runs = statusUpdates.getRuns();
        if (!runs.isStarted()) {
            return Optional.empty();
        }
        return Optional.of(builder()
                .jobRuns(runs)
                .jobId(statusUpdates.getJobId())
                .expiryDate(statusUpdates.getFirstRecord().getExpiryDate())
                .build());
    }

    public String getJobId() {
        return jobId;
    }

    public int getInputFilesCount() {
        return jobRuns.getLatestRun()
                .map(ProcessRun::getStartedStatus)
                .filter(startedUpdate -> startedUpdate instanceof IngestJobInfoStatus)
                .map(startedUpdate -> (IngestJobInfoStatus) startedUpdate)
                .map(IngestJobInfoStatus::getInputFileCount)
                .orElse(0);
    }

    public Instant getExpiryDate() {
        return expiryDate;
    }

    public boolean isTaskIdAssigned(String taskId) {
        return jobRuns.isTaskIdAssigned(taskId);
    }

    public List<ProcessRun> getJobRuns() {
        return jobRuns.getRunsLatestFirst();
    }

    public boolean isUnfinishedOrAnyRunInProgress() {
        Set<IngestJobStatusType> runStatuses = runStatusTypes().collect(toSet());
        return runStatuses.stream().anyMatch(IngestJobStatusType::isRunInProgress)
                || runStatuses.stream().noneMatch(IngestJobStatusType::isEndOfJob);
    }

    public boolean isAnyRunSuccessful() {
        return runStatusTypes().anyMatch(status -> status == FINISHED);
    }

    public boolean isInPeriod(Instant windowStartTime, Instant windowEndTime) {
        TimeWindowQuery timeWindowQuery = new TimeWindowQuery(windowStartTime, windowEndTime);
        if (jobRuns.isFinishedAndNoRunsInProgress()) {
            return timeWindowQuery.isFinishedProcessInWindow(
                    jobRuns.firstTime().orElseThrow(), jobRuns.lastTime().orElseThrow());
        } else {
            return timeWindowQuery.isUnfinishedProcessInWindow(jobRuns.firstTime().orElseThrow());
        }
    }

    public IngestJobStatusType getFurthestStatusType() {
        return runStatusTypes()
                .max(comparing(IngestJobStatusType::getOrder))
                .orElseThrow();
    }

    private Stream<IngestJobStatusType> runStatusTypes() {
        return jobRuns.getRunsLatestFirst().stream()
                .map(ProcessRun::getLatestUpdate)
                .map(IngestJobStatusType::of);
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
        return jobId.equals(that.jobId)
                && jobRuns.equals(that.jobRuns)
                && Objects.equals(expiryDate, that.expiryDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, jobRuns, expiryDate);
    }

    @Override
    public String toString() {
        return "IngestJobStatus{" +
                "jobId='" + jobId + '\'' +
                ", jobRuns=" + jobRuns +
                ", expiryDate=" + expiryDate +
                '}';
    }

    public static final class Builder {
        private String jobId;
        private ProcessRuns jobRuns;
        private Instant expiryDate;

        private Builder() {
        }

        public Builder jobId(String jobId) {
            this.jobId = jobId;
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
