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

import static java.util.stream.Collectors.toUnmodifiableSet;
import static sleeper.ingest.job.status.IngestJobStatusType.FINISHED;

/**
 * Stores the status of an ingest job. This is used for reporting on the state of ingest jobs.
 */
public class IngestJobStatus {
    private final String jobId;
    private final ProcessRuns jobRuns;
    private final transient Set<IngestJobStatusType> runStatusTypes;
    private final transient IngestJobStatusType furthestRunStatusType;
    private final Instant expiryDate;

    private IngestJobStatus(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        jobRuns = Objects.requireNonNull(builder.jobRuns, "jobRuns must not be null");
        runStatusTypes = jobRuns.getRunsLatestFirst().stream()
                .map(IngestJobStatusType::statusTypeOfJobRun)
                .collect(toUnmodifiableSet());
        furthestRunStatusType = IngestJobStatusType.statusTypeOfFurthestRunOfJob(runStatusTypes);
        expiryDate = builder.expiryDate;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a stream of ingest job statuses from a stream of process status update records. The records passed into
     * this method come from the underlying {@link IngestJobStatusStore}, and this constructor collects them, building
     * them into a format used for reporting.
     *
     * @param  records the stream of {@link ProcessStatusUpdateRecord}s
     * @return         a stream of ingest job statuses
     */
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

    /**
     * Checks whether the task ID is assigned to one or more job runs.
     *
     * @param  taskId the task ID to check
     * @return        whether the task ID is assigned to one or more job runs
     */
    public boolean isTaskIdAssigned(String taskId) {
        return jobRuns.isTaskIdAssigned(taskId);
    }

    public List<ProcessRun> getJobRuns() {
        return jobRuns.getRunsLatestFirst();
    }

    /**
     * Checks whether the job is unfinished. Counts as unfinished if a run of the job is accepted or in progress, or if
     * all the runs of the job failed and it may be retried. If the job was rejected, it will not count as unfinished.
     *
     * @return true if the job is unfinished
     */
    public boolean isUnfinishedOrAnyRunInProgress() {
        return runStatusTypes().anyMatch(IngestJobStatusType::isRunInProgress)
                || runStatusTypes().noneMatch(IngestJobStatusType::isEndOfJob);
    }

    public boolean isAnyRunSuccessful() {
        return runStatusTypes.contains(FINISHED);
    }

    public boolean isAnyRunInProgress() {
        return runStatusTypes().anyMatch(status -> status.isRunInProgress());
    }

    /**
     * Checks whether one or more job runs were performed in a time window.
     *
     * @param  windowStartTime the start of the time window
     * @param  windowEndTime   the end of the time window
     * @return                 whether one or more job runs were performed in the time window
     */
    public boolean isInPeriod(Instant windowStartTime, Instant windowEndTime) {
        TimeWindowQuery timeWindowQuery = new TimeWindowQuery(windowStartTime, windowEndTime);
        if (isUnfinishedOrAnyRunInProgress()) {
            return timeWindowQuery.isUnfinishedProcessInWindow(jobRuns.firstTime().orElseThrow());
        } else {
            return timeWindowQuery.isFinishedProcessInWindow(
                    jobRuns.firstTime().orElseThrow(), jobRuns.lastTime().orElseThrow());
        }
    }

    public IngestJobStatusType getFurthestRunStatusType() {
        return furthestRunStatusType;
    }

    private Stream<IngestJobStatusType> runStatusTypes() {
        return runStatusTypes.stream();
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

    /**
     * Builder class for ingest job status objects.
     */
    public static final class Builder {
        private String jobId;
        private ProcessRuns jobRuns;
        private Instant expiryDate;

        private Builder() {
        }

        /**
         * Sets the ingest job ID.
         *
         * @param  jobId the ingest job ID
         * @return       the builder
         */
        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        /**
         * Sets the job runs.
         *
         * @param  jobRuns the job runs
         * @return         the builder
         */
        public Builder jobRuns(ProcessRuns jobRuns) {
            this.jobRuns = jobRuns;
            return this;
        }

        /**
         * Sets the expiry date. This is the date after which the job status will be removed from the status store.
         *
         * @param  expiryDate the expiry date
         * @return            the builder
         */
        public Builder expiryDate(Instant expiryDate) {
            this.expiryDate = expiryDate;
            return this;
        }

        public IngestJobStatus build() {
            return new IngestJobStatus(this);
        }
    }
}
