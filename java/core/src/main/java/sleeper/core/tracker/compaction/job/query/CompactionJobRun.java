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
import sleeper.core.tracker.job.run.JobRunReport;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.status.JobRunEndUpdate;
import sleeper.core.tracker.job.status.JobStatusUpdate;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * A report of a run of a compaction job held in the job tracker.
 */
public class CompactionJobRun implements JobRunReport {

    private final JobRun run;
    private final CompactionJobStatusType statusType;
    private final CompactionJobStartedStatus startedStatus;
    private final JobRunEndUpdate endedStatus;
    private final CompactionJobFinishedStatus finishedStatus;
    private final CompactionJobCommittedStatus committedStatus;

    public CompactionJobRun(JobRun run) {
        this.run = run;
        this.statusType = CompactionJobStatusType.statusTypeOfJobRun(run);
        this.startedStatus = run.getLastStatusOfType(CompactionJobStartedStatus.class).orElse(null);
        this.endedStatus = run.getLastStatusOfType(JobRunEndUpdate.class).orElse(null);
        this.finishedStatus = run.getLastStatusOfType(CompactionJobFinishedStatus.class).orElse(null);
        this.committedStatus = run.getLastStatusOfType(CompactionJobCommittedStatus.class).orElse(null);
    }

    public CompactionJobStatusType getStatusType() {
        return statusType;
    }

    public Optional<CompactionJobFinishedStatus> getSuccessfulFinishedStatus() {
        return Optional.ofNullable(finishedStatus);
    }

    public Optional<CompactionJobCommittedStatus> getCommittedStatus() {
        return Optional.ofNullable(committedStatus);
    }

    @Override
    public String getTaskId() {
        return run.getTaskId();
    }

    @Override
    public List<JobStatusUpdate> getStatusUpdates() {
        return run.getStatusUpdates();
    }

    @Override
    public boolean isFinished() {
        return endedStatus != null;
    }

    @Override
    public boolean isFinishedSuccessfully() {
        return finishedStatus != null;
    }

    @Override
    public Instant getStartTime() {
        if (startedStatus != null) {
            return startedStatus.getStartTime();
        } else {
            return null;
        }
    }

    @Override
    public Instant getFinishTime() {
        if (isFinished()) {
            return endedStatus.getFinishTime();
        } else {
            return null;
        }
    }

    @Override
    public JobRunSummary getFinishedSummary() {
        if (endedStatus != null) {
            Instant startTime = Optional.ofNullable(startedStatus)
                    .map(CompactionJobStartedStatus::getStartTime)
                    .orElseGet(endedStatus::getFinishTime);
            return JobRunSummary.from(startTime, endedStatus);
        } else {
            return null;
        }
    }

    @Override
    public String getFailureReasons(int maxLength) {
        return JobRunReport.getFailureReasons(endedStatus, maxLength);
    }

    @Override
    public String toString() {
        return run.toString();
    }

}
