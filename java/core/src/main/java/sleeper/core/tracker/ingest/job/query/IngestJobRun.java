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
package sleeper.core.tracker.ingest.job.query;

import sleeper.core.tracker.job.run.JobRun;
import sleeper.core.tracker.job.run.JobRunReport;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.status.JobRunEndUpdate;
import sleeper.core.tracker.job.status.JobRunStartedUpdate;
import sleeper.core.tracker.job.status.JobStatusUpdate;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * A report of a run of an ingest or bulk import job held in the job tracker.
 */
public class IngestJobRun implements JobRunReport {

    private final JobRun run;
    private final IngestJobStatusType statusType;
    private final IngestJobFilesWrittenAndAdded filesWrittenAndAdded;
    private final IngestJobValidatedStatus validatedStatus;
    private final IngestJobStartedStatus startedStatus;
    private final IngestJobFinishedStatus finishedStatus;
    private final JobRunStartedUpdate validatedOrStartedStatus;
    private final JobRunEndUpdate endedStatus;
    private final IngestJobInfoStatus jobInfoStatus;

    public IngestJobRun(JobRun run) {
        this.run = run;
        this.statusType = IngestJobStatusType.statusTypeOfJobRun(run);
        this.filesWrittenAndAdded = IngestJobFilesWrittenAndAdded.from(run);
        this.validatedStatus = run.getLastStatusOfType(IngestJobValidatedStatus.class).orElse(null);
        this.startedStatus = run.getLastStatusOfType(IngestJobStartedStatus.class).orElse(null);
        this.finishedStatus = run.getLastStatusOfType(IngestJobFinishedStatus.class).orElse(null);
        this.endedStatus = run.getLastStatusOfType(JobRunEndUpdate.class).orElse(null);
        this.jobInfoStatus = run.getLastStatusOfType(IngestJobInfoStatus.class).orElse(null);
        if (validatedStatus != null) {
            this.validatedOrStartedStatus = validatedStatus;
        } else {
            this.validatedOrStartedStatus = startedStatus;
        }
    }

    public IngestJobStatusType getStatusType() {
        return statusType;
    }

    public IngestJobFilesWrittenAndAdded getFilesWrittenAndAdded() {
        return filesWrittenAndAdded;
    }

    /**
     * Retrieves the number of input files in the ingest job.
     *
     * @return the number of input files, or 0 if this was not tracked
     */
    public int getInputFileCount() {
        if (jobInfoStatus != null) {
            return jobInfoStatus.getInputFileCount();
        } else {
            return 0;
        }
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
        if (validatedOrStartedStatus != null) {
            return validatedOrStartedStatus.getStartTime();
        } else {
            return null;
        }
    }

    @Override
    public Instant getFinishTime() {
        if (endedStatus != null) {
            return endedStatus.getFinishTime();
        } else {
            return null;
        }
    }

    @Override
    public JobRunSummary getFinishedSummary() {
        if (endedStatus != null) {
            Instant startTime = Optional.ofNullable(startedStatus)
                    .map(IngestJobStartedStatus::getStartTime)
                    .orElseGet(endedStatus::getFinishTime);
            return JobRunSummary.from(startTime, endedStatus);
        } else {
            return null;
        }
    }

    @Override
    public List<String> getFailureReasons() {
        return Optional.ofNullable(endedStatus).map(JobRunEndUpdate::getFailureReasons).orElseGet(List::of);
    }

    @Override
    public String toString() {
        return run.toString();
    }

}
