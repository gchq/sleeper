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
package sleeper.core.tracker.ingest.job.query;

import sleeper.core.tracker.job.run.JobRun;
import sleeper.core.tracker.job.run.JobRunReport;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.status.JobRunEndUpdate;
import sleeper.core.tracker.job.status.JobStatusUpdate;

import java.time.Instant;
import java.util.List;

/**
 * A report of a run of an ingest or bulk import job held in the job tracker.
 */
public class IngestJobRun implements JobRunReport {

    private final JobRun run;
    private final JobRunEndUpdate endedStatus;
    private final IngestJobFinishedStatus finishedStatus;

    public IngestJobRun(JobRun run) {
        this.run = run;
        this.endedStatus = run.getLastStatusOfType(JobRunEndUpdate.class).orElse(null);
        this.finishedStatus = run.getLastStatusOfType(IngestJobFinishedStatus.class).orElse(null);
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
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getStartTime'");
    }

    @Override
    public Instant getFinishTime() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getFinishTime'");
    }

    @Override
    public JobRunSummary getFinishedSummary() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getFinishedSummary'");
    }

}
