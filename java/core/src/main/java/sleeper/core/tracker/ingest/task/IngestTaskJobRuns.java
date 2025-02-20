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
package sleeper.core.tracker.ingest.task;

import sleeper.core.tracker.job.run.JobRun;
import sleeper.core.tracker.job.run.JobRunReport;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.status.AggregatedTaskJobsFinishedStatus;
import sleeper.core.tracker.job.status.JobStatusUpdate;

import java.time.Instant;
import java.util.List;

/**
 * A summary report of job runs that occurred in an ingest task.
 */
public class IngestTaskJobRuns implements JobRunReport {

    private final JobRun aggregatedRun;
    private final IngestTaskStartedStatus startedStatus;
    private final AggregatedTaskJobsFinishedStatus finishedStatus;

    public IngestTaskJobRuns(JobRun aggregatedRun) {
        this.aggregatedRun = aggregatedRun;
        this.startedStatus = aggregatedRun.getLastStatusOfType(IngestTaskStartedStatus.class).orElseThrow();
        this.finishedStatus = aggregatedRun.getLastStatusOfType(AggregatedTaskJobsFinishedStatus.class).orElse(null);
    }

    @Override
    public String getTaskId() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTaskId'");
    }

    @Override
    public List<JobStatusUpdate> getStatusUpdates() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getStatusUpdates'");
    }

    @Override
    public boolean isFinished() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isFinished'");
    }

    @Override
    public boolean isFinishedSuccessfully() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isFinishedSuccessfully'");
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
