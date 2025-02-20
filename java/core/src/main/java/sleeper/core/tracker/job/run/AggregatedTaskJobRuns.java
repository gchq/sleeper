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
package sleeper.core.tracker.job.run;

import sleeper.core.tracker.job.status.JobRunFinishedStatus;
import sleeper.core.tracker.job.status.JobRunStartedUpdate;
import sleeper.core.tracker.job.status.JobStatusUpdate;

import java.time.Instant;
import java.util.List;

/**
 * A summary report of job runs that occurred in a task.
 */
public class AggregatedTaskJobRuns implements JobRunReport {

    private final JobRun aggregatedRun;
    private final JobRunStartedUpdate startedStatus;
    private final JobRunFinishedStatus finishedStatus;

    public AggregatedTaskJobRuns(JobRun aggregatedRun) {
        this.aggregatedRun = aggregatedRun;
        this.startedStatus = aggregatedRun.getLastStatusOfType(JobRunStartedUpdate.class).orElseThrow();
        this.finishedStatus = aggregatedRun.getLastStatusOfType(JobRunFinishedStatus.class).orElse(null);
    }

    @Override
    public String getTaskId() {
        return aggregatedRun.getTaskId();
    }

    @Override
    public List<JobStatusUpdate> getStatusUpdates() {
        return aggregatedRun.getStatusUpdates();
    }

    @Override
    public boolean isFinished() {
        return finishedStatus != null;
    }

    @Override
    public boolean isFinishedSuccessfully() {
        return finishedStatus != null;
    }

    @Override
    public Instant getStartTime() {
        return startedStatus.getStartTime();
    }

    @Override
    public Instant getFinishTime() {
        if (finishedStatus != null) {
            return finishedStatus.getFinishTime();
        } else {
            return null;
        }
    }

    @Override
    public JobRunSummary getFinishedSummary() {
        if (finishedStatus != null) {
            return JobRunSummary.from(startedStatus, finishedStatus);
        } else {
            return null;
        }
    }

}
