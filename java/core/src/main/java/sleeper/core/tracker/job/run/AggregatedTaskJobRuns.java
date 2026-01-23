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
package sleeper.core.tracker.job.run;

import sleeper.core.tracker.job.status.AggregatedTaskJobsFinishedStatus;
import sleeper.core.tracker.job.status.JobRunStartedUpdate;
import sleeper.core.tracker.job.status.JobStatusUpdate;

import java.time.Instant;
import java.util.List;

/**
 * A summary report of job runs that occurred in a task.
 */
public class AggregatedTaskJobRuns implements JobRunReport {

    private final String taskId;
    private final JobRunStartedUpdate startedStatus;
    private final AggregatedTaskJobsFinishedStatus finishedStatus;

    private AggregatedTaskJobRuns(String taskId, JobRunStartedUpdate startedStatus, AggregatedTaskJobsFinishedStatus finishedStatus) {
        this.taskId = taskId;
        this.startedStatus = startedStatus;
        this.finishedStatus = finishedStatus;
    }

    /**
     * Creates an aggregated report of job runs in a task.
     *
     * @param  taskId         the task ID
     * @param  startedStatus  the status update when the task started
     * @param  finishedStatus the aggregated result of finished jobs in the task, or null if the task is not finished
     * @return                the report
     */
    public static AggregatedTaskJobRuns from(String taskId, JobRunStartedUpdate startedStatus, AggregatedTaskJobsFinishedStatus finishedStatus) {
        return new AggregatedTaskJobRuns(taskId, startedStatus, finishedStatus);
    }

    @Override
    public String getTaskId() {
        return taskId;
    }

    @Override
    public List<JobStatusUpdate> getStatusUpdates() {
        if (finishedStatus != null) {
            return List.of(startedStatus, finishedStatus);
        } else {
            return List.of(startedStatus);
        }
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

    @Override
    public List<String> getFailureReasons() {
        return List.of();
    }

}
