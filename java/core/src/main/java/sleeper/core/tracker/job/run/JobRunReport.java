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

import sleeper.core.tracker.job.status.JobStatusUpdate;

import java.time.Instant;
import java.util.List;

/**
 * A summary of the status of a job run. Implemented separately for compaction and ingest.
 */
public interface JobRunReport {

    /**
     * Retrieves the ID of the task that ran the job.
     *
     * @return the task ID
     */
    String getTaskId();

    /**
     * Returns a list of status updates that occurred during this job run. These are ordered by update time as recorded
     * in the process that made them, earliest time first.
     *
     * @return the updates
     */
    List<JobStatusUpdate> getStatusUpdates();

    /**
     * Checks if the run has finished. If the results of the run are committed as a separate operation, it will count
     * as finished here even if it is uncommitted.
     *
     * @return true if the run has either failed, or successfully finished
     */
    boolean isFinished();

    /**
     * Checks if the run has finished successfully. If the results of the run are committed as a separate operation, it
     * will count as finished here even if it is uncommitted.
     *
     * @return true if the run has finished successfully
     */
    boolean isFinishedSuccessfully();

    /**
     * Retrieves the time of the first update in the workflow of this job run. This is when the job was received, which
     * may or may not be when it started processing. For example, bulk import jobs are accepted and then passed to be
     * run in a Spark cluster.
     * <p>
     * This should be based on the type of update, so when a bulk import is accepted and then starts in a cluster it
     * should always be the accepted update, even if the clock in the cluster says the time is before it was accepted.
     * Because the processing of a run can be distributed, and different machines can disagree on the time, it's
     * possible that this is not the first update according to the times recorded in each update.
     *
     * @return the start time
     */
    Instant getStartTime();

    /**
     * Retrieves the time of the last update in the workflow of this job run, if it is finished or failed. If the run
     * has not yet finished, this will return null. If the results of the run are committed as a separate operation, the
     * commit time is never returned from this method.
     * <p>
     * This should be based on the type of update, so it's possible that another update in the run may be after this
     * update according to the times recorded in each update. This is because the processing of a run can be
     * distributed, and different machines can disagree on the time.
     *
     * @return the finish time, or null if it is not finished
     */
    Instant getFinishTime();

    /**
     * Retrieves a summary of the job run if it has finished or failed. This should report the time spent actively
     * working on the job and processing records.
     *
     * @return the finished summary, or null if the run has not finished
     */
    JobRunSummary getFinishedSummary();

    /**
     * Retrieves descriptions of the reasons why the job run failed.
     *
     * @return the list of reasons for the failure
     */
    List<String> getFailureReasons();

    /**
     * Generates a string list detailing all the reasons provided why the job run failed. The length of the string
     * created will be restricted to the given length.
     *
     * @param  maxLength the maximum length of the resulting string before it is concatenated
     * @return           combined list of reasons for failure
     */
    default String getFailureReasonsDisplay(int maxLength) {
        return JobRunFailureReasons.getFailureReasonsDisplay(maxLength, getFailureReasons());
    }

}
