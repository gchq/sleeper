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

package sleeper.core.tracker.job.status;

import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Information about runs of a job that were tracked. A job may be run multiple times, potentially in parallel on
 * different tasks. These are detected by correlating updates stored in the job tracker.
 */
public class ProcessRuns {
    private final List<ProcessRun> latestFirst;

    ProcessRuns(List<ProcessRun> latestFirst) {
        this.latestFirst = Collections.unmodifiableList(Objects.requireNonNull(latestFirst, "latestFirst must not be null"));
    }

    /**
     * Creates an instance of this class from a list of runs. These must be sorted by the order that each run started,
     * most recently started run first.
     *
     * @param  latestFirst the sorted list of runs
     * @return             an instance of this class
     */
    public static ProcessRuns latestFirst(List<ProcessRun> latestFirst) {
        return new ProcessRuns(latestFirst);
    }

    /**
     * Creates an instance of this class from records in a job tracker. The records must be sorted by the time of the
     * update, most recent first. These will be correlated to find which updates occurred in the same run.
     *
     * @param  recordList the list of records sorted by latest first
     * @return            an instance of this class
     */
    public static ProcessRuns fromRecordsLatestFirst(List<ProcessStatusUpdateRecord> recordList) {
        ProcessRunsBuilder builder = new ProcessRunsBuilder();
        for (int i = recordList.size() - 1; i >= 0; i--) {
            builder.add(recordList.get(i));
        }
        return builder.build();
    }

    public boolean isStarted() {
        return !latestFirst.isEmpty();
    }

    /**
     * Checks if any process run was assigned to the provided task ID.
     *
     * @param  taskId the task ID to check
     * @return        whether a process run was assigned to the task ID
     */
    public boolean isTaskIdAssigned(String taskId) {
        return latestFirst.stream().anyMatch(run -> taskId.equals(run.getTaskId()));
    }

    /**
     * Gets the latest update time from any run.
     *
     * @return the update time, or an empty optional if there are no runs
     */
    public Optional<Instant> lastTime() {
        return latestFirst.stream().map(ProcessRun::getLatestUpdateTime).max(Comparator.naturalOrder());
    }

    /**
     * Gets the first update time from the oldest run.
     *
     * @return the update time, or an empty optional if there are no runs
     */
    public Optional<Instant> firstTime() {
        return getFirstRun().map(ProcessRun::getStartUpdateTime);
    }

    public Optional<ProcessRun> getLatestRun() {
        return latestFirst.stream().findFirst();
    }

    /**
     * Gets the oldest process run.
     *
     * @return the oldest process run, or an empty optional if there are no runs
     */
    public Optional<ProcessRun> getFirstRun() {
        if (latestFirst.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(latestFirst.get(latestFirst.size() - 1));
    }

    public List<ProcessRun> getRunsLatestFirst() {
        return latestFirst;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProcessRuns that = (ProcessRuns) o;
        return latestFirst.equals(that.latestFirst);
    }

    @Override
    public int hashCode() {
        return Objects.hash(latestFirst);
    }

    @Override
    public String toString() {
        return "ProcessRuns{" +
                "latestFirst=" + latestFirst +
                '}';
    }
}
