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

import sleeper.core.tracker.job.JobRunSummary;

import java.util.List;

/**
 * Interface for a status update that marks the end of a process run.
 */
public interface ProcessRunFinishedUpdate extends ProcessStatusUpdate {

    /**
     * Gets a summary of records processed by this run.
     *
     * @return the summary
     */
    JobRunSummary getSummary();

    default boolean isSuccessful() {
        return true;
    }

    default List<String> getFailureReasons() {
        return List.of();
    }

    default boolean isPartOfRun() {
        return true;
    }
}
