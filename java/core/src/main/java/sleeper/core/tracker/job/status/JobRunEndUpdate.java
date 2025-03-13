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

package sleeper.core.tracker.job.status;

import sleeper.core.tracker.job.run.RecordsProcessed;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Interface for a status update that marks the end of a process run.
 */
public interface JobRunEndUpdate extends JobRunStatusUpdate {

    /**
     * Gets the time the job finished in the task.
     *
     * @return the time
     */
    Instant getFinishTime();

    /**
     * Gets a summary of records processed by this run.
     *
     * @return the summary
     */
    RecordsProcessed getRecordsProcessed();

    default Optional<Duration> getTimeInProcess() {
        return Optional.empty();
    }

    default boolean isSuccessful() {
        return true;
    }

    default List<String> getFailureReasons() {
        return List.of();
    }
}
