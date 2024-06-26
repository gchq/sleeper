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
package sleeper.core.record.process.status;

import java.time.Instant;

/**
 * Interface for a status update that marks the start of a process run.
 */
public interface ProcessRunStartedUpdate extends ProcessStatusUpdate {
    /**
     * Get the start time of this run.
     *
     * @return the start time
     */
    Instant getStartTime();

    default boolean isStartOfRun() {
        return true;
    }

    default boolean isPartOfRun() {
        return true;
    }
}
