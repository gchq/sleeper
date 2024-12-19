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
package sleeper.core.tracker.ingest.job.update;

/**
 * An event that occurred in the life of an ingest job. Used in the ingest job tracker.
 */
public interface IngestJobEvent {

    /**
     * Gets the ingest job ID.
     *
     * @return the ID
     */
    String getJobId();

    /**
     * Gets the Sleeper table ID.
     *
     * @return the ID
     */
    String getTableId();

    /**
     * Gets the ingest task ID.
     *
     * @return the ID, or null if the event did not occur on a task
     */
    String getTaskId();

}
