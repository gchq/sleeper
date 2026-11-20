/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.clients.report.filestatus;

import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;

import java.util.Optional;

/**
 * A function to look up a compaction in the job tracker.
 */
@FunctionalInterface
public interface CompactionJobTrackerLookup {

    /**
     * Retrieves the currently tracked status of a job. This will be derived from all events that have been tracked for
     * that job. If the job tracker is disabled for the instance, nothing will be returned.
     *
     * @param  jobId the job ID
     * @return       the status of the job, if it has been tracked
     */
    Optional<CompactionJobStatus> getJob(String jobId);

    /**
     * Creates a lookup to query the job tracker, ignoring if the tracker is disabled for the instance.
     *
     * @param  tracker the job tracker
     * @return         the lookup
     */
    static CompactionJobTrackerLookup from(CompactionJobTracker tracker) {
        return id -> {
            try {
                return tracker.getJob(id);
            } catch (UnsupportedOperationException e) {
                return Optional.empty();
            }
        };
    }

    /**
     * Creates a lookup that will never query the job tracker.
     *
     * @return the lookup
     */
    static CompactionJobTrackerLookup none() {
        return id -> Optional.empty();
    }

}
