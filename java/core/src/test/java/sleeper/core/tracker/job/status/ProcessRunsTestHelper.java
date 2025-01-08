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

import java.util.List;
import java.util.stream.Collectors;

import static sleeper.core.tracker.job.status.TestProcessStatusUpdateRecords.records;

/**
 * A test helper for creating process runs objects.
 */
public class ProcessRunsTestHelper {

    private ProcessRunsTestHelper() {
    }

    /**
     * Creates a process runs object from a collection of process status updates.
     *
     * @param  updates the process status updates
     * @return         a {@link ProcessRuns} object
     */
    public static ProcessRuns runsFromUpdates(ProcessStatusUpdate... updates) {
        return runsFrom(records().fromUpdates(updates));
    }

    /**
     * Creates a process runs object from a collection of task updates.
     *
     * @param  taskUpdates the task updates
     * @return             a {@link ProcessRuns} object
     */
    public static ProcessRuns runsFromUpdates(
            TestProcessStatusUpdateRecords.TaskUpdates... taskUpdates) {
        return runsFrom(records().fromUpdates(taskUpdates));
    }

    private static ProcessRuns runsFrom(TestProcessStatusUpdateRecords records) {
        List<JobStatusUpdates> built = JobStatusUpdates.streamFrom(records.stream())
                .collect(Collectors.toList());
        if (built.size() != 1) {
            throw new IllegalStateException("Expected single status");
        }
        return built.get(0).getRuns();
    }
}
