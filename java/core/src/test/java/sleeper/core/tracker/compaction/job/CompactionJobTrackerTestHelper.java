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
package sleeper.core.tracker.compaction.job;

import sleeper.core.tracker.compaction.job.query.CompactionJobRun;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatusType;
import sleeper.core.tracker.job.run.RowsProcessed;

import java.util.List;

/**
 * A test helper with shortcuts for using a compaction job tracker in tests.
 */
public class CompactionJobTrackerTestHelper {

    private CompactionJobTrackerTestHelper() {
    }

    /**
     * Retrieves the number of rows processed by a compaction job. Fails if the job does not have a single successful
     * run.
     *
     * @param  tracker the job tracker
     * @param  jobId   the job ID
     * @return         the numbers of rows processed
     */
    public static RowsProcessed getRowsProcessed(CompactionJobTracker tracker, String jobId) {
        CompactionJobStatus status = tracker.getJob(jobId).orElseThrow();
        List<CompactionJobRun> jobRuns = status.getRunsLatestFirst();
        if (jobRuns.size() != 1) {
            throw new IllegalStateException("Expected one job run, found: " + jobRuns);
        }
        CompactionJobRun jobRun = jobRuns.get(0);
        if (jobRun.getStatusType() != CompactionJobStatusType.FINISHED) {
            throw new IllegalStateException("Expected successful job run, found: " + jobRun);
        }
        return jobRun.getFinishedSummary().getRowsProcessed();
    }

}
