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

import sleeper.core.statestore.FileReference;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Information in the state store about a compaction currently assigned to files.
 */
public class TableFilesCompaction {

    private final String jobId;
    private final List<FileReference> fileReferences;
    private final boolean checkedInJobTracker;
    private final CompactionJobStatus trackedStatus;

    private TableFilesCompaction(String jobId, List<FileReference> fileReferences, boolean checkedInJobTracker, CompactionJobStatus trackedStatus) {
        this.jobId = jobId;
        this.fileReferences = fileReferences;
        this.checkedInJobTracker = checkedInJobTracker;
        this.trackedStatus = trackedStatus;
    }

    /**
     * Creates information about a compaction, looking up its status in the job tracker if one is available.
     *
     * @param  jobId          the compaction job ID
     * @param  fileReferences the file references assigned to the job
     * @param  jobTracker     the job tracker lookup
     * @return                the compaction
     */
    public static TableFilesCompaction from(String jobId, List<FileReference> fileReferences, CompactionJobTrackerLookup jobTracker) {
        if (!jobTracker.isTrackerAvailable()) {
            return new TableFilesCompaction(jobId, fileReferences, false, null);
        }
        return new TableFilesCompaction(jobId, fileReferences, true, jobTracker.getJob(jobId).orElse(null));
    }

    public String getJobId() {
        return jobId;
    }

    public List<FileReference> getFileReferences() {
        return fileReferences;
    }

    public Instant getAssignmentTime() {
        return fileReferences.get(0).getLastStateStoreUpdateTime();
    }

    /**
     * Checks whether the job tracker was queried for this compaction.
     *
     * @return true if the job tracker was queried
     */
    public boolean isCheckedInJobTracker() {
        return checkedInJobTracker;
    }

    /**
     * Retrieves the status of the job in the job tracker. This will be empty if the job was not found in the tracker,
     * or if the tracker was not queried.
     *
     * @return the status of the job in the tracker, if found
     */
    public Optional<CompactionJobStatus> getTrackedStatus() {
        return Optional.ofNullable(trackedStatus);
    }

}
