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
package sleeper.core.tracker.ingest.job.query;

import sleeper.core.tracker.job.run.JobRun;
import sleeper.core.tracker.job.status.JobStatusUpdate;

/**
 * Tracks how many files were tracked as written and added to the state store.
 */
public class IngestJobFilesWrittenAndAdded {
    private final int filesWrittenAtJobFinish;
    private final int filesAddedToStateStore;

    private IngestJobFilesWrittenAndAdded(int filesWrittenAtJobFinish, int filesAddedToStateStore) {
        this.filesWrittenAtJobFinish = filesWrittenAtJobFinish;
        this.filesAddedToStateStore = filesAddedToStateStore;
    }

    public int getFilesWrittenAtJobFinish() {
        return filesWrittenAtJobFinish;
    }

    public int getFilesAddedToStateStore() {
        return filesAddedToStateStore;
    }

    /**
     * Checks whether all files written have been added to the state store.
     *
     * @return true if all files that have been written have also been added to the state store
     */
    public boolean haveAllFilesBeenAdded() {
        return filesWrittenAtJobFinish == filesAddedToStateStore;
    }

    /**
     * Finds how many files were written and added to the state store during a run.
     *
     * @param  run the run
     * @return     the count of files
     */
    public static IngestJobFilesWrittenAndAdded from(JobRun run) {
        int filesWritten = 0;
        int filesAdded = 0;
        boolean committedBySeparateFileUpdates = true;
        for (JobStatusUpdate update : run.getStatusUpdates()) {
            if (update instanceof IngestJobAddedFilesStatus) {
                IngestJobAddedFilesStatus addedFiles = (IngestJobAddedFilesStatus) update;
                filesAdded += addedFiles.getFileCount();
            } else if (update instanceof IngestJobFinishedStatus) {
                IngestJobFinishedStatus finishedStatus = (IngestJobFinishedStatus) update;
                filesWritten = finishedStatus.getNumFilesWrittenByJob();
                committedBySeparateFileUpdates = finishedStatus.isCommittedBySeparateFileUpdates();
            }
        }
        if (committedBySeparateFileUpdates) {
            return new IngestJobFilesWrittenAndAdded(filesWritten, filesAdded);
        } else {
            return new IngestJobFilesWrittenAndAdded(filesWritten, filesWritten);
        }
    }

}
