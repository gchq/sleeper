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

import java.time.Instant;
import java.util.List;

/**
 * Information in the state store about a compaction currently assigned to files.
 */
public class TableFilesCompaction {

    private final String jobId;
    private final List<FileReference> fileReferences;

    public TableFilesCompaction(String jobId, List<FileReference> fileReferences) {
        this.jobId = jobId;
        this.fileReferences = fileReferences;
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

}
