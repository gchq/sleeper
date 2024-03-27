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
package sleeper.core.statestore;

import java.util.List;

/**
 * A request to apply the results of a compaction in the state store. The old references will be deleted and a new
 * reference will be created in one transaction.
 */
public class ReplaceFileReferencesRequest {
    private final String jobId;
    private final String partitionId;
    private final List<String> inputFiles;
    private final FileReference newReference;

    public static ReplaceFileReferencesRequest replaceJobFileReferences(
            String jobId, String partitionId, List<String> inputFiles, FileReference newReference) {
        return new ReplaceFileReferencesRequest(jobId, partitionId, inputFiles, newReference);
    }

    private ReplaceFileReferencesRequest(String jobId, String partitionId, List<String> inputFiles, FileReference newReference) {
        this.jobId = jobId;
        this.partitionId = partitionId;
        this.inputFiles = inputFiles;
        this.newReference = newReference;
    }

    public String getJobId() {
        return jobId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public List<String> getInputFiles() {
        return inputFiles;
    }

    public FileReference getNewReference() {
        return newReference;
    }

}
