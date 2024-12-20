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
import java.util.Objects;

/**
 * A request to apply the results of a compaction in the state store. The old references will be deleted and a new
 * reference will be created in one transaction.
 */
public class ReplaceFileReferencesRequest {
    private final String jobId;
    private final List<String> inputFiles;
    private final FileReference newReference;

    /**
     * Creates a request to commit one job.
     *
     * @param  jobId        the job ID
     * @param  inputFiles   the filenames of the job's input files
     * @param  newReference the new reference to replace the input file references on the partition
     *
     * @return              the request
     */
    public static ReplaceFileReferencesRequest replaceJobFileReferences(
            String jobId, List<String> inputFiles, FileReference newReference) {
        return ReplaceFileReferencesRequest.builder()
                .jobId(jobId)
                .inputFiles(inputFiles)
                .newReference(newReference)
                .build();
    }

    private ReplaceFileReferencesRequest(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        inputFiles = Objects.requireNonNull(builder.inputFiles, "inputFiles must not be null");
        newReference = Objects.requireNonNull(builder.newReference, "newReference must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    private ReplaceFileReferencesRequest(String jobId, List<String> inputFiles, FileReference newReference) {
        this.jobId = jobId;
        this.inputFiles = inputFiles;
        this.newReference = newReference;
    }

    /**
     * Creates a copy of this request with the update time removed in the file reference. Used when storing a
     * transaction to apply this request where the update time is held separately.
     *
     * @return the copy
     */
    public ReplaceFileReferencesRequest withNoUpdateTime() {
        return new ReplaceFileReferencesRequest(jobId, inputFiles,
                newReference.toBuilder().lastStateStoreUpdateTime(null).build());
    }

    public String getJobId() {
        return jobId;
    }

    public String getPartitionId() {
        return newReference.getPartitionId();
    }

    public List<String> getInputFiles() {
        return inputFiles;
    }

    public FileReference getNewReference() {
        return newReference;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, inputFiles, newReference);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ReplaceFileReferencesRequest)) {
            return false;
        }
        ReplaceFileReferencesRequest other = (ReplaceFileReferencesRequest) obj;
        return Objects.equals(jobId, other.jobId) && Objects.equals(inputFiles, other.inputFiles) && Objects.equals(newReference, other.newReference);
    }

    @Override
    public String toString() {
        return "ReplaceFileReferencesRequest{jobId=" + jobId + ", inputFiles=" + inputFiles + ", newReference=" + newReference + "}";
    }

    /**
     * Builder for replace file references requests.
     */
    public static final class Builder {

        private String jobId;
        private List<String> inputFiles;
        private FileReference newReference;

        private Builder() {
        }

        public ReplaceFileReferencesRequest build() {
            return new ReplaceFileReferencesRequest(this);
        }

        /**
         * Sets the ID of the compaction job being committed.
         *
         * @param  jobId the job ID
         * @return       this builder
         */
        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        /**
         * Sets the filenames of the job's input files.
         *
         * @param  inputFiles the input files
         * @return            this builder
         */
        public Builder inputFiles(List<String> inputFiles) {
            this.inputFiles = inputFiles;
            return this;
        }

        /**
         * Sets the new reference to replace the input file references on the partition.
         *
         * @param  newReference the output file reference
         * @return              this builder
         */
        public Builder newReference(FileReference newReference) {
            this.newReference = newReference;
            return this;
        }

    }
}
