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

package sleeper.core.statestore;

import java.util.List;
import java.util.Objects;

/**
 * A request to assign a job to its input files. A job will operate on files in a single partition.
 * <p>
 * This request will be applied atomically, assigning the given job ID to all references to the given files that exist
 * in the specified partition.
 */
public class AssignJobIdRequest {
    private final String jobId;
    private final String partitionId;
    private final List<String> filenames;

    private AssignJobIdRequest(String jobId, String partitionId, List<String> filenames) {
        this.jobId = jobId;
        this.partitionId = partitionId;
        this.filenames = filenames;
    }

    /**
     * Builds a request to assign files on a single partition to a job.
     *
     * @param  jobId       the ID of the job
     * @param  partitionId the ID of the partition
     * @param  filenames   the filenames that identify the files in the state store
     * @return             the request
     */
    public static AssignJobIdRequest assignJobOnPartitionToFiles(String jobId, String partitionId, List<String> filenames) {
        return new AssignJobIdRequest(jobId, partitionId, filenames);
    }

    public String getJobId() {
        return jobId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public List<String> getFilenames() {
        return filenames;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, partitionId, filenames);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof AssignJobIdRequest)) {
            return false;
        }
        AssignJobIdRequest other = (AssignJobIdRequest) obj;
        return Objects.equals(jobId, other.jobId) && Objects.equals(partitionId, other.partitionId) && Objects.equals(filenames, other.filenames);
    }

    @Override
    public String toString() {
        return "AssignJobIdRequest{jobId=" + jobId + ", partitionId=" + partitionId + ", filenames=" + filenames + "}";
    }
}
